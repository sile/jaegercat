use chrono::{Local, NaiveDateTime, TimeZone};
use std::collections::BTreeMap;
use thrift_codec::data::{Data, DataRef, List, Struct};
use thrift_codec::message::{Message, MessageKind};
use thrift_codec::{BinaryDecode, CompactDecode};
use trackable::error::{ErrorKindExt, Failed, Failure};

use Result;

#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    Compact,
    Binary,
}

#[derive(Debug, Serialize)]
pub struct EmitBatchNotification {
    #[serde(rename = "emit_batch")]
    pub batch: Batch,
}
impl EmitBatchNotification {
    pub fn decode(mut buf: &[u8], protocol: Protocol) -> Result<Self> {
        let message = match protocol {
            Protocol::Compact => {
                track!(Message::compact_decode(&mut buf).map_err(|e| Failed.takes_over(e)))?
            }
            Protocol::Binary => {
                track!(Message::binary_decode(&mut buf).map_err(|e| Failed.takes_over(e)))?
            }
        };
        track_assert_eq!(message.method_name(), "emitBatch", Failed);
        track_assert_eq!(message.kind(), MessageKind::Oneway, Failed);
        let batch = track!(Batch::try_from(message.body()))?;
        Ok(EmitBatchNotification { batch })
    }
}

#[derive(Debug, Serialize)]
pub struct Batch {
    pub process: Process,
    pub spans: Vec<Span>,
}
impl Batch {
    fn try_from(f: &Struct) -> Result<Self> {
        let s0 = track!(f.struct_field(1))?;
        let s1 = track!(s0.struct_field(1))?;
        let process = track!(Process::try_from(&s1))?;
        let spans = track!(
            track!(s0.list_field(2))?
                .iter()
                .map(|x| Span::try_from_data(&x))
                .collect::<Result<Vec<_>>>()
        )?;
        Ok(Batch { process, spans })
    }
}

#[derive(Debug, Serialize)]
pub struct Process {
    pub service_name: String,
    pub tags: Tags,
}
impl Process {
    fn try_from(f: &Struct) -> Result<Self> {
        let service_name = track!(f.string_field(1))?;
        let tags = track!(f.list_field(2).and_then(|x| Tags::try_from_list(&x)))?;
        Ok(Process { service_name, tags })
    }
}

#[derive(Debug, Serialize)]
pub struct Tags(pub BTreeMap<String, TagValue>);
impl Tags {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn try_from_list(f: &List) -> Result<Self> {
        let mut map = BTreeMap::new();
        for data in f.iter() {
            let tag = track!(Tag::try_from_data(&data))?;
            let (key, value) = match tag {
                Tag::Bool { key, value } => (key, TagValue::Bool(value)),
                Tag::Long { key, value } => (key, TagValue::I64(value)),
                Tag::Double { key, value } => (key, TagValue::F64(value)),
                Tag::String { key, value } => (key, TagValue::String(value)),
                Tag::Binary { key, value } => (key, TagValue::Binary(value)),
            };
            map.insert(key, value);
        }
        Ok(Tags(map))
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TagValue {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
}

enum Tag {
    String { key: String, value: String },
    Double { key: String, value: f64 },
    Bool { key: String, value: bool },
    Long { key: String, value: i64 },
    Binary { key: String, value: Vec<u8> },
}
impl Tag {
    fn try_from_data(f: &DataRef) -> Result<Self> {
        let s = if let DataRef::Struct(s) = *f {
            s
        } else {
            track_panic!(Failed, "Not a struct: {:?}", f);
        };

        let key = track!(s.string_field(1))?;
        let kind = track!(s.i32_field(2))?;
        Ok(match kind {
            0 => Tag::String {
                key,
                value: track!(s.string_field(3))?,
            },
            1 => Tag::Double {
                key,
                value: track!(s.f64_field(4))?,
            },
            2 => Tag::Bool {
                key,
                value: track!(s.bool_field(5))?,
            },
            3 => Tag::Long {
                key,
                value: track!(s.i64_field(6))?,
            },
            4 => Tag::Binary {
                key,
                value: track!(s.binary_field(7))?,
            },
            _ => track_panic!(Failed, "Unknown tag kind: {}", kind),
        })
    }
}

fn trace_id(high: i64, low: i64) -> String {
    if high == 0 {
        format!("0x{:x}", low)
    } else {
        format!("0x{:x}{:016x}", high, low)
    }
}

fn span_id(id: i64) -> String {
    if id == 0 {
        "".to_owned()
    } else {
        format!("0x{:x}", id)
    }
}

#[derive(Debug, Serialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,
    pub operation_name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SpanRef>,
    pub flags: i32,
    pub start_datetime: String,
    pub start_unixtime: f64,
    pub duration: f64, // seconds
    #[serde(skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<Log>,
}
impl Span {
    fn try_from_data(f: &DataRef) -> Result<Self> {
        let s = if let DataRef::Struct(s) = *f {
            s
        } else {
            track_panic!(Failed, "Not a struct: {:?}", f);
        };
        let start_time_us = track!(s.i64_field(8))?;
        let duration_us = track!(s.i64_field(9))?;
        Ok(Span {
            trace_id: trace_id(track!(s.i64_field(2))?, track!(s.i64_field(1))?),
            span_id: span_id(track!(s.i64_field(3))?),
            parent_span_id: span_id(track!(s.i64_field(4))?),
            operation_name: track!(s.string_field(5))?,
            references: track!(s.list_field(6).and_then(|x| SpanRef::try_from_list(&x)))?,
            flags: track!(s.i32_field(7))?,
            start_unixtime: start_time_us as f64 / 1_000_000.0,
            start_datetime: unixtime_to_datetime(start_time_us),
            duration: duration_us as f64 / 1_000_000.0,
            tags: track!(s.list_field(10).and_then(|x| Tags::try_from_list(&x)))?,
            logs: track!(s.list_field(11).and_then(|x| Log::try_from_list(&x)))?,
        })
    }
}

fn unixtime_to_datetime(unixtime_us: i64) -> String {
    Local
        .from_utc_datetime(&NaiveDateTime::from_timestamp(
            unixtime_us / 1_000_000,
            (unixtime_us % 1_000_000 * 1000) as u32,
        ))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

#[derive(Debug, Serialize)]
pub enum SpanRef {
    ChildOf { trace_id: String, span_id: String },
    FollowsFrom { trace_id: String, span_id: String },
}
impl SpanRef {
    fn try_from_list(f: &List) -> Result<Vec<Self>> {
        track!(
            f.iter()
                .map(|x| Self::try_from_data(&x))
                .collect::<Result<Vec<_>>>()
        )
    }
    fn try_from_data(f: &DataRef) -> Result<Self> {
        let s = if let DataRef::Struct(s) = *f {
            s
        } else {
            track_panic!(Failed, "Not a struct: {:?}", f);
        };

        let kind = track!(s.i32_field(1))?;
        let trace_id = trace_id(track!(s.i64_field(3))?, track!(s.i64_field(2))?);
        let span_id = span_id(track!(s.i64_field(4))?);
        Ok(match kind {
            0 => SpanRef::ChildOf { trace_id, span_id },
            1 => SpanRef::FollowsFrom { trace_id, span_id },
            _ => track_panic!(Failed, "Unknown span reference kind: {}", kind),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct Log {
    pub datetime: String,
    pub unixtime: f64,
    pub fields: Tags,
}
impl Log {
    fn try_from_list(f: &List) -> Result<Vec<Self>> {
        track!(
            f.iter()
                .map(|x| Self::try_from_data(&x))
                .collect::<Result<Vec<_>>>()
        )
    }
    fn try_from_data(f: &DataRef) -> Result<Self> {
        let s = if let DataRef::Struct(s) = *f {
            s
        } else {
            track_panic!(Failed, "Not a struct: {:?}", f);
        };

        let timestamp_us = track!(s.i64_field(1))?;
        let fields = track!(s.list_field(2).and_then(|x| Tags::try_from_list(&x)))?;
        Ok(Log {
            unixtime: timestamp_us as f64 / 1_000_000.0,
            datetime: unixtime_to_datetime(timestamp_us),
            fields,
        })
    }
}

trait StructExt {
    fn bool_field(&self, id: i16) -> Result<bool>;
    fn i32_field(&self, id: i16) -> Result<i32>;
    fn i64_field(&self, id: i16) -> Result<i64>;
    fn f64_field(&self, id: i16) -> Result<f64>;
    fn binary_field(&self, id: i16) -> Result<Vec<u8>>;
    fn string_field(&self, id: i16) -> Result<String>;
    fn list_field(&self, id: i16) -> Result<List>;
    fn struct_field(&self, id: i16) -> Result<Struct>;
}
impl StructExt for Struct {
    fn struct_field(&self, id: i16) -> Result<Struct> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::Struct(ref f) = *field.data() {
            Ok(f.clone())
        } else {
            track_panic!(Failed, "not a struct field: {:?}", field)
        }
    }
    fn string_field(&self, id: i16) -> Result<String> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::Binary(ref f) = *field.data() {
            track!(String::from_utf8(f.to_owned()).map_err(Failure::from_error))
        } else {
            track_panic!(Failed, "not a string field: {:?}", field)
        }
    }
    fn binary_field(&self, id: i16) -> Result<Vec<u8>> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::Binary(ref f) = *field.data() {
            Ok(f.clone())
        } else {
            track_panic!(Failed, "not a binary field: {:?}", field)
        }
    }
    fn bool_field(&self, id: i16) -> Result<bool> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::Bool(ref f) = *field.data() {
            Ok(*f)
        } else {
            track_panic!(Failed, "not a bool field: {:?}", field)
        }
    }
    fn i32_field(&self, id: i16) -> Result<i32> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::I32(ref f) = *field.data() {
            Ok(*f)
        } else {
            track_panic!(Failed, "not an i32 field: {:?}", field)
        }
    }
    fn i64_field(&self, id: i16) -> Result<i64> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::I64(ref f) = *field.data() {
            Ok(*f)
        } else {
            track_panic!(Failed, "not an i64 field: {:?}", field)
        }
    }
    fn f64_field(&self, id: i16) -> Result<f64> {
        let field = track_assert_some!(
            self.fields().iter().find(|f| f.id() == id),
            Failed,
            "missing field: id={}",
            id
        );
        if let Data::Double(ref f) = *field.data() {
            Ok(*f)
        } else {
            track_panic!(Failed, "not a f64 field: {:?}", field)
        }
    }
    fn list_field(&self, id: i16) -> Result<List> {
        if let Some(field) = self.fields().iter().find(|f| f.id() == id) {
            if let Data::List(ref f) = *field.data() {
                Ok(f.clone())
            } else {
                track_panic!(Failed, "not a list field: {:?}", field)
            }
        } else {
            // dummy
            Ok(List::from(Vec::<bool>::new()))
        }
    }
}
