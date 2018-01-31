use thrift_codec::CompactDecode;
use thrift_codec::data::{Data, DataRef, List, Struct};
use thrift_codec::message::{Message, MessageKind};
use trackable::error::{Failed, Failure};

use Result;

#[derive(Debug)]
pub struct EmitBatchNotification {
    pub batch: Batch,
}
impl EmitBatchNotification {
    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        let message = track_try_unwrap!(Message::compact_decode(&mut buf));
        track_assert_eq!(message.method_name(), "emitBatch", Failed);
        track_assert_eq!(message.kind(), MessageKind::Oneway, Failed);
        let batch = track!(Batch::try_from(message.body()))?;
        Ok(EmitBatchNotification { batch })
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Process {
    pub service_name: String,
    pub tags: Vec<Tag>,
}
impl Process {
    fn try_from(f: &Struct) -> Result<Self> {
        let service_name = track!(f.string_field(1))?;
        let tags = track!(f.list_field(2).and_then(|x| Tag::try_from_list(&x)))?;
        Ok(Process { service_name, tags })
    }
}

#[derive(Debug)]
pub enum Tag {
    String { key: String, value: String },
    Double { key: String, value: f64 },
    Bool { key: String, value: bool },
    Long { key: String, value: i64 },
    Binary { key: String, value: Vec<u8> },
}
impl Tag {
    fn try_from_list(f: &List) -> Result<Vec<Self>> {
        track!(
            f.iter()
                .map(|x| Tag::try_from_data(&x))
                .collect::<Result<Vec<_>>>()
        )
    }
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

#[derive(Debug)]
pub struct Span {
    pub trace_id_low: i64,
    pub trace_id_high: i64,
    pub span_id: i64,
    pub parent_span_id: i64,
    pub operation_name: String,
    pub references: Vec<SpanRef>,
    pub flags: i32,
    pub start_time: i64,
    pub duration: i64,
    pub tags: Vec<Tag>,
    pub logs: Vec<Log>,
}
impl Span {
    pub fn trace_id(&self) -> String {
        format!(
            "0x{:016x}{:016x}",
            self.trace_id_high as u64, self.trace_id_low as u64
        )
    }
    pub fn span_id(&self) -> String {
        format!("0x{:016x}", self.span_id as u64)
    }
    pub fn parent_span_id(&self) -> String {
        format!("0x{:016x}", self.parent_span_id as u64)
    }

    fn try_from_data(f: &DataRef) -> Result<Self> {
        let s = if let DataRef::Struct(s) = *f {
            s
        } else {
            track_panic!(Failed, "Not a struct: {:?}", f);
        };
        Ok(Span {
            trace_id_low: track!(s.i64_field(1))?,
            trace_id_high: track!(s.i64_field(2))?,
            span_id: track!(s.i64_field(3))?,
            parent_span_id: track!(s.i64_field(4))?,
            operation_name: track!(s.string_field(5))?,
            references: track!(s.list_field(6).and_then(|x| SpanRef::try_from_list(&x)))?,
            flags: track!(s.i32_field(7))?,
            start_time: track!(s.i64_field(8))?,
            duration: track!(s.i64_field(9))?,
            tags: track!(s.list_field(10).and_then(|x| Tag::try_from_list(&x)))?,
            logs: track!(s.list_field(11).and_then(|x| Log::try_from_list(&x)))?,
        })
    }
}

#[derive(Debug)]
pub enum SpanRef {
    ChildOf {
        trace_id_low: i64,
        trace_id_high: i64,
        span_id: i64,
    },
    FollowsFrom {
        trace_id_low: i64,
        trace_id_high: i64,
        span_id: i64,
    },
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
        let trace_id_low = track!(s.i64_field(2))?;
        let trace_id_high = track!(s.i64_field(3))?;
        let span_id = track!(s.i64_field(4))?;
        Ok(match kind {
            0 => SpanRef::ChildOf {
                trace_id_low,
                trace_id_high,
                span_id,
            },
            1 => SpanRef::FollowsFrom {
                trace_id_low,
                trace_id_high,
                span_id,
            },
            _ => track_panic!(Failed, "Unknown span reference kind: {}", kind),
        })
    }
}

#[derive(Debug)]
pub struct Log {
    pub timestamp: i64,
    pub fields: Vec<Tag>,
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

        let timestamp = track!(s.i64_field(1))?;
        let fields = track!(s.list_field(2).and_then(|x| Tag::try_from_list(&x)))?;
        Ok(Log { timestamp, fields })
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
