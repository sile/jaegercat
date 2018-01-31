extern crate clap;
extern crate jaegercat;
extern crate serdeconv;
#[macro_use]
extern crate slog;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc;
use std::thread;
use clap::{App, Arg};
use jaegercat::thrift::EmitBatchNotification;
use sloggers::Build;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::SourceLocation;
use trackable::error::Failure;

macro_rules! try_parse {
    ($expr:expr) => { track_try_unwrap!($expr.parse().map_err(Failure::from_error)) }
}

fn main() {
    let matches = App::new("jaegercat")
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("COMPACT_THRIFT_PORT")
                .long("compact-thrift-port")
                .takes_value(true)
                .default_value("6831"),
        )
        .arg(
            Arg::with_name("BINARY_THRIFT_PORT")
                .long("binary-thrift-port")
                .takes_value(true)
                .default_value("6832"),
        )
        .arg(
            Arg::with_name("FORMAT")
                .short("f")
                .long("format")
                .takes_value(true)
                .default_value("json")
                .possible_values(&["raw", "json", "json-pretty"]),
        )
        .arg(
            Arg::with_name("UDP_BUFFER_SIZE")
                .short("b")
                .long("udp-buffer-size")
                .takes_value(true)
                .default_value("65000"),
        )
        .arg(
            Arg::with_name("LOG_LEVEL")
                .long("log-level")
                .takes_value(true)
                .default_value("info")
                .possible_values(&["debug", "info", "error"]),
        )
        .get_matches();

    let compact_thrift_port: u16 = try_parse!(matches.value_of("COMPACT_THRIFT_PORT").unwrap());
    let binary_thrift_port: u16 = try_parse!(matches.value_of("BINARY_THRIFT_PORT").unwrap());
    let udp_buffer_size: usize = try_parse!(matches.value_of("UDP_BUFFER_SIZE").unwrap());
    let format = match matches.value_of("FORMAT").unwrap() {
        "raw" => Format::Raw,
        "json" => Format::Json,
        "json-pretty" => Format::JsonPretty,
        _ => unreachable!(),
    };
    let log_level = try_parse!(matches.value_of("LOG_LEVEL").unwrap());
    let logger = track_try_unwrap!(
        TerminalLoggerBuilder::new()
            .source_location(SourceLocation::None)
            .destination(Destination::Stderr)
            .level(log_level)
            .build()
    );
    let (tx, rx) = mpsc::channel();
    for port in [compact_thrift_port, binary_thrift_port].iter().cloned() {
        let tx = tx.clone();
        let addr: SocketAddr = try_parse!(format!("0.0.0.0:{}", port));
        let socket = track_try_unwrap!(UdpSocket::bind(addr).map_err(Failure::from_error));
        let logger = logger.new(o!("port" => port));
        info!(logger, "UDP server started");

        thread::spawn(move || {
            let mut buf = vec![0; udp_buffer_size];
            loop {
                let (recv_size, peer) =
                    track_try_unwrap!(socket.recv_from(&mut buf).map_err(Failure::from_error));
                debug!(logger, "Received {} bytes from {}", recv_size, peer);
                let bytes = Vec::from(&buf[..recv_size]);
                match track!(EmitBatchNotification::decode(&bytes)) {
                    Err(e) => {
                        error!(logger, "Received malformed or unknown message: {}", e);
                        debug!(logger, "Bytes: {:?}", bytes);
                    }
                    Ok(message) => {
                        if tx.send((message, bytes)).is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }

    loop {
        let (message, bytes) = track_try_unwrap!(rx.recv().map_err(Failure::from_error));
        match format {
            Format::Raw => {
                track_try_unwrap!(
                    io::copy(&mut &bytes[..], &mut io::stdout()).map_err(Failure::from_error)
                );
            }
            Format::Json => {
                let json = track_try_unwrap!(serdeconv::to_json_string(&message));
                println!("{}", json);
            }
            Format::JsonPretty => {
                let json = track_try_unwrap!(serdeconv::to_json_string_pretty(&message));
                println!("{}", json);
            }
        }
    }
}

enum Format {
    Raw,
    Json,
    JsonPretty,
}
