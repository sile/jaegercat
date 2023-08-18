extern crate clap;
extern crate jaegercat;
extern crate serdeconv;
#[macro_use]
extern crate slog;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use clap::{Parser, ValueEnum};
use jaegercat::thrift::{EmitBatchNotification, Protocol};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::SourceLocation;
use sloggers::Build;
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use trackable::error::Failure;

macro_rules! try_parse {
    ($expr:expr) => {
        track_try_unwrap!($expr.parse().map_err(Failure::from_error))
    };
}

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[clap(long = "compact-thrift-port", default_value_t = 6831)]
    compact_thrift_port: u16,

    #[clap(long = "binary-thrift-port", default_value_t = 6832)]
    binary_thrift_port: u16,

    #[clap(short = 'f', long = "format", default_value = "json")]
    format: FormatArg,

    #[clap(short = 'b', long = "udp-buffer-size", default_value_t = 65000)]
    udp_buffer_size: usize,

    #[clap(long = "log-level", default_value = "info")]
    log_level: LogLevelArg,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum FormatArg {
    Raw,
    Json,
    JsonPretty,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum LogLevelArg {
    Debug,
    Info,
    Error,
}

fn main() {
    let args = Args::parse();

    let format = match args.format {
        FormatArg::Raw => Format::Raw,
        FormatArg::Json => Format::Json,
        FormatArg::JsonPretty => Format::JsonPretty,
    };
    let log_level = match args.log_level {
        LogLevelArg::Debug => sloggers::types::Severity::Debug,
        LogLevelArg::Info => sloggers::types::Severity::Info,
        LogLevelArg::Error => sloggers::types::Severity::Error,
    };
    let logger = track_try_unwrap!(TerminalLoggerBuilder::new()
        .source_location(SourceLocation::None)
        .destination(Destination::Stderr)
        .level(log_level)
        .build());

    let mut threads = Vec::new();
    for (port, protocol) in [
        (args.compact_thrift_port, Protocol::Compact),
        (args.binary_thrift_port, Protocol::Binary),
    ]
    .iter()
    .cloned()
    {
        let addr: SocketAddr = try_parse!(format!("0.0.0.0:{}", port));
        let socket = track_try_unwrap!(UdpSocket::bind(addr).map_err(Failure::from_error));
        let logger = logger.new(o!("port" => port, "thrift_protocol" => format!("{:?}", protocol)));
        info!(logger, "UDP server started");

        let udp_buffer_size = args.udp_buffer_size;
        let thread = thread::spawn(move || {
            let mut buf = vec![0; udp_buffer_size];
            loop {
                let (recv_size, peer) =
                    track_try_unwrap!(socket.recv_from(&mut buf).map_err(Failure::from_error));
                debug!(logger, "Received {} bytes from {}", recv_size, peer);
                let mut bytes = &buf[..recv_size];
                match track!(EmitBatchNotification::decode(bytes, protocol)) {
                    Err(e) => {
                        error!(logger, "Received malformed or unknown message: {}", e);
                        debug!(logger, "Bytes: {:?}", bytes);
                    }
                    Ok(message) => {
                        let stdout = io::stdout();
                        let mut stdout = stdout.lock();
                        match format {
                            Format::Raw => {
                                track_try_unwrap!(
                                    io::copy(&mut bytes, &mut stdout).map_err(Failure::from_error)
                                );
                            }
                            Format::Json => {
                                let json = track_try_unwrap!(serdeconv::to_json_string(&message));
                                track_try_unwrap!(
                                    writeln!(stdout, "{}", json).map_err(Failure::from_error)
                                );
                            }
                            Format::JsonPretty => {
                                let json =
                                    track_try_unwrap!(serdeconv::to_json_string_pretty(&message));
                                track_try_unwrap!(
                                    writeln!(stdout, "{}", json).map_err(Failure::from_error)
                                );
                            }
                        }
                    }
                }
            }
        });
        threads.push(thread);
    }
    for t in threads {
        let _ = t.join();
    }
}

#[derive(Clone, Copy)]
enum Format {
    Raw,
    Json,
    JsonPretty,
}
