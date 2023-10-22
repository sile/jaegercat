extern crate clap;
extern crate jaegercat;
extern crate serdeconv;
#[macro_use]
extern crate trackable;

use clap::{Parser, ValueEnum};
use jaegercat::thrift::{EmitBatchNotification, Protocol};
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
    env_logger::init();

    let args = Args::parse();

    let format = match args.format {
        FormatArg::Raw => Format::Raw,
        FormatArg::Json => Format::Json,
        FormatArg::JsonPretty => Format::JsonPretty,
    };

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
        log::info!("UDP server started: port={port}, protocol={protocol:?}");

        let udp_buffer_size = args.udp_buffer_size;
        let thread = thread::spawn(move || {
            let mut buf = vec![0; udp_buffer_size];
            loop {
                let (recv_size, peer) =
                    track_try_unwrap!(socket.recv_from(&mut buf).map_err(Failure::from_error));
                log::debug!("Received {recv_size} bytes from {peer}");
                let mut bytes = &buf[..recv_size];
                match track!(EmitBatchNotification::decode(bytes, protocol)) {
                    Err(e) => {
                        log::error!("Received malformed or unknown message: {e}");
                        log::debug!("Bytes: {bytes:?}");
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
