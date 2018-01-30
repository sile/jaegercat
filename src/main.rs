extern crate clap;
extern crate jaegercat;

use clap::{App, Arg};

fn main() {
    let _matches = App::new("jaegercat")
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("COMPACT_SERVER_PORT")
                .long("compact-server-port")
                .takes_value(true)
                .default_value("6831"),
        )
        .arg(
            Arg::with_name("BINARY_SERVER_PORT")
                .long("binary-server-port")
                .takes_value(true)
                .default_value("6832"),
        )
        .arg(
            Arg::with_name("FORMAT")
                .short("f")
                .long("format")
                .takes_value(true)
                .default_value("raw")
                .possible_values(&["raw", "json"]),
        )
        .arg(
            Arg::with_name("UDP_BUFFER_SIZE")
                .short("b")
                .long("udp-buffer-size")
                .takes_value(true)
                .default_value("65000"),
        )
        .get_matches();
}
