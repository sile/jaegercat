jaegercat
=========

[![jaegercat](http://meritbadge.herokuapp.com/jaegercat)](https://crates.io/crates/jaegercat)
[![Documentation](https://docs.rs/jaegercat/badge.svg)](https://docs.rs/jaegercat)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A handy command line tool showing traces emitted by [Jaeger][jaeger] clients.

Install
--------

### Precompiled binaries

A precompiled binary for Linux environment is available in the [releases] page.

```console
$ curl https://github.com/sile/jaegercat/releases/download/0.1.0/jaegercat-0.1.0.linux -o jaegercat
$ chmod +x jaegercat
$ ./jaegercat -h
jaegercat 0.1.0

USAGE:
    jaegercat [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --binary-thrift-port <BINARY_THRIFT_PORT>       [default: 6832]
        --compact-thrift-port <COMPACT_THRIFT_PORT>     [default: 6831]
    -f, --format <FORMAT>                               [default: json]  [values: raw, json, json-pretty]
        --log-level <LOG_LEVEL>                         [default: info]  [values: debug, info, error]
    -b, --udp-buffer-size <UDP_BUFFER_SIZE>             [default: 65000]
```

### Using Cargo

If you have already installed [Cargo][cargo], you can install `jaegercat` easily in the following command:

```console
$ cargo install jaegercat
```

Examples
--------

### Basic Usage

Starts `jaegercat` in a terminal:

```console
$ jaegercat
Jan 31 14:18:06.989 INFO UDP server started, port: 6831
Jan 31 14:18:06.990 INFO UDP server started, port: 6832
```

Emits a trace in another terminal:
```console
$ git clone https://github.com/sile/rustracing_jaeger.git
$ cd rustracing_jaeger
$ cargo run --example report
```

`jaegercat` will output a JSON like the following:
```console
$ jaegercat
{"emit_batch":{"process":{"service_name":"example","tags":{"hello":"world","hostname":"DESKTOP-FJQCKIF","jaeger.version":"rustracing_jaeger-0.1.3"}},"spans":[{"trace_id":"0x154050ce43d48b612ae64ad7cd070e8e","span_id":"0x4c123d1fd41219d5","parent_span_id":"0x87a5fd207c065420","operation_name":"sub","references":[{"ChildOf":{"trace_id":"0x154050ce43d48b612ae64ad7cd070e8e","span_id":"0x87a5fd207c065420"}}],"flags":1,"start_datetime":"2018-01-31 14:24:18","start_unixtime":1517376258.665418,"duration":0.010196,"tags":{"foo":"bar"},"logs":[{"datetime":"2018-01-31 14:24:18","unixtime":1517376258.665475,"fields":{"event":"error","message":"something wrong"}}]},{"trace_id":"0x154050ce43d48b612ae64ad7cd070e8e","span_id":"0x87a5fd207c065420","operation_name":"main","flags":1,"start_datetime":"2018-01-31 14:24:18","start_unixtime":1517376258.654844,"duration":0.020779}]}}
```

### Using [`jq`][jq] command

It is convenient to use [`jq`][jq] command for processing the resulting JSON.

```console
// Filter only spans whose operation name is "main".
$ jaegercat | jq '.emit_batch.spans[] | select(.operation_name == "main")'
{
  "trace_id": "0x3dfcffdfe5b53b1d1fb792d1fbea9f8b",
  "span_id": "0xeba0e30f2f2d6e51",
  "operation_name": "main",
  "flags": 1,
  "start_datetime": "2018-01-31 14:36:01",
  "start_unixtime": 1517376961.354905,
  "duration": 0.020933
}
```

[jaeger]: https://jaeger.readthedocs.io/
[cargo]: https://doc.rust-lang.org/cargo/
[jq]: https://stedolan.github.io/jq/
[releases]: https://github.com/sile/jaegercat/releases
