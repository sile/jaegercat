extern crate chrono;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate thrift_codec;
#[macro_use]
extern crate trackable;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

pub mod thrift;
