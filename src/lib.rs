extern crate thrift_codec;
#[macro_use]
extern crate trackable;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

pub mod thrift;
