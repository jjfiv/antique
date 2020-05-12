pub mod galago_btree;
pub mod galago_postings;
pub mod io_helper;

#[macro_use]
extern crate serde_derive;

use fnv::FnvHashMap as HashMap;

use std::io;

#[derive(Debug)]
pub enum Error {
    PathNotOK,
    IO(io::Error),
    BadGalagoMagic(u64),
    BadManifest(serde_json::Error),
    InternalSizeErr,
    Context(String, Box<Error>)
}

impl Error {
    pub fn with_context<S>(self, msg: S) -> Error where S: Into<String> {
        Error::Context(msg.into(), Box::new(self))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
