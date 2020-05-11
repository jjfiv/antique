pub mod galago_btree;
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
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
