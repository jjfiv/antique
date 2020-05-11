pub mod galago_btree;

#[macro_use]
extern crate serde_derive;

use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use std::collections::BTreeMap;
use std::collections::BTreeSet;

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
