pub mod galago;
pub mod heap_collection;
pub mod indri;
pub mod io_helper;
pub mod lang;
pub mod movement;
pub mod scoring;
pub mod stats;

#[macro_use]
extern crate serde_derive;

use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use std::{ffi::OsString, io, str::Utf8Error};

#[derive(Debug)]
pub enum Error {
    PathNotOK,
    MissingSplitFiles,
    ThreadFailure,
    UnknownStemmer(String),
    UnknownIndexPart(String),
    CompressionError,
    QueryErrors(Vec<lang::QErr>),
    IO(io::Error),
    BadFileName(OsString),
    BadGalagoMagic(u64),
    BadBulkTreeSize,
    BadBulkTreeBlock(u32),
    BadManifest(serde_json::Error),
    InternalSizeErr,
    Utf8DecodeError(Utf8Error),
    BadDocId(isize),
    Context(String, Box<Error>),
    MissingGalagoReader(String),
    MissingField,
    QueryInit,
    BadParameters,
    XML(roxmltree::Error),
    KeyfileError(Box<indri::keyfile::KFErr>),
}

impl Error {
    pub fn with_context<S>(self, msg: S) -> Error
    where
        S: Into<String>,
    {
        Error::Context(msg.into(), Box::new(self))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
impl From<roxmltree::Error> for Error {
    fn from(err: roxmltree::Error) -> Error {
        Error::XML(err)
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Error {
        Error::Utf8DecodeError(err)
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
#[repr(transparent)]
pub struct DocId(u64);

impl DocId {
    pub fn is_done(&self) -> bool {
        self.0 == std::u64::MAX
    }
    pub fn no_more() -> DocId {
        DocId(std::u64::MAX)
    }
    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub fn next(&self) -> DocId {
        return DocId(self.0 + 1);
    }
}

/// Lucene does this really nice:
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum DataNeeded {
    Docs,
    Counts,
    Positions,
}
