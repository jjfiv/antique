//! Called CompressedCollection in indri

use super::keyfile::Keyfile;
use crate::io_helper::open_mmap_file;
use crate::Error;
use flate2::read::ZlibDecoder;
use memmap::Mmap;
use std::io::prelude::*;
use std::{path::Path, sync::Arc};

pub struct CompressedCollection {
    storage: Arc<Mmap>,
    lookup: Keyfile,
}

pub struct ParsedDocument {
    text: String,
    content: String,
    terms: Vec<String>,
}

impl CompressedCollection {
    pub fn open(dir: &Path) -> Result<CompressedCollection, Error> {
        let lookup_path = dir.join("lookup");
        let storage_path = dir.join("storage");
        let storage = open_mmap_file(&storage_path)?;
        let lookup = Keyfile::open(&lookup_path)?;

        Ok(CompressedCollection { lookup, storage })
    }
    fn get_offset(&self, doc: isize) -> Result<Option<usize>, Error> {
        if doc <= 0 {
            return Err(Error::BadDocId(doc));
        }
        if let Some(offset) = self.lookup.lookup_int(doc)? {
            let offset = offset.as_le_u64()? as usize;
            Ok(Some(offset))
        } else {
            Ok(None)
        }
    }
    pub fn read(&self, doc: isize) -> Result<Option<Vec<u8>>, Error> {
        if let Some(start) = self.get_offset(doc)? {
            let mut zlib = ZlibDecoder::new(&self.storage[start..]);
            let mut contents = Vec::with_capacity(4096);
            let length = zlib.read_to_end(&mut contents)?;
            println!(
                "Read {} zlib bytes at {}.. for docid {} len={}",
                length,
                start,
                doc,
                self.storage.len()
            );
            Ok(Some(contents))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let reader = CompressedCollection::open(Path::new("data/index.indri/collection")).unwrap();

        for docid in 1..7 {
            let parsed = reader.read(docid).unwrap().unwrap();
            assert_ne!(parsed.len(), 0);
        }
    }
}
