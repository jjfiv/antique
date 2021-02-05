//! Called CompressedCollection in indri

use super::keyfile::Keyfile;
use crate::io_helper::open_mmap_file;
use crate::Error;
use flate2::read::ZlibDecoder;
use memmap::Mmap;
use std::io::prelude::*;
use std::mem::size_of;
use std::{convert::TryInto, path::Path, sync::Arc};

pub struct CompressedCollection {
    storage: Arc<Mmap>,
    lookup: Keyfile,
}

pub struct DocumentDecoder {
    buffer: DocumentBuffer,
    text: Option<StartEnd>,
    content: Option<StartEnd>,
    positions: Option<StartEnd>,
    metadata: Vec<MetadataPair>,
}

#[derive(Debug)]
struct StartEnd(u32, u32);

struct MetadataPair {
    name: String,
    value: StartEnd,
}

impl DocumentDecoder {
    fn new(buffer: Vec<u8>) -> Result<DocumentDecoder, Error> {
        let buffer = DocumentBuffer(buffer);
        let num_fields = buffer.read_word(buffer.len() - 4) as usize;
        let field_info_size = 2 * num_fields * size_of::<u32>();
        let metadata_start = buffer.len() - 4 - field_info_size;

        let mut text: Option<StartEnd> = None;
        let mut content_start: Option<u32> = None;
        let mut content_length: Option<u32> = None;
        let mut positions: Option<StartEnd> = None;
        let mut metadata = Vec::new();

        for i in 0..num_fields {
            let info_addr = metadata_start + 2 * i * size_of::<u32>();
            let key_start = buffer.read_word(info_addr);
            let val_start = buffer.read_word(info_addr + 4);

            let val_end = if i == num_fields - 1 {
                metadata_start as u32
            } else {
                // key_start of next entry
                buffer.read_word(info_addr + 8) as u32
            };

            let value_bounds = StartEnd(val_start, val_end);
            // drop null-terminator from keys slice:
            let key_end = val_start - 1;
            let key = std::str::from_utf8(buffer.slice(key_start as usize, key_end as usize))?;
            println!("found key = {}", key);
            match key {
                "#TEXT#" => {
                    // drop null-char:
                    text = Some(StartEnd(val_start, val_end - 1));
                }
                "#POSITIONS#" => {
                    positions = Some(value_bounds);
                }
                "#CONTENT#" => content_start = Some(buffer.read_word(val_start as usize)),
                "#CONTENTLENGTH#" => content_length = Some(buffer.read_word(val_start as usize)),
                other => metadata.push(MetadataPair {
                    name: other.to_owned(),
                    value: value_bounds,
                }),
            }
        }

        // content-start is relative to text-start:
        let content: Option<StartEnd> = if let Some(StartEnd(text_start, _)) = text {
            match (content_start, content_length) {
                (None, None) => None,
                (Some(start), Some(len)) => {
                    let start = start + text_start;
                    let end = start + len;
                    Some(StartEnd(start, end))
                }
                _ => {
                    return Err(Error::MissingField
                        .with_context("content start or length without the other!"))
                }
            }
        } else {
            None
        };

        Ok(DocumentDecoder {
            buffer,
            text,
            positions,
            content,
            metadata,
        })
    }

    pub fn get_content(&self) -> Result<&str, Error> {
        let StartEnd(start, end) = self
            .content
            .as_ref()
            .ok_or_else(|| Error::MissingField.with_context("content"))?;
        Ok(std::str::from_utf8(
            self.buffer.slice(*start as usize, *end as usize),
        )?)
    }
    pub fn get_text(&self) -> Result<&str, Error> {
        let StartEnd(start, end) = self
            .text
            .as_ref()
            .ok_or_else(|| Error::MissingField.with_context("text"))?;
        Ok(std::str::from_utf8(
            self.buffer.slice(*start as usize, *end as usize),
        )?)
    }
}

struct DocumentBuffer(Vec<u8>);

impl DocumentBuffer {
    fn len(&self) -> usize {
        return self.0.len();
    }
    fn slice(&self, start: usize, end: usize) -> &[u8] {
        return &self.0[start..end];
    }
    fn read_word(&self, addr: usize) -> u32 {
        let word = &self.0[addr..addr + 4];
        u32::from_le_bytes(word.try_into().unwrap())
    }
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
    pub fn read(&self, doc: isize) -> Result<Option<DocumentDecoder>, Error> {
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
            let doc = DocumentDecoder::new(contents)?;
            Ok(Some(doc))
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
            println!("{:?}", parsed.get_content());
            println!("{:?}", parsed.get_text());
        }
    }
}
