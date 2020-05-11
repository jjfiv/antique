use crate::Error;
use crate::HashMap;
use memmap::{Mmap, MmapOptions};
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;
use std::{
    convert::TryInto,
    fmt,
    path::{Path, PathBuf},
    str,
};

// Notes on the format:
// Java's DataInputStream/DataOutputStream classes write data as big-endian.

/// Last 8 bytes of the file should be this:
const MAGIC_NUMBER: u64 = 0x1a2b3c4d5e6f7a8d;

/// size_of(
/// vocabulary_offset: u64
/// manifest_offset: u64
/// block_size: u32
/// magic_number: u64
/// )
const FOOTER_SIZE: usize = 8 + 8 + 4 + 8;

/// The bottom of a Galago file will have this data:
#[derive(Debug, Clone)]
pub struct TreeReader {
    mmap: Arc<Mmap>,
    location: TreeLocation,
    vocabulary_offset: u64,
    manifest_offset: u64,
    block_size: u32,
    magic_number: u64,
    manifest: Manifest,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    max_key_size: usize,
    block_count: u64,
    block_size: usize,
    empty_index_file: bool,
    cache_group_size: Option<usize>,
    /// I love Serde so much; this was "filename" in practice but should be camel or snake-case.
    #[serde(alias = "filename")]
    file_name: String,
    reader_class: String,
    writer_class: Option<String>,
    merger_class: Option<String>,
    key_count: u64,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
enum TreeLocation {
    SingleFile(PathBuf),
    Split { keys: PathBuf },
}

impl TreeLocation {
    fn new(path: &Path) -> Result<TreeLocation, Error> {
        if path.is_dir() {
            let inner = path.join("split.keys");
            if inner.is_file() {
                Ok(TreeLocation::Split { keys: inner })
            } else {
                Err(Error::PathNotOK)
            }
        } else {
            Ok(TreeLocation::SingleFile(path.into()))
        }
    }
    fn keys_path(&self) -> &Path {
        match self {
            TreeLocation::SingleFile(p) => &p,
            TreeLocation::Split { keys, .. } => &keys,
        }
    }
}

/// Is this a Galago Btree?
pub fn file_matches(path: &Path) -> Result<bool, Error> {
    // Step into split.keys if-need-be.
    let location = TreeLocation::new(path)?;

    // Use Memory-Mapped I/O:
    let file = File::open(location.keys_path())?;
    let opts = MmapOptions::new();
    let mmap: Mmap = unsafe { opts.map(&file)? };
    let file_length = mmap.len();
    let mut reader = SliceInputStream::new(&mmap[file_length - 8..]);
    // Last u64 in file should be:
    let maybe_magic = reader.read_u64()?;
    Ok(maybe_magic == MAGIC_NUMBER)
}

/// Read footer:
pub fn read_info(path: &Path) -> Result<TreeReader, Error> {
    let location = TreeLocation::new(path)?;

    // Use Memory-Mapped I/O:
    let file = File::open(location.keys_path())?;
    let opts = MmapOptions::new();
    let mmap: Mmap = unsafe { opts.map(&file)? };
    let file_length = mmap.len();

    let footer_start = file_length - FOOTER_SIZE;
    let mut reader = SliceInputStream::new(&mmap[footer_start..]);

    let vocabulary_offset = reader.read_u64()?;
    let manifest_offset = reader.read_u64()?;
    let block_size = reader.read_u32()?;
    let magic_number = reader.read_u64()?;
    debug_assert!(reader.eof());

    if magic_number != MAGIC_NUMBER {
        return Err(Error::BadGalagoMagic(magic_number));
    }

    let manifest = serde_json::from_slice(&mmap[(manifest_offset as usize)..footer_start])
        .map_err(|details| Error::BadManifest(details))?;

    Ok(TreeReader {
        mmap: Arc::new(mmap),
        location,
        vocabulary_offset,
        manifest_offset,
        block_size,
        magic_number,
        manifest,
    })
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Bytes<'src> {
    data: &'src [u8],
}
impl fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(readable) = str::from_utf8(self.data) {
            write!(f, "{}", readable)
        } else {
            write!(f, "{:?}", &self.data)
        }
    }
}

pub struct SliceInputStream<'src> {
    data: &'src [u8],
    // TODO: keeping this separate in case we need to rewind...
    position: usize,
}

impl<'src> SliceInputStream<'src> {
    fn new(data: &'src [u8]) -> Self {
        Self { data, position: 0 }
    }
    fn eof(&self) -> bool {
        self.position >= self.data.len()
    }
    #[inline]
    fn consume(&mut self, n: usize) -> Result<&'src [u8], Error> {
        let end = self.position + n;
        if end > self.data.len() {
            return Err(Error::InternalSizeErr);
        }
        let found = &self.data[self.position..end];
        self.position = end;
        Ok(found)
    }
    pub fn read_vbyte(&mut self) -> Result<u64, Error> {
        let mut result: u64 = 0;
        let mut bit_p: u8 = 0;
        while self.position < self.data.len() {
            // read_byte:
            let byte = self.data[self.position] as u64;
            self.position += 1;

            // if highest bit set we're done!
            if byte & 0x80 > 0 {
                result |= (byte & 0x7f) << bit_p;
                return Ok(result);
            }
            result |= byte << bit_p;
            bit_p += 7;
        }
        Err(Error::InternalSizeErr)
    }
    pub fn read_bytes(&mut self, n: usize) -> Result<Bytes<'src>, Error> {
        Ok(Bytes {
            data: self.consume(n)?,
        })
    }
    pub fn read_u64(&mut self) -> Result<u64, Error> {
        let exact = self.consume(8)?;
        Ok(u64::from_be_bytes(exact.try_into().unwrap()))
    }
    pub fn read_u32(&mut self) -> Result<u32, Error> {
        let exact = self.consume(4)?;
        Ok(u32::from_be_bytes(exact.try_into().unwrap()))
    }
}

/// VocabularyReader.IndexBlockInfo in Galago Source
pub struct VocabularyBlock<'src> {
    pub first_key: Bytes<'src>,
    pub next_block_key: Bytes<'src>,
    pub begin: usize,
    /// Note we store end rather than length.
    pub end: usize,
    pub header_length: u32,
}
pub struct Vocabulary<'src> {
    pub region: SliceInputStream<'src>,
    pub blocks: Vec<VocabularyBlock<'src>>,
}

pub fn read_vocabulary(info: &TreeReader) -> Result<Vocabulary, Error> {
    let vocab_end = info.manifest_offset as usize;
    let vocab_start = info.vocabulary_offset as usize;
    let value_data_end = vocab_start;

    let mut vocab = SliceInputStream::new(&info.mmap[vocab_start..vocab_end]);

    let final_key_length = vocab.read_u32()? as usize;
    let final_key = vocab.read_bytes(final_key_length)?;

    let mut blocks: Vec<VocabularyBlock> = Vec::new();

    while !vocab.eof() {
        let length = vocab.read_vbyte()? as usize;
        let key = vocab.read_bytes(length)?;
        let offset = vocab.read_vbyte()? as usize;
        let header_length = vocab.read_vbyte()? as u32;

        // Found a new block; correct end and next_block_key of previous block, if any.
        if let Some(prev) = blocks.last_mut() {
            prev.end = offset;
            prev.next_block_key = key;
        }

        blocks.push(VocabularyBlock {
            begin: offset,
            header_length,
            first_key: key,
            // Rather than patch these after the loop for the final block, just start all blocks with the end values.
            end: value_data_end,
            next_block_key: final_key,
        })
    }

    Ok(Vocabulary {
        region: vocab,
        blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Galago's VByte compression (trevor, jfoley)
    fn compress_u32(i: u32, out: &mut Vec<u8>) {
        if i < 1 << 7 {
            out.push((i | 0x80) as u8);
        } else if i < 1 << 14 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) | 0x80) as u8);
        } else if i < 1 << 21 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) | 0x80) as u8);
        } else if i < 1 << 28 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) & 0x7f) as u8);
            out.push(((i >> 21) | 0x80) as u8);
        } else {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) & 0x7f) as u8);
            out.push(((i >> 21) & 0x7f) as u8);
            out.push(((i >> 28) | 0x80) as u8);
        }
    }
    #[test]
    fn test_vbytes() {
        let expected = &[
            0, 0xf, 0xef, 0xeef, 0xbeef, 0xdbeef, 0xadbeef, 0xeadbeef, 0xdeadbeef,
        ];
        let mut buf = Vec::new();
        for x in expected {
            compress_u32(*x, &mut buf)
        }

        let mut rdr = SliceInputStream::new(&buf[0..]);
        for x in expected {
            let x = *x as u64;
            assert_eq!(x, rdr.read_vbyte().unwrap());
        }
        assert!(rdr.eof());
    }

    #[test]
    fn test_read_u32() {
        let expected = &[0x11, 0x22, 0x33, 0x44];
        let mut rdr = SliceInputStream::new(&expected[0..]);
        assert_eq!(0x11223344, rdr.read_u32().unwrap());
        assert!(rdr.eof());
    }

    fn read_path(input: &str) -> Result<bool, Error> {
        let path = Path::new(&input);

        if !file_matches(&path)? {
            println!("{} is NOT a galago btree!", input);
            return Ok(false);
        }
        println!("{} is a galago_btree!", input);

        let footer = read_info(&path)?;
        println!("Footer: {:?}", footer);

        let vocab = read_vocabulary(&footer)?;
        for block in vocab.blocks {
            println!("block: {:?} .. {:?}", block.first_key, block.next_block_key);
        }
        Ok(true)
    }
    #[test]
    fn test_index_parts() {
        assert_eq!(true, read_path("data/index.galago/names").unwrap());
        assert_eq!(true, read_path("data/index.galago/names.reverse").unwrap());
        assert_eq!(true, read_path("data/index.galago/postings").unwrap());
        assert_eq!(
            true,
            read_path("data/index.galago/postings.krovetz").unwrap()
        );
        assert_eq!(true, read_path("data/index.galago/corpus").unwrap());
        assert_eq!(
            true,
            read_path("data/index.galago/corpus/split.keys").unwrap()
        );
    }
}
