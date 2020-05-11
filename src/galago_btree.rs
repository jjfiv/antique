use crate::io_helper::{Bytes, SliceInputStream};
use crate::{Error, HashMap};
use memmap::{Mmap, MmapOptions};
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;
use std::{
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
