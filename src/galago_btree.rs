use crate::io_helper::{Bytes, SliceInputStream};
use crate::{Error, HashMap};
use memmap::{Mmap, MmapOptions};
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;
use std::{
    cmp,
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
    pub location: TreeLocation,
    pub block_size: u32,
    pub magic_number: u64,
    pub manifest: Manifest,
    pub vocabulary: Vocabulary,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub max_key_size: usize,
    pub block_count: u64,
    pub block_size: usize,
    pub empty_index_file: bool,
    cache_group_size: Option<usize>,
    /// I love Serde so much; this was "filename" in practice but should be camel or snake-case.
    #[serde(alias = "filename")]
    file_name: String,
    pub reader_class: String,
    writer_class: Option<String>,
    merger_class: Option<String>,
    pub key_count: u64,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// VocabularyReader.IndexBlockInfo in Galago Source
#[derive(Debug, Clone)]
pub struct VocabularyBlock {
    pub first_key: Bytes,
    pub begin: usize,
    /// Note we store end rather than length.
    pub end: usize,
    pub header_length: u32,
}

#[derive(Debug, Clone)]
pub struct Vocabulary {
    pub blocks: Vec<VocabularyBlock>,
}

#[derive(Debug, Clone)]
pub enum TreeLocation {
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

    let vocab_end = manifest_offset as usize;
    let vocab_start = vocabulary_offset as usize;
    let value_data_end = vocab_start;
    let mut vocab = SliceInputStream::new(&mmap[vocab_start..vocab_end]);
    let vocabulary = read_vocabulary(&mut vocab, value_data_end)?;

    Ok(TreeReader {
        mmap: Arc::new(mmap),
        location,
        block_size,
        magic_number,
        manifest,
        vocabulary,
    })
}

fn read_vocabulary(
    vocab: &mut SliceInputStream,
    value_data_end: usize,
) -> Result<Vocabulary, Error> {
    // Note: these keys are historical, only!
    // Writers are no longer inserting them correctly, only as "\0".
    let final_key_length = vocab.read_u32()? as usize;
    // It's convenient for binary-search reasons to have a final key, but it looks like they may be incorrect :(
    let _final_key = vocab.read_bytes(final_key_length)?;

    let mut blocks: Vec<VocabularyBlock> = Vec::new();

    while !vocab.eof() {
        let length = vocab.read_vbyte()? as usize;
        let key = vocab.read_bytes(length)?;
        let offset = vocab.read_vbyte()? as usize;
        let header_length = vocab.read_vbyte()? as u32;

        // Found a new block; correct end based on the start of this current block.
        if let Some(prev) = blocks.last_mut() {
            prev.end = offset;
        }

        blocks.push(VocabularyBlock {
            begin: offset,
            header_length,
            first_key: Bytes::from_slice(key),
            // Rather than patch these after the loop for the final block, just start all blocks with the end values.
            end: value_data_end,
        })
    }

    Ok(Vocabulary { blocks })
}

impl TreeReader {
    pub fn find_str(&self, text: &str) -> Result<Option<ValueEntry>, Error> {
        self.find_bytes(text.as_bytes())
    }
    pub fn find_bytes(&self, key: &[u8]) -> Result<Option<ValueEntry>, Error> {
        let block_index = self.vocabulary.block_binary_search(key);
        self.vocabulary.blocks[block_index].decode_search(key, self.mmap.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ValueEntry {
    pub(crate) source: Arc<Mmap>,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl ValueEntry {
    pub fn len(&self) -> usize {
        self.end - self.start
    }
}

impl VocabularyBlock {
    // TODO: refactor this to be an iterator, and search to be simple over that iterator.
    fn decode_search(
        &self,
        find_key: &[u8],
        source: Arc<Mmap>,
    ) -> Result<Option<ValueEntry>, Error> {
        let value_start = self.begin + (self.header_length as usize);
        // loadBlockHeader:
        let mut header = SliceInputStream::new(&source[self.begin..value_start]);
        // This is a writer-mistake to be a u64.
        let key_count = header.read_u64()? as usize;

        // Now the format is defined in DiskBTreeIterator.cacheKeys...
        let first_key_length = header.read_vbyte()? as usize;
        let first_key = header.read_bytes(first_key_length)?;
        // The location of values are encoded as differences from the end of the value strip.
        let end_value_offset = header.read_vbyte()? as usize;
        let mut last_end = self.end - end_value_offset;

        if find_key == first_key {
            return Ok(Some(ValueEntry {
                source,
                start: value_start,
                // TBD: is this correct? OR is it (self.end - end_value_offset)
                end: last_end,
            }));
        }

        let mut key_buffer: Vec<u8> = Vec::with_capacity(first_key_length);
        key_buffer.extend_from_slice(first_key);

        // The remaining keys are prefix encoded!
        for _ in 1..key_count {
            let start = last_end;
            let common = header.read_vbyte()? as usize;
            let key_length = header.read_vbyte()? as usize;
            let suffix = header.read_bytes(key_length - common)?;
            let end_value_offset = header.read_vbyte()? as usize;
            last_end = self.end - end_value_offset;

            // compose the current string in buffer
            key_buffer.truncate(common); // keep the first ..common chars
            key_buffer.extend_from_slice(suffix); // extend with what was encoded here.

            match find_key.cmp(&key_buffer) {
                // Continue linear search:
                cmp::Ordering::Greater => {
                    continue;
                }
                // Found desired key:
                cmp::Ordering::Equal => {
                    debug_assert!(start >= self.begin);
                    debug_assert!(start < self.end);
                    debug_assert!(last_end > self.begin);
                    debug_assert!(last_end <= self.end);
                    return Ok(Some(ValueEntry {
                        source,
                        start,
                        end: last_end,
                    }));
                }
                // Found a key larger than our query, stop immediately.
                cmp::Ordering::Less => {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
}

impl Vocabulary {
    fn block_binary_search(&self, key: &[u8]) -> usize {
        let mut left = 0;
        let mut right = self.blocks.len() - 1;

        // While we can narrow our search further without loading blocks:
        while right - left > 1 {
            let middle = (right - left) / 2 + left;

            match key.cmp(self.blocks[middle].first_key.as_bytes()) {
                // We know it's the first key in the block (rare).
                std::cmp::Ordering::Equal => return middle,
                std::cmp::Ordering::Less => {
                    right = middle;
                }
                std::cmp::Ordering::Greater => {
                    left = middle; // no + 1 here because we can't exclude the other keys in the block.
                }
            }
        }
        // found A,B: if key < B.start, return A
        match key.cmp(self.blocks[right].first_key.as_bytes()) {
            cmp::Ordering::Less => left,
            _ => right,
        }
    }
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

        let reader = read_info(&path)?;
        println!("Manifest: {:?}", reader.manifest);

        for block in reader.vocabulary.blocks {
            println!("block: {:?} ..", block.first_key);
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

    #[test]
    fn test_block_bsearch() {
        // helper to make a bunch of fake blocks
        fn mk_block(text: &str, index: usize) -> VocabularyBlock {
            VocabularyBlock {
                first_key: Bytes::from_slice(text.as_bytes()),
                begin: index,
                end: 0,
                header_length: 0,
            }
        }

        let mut blocks = Vec::new();
        for (i, letter) in ["B", "D", "F"].iter().enumerate() {
            blocks.push(mk_block(letter, i));
        }

        let vocab = Vocabulary { blocks };

        // B..
        assert_eq!(vocab.block_binary_search("A".as_bytes()), 0);
        assert_eq!(vocab.block_binary_search("B".as_bytes()), 0);
        assert_eq!(vocab.block_binary_search("C".as_bytes()), 0);
        // D..
        assert_eq!(vocab.block_binary_search("D".as_bytes()), 1);
        assert_eq!(vocab.block_binary_search("E".as_bytes()), 1);
        // F->
        assert_eq!(vocab.block_binary_search("F".as_bytes()), 2);
        assert_eq!(vocab.block_binary_search("G".as_bytes()), 2);
        assert_eq!(vocab.block_binary_search("Z".as_bytes()), 2);
    }

    #[test]
    fn postings_for_stopwords_are_long() {
        let reader = read_info(&Path::new("data/index.galago/postings")).unwrap();
        let the_entry = reader.find_str("the").unwrap().unwrap();
        let chapter_entry = reader.find_str("chapter").unwrap().unwrap();
        assert!(the_entry.end - the_entry.start > chapter_entry.end - chapter_entry.start);
    }
}
