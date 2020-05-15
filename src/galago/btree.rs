use crate::io_helper::{Bytes, DataInputStream, InputStream, SliceInputStream};
use crate::{galago::postings::IndexPartType, DocId};
use crate::{Error, HashMap};
use memmap::{Mmap, MmapOptions};
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::{
    cmp,
    path::{Path, PathBuf},
    str,
};

/// Used externally by Index.
pub fn is_btree(path: &Path) -> bool {
    match open_file_magic(path, MAGIC_NUMBER) {
        Ok(_) => true,
        Err(_) => false,
    }
}

// Notes on the format:
// Java's DataInputStream/DataOutputStream classes write data as big-endian.

/// Last 8 bytes of the file should be this:
pub(crate) const MAGIC_NUMBER: u64 = 0x1a2b3c4d5e6f7a8d;
// For split.keys accompanying files:
const VALUE_MAGIC_NUMBER: u64 = 0x2b3c4d5e6f7a8b9c;

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
    /// These are opened lazily:
    pub value_readers: Arc<Mutex<HashMap<u32, Arc<Mmap>>>>,
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
    pub stemmer: Option<String>,
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
    SplitKeys(PathBuf),
}

impl TreeLocation {
    pub(crate) fn new(path: &Path) -> Result<TreeLocation, Error> {
        if path.is_dir() {
            let inner = path.join("split.keys");
            if inner.is_file() {
                Ok(TreeLocation::SplitKeys(inner))
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
            TreeLocation::SplitKeys(keys) => &keys,
        }
    }
}

/// Is this a Galago Btree?
pub(crate) fn open_file_magic(path: &Path, magic: u64) -> Result<Mmap, Error> {
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
    if maybe_magic == magic {
        Ok(mmap)
    } else {
        Err(Error::BadGalagoMagic(maybe_magic))
    }
}

impl TreeReader {
    pub fn new(path: &Path) -> Result<TreeReader, Error> {
        read_info(path)
    }

    pub fn file_name(&self) -> Result<&str, Error> {
        self.location
            .keys_path()
            .file_name()
            .and_then(|os_str| os_str.to_str())
            .ok_or_else(|| Error::BadFileName(self.location.keys_path().into()))
    }

    /// WARNING: know what you're doing here.
    /// A lot of indexes will have too many keys for this to be efficient.
    /// This is for lengths, where there's 1-key per field.
    pub fn collect_string_keys(&self) -> Result<Vec<String>, Error> {
        let mut output = Vec::with_capacity(self.manifest.key_count as usize);

        let mut key_buffer = Vec::new();
        for block in self.vocabulary.blocks.iter() {
            let mut block_iter = block.iterator(&self.mmap, &mut key_buffer)?;
            while let Some(_) = block_iter.read_next(&mut key_buffer)? {
                output.push(str::from_utf8(&key_buffer)?.to_owned());
            }
        }

        Ok(output)
    }

    pub fn index_part_type(&self) -> Result<IndexPartType, Error> {
        return IndexPartType::from_reader_class(&self.manifest.reader_class);
    }

    pub fn read_name_to_id(&self) -> Result<HashMap<String, DocId>, Error> {
        match self.index_part_type()? {
            IndexPartType::NamesReverse => {}
            other => panic!("Don't call read_name_to_id on {:?}", other),
        }

        let mut output = HashMap::default();
        output.reserve(self.manifest.key_count as usize);

        let source = self.mmap.clone();
        let mut key_buffer = Vec::new();
        for block in self.vocabulary.blocks.iter() {
            let mut block_iter = block.iterator(&self.mmap, &mut key_buffer)?;
            while let Some(entry) = block_iter.read_next(&mut key_buffer)? {
                let mut reader = SliceInputStream::new(&source[entry.start..entry.end]);
                let docid = DocId(reader.read_u64()?);
                output.insert(str::from_utf8(&key_buffer)?.to_owned(), docid);
            }
        }

        Ok(output)
    }

    fn get_value_source(&self, index: u32) -> Result<Arc<Mmap>, Error> {
        Ok(match &self.location {
            TreeLocation::SingleFile(_) => self.mmap.clone(),
            TreeLocation::SplitKeys(path) => {
                let mut value_readers = self
                    .value_readers
                    .lock()
                    .map_err(|_| Error::ThreadFailure)?;
                let source: Arc<Mmap> = match value_readers.entry(index) {
                    Entry::Occupied(source) => source.get().clone(),
                    Entry::Vacant(entry) => {
                        if let Some(dir) = path.parent() {
                            let other_file = dir.join(format!("{}", index));
                            let mmap: Mmap = open_file_magic(&other_file, VALUE_MAGIC_NUMBER)?;
                            entry.insert(Arc::new(mmap)).clone()
                        } else {
                            return Err(Error::MissingSplitFiles);
                        }
                    }
                };
                source
            }
        })
    }
}

/// Read footer:
pub fn read_info(path: &Path) -> Result<TreeReader, Error> {
    let location = TreeLocation::new(path)?;

    // Use Memory-Mapped I/O:
    let mmap: Mmap = open_file_magic(location.keys_path(), MAGIC_NUMBER)?;
    let file_length = mmap.len();

    let footer_start = file_length - FOOTER_SIZE;
    let mut reader = SliceInputStream::new(&mmap[footer_start..]);

    let vocabulary_offset = reader.read_u64()?;
    let manifest_offset = reader.read_u64()?;
    let block_size = reader.read_u32()?;
    let magic_number = reader.read_u64()?;
    debug_assert!(reader.eof());

    // We already checked this while opening, but w/e.
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

    let value_readers = Arc::new(Mutex::new(HashMap::<u32, _>::default()));

    Ok(TreeReader {
        mmap: Arc::new(mmap),
        location,
        block_size,
        magic_number,
        manifest,
        vocabulary,
        value_readers,
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
        let mut key_buffer: Vec<u8> = Vec::new();

        // Can't impl Iterator without heap allocation; much like stdlib's read_line vs. lines()
        let mut iter = self.vocabulary.blocks[block_index].iterator(&self.mmap, &mut key_buffer)?;

        while let Some(found) = iter.read_next(&mut key_buffer)? {
            if key == key_buffer.as_slice() {
                match &self.location {
                    TreeLocation::SingleFile(_) => {
                        return Ok(Some(ValueEntry {
                            source: self.mmap.clone(),
                            start: found.start,
                            end: found.end,
                        }));
                    }
                    TreeLocation::SplitKeys(_) => {
                        let mut reader = SliceInputStream::new(&self.mmap[found.start..found.end]);
                        let file_id = reader.read_u32()?;
                        let start = reader.read_u64()? as usize;
                        let length = reader.read_u64()? as usize;
                        let source = self.get_value_source(file_id)?;
                        return Ok(Some(ValueEntry {
                            source,
                            start,
                            end: start + length,
                        }));
                    }
                };
            } else if key_buffer.as_slice() > key {
                break;
            }
        }
        Ok(None)
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
    pub fn to_str(&self) -> Result<&str, Error> {
        Ok(std::str::from_utf8(&self.source[self.start..self.end])?)
    }
}

struct VocabIterValue {
    start: usize,
    end: usize,
}

struct VocabularyBlockIter<'src> {
    stream: SliceInputStream<'src>,
    value_end: usize,
    last_end: usize,
    key_index: usize,
    key_count: usize,
    first: Option<VocabIterValue>,
}

impl<'src> VocabularyBlockIter<'src> {
    fn read_next(&mut self, key_buffer: &mut Vec<u8>) -> Result<Option<VocabIterValue>, Error> {
        // First iteration; return prepared value:
        if let Some(first) = self.first.take() {
            Ok(Some(VocabIterValue {
                start: first.start,
                end: first.end,
            }))
        } else if self.key_index < self.key_count {
            // 2..n iterations: read from stream as necessary:
            // The remaining keys are prefix encoded!
            let start = self.last_end;
            let common = self.stream.read_vbyte()? as usize;
            let key_length = self.stream.read_vbyte()? as usize;
            let suffix = self.stream.read_bytes(key_length - common)?;
            let end_value_offset = self.stream.read_vbyte()? as usize;
            self.last_end = self.value_end - end_value_offset;

            // compose the current string in buffer
            key_buffer.truncate(common); // keep the first ..common chars
            key_buffer.extend_from_slice(suffix); // extend with what was encoded here.
            self.key_index += 1;

            Ok(Some(VocabIterValue {
                start,
                end: self.last_end,
            }))
        } else {
            Ok(None)
        }
    }
}

impl VocabularyBlock {
    fn iterator<'src, 'b>(
        &self,
        source: &'src Mmap,
        key_buffer: &'b mut Vec<u8>,
    ) -> Result<VocabularyBlockIter<'src>, Error> {
        // Now the format is defined in DiskBTreeIterator.cacheKeys...
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
        let last_end = self.end - end_value_offset;
        key_buffer.extend_from_slice(first_key);

        Ok(VocabularyBlockIter {
            stream: header,
            value_end: self.end,
            last_end,
            key_count,
            key_index: 1,
            first: Some(VocabIterValue {
                start: value_start,
                end: last_end,
            }),
        })
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

    // Galago bakes absolute paths into everything:
    const PREFIX: &str = "/home/jfoley/antique";
    use crate::galago::corpus::decompress_document;
    use crate::galago::tokenizer::State as Tokenizer;

    use crate::HashSet;
    use std::fs;

    #[test]
    fn corpus_has_all_files() {
        let reader = read_info(&Path::new("data/index.galago/names.reverse")).unwrap();
        let keys = reader.read_name_to_id().unwrap();

        let corpus = read_info(&Path::new("data/index.galago/corpus")).unwrap();
        for (name, doc) in keys.iter() {
            assert!(name.starts_with(PREFIX));
            let rel_path = Path::new(&name[PREFIX.len() + 1..]);
            println!("{:?}", rel_path);
            let repr = doc.to_be_bytes();

            let stored = corpus.find_bytes(&repr).unwrap().unwrap();
            let document = decompress_document(stored).unwrap().into_tokenized();

            let expected = fs::read_to_string(rel_path).unwrap();
            let mut tok = Tokenizer::new(&expected);
            tok.parse();
            let found = tok.into_document(HashSet::default());
            assert_eq!(found.text, document.text);
            assert_eq!(found.terms, document.terms);
            assert_eq!(found, document);
        }
    }
}
