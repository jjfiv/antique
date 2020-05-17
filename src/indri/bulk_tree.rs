use crate::io_helper::*;
use crate::Error;
use crate::HashMap;
use memmap::{Mmap, MmapOptions};
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::{
    cmp::Ordering,
    convert::TryInto,
    fs,
    io::{Seek, SeekFrom},
};

// Blocks are 8k.
const BLOCK_SIZE: usize = 8 * 1024;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
struct BlockId(u32);
impl BlockId {
    fn bounds(self) -> (usize, usize) {
        let start = (self.0 as usize) * BLOCK_SIZE;
        let end = start + BLOCK_SIZE;
        (start, end)
    }
}

/// In order to be a valid "bulk_tree", the size must be divisble by the "block size" and have at least 1 block!
pub fn is_maybe_bulk_tree(path: &Path) -> Result<bool, Error> {
    if !path.is_file() {
        return Ok(false);
    }

    let mut fp = fs::File::open(&path)?;
    let length = fp.seek(SeekFrom::End(0))? as usize;

    if length % BLOCK_SIZE == 0 && length >= BLOCK_SIZE {
        Ok(true)
    } else {
        Ok(false)
    }
}

// Sadly, Indri's bulk_tree doesn't have a magic number, AFAICT.
pub struct BulkTreeReader {
    mmap: Arc<Mmap>,
    // OK to use usize for num_blocks since _fetch in Indri takes u32.
    num_blocks: usize,
}

impl BulkTreeReader {
    pub fn open(path: &Path) -> Result<BulkTreeReader, Error> {
        let file = fs::File::open(path)?;
        let opts = MmapOptions::new();
        let mmap: Mmap = unsafe { opts.map(&file)? };
        let file_length = mmap.len();

        if file_length < BLOCK_SIZE || file_length % BLOCK_SIZE != 0 {
            return Err(Error::BadBulkTreeSize);
        }

        let num_blocks = (file_length / BLOCK_SIZE) as usize;

        Ok(BulkTreeReader {
            mmap: Arc::new(mmap),
            num_blocks,
        })
    }
    fn root_id(&self) -> BlockId {
        BlockId((self.num_blocks - 1) as u32)
    }
    fn fetch(&self, id: BlockId) -> Result<&[u8], Error> {
        if (id.0 as usize) >= self.num_blocks {
            return Err(Error::BadBulkTreeBlock(id.0));
        }
        let (start, end) = id.bounds();
        // Most of the work in BulkTree::fetch involves managing the cache. Since we're mmapping the file; we can trust the OS/FS cache for now.
        return Ok(&self.mmap[start..end]);
    }
    pub fn find_str(&self, key: &str) -> Result<Option<Bytes>, Error> {
        self.find_value(key.as_bytes())
    }
    pub fn find_value(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        let mut next_id = self.root_id();
        loop {
            let block = BulkTreeBlock(next_id, self.fetch(next_id)?);
            if block.is_leaf() {
                break;
            }
            let entry_id: u16 = block.find_approx(key);
            // if a block is NOT a leaf, interpret value as our "next_id".
            // Indri uses int here and casts it to a char*.
            // Let's assume it's a u32 in little-endian:
            let val_bytes = block.value(entry_id);
            assert_eq!(4, val_bytes.len());
            next_id = BlockId(u32::from_le_bytes(
                val_bytes
                    .try_into()
                    .map_err(|_| Error::BadBulkTreeBlock(next_id.0))?,
            ));
        }
        let block = BulkTreeBlock(next_id, self.fetch(next_id)?);

        if let Some(index) = block.find_exact(key) {
            Ok(Some(Bytes::from_slice(block.value(index))))
        } else {
            Ok(None)
        }
    }
}

/// This struct is transient.
/// We point it at a memory address to have OOP-style accessors.
/// Unlike the indri version, we don't keep it around.
struct BulkTreeBlock<'b>(BlockId, &'b [u8]);

impl<'b> BulkTreeBlock<'b> {
    /// Get the block-id back out.
    fn id(&self) -> BlockId {
        return self.0;
    }
    /// Indri is primarily run on little-endian machines:
    /// Often the first word of the block is cast to a UINT16.
    /// 00000000: 0180 616c 7068 616f 6d65 6761 0000 0000  ..alphaomega....
    #[inline]
    fn nth_le_u16(&self, index: u32) -> u16 {
        let byte_offset = (index * 2) as usize;
        ((self.1[byte_offset + 1] as u16) << 8) | (self.1[byte_offset] as u16)
    }
    fn is_leaf(&self) -> bool {
        return (self.nth_le_u16(0) & 0x8000) != 0;
    }
    /// Maximum number of keys in a block is 2^15-1.
    fn count(&self) -> u16 {
        self.nth_le_u16(0) & 0x7fff
    }

    fn value_bounds(&self, index: u16) -> (usize, usize) {
        // UINT16* blockEnd = _buffer + BULK_BLOCK_SIZE;
        // UINT16  keyEnd = blockEnd[ -(index*2+2) ];
        // UINT16 valueEnd = blockEnd[-(index*2+1)]
        // So the file has (keyEnd, valueEnd) pairs encoded backwards:
        // ... [keyEnd[1], valueEnd[1]], [keyEnd[0], valueEnd[0]]
        // ... a stack that grows down, essentially:
        // we call keyEnd value_start_addr:
        let index = index as usize;
        let value_start_addr = BLOCK_SIZE - 2 * (index * 2 + 2);
        let value_start_index = (value_start_addr / 2) as u32;
        // Now look it up and byteswap it:
        let value_start = self.nth_le_u16(value_start_index);
        let value_end = self.nth_le_u16(value_start_index + 1);
        (value_start as usize, value_end as usize)
    }

    fn key_bounds(&self, index: u16) -> (usize, usize) {
        // since key_start is problematically defined as "value_end(index - 1)"
        // which bottoms out with negative value-ends being: sizeof(UINT16)
        let key_start = if index == 0 {
            2
        } else {
            let (_, end) = self.value_bounds(index - 1);
            end
        };
        let (key_end, _) = self.value_bounds(index);
        (key_start, key_end)
    }

    fn key(&self, index: u16) -> &'b [u8] {
        let (start, end) = self.key_bounds(index);
        return &self.1[start..end];
    }

    fn value(&self, index: u16) -> &'b [u8] {
        let (start, end) = self.value_bounds(index);
        return &self.1[start..end];
    }

    fn find_exact(&self, key: &[u8]) -> Option<u16> {
        let mut left = 0;
        let mut right = self.count();

        while left < right {
            let middle = left + (right - left) / 2;
            match key.cmp(self.key(middle as u16)) {
                Ordering::Less => {
                    right = middle;
                }
                Ordering::Equal => return Some(middle),
                Ordering::Greater => {
                    left = middle + 1;
                }
            }
        }

        None
    }
    /// Used at least to find which leaf to pursue.
    /// Return the index that is greater than or equal to this key OR the last index.
    fn find_approx(&self, key: &[u8]) -> u16 {
        let mut left = 0;
        let mut right = self.count() - 1;

        while left + 1 < right {
            let middle = left + (right - left) / 2;
            match key.cmp(self.key(middle as u16)) {
                Ordering::Less => {
                    right = middle;
                }
                Ordering::Equal => return middle,
                Ordering::Greater => {
                    left = middle;
                }
            }
        }
        if key >= self.key(right) {
            right
        } else {
            left
        }
    }
}

#[derive(Debug)]
struct TermFieldStats {
    total_count: u64,
    doc_count: u32,
}
#[derive(Debug)]
struct DiskTermData {
    corpus_total_count: u64,
    corpus_doc_count: u32,
    max_doc_len: u32,
    min_doc_len: u32,
    field_stats: Vec<TermFieldStats>,
}

impl DiskTermData {
    fn from_stream<I>(input: &mut I, num_fields: usize) -> Result<DiskTermData, Error>
    where
        I: DataInputStream,
    {
        let corpus_total_count = input.read_vbyte()?;
        let corpus_doc_count = input.read_vbyte()? as u32;

        let max_doc_len = input.read_vbyte()? as u32;
        let min_doc_len = input.read_vbyte()? as u32;

        let mut field_stats = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            let total_count = input.read_vbyte()?;
            let doc_count = input.read_vbyte()? as u32;
            field_stats.push(TermFieldStats {
                total_count,
                doc_count,
            })
        }

        Ok(DiskTermData {
            corpus_total_count,
            corpus_doc_count,
            max_doc_len,
            min_doc_len,
            field_stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    #[test]
    fn test_dict() {
        let dictionary = BulkTreeReader::open(Path::new("data/dict.bulktree")).unwrap();
        let lookup = |key: &str| {
            let val = dictionary.find_value(key.as_bytes()).unwrap().unwrap();
            let str_val = str::from_utf8(val.as_bytes()).unwrap();
            let num_val = str_val.parse::<usize>().unwrap();

            if num_val != key.len() {
                panic!("key: {}, str_val: {}, num_val: {}", key, str_val, num_val);
            }
        };

        lookup("a");
        lookup("antidisciplinarian");
        lookup("clarifiant");
        lookup("macrocarpous");
        lookup("hexadic");
        lookup("protopin");
        lookup("postcolon");
        lookup("zyzzogeton");
    }

    #[test]
    fn test_in_index() {
        let str_to_term_id =
            BulkTreeReader::open(Path::new("data/index.indri/index/0/infrequentString")).unwrap();
        let data = str_to_term_id.find_str("the").unwrap().unwrap();
        let mut stream = data.stream();
        let term_info = DiskTermData::from_stream(&mut stream, 0).unwrap();

        assert_eq!(term_info.corpus_doc_count, 5);
        assert_eq!(term_info.max_doc_len, 1717);
        assert_eq!(term_info.min_doc_len, 831);
        println!("{:?}", term_info);
    }
}
