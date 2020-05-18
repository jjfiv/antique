//! Lemur's Keyfile
//!
//! ```c
//! /*                                                               */
//! /* Copyright 1984,1985,1986,1988,1989,1990,2003,2004,2005,       */
//! /*   2006, 2007 by Howard Turtle                                 */
//! /*                                                               */
//! ```
use crate::{
    io_helper::{DataInputStream, InputStream, SliceInputStream, ValueEntry},
    Error,
};
use memmap::{Mmap, MmapOptions};
use std::fs;
use std::io;
use std::str;
use std::{cmp::Ordering, convert::TryInto, path::Path, sync::Arc};

#[derive(Debug)]
pub enum KFErr {
    IO(io::Error),
    TODO,
    KeyTooLong,
    Code(u32),
    BadMagic(u32),
    BadVersion(u32, u32),
    General(Error),
    FileNotOk(u32),
}
impl From<Error> for KFErr {
    fn from(e: Error) -> KFErr {
        KFErr::General(e)
    }
}
impl From<io::Error> for KFErr {
    fn from(e: io::Error) -> KFErr {
        KFErr::IO(e)
    }
}

#[derive(Debug)]
pub struct Keyfile {
    // TODO, cache
    segments: Vec<Arc<Mmap>>,
    version: u32,
    primary_levels: Vec<u32>,
    first_free_blocks: Vec<Vec<SegmentAndBlock>>,
    first_at_level: Vec<Vec<SegmentAndBlock>>,
    last_ptr: Vec<Vec<SegmentAndBlock>>,
    max_file_location: u64,
    segment_lengths: Vec<u64>,
    max_inline_record: u32,
}

struct DataSlice(usize, usize);

impl Keyfile {
    // open_key, get_kf_version, kf7_open_key, read_fib!
    pub fn open(path: &Path) -> Result<Keyfile, KFErr> {
        let file = fs::File::open(path)?;
        let opts = MmapOptions::new();
        let mmap = Arc::new(unsafe { opts.map(&file)? });

        let mut header = SliceInputStream::new(&mmap[..4096]);
        let error_code = header.read_u32()?;
        if error_code != 0 {
            return Err(KFErr::Code(error_code));
        }

        // read_fib
        let version = header.read_u32()?;
        let minor_version = header.read_u32()?;

        if version != 7 && minor_version != 0 {
            return Err(KFErr::BadVersion(version, minor_version));
        }
        let num_segments: u32 = header.read_u32()?;

        let mut segments = Vec::new();
        segments.push(mmap.clone());
        for i in 1..num_segments {
            panic!("TODO: implement multiple segment files!")
        }

        let mut primary_levels = Vec::new();
        for _ in 0..MAX_INDEX {
            primary_levels.push(header.read_u32()?);
        }
        let marker = header.read_u32()?;
        if marker != 32472 {
            return Err(KFErr::BadMagic(marker));
        }
        let file_ok = header.read_u32()?;
        if file_ok == 0 {
            return Err(KFErr::FileNotOk(file_ok));
        }

        let mut first_free_blocks = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            let mut by_level = Vec::with_capacity(MAX_INDEX);
            for _ in 0..MAX_INDEX {
                by_level.push(SegmentAndBlock::from_stream(&mut header)?)
            }
            first_free_blocks.push(by_level);
        }
        let mut first_at_level = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            let mut by_level = Vec::with_capacity(MAX_INDEX);
            for _ in 0..MAX_INDEX {
                by_level.push(SegmentAndBlock::from_stream(&mut header)?)
            }
            first_at_level.push(by_level);
        }
        let mut last_ptr = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            let mut by_level = Vec::with_capacity(MAX_INDEX);
            for _ in 0..MAX_INDEX {
                by_level.push(SegmentAndBlock::from_stream(&mut header)?)
            }
            last_ptr.push(by_level);
        }
        let max_file_location = header.read_u64()?;
        let mut segment_lengths = Vec::new();
        for _ in 0..MAX_SEGMENT {
            segment_lengths.push(header.read_u64()?);
        }
        // data_in_index_lc
        let max_inline_record = header.read_u32()?;
        // open_key:
        // init_key(f,id,lc)

        Ok(Keyfile {
            segments,
            version,
            primary_levels,
            first_free_blocks,
            first_at_level,
            last_ptr,
            max_file_location,
            segment_lengths,
            max_inline_record,
        })
    }

    // get_ptr
    pub fn lookup(&self, key: &[u8]) -> Result<Option<ValueEntry>, KFErr> {
        // kf7_get_ptr .. etc.
        if key.len() > MAX_KEY_LENGTH {
            return Err(KFErr::KeyTooLong);
        }
        const LEVEL_BEFORE_LEAVES: u32 = 1;
        if let Some(b) = self.search_index(0, LEVEL_BEFORE_LEAVES, key)? {
            // Search the leaf we've been pointed to:
            let page = self.read_page(b)?;
            match page.search(key)? {
                BlockSearchResult::NotFound(_) => return Ok(None),
                BlockSearchResult::Found(ix) => {
                    if ix >= page.keys_in_block {
                        if page.next.is_null() {
                            // Not found is OK because we're at the end.
                            return Ok(None);
                        } else {
                            panic!(
                                "found-index suggest after this block but we have no more blocks!"
                            );
                        }
                    }
                    // the index is valid & exists!
                    // unpack0_ptr_and_rec
                    // extract_next
                    let record = page.get_leaf_value(ix, self.max_inline_record)?;
                    let value = self.read_record(record)?;
                    Ok(Some(value))
                }
            }
        } else {
            panic!("We should have a non-null answer for first-round of searching...");
        }
    }

    fn read_record(&self, r: Record) -> Result<ValueEntry, KFErr> {
        self.read_address(
            SegmentAndBlock {
                segment: r.segment,
                block: r.block as u64,
            },
            r.offset,
            r.length,
        )
    }

    fn read_address(
        &self,
        addr: SegmentAndBlock,
        offset: usize,
        len: usize,
    ) -> Result<ValueEntry, KFErr> {
        if addr.is_null() {
            // Debug so I can get a backtrace.
            panic!("read_page of null!");
        }
        let file = &self.segments[addr.segment as usize];
        let start = offset + ((addr.block << BLOCK_SHIFT) as usize);
        let end = start + len;

        Ok(ValueEntry {
            source: file.clone(),
            start,
            end,
        })
    }

    fn read_page(&self, addr: SegmentAndBlock) -> Result<IndexBlock, KFErr> {
        if addr.is_null() {
            // Debug so I can get a backtrace.
            panic!("read_page of null!");
        }
        let file = &self.segments[addr.segment as usize];
        let offset = (addr.block << BLOCK_SHIFT) as usize;

        let mut page = SliceInputStream::new(&file[offset..offset + BLOCK_LC]);
        let keys_in_block = page.read_u16()?;
        let chars_in_use = page.read_u16()?;
        let index_type = page.get()?;
        let prefix_lc = page.get()?;
        let _unused = page.get()?;
        let level = page.get()?;
        let next = SegmentAndBlock::from_stream(&mut page)?;
        let prev = SegmentAndBlock::from_stream(&mut page)?;
        let here = page.tell();
        let remaining = BLOCK_LC - here;
        debug_assert!(remaining % 2 == 0);

        let keys = &file[(offset + here)..(offset + BLOCK_LC)];

        Ok(IndexBlock {
            addr,
            keys_offset: here,
            keys_in_block,
            chars_in_use,
            index_type,
            prefix_lc,
            level,
            next,
            prev,
            keys,
        })
    }

    pub fn search_index(
        &self,
        depth: usize,
        stop_level: u32,
        key: &[u8],
    ) -> Result<Option<SegmentAndBlock>, KFErr> {
        // search_index searches index blocks down to stop_lvl and returns
        //   a pointer to the block at stop_lvl-1 in which the key lies.
        //   By construction, the key must be smaller than some key in
        //   each block searched unless it is in the rightmost block at
        //   this level.  If a key is larger than any in this level, then
        //   the last_pntr pointer is the returned.
        let mut child = self.first_at_level[self.primary_levels[depth] as usize][depth];

        loop {
            let page = self.read_page(child)?;
            let done = (page.level as u32) <= stop_level;

            let index = match page.search(key)? {
                BlockSearchResult::NotFound(ix) => ix,
                BlockSearchResult::Found(ix) => ix,
            };
            // prep to loop:
            if index < page.keys_in_block {
                child = page.get_value_as_page_addr(index)?;
            // only stop if done.
            } else {
                // larger than any key:
                if page.next.is_null() {
                    child = self.last_ptr[page.level as usize][depth as usize];
                // only stop if done.
                } else {
                    return Ok(None);
                }
            }

            // mimic the do-while.
            if done == true {
                break;
            }
        }

        if child.is_null() {
            Ok(None)
        } else {
            Ok(Some(child))
        }
    }
}

struct IndexBlock<'r> {
    addr: SegmentAndBlock,
    keys_offset: usize,
    keys_in_block: u16,
    chars_in_use: u16,
    index_type: u8,
    prefix_lc: u8,
    level: u8,
    next: SegmentAndBlock,
    prev: SegmentAndBlock,
    // stored as a u16* rather unsafely in original code.
    keys: &'r [u8],
}

pub struct Record {
    segment: u16,
    block: usize,
    offset: usize,
    length: usize,
}

enum BlockSearchResult {
    NotFound(u16),
    Found(u16),
}
impl<'r> IndexBlock<'r> {
    fn get_prefix(&self) -> Result<&'r [u8], KFErr> {
        // TODO:
        // start = b->keys + keyspace_lc - prefix_lc
        // length = prefix_lc
        // I think this is basically just the last (prefix_lc) bytes in the block.
        let prefix_lc = self.prefix_lc as usize;
        let end = self.keys.len();
        let start = end - prefix_lc;
        Ok(&self.keys[start..end])
    }
    fn key_ptr(&self, index: u16) -> u16 {
        let rel_addr = (index * 2) as usize;
        u16::from_be_bytes(self.keys[rel_addr..rel_addr + 2].try_into().unwrap())
    }
    fn get_key(&self, index: u16) -> Result<&'r [u8], KFErr> {
        let key_ptr = self.key_ptr(index) as usize;
        let mut key_stream = SliceInputStream::new(&self.keys[key_ptr..]);
        // uncompress_key_lc
        let key_length = key_stream.read_lemur_vbyte()? as usize;
        debug_assert!(key_length < MAX_KEY_LENGTH);
        Ok(key_stream.consume(key_length)?)
    }
    // unpackn_ptr
    fn get_value_as_page_addr(&self, index: u16) -> Result<SegmentAndBlock, KFErr> {
        // cp = keys + pntr_sc(b, ix)
        // Value is basically right after the key.
        let key_ptr = self.key_ptr(index) as usize;
        let mut stream = SliceInputStream::new(&self.keys[key_ptr..]);
        let key_length = stream.read_lemur_vbyte()? as usize;
        // skip the key:
        let _ = stream.advance(key_length)?;
        // Read the value:
        SegmentAndBlock::decompress(&mut stream)
    }
    fn get_leaf_value(&self, index: u16, max_inline_record: u32) -> Result<Record, KFErr> {
        // unpack0_ptr_and_rec
        // cp = keys + pntr_sc(b, ix)
        // Value is basically right after the key.
        let key_ptr = self.key_ptr(index) as usize;
        let mut stream = SliceInputStream::new(&self.keys[key_ptr..]);
        let key_length = stream.read_lemur_vbyte()? as usize;
        // skip the key:
        let _ = stream.advance(key_length)?;
        let value_length = stream.read_lemur_vbyte()? as usize;
        if value_length > (max_inline_record as usize) {
            let esc = stream.read_u64()? as usize;
            let sc = (esc >> 1) * RECORD_ALLOCATION_UNIT;
            let segment = if esc & 1 > 0 { stream.read_u16()? } else { 0 };
            Ok(Record {
                length: value_length,
                block: 0,
                offset: sc,
                segment,
            })
        } else {
            // TODO; maybe pass buffer in?
            // let mut out = Vec::with_capacity(value_length);
            // out.extend(stream.consume(value_length)?);
            Ok(Record {
                length: value_length,
                block: self.addr.block as usize,
                segment: self.addr.segment,
                offset: self.keys_offset + key_ptr + stream.tell(),
            })
        }
    }
    fn compare_key(&self, key: &[u8], index: u16) -> Result<Ordering, KFErr> {
        let at_index = self.get_key(index)?;
        // TODO: ordering correct?
        Ok(key.cmp(at_index))
    }
    // compare_key, search_block
    fn search(&self, key: &[u8]) -> Result<BlockSearchResult, KFErr> {
        if self.keys_in_block == 0 {
            return Ok(BlockSearchResult::NotFound(0));
        }
        // entries in block have had prefix removed:
        let prefix = self.get_prefix()?;

        // if the key is not as long as the prefix, it's either too big or too small for the whole block:
        if key.len() < prefix.len() {
            return Ok(BlockSearchResult::NotFound(
                match key.cmp(&prefix[..key.len()]) {
                    Ordering::Greater => self.keys_in_block,
                    _ => 0,
                },
            ));
        }
        // OK, they're at least the same length:
        let key_remainder = if prefix.len() > 0 {
            match key[..prefix.len()].cmp(prefix) {
                Ordering::Less => return Ok(BlockSearchResult::NotFound(0)),
                Ordering::Greater => return Ok(BlockSearchResult::NotFound(self.keys_in_block)),
                Ordering::Equal => {}
            }
            // slide key forward:
            &key[prefix.len()..]
        } else {
            key
        };

        let mut left: usize = 0;
        let mut right = (self.keys_in_block - 1) as usize;
        while left <= right {
            let mid = left + (right - left) / 2;
            match self.compare_key(key_remainder, mid as u16)? {
                Ordering::Equal => return Ok(BlockSearchResult::Found(mid as u16)),
                Ordering::Greater => {
                    left = mid + 1;
                }
                Ordering::Less => {
                    if mid == 0 {
                        return Ok(BlockSearchResult::NotFound(0));
                    }
                    right = mid - 1;
                }
            }
        }
        // left is now the first entry>=k
        Ok(BlockSearchResult::NotFound(left as u16))
    }
}

const BLOCK_LC: usize = 4096;
const BLOCK_SHIFT: usize = 12;
const MAX_KEY_LENGTH: usize = 512;
const MAX_INDEX: usize = 3;
const MAX_LEVEL: usize = 32;
const MAX_SEGMENT: usize = 127;
const RECORD_ALLOCATION_UNIT: usize = 8;

/// leveln_pntrs point to index blocks and are the pointers stored  
///   in index blocks above level0.  They are always compressed    
///   when stored in index blocks; segment is usually small (less
///   that max_segment), block is a block number (not a file      
///   offset).  leveln_lc is the size of the pointer on disk.     
#[derive(Default, Debug, Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct SegmentAndBlock {
    /// Usually small (less than MAX_SEGMENT); could be a u8 based on that value.
    segment: u16,
    /// A Block number (not a file-offset).
    block: u64,
}

impl SegmentAndBlock {
    fn is_null(&self) -> bool {
        self.block == 0 && self.segment == MAX_SEGMENT as u16
    }
    // unpackn_ptr
    fn decompress<I>(stream: &mut I) -> Result<SegmentAndBlock, KFErr>
    where
        I: DataInputStream,
    {
        let block_raw = stream.read_lemur_vbyte()?;
        let block = block_raw >> 1;
        // if it has a segment;
        let segment = if block_raw & 1 > 0 {
            stream.read_lemur_vbyte()? as u16
        } else {
            0u16
        };
        Ok(SegmentAndBlock { block, segment })
    }
    fn from_stream<I>(stream: &mut I) -> Result<SegmentAndBlock, KFErr>
    where
        I: DataInputStream,
    {
        let segment = stream.read_u16()?;
        let block = stream.read_u64()?;
        Ok(SegmentAndBlock { segment, block })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::BufRead;
    use std::io;
    use std::str;

    #[test]
    fn test_open_keyfile() {
        let kf = Keyfile::open(Path::new("data/vocab.keyfile")).unwrap();
        assert_eq!(kf.version, 7);
        let record = kf.lookup("the".as_bytes()).unwrap().unwrap();
        // Value should be "3" which has size 1.
        assert_eq!(record.len(), 1);
        assert_eq!(record.as_slice(), "3".as_bytes());
    }

    #[test]
    fn test_dict() {
        let dictionary = Keyfile::open(Path::new("data/vocab.keyfile")).unwrap();
        let lookup = |key: &str| {
            let val = dictionary.lookup(key.as_bytes()).unwrap().unwrap();
            let str_val = str::from_utf8(val.as_slice()).unwrap();
            let num_val = str_val.parse::<usize>().unwrap();

            // because we wrote the length from python, we count chars here, not bytes.
            if num_val != key.chars().count() {
                panic!("key: {}, str_val: {}, num_val: {}", key, str_val, num_val);
            }
        };

        let f = fs::File::open("data/vocab.txt").unwrap();
        for line in io::BufReader::new(f).lines() {
            lookup(line.unwrap().trim());
        }
    }

    #[test]
    fn test_block_shift() {
        // Rather than carry this around in RAM, just make sure our constants are computed by hand right.
        // from "set_block_shift"
        let mut block = BLOCK_LC;
        let mut block_shift = 0;
        while block > 0 {
            block = block >> 1;
            if block > 0 {
                block_shift += 1;
            }
        }
        assert_eq!(BLOCK_SHIFT, block_shift);
    }
}
