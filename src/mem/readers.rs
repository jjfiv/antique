use std::{io, path::Path, sync::Arc};

use io_helper::{DataInputStream, SliceInputStream};
use memmap::Mmap;

use crate::io_helper;
use crate::mem::key_val_files::{DENSE_LEAF_BLOCK, NODE_BLOCK, SPARSE_LEAF_BLOCK, STR_LEAF_BLOCK};
use crate::Error;

use super::key_val_files::U32_KEY_WRITER_MAGIC;

struct SkippedTreeReader {
    mmap: Arc<Mmap>,
    page_size: u32,
    total_keys: u32,
    metadata_addr: usize,
    root_addr: usize,
    nodes_start: usize,
}

#[derive(Debug, Clone)]
struct NodePointer<K>
where
    K: std::fmt::Debug,
{
    id: K,
    target_addr: usize,
}

// NODE_BLOCK:
// num_ptrs: v32
// repeated { left: v32, block_addr: v64 }

// LEAF_BLOCK:
// -=- dense, #-of-keys, first ; val-data*
// -=- sparse, #-of-keys, delta-gapped keys* ; val-data*;

const FOOTER_SIZE: usize = 8 * 5;

/// key, reader, offset -> use reader, offset specifically to find the value you care about!
pub struct KeyRef<'a> {
    /// The key that was queried.
    key: u32,
    /// Reader, cued to the first value in key block.
    reader: SliceInputStream<'a>,
    /// Index of desired value.
    offset: u32,
}

impl SkippedTreeReader {
    fn open(path: &Path) -> Result<SkippedTreeReader, Error> {
        let mmap = io_helper::open_mmap_file(path)?;
        let mut footer = SliceInputStream::new(&mmap[mmap.len() - FOOTER_SIZE..]);

        let metadata_addr = footer.read_u64()? as usize;
        let root_addr = footer.read_u64()? as usize;
        let nodes_start = footer.read_u64()? as usize;
        let total_keys = footer.read_u32()?;
        let page_size = footer.read_u32()?;
        let magic_number = footer.read_u64()?;

        assert!(magic_number == U32_KEY_WRITER_MAGIC);

        Ok(SkippedTreeReader {
            mmap,
            page_size,
            total_keys,
            metadata_addr,
            root_addr,
            nodes_start,
        })
    }

    fn read_node_block(&self, addr: usize) -> Result<Vec<NodePointer<u32>>, Error> {
        let mut pointers = Vec::new();
        let mut stream = SliceInputStream::new(&self.mmap[addr..]);
        let byte = stream.consume(1)?[0];
        match byte {
            DENSE_LEAF_BLOCK | SPARSE_LEAF_BLOCK | STR_LEAF_BLOCK => {
                panic!("{} is LEAF_BLOCK?", addr)
            }
            NODE_BLOCK => {}
            _ => panic!("{} is neither NODE_BLOCK or LEAF_BLOCK. Corruption.", addr),
        };
        let num_pointers = stream.read_vbyte()? as u32;

        for _ in 0..num_pointers {
            let id = stream.read_vbyte()? as u32;
            let addr = stream.read_vbyte()? as usize;
            pointers.push(NodePointer {
                id,
                target_addr: addr,
            })
        }

        Ok(pointers)
    }

    fn read_root_block(&self) -> Result<Vec<NodePointer<u32>>, Error> {
        self.read_node_block(self.root_addr)
    }

    fn find_key_u32(&self, key: u32) -> Result<Option<KeyRef>, Error> {
        let mut current_block = NodePointer {
            id: 0,
            target_addr: self.root_addr,
        };
        let mut block_ptrs = Vec::with_capacity(64);

        // Considering our B-Trees are B=128; 128**10 is an incredibly huge number.
        for _ in 0..10 {
            let mut block = SliceInputStream::new(&self.mmap[current_block.target_addr..]);
            let control = block.consume(1)?[0];
            //println!("current_block={:?}, control={}", current_block, control);
            match control {
                DENSE_LEAF_BLOCK => {
                    let num_keys = block.read_vbyte()? as u32;
                    let first = block.read_vbyte()? as u32;
                    debug_assert_eq!(current_block.id, first);
                    let offset = key - first;
                    if offset < num_keys {
                        return Ok(Some(KeyRef {
                            key,
                            reader: block,
                            offset,
                        }));
                    } else {
                        // We'll never come here unless it's our last hope.
                        return Ok(None);
                    }
                }
                SPARSE_LEAF_BLOCK => {
                    let num_keys = block.read_vbyte()? as u32;
                    let first = block.read_vbyte()? as u32;
                    debug_assert_eq!(current_block.id, first);
                    let mut offset = None;
                    let mut current = first;
                    // first is part of 'num_keys' in the SPARSE format; it's not repeated!
                    if current == key {
                        offset = Some(0);
                    }
                    for i in 1..num_keys {
                        current += block.read_vbyte()? as u32;
                        //println!("sparse-keys={} q={}, offset={:?}", current, key, offset);
                        if current == key {
                            offset = Some(i);
                            // note; no break here because we must decode all keys.
                        }
                    }
                    if let Some(offset) = offset {
                        return Ok(Some(KeyRef {
                            key,
                            reader: block,
                            offset,
                        }));
                    } else {
                        return Ok(None);
                    }
                }
                STR_LEAF_BLOCK => panic!("Better error for u32 key against STR index."),
                NODE_BLOCK => {
                    block_ptrs.clear();

                    // read block and buffer...
                    let num_pointers = block.read_vbyte()? as u32;
                    let mut found_addr = None;
                    for _ in 0..num_pointers {
                        let id = block.read_vbyte()? as u32;
                        let addr = block.read_vbyte()? as usize;
                        if key < id {
                            found_addr = block_ptrs.last();
                            break;
                        }
                        block_ptrs.push(NodePointer {
                            id,
                            target_addr: addr,
                        });
                        if key == id {
                            found_addr = block_ptrs.last();
                            break;
                        }
                    }
                    if let Some(fa) = found_addr {
                        current_block = fa.clone();
                    } else {
                        current_block = block_ptrs.last().unwrap().clone();
                    }
                }
                _ => panic!(
                    "Corrupted block addr? Found control={} at {} for block.id={}, key={}",
                    control, current_block.target_addr, current_block.id, key
                ),
            }
        }
        panic!(
            "Infinite loop in key search? key={}, current_block@{} id={}",
            key, current_block.target_addr, current_block.id
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::TempDir;

    use crate::{
        io_helper::DataInputStream,
        mem::{index::BTreeMapChunkedIter, key_val_files::U32KeyWriter},
    };

    use super::SkippedTreeReader;

    #[test]
    fn test_dense_round_trip() {
        let tmpdir = TempDir::new().unwrap();
        let mut data = BTreeMap::new();
        for i in 0..10000u32 {
            data.insert(i, i * 3);
        }
        let total_keys = data.len() as u32;
        let page_size = 64;

        let mut leaf_key_starts = Vec::new();

        let path = tmpdir.path().join("rtt.map");
        {
            let mut writer = U32KeyWriter::create(&path, total_keys, page_size).unwrap();
            let mut iter = BTreeMapChunkedIter::new(&data, page_size as usize);
            while let Some(first) = iter.next() {
                leaf_key_starts.push(first);
                let kv: Vec<u32> = iter.keys().iter().cloned().cloned().collect();
                writer.start_key_block(&kv).unwrap();
                for v in iter.vals() {
                    writer.write_v32(**v).unwrap();
                }
            }
            writer.finish(&42).unwrap();
        }

        let reader = SkippedTreeReader::open(&path).unwrap();

        assert_eq!(reader.total_keys, total_keys);
        assert_eq!(reader.page_size, page_size);

        for i in 0..10000u32 {
            let maybe = reader.find_key_u32(i).expect("No I/O errors...");
            assert!(maybe.is_some());
            let mut keyref = maybe.unwrap();
            for _ in 0..keyref.offset {
                let _ = keyref.reader.read_vbyte().expect("No I/O");
            }
            let value = keyref.reader.read_vbyte().unwrap() as u32;
            assert_eq!(value, i * 3);
        }
    }

    #[test]
    fn test_sparse_round_trip() {
        let tmpdir = TempDir::new().unwrap();
        let mut data = BTreeMap::new();
        for i in 0..10000u32 {
            data.insert(i * 7, i * 3);
        }
        let total_keys = data.len() as u32;
        let page_size = 64;

        let mut leaf_key_starts = Vec::new();

        let path = tmpdir.path().join("rtt-sparse.map");
        {
            let mut writer = U32KeyWriter::create(&path, total_keys, page_size).unwrap();
            let mut iter = BTreeMapChunkedIter::new(&data, page_size as usize);
            while let Some(first) = iter.next() {
                leaf_key_starts.push(first);
                let kv: Vec<u32> = iter.keys().iter().cloned().cloned().collect();
                writer.start_key_block(&kv).unwrap();
                for v in iter.vals() {
                    writer.write_v32(**v).unwrap();
                }
            }
            writer.finish(&42).unwrap();
        }

        let reader = SkippedTreeReader::open(&path).unwrap();

        assert_eq!(reader.total_keys, total_keys);
        assert_eq!(reader.page_size, page_size);

        for i in 0..10000u32 {
            let key = i * 7;
            let expected = i * 3;
            let maybe = reader.find_key_u32(key).expect("No I/O errors...");
            assert!(maybe.is_some());
            let mut keyref = maybe.unwrap();
            for _ in 0..keyref.offset {
                let _ = keyref.reader.read_vbyte().expect("No I/O");
            }
            let value = keyref.reader.read_vbyte().unwrap() as u32;
            assert_eq!(value, expected);
        }
    }
}
