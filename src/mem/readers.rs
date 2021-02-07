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

#[derive(Debug)]
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::TempDir;

    use crate::mem::{index::BTreeMapChunkedIter, key_val_files::U32KeyWriter};

    use super::{NodePointer, SkippedTreeReader};

    #[test]
    fn test_round_trip() {
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
                println!("{}: .len={}", first, kv.len());
                writer.start_key_block(&kv).unwrap();
            }
            writer.finish(&42).unwrap();
        }

        let reader = SkippedTreeReader::open(&path).unwrap();

        assert_eq!(reader.total_keys, total_keys);
        assert_eq!(reader.page_size, page_size);

        let root_block_ptrs = reader.read_root_block().unwrap();
        let mut first_level: Vec<NodePointer<_>> = Vec::new();
        for ptr in root_block_ptrs {
            let mut ptrs = reader.read_node_block(ptr.target_addr).unwrap();
            first_level.append(&mut ptrs);
        }
        println!("{:?}", first_level)
    }
}
