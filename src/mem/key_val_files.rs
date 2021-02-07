use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::io_helper::Teller;

use super::{
    encoders::{write_vbyte, write_vbyte_u64},
    index::is_contiguous,
};

// Version up to 256:
const DENSE_KEY_WRITER_MAGIC: u64 = 0xf1e2_d3c4_b5a6_0000 | 0x0001;

// Two types of blocks in a keys-file:
const DENSE_LEAF_BLOCK: u8 = 0xaf; // 11101111
const SPARSE_LEAF_BLOCK: u8 = 0xa0; // 1110000;
const NODE_BLOCK: u8 = 0x10; // 00010000

const PAGE_4K: usize = 4096;

pub struct CountingFileWriter {
    path: PathBuf,
    output: Option<File>,
    buffer: Vec<u8>,
    written: u64,
}

impl io::Write for CountingFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.len() > PAGE_4K {
            self.flush_buffer()?;
        }
        self.buffer.extend(buf);
        self.written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        if let Some(out) = self.output.as_mut() {
            out.flush()?;
        }
        Ok(())
    }
}

impl CountingFileWriter {
    pub fn put(&mut self, x: u8) {
        self.buffer.push(x);
        self.written += 1;
    }
    pub fn tell(&self) -> u64 {
        self.written
    }
    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.len() > 0 {
            // open file if needed
            if self.output.is_none() {
                self.output = Some(File::create(&self.path)?)
            }
            self.output.as_mut().unwrap().write_all(&mut self.buffer)?;
            self.buffer.clear();
        }
        Ok(())
    }
    pub fn new(file: File) -> io::Result<Self> {
        let mut output = file;
        let written = output.tell()?;
        Ok(Self {
            path: PathBuf::new(),
            output: Some(output),
            buffer: Vec::with_capacity(PAGE_4K),
            written,
        })
    }
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            output: None,
            buffer: Vec::with_capacity(PAGE_4K),
            written: 0,
        })
    }
}

pub struct U32KeyWriter {
    output: CountingFileWriter,
    skips: Vec<IdAndValueAddr>,
    total_keys: u32,
    keys_written: u32,
    nodes_start: u64,
    root_addr: u64,
    page_size: u32,
}

impl U32KeyWriter {
    pub fn create(path: &Path, total_keys: u32, page_size: u32) -> io::Result<Self> {
        let mut output = CountingFileWriter::new(File::create(path)?)?;
        // u64-MAGIC
        output.write_all(&DENSE_KEY_WRITER_MAGIC.to_le_bytes())?;
        Ok(Self {
            output,
            total_keys,
            page_size,

            keys_written: 0,
            nodes_start: 0,
            root_addr: 0,
            skips: Vec::new(),
        })
    }

    /// Every block is either dense or sparse:
    /// dense, #-of-keys, first ; val-data*
    /// sparse, #-of-keys, delta-gapped keys* ; val-data*;
    pub fn start_key_block(&mut self, keys: &[u32]) -> io::Result<()> {
        // record this block for posterity;
        self.skips
            .push(IdAndValueAddr::new(keys[0], self.output.tell()));

        let num_keys = keys.len() as u32;
        if is_contiguous(keys) {
            self.output.put(DENSE_LEAF_BLOCK);
            self.write_v32(num_keys)?;
            self.write_v32(keys[0])?;
        } else {
            self.output.put(SPARSE_LEAF_BLOCK);
            self.write_v32(num_keys)?;

            // delta-gap and write keys:
            let mut prev = 0; // first is not delta-gapped.
            for k in keys {
                self.write_v32(k - prev)?;
                prev = *k;
            }
        }
        self.keys_written += keys.len() as u32;

        Ok(())
    }

    pub fn write_v64(&mut self, x: u64) -> io::Result<usize> {
        write_vbyte_u64(x, &mut self.output)
    }
    pub fn write_v32(&mut self, x: u32) -> io::Result<usize> {
        write_vbyte(x, &mut self.output)
    }
    pub fn write_bytes(&mut self, x: &[u8]) -> io::Result<usize> {
        self.output.write_all(x)?;
        Ok(x.len())
    }
    pub fn put(&mut self, x: u8) -> io::Result<()> {
        self.output.put(x);
        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        // make sure this is statefully called in the correct order.
        assert_eq!(self.keys_written, self.total_keys);
        assert_eq!(self.nodes_start, 0);
        self.nodes_start = self.output.tell();

        while self.skips.len() > 1 {
            let current_level: Vec<_> = self.skips.drain(..).collect();
            println!("self.skips; current_level.len={}", current_level.len());
            for ptrs in current_level.chunks(self.page_size as usize) {
                // build next, logarithmically smaller level of tree:
                let here = self.output.tell();
                self.skips.push(IdAndValueAddr::new(ptrs[0].id, here));

                // start node-block:
                self.output.put(NODE_BLOCK);
                self.write_v32(ptrs.len() as u32)?;
                // write the links in this level.
                for link in ptrs {
                    self.write_v32(link.id)?;
                    // TODO: delta-gap.
                    self.write_v64(link.addr)?;
                }
            }
        }

        assert!(self.skips.len() == 1);
        self.root_addr = self.skips[0].addr;

        // write file-footer.
        // align to 64-byte window.
        while self.output.tell() % 64 != 0 {
            self.output.put(0);
        }
        // u64
        self.output.write_all(&self.root_addr.to_le_bytes())?;
        // u64
        self.output.write_all(&self.nodes_start.to_le_bytes())?;
        // u32
        self.output.write_all(&self.total_keys.to_le_bytes())?;
        // u32
        self.output.write_all(&self.page_size.to_le_bytes())?;
        // u64-MAGIC
        self.output
            .write_all(&DENSE_KEY_WRITER_MAGIC.to_le_bytes())?;

        // make sure it gets out of RAM.
        self.output.flush()?;
        Ok(())
    }
}
impl Drop for U32KeyWriter {
    fn drop(&mut self) {
        if self.root_addr == 0 {
            self.finish()
                .expect("Error in finish for U32KeyWriter drop!")
        }
    }
}

#[derive(Default, Clone, Debug)]
struct IdAndValueAddr {
    /// This is a key.
    id: u32,
    /// This is it's associated value.
    addr: u64,
}

impl IdAndValueAddr {
    fn new(id: u32, addr: u64) -> Self {
        Self { id, addr }
    }
}

#[cfg(test)]
mod tests {}