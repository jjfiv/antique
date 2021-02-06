use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::io_helper::Teller;

use super::encoders::{write_vbyte, write_vbyte_u64};

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

pub struct DenseKeyWriter {
    pub output: CountingFileWriter,
    skips: Vec<IdAndValueAddr>,
    total_keys: u32,
    keys_written: u32,
    nodes_start: u64,
    root_addr: u64,
    page_size: u32,
}

impl DenseKeyWriter {
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
    pub fn write_key(&mut self, key: u32) -> io::Result<()> {
        debug_assert_eq!(key, self.keys_written);
        if key % self.page_size == 0 {
            self.write_key_block(key, (self.total_keys - key).min(self.page_size))?;
        }
        self.keys_written += 1;
        Ok(())
    }
    /// Framing it this way ensures that writer has control of key-block sizes.
    fn write_key_block(&mut self, first_key: u32, num_keys: u32) -> io::Result<()> {
        self.skips
            .push(IdAndValueAddr::new(first_key, self.output.tell()));
        self.output.put(LEAF_BLOCK);
        self.write_v32(num_keys)?;
        Ok(())
    }
    pub fn write_v64(&mut self, x: u64) -> io::Result<usize> {
        write_vbyte_u64(x, &mut self.output)
    }
    pub fn write_v32(&mut self, x: u32) -> io::Result<usize> {
        write_vbyte(x, &mut self.output)
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
impl Drop for DenseKeyWriter {
    fn drop(&mut self) {
        if self.root_addr == 0 {
            self.finish()
                .expect("Error in finish for DenseKeyWriter drop!")
        }
    }
}
// Version up to 256:
const DENSE_KEY_WRITER_MAGIC: u64 = 0xf1e2_d3c4_b5a6_0000 | 0x0001;

struct PagePacker {
    page_size: usize,
    current_page: Vec<u8>,
    scratch: Vec<u8>,
}

impl Default for PagePacker {
    fn default() -> Self {
        Self::new(PAGE_4K)
    }
}

impl PagePacker {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            current_page: Vec::with_capacity(page_size),
            scratch: Vec::new(),
        }
    }
    fn pad_zeros(&mut self) {
        while self.current_page.len() < self.page_size {
            self.current_page.push(0);
        }
    }
    fn pad_alignment(&mut self, n: usize) {
        while self.current_page.len() % n != 0 {
            self.current_page.push(0);
        }
    }
    #[cfg(test)]
    pub(crate) fn take_page(&mut self) -> Vec<u8> {
        let mut new = Vec::with_capacity(self.page_size);
        let _scratch_len = self.scratch.len();
        let _page_len = self.current_page.len();
        std::mem::swap(&mut self.current_page, &mut new);
        std::mem::swap(&mut self.current_page, &mut self.scratch);
        debug_assert_eq!(0, self.scratch.len());
        debug_assert_eq!(_scratch_len, self.current_page.len());
        debug_assert_eq!(_page_len, new.len());
        new
    }
    fn available(&self) -> usize {
        self.page_size - self.current_page.len()
    }
    fn pending_fits(&self) -> bool {
        self.scratch.len() <= self.available()
    }
    fn accept(&mut self) {
        self.current_page.extend(&self.scratch);
        self.scratch.clear();
    }
}

impl io::Write for PagePacker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.scratch.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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

pub(crate) struct KeyValueWriter {
    keys_file: CountingFileWriter,
    vals_file: CountingFileWriter,
    key_packer: PagePacker,
    page_item_count: u32,
    block_starts: Vec<IdAndValueAddr>,
    last_pair: IdAndValueAddr,
}

// Two types of blocks in a keys-file:
const LEAF_BLOCK: u8 = 0xaf; // 11101111
const NODE_BLOCK: u8 = 0x10; // 00010000
/// Maximum of 512 keys-per-page; we don't want to overwhelm memory while reading, and log_{512} is great.
const MAX_PAGE_ITEM_COUNT: u32 = 512;

/// Magic + Version number
const KEY_MAGIC: &[u8] = b"jf..key\x01";
/// Magic + Version number
const VAL_MAGIC: &[u8] = b"jf..val\x01";

impl KeyValueWriter {
    pub(crate) fn create(dir: &PathBuf, base: &str) -> io::Result<Self> {
        let keys_path = dir.join(format!("{}.keys", base));
        let vals_path = dir.join(format!("{}.vals", base));
        Self::new(File::create(&keys_path)?, File::create(&vals_path)?)
    }
    fn new(keys_file: File, vals_file: File) -> io::Result<Self> {
        // Tag files with magic-numbers:
        let mut keys_file = CountingFileWriter::new(keys_file)?;
        let mut vals_file = CountingFileWriter::new(vals_file)?;
        keys_file.write_all(KEY_MAGIC)?;
        vals_file.write_all(VAL_MAGIC)?;

        // TODO: magic-numbers for keys_file and vals_file...
        let mut writer = Self {
            keys_file,
            page_item_count: 0,
            block_starts: Vec::new(),
            key_packer: PagePacker::default(),
            last_pair: IdAndValueAddr::default(),
            vals_file,
        };

        writer._start_leaf_block()?;
        Ok(writer)
    }

    /// Leaf blocks contain vbyte-delta-encoded arrays of (key, val_start, val_len).
    fn _start_leaf_block(&mut self) -> io::Result<()> {
        debug_assert_eq!(self.key_packer.current_page.len(), 0);

        // hold onto this block's leftmost id and it's start_addr.
        let current_block_addr = self._key_tell()?;
        self.block_starts
            .push(IdAndValueAddr::new(self.last_pair.id, current_block_addr));
        /*
        println!(
            " - Write leaf, left_id={} @{} target={}",
            self.last_pair.id, current_block_addr, self.last_pair.addr
        );
        */

        self.key_packer.current_page.push(LEAF_BLOCK);
        Ok(())
    }

    /// Node blocks contain vbyte-delta-encoded arrays of (key, block_addr), which may point at another node block or leaf block.
    fn _start_node_block(&mut self, left_id: u32) -> io::Result<()> {
        debug_assert_eq!(self.key_packer.current_page.len(), 0);

        // hold onto this block's leftmost id and it's start_addr.
        let current_block_addr = self._key_tell()?;
        self.block_starts
            .push(IdAndValueAddr::new(left_id, current_block_addr));

        self.key_packer.current_page.push(NODE_BLOCK);
        Ok(())
    }

    /// Begin a key-value pair with key=k.
    /// Call finish_pair when done writing the value!
    pub(crate) fn begin_pair(&mut self, k: u32) -> io::Result<()> {
        self.page_item_count += 1;
        if self.page_item_count > MAX_PAGE_ITEM_COUNT {
            // Start a new leaf block for this key.
            self._flush_current_block()?;
            self._start_leaf_block()?;
        }
        let val_addr = self._val_tell()?;

        // delta-gap address and key if the same-block.
        let val_addr_diff = val_addr - self.last_pair.addr;
        let key_diff = k - self.last_pair.id;

        // store absolute in case we need to roll.
        self.last_pair.id = k;
        self.last_pair.addr = val_addr;

        write_vbyte(key_diff, &mut self.key_packer)?;
        write_vbyte_u64(val_addr_diff, &mut self.key_packer)?;

        Ok(())
    }

    /// use this after 'begin_pair'.
    pub(crate) fn value_writer(&mut self) -> &mut CountingFileWriter {
        &mut self.vals_file
    }

    /// Use this after 'value_writer'
    pub(crate) fn finish_pair(&mut self) -> io::Result<()> {
        let current_addr = self._val_tell()?;
        let val_length = current_addr - self.last_pair.addr;
        write_vbyte_u64(val_length, &mut self.key_packer)?;

        if self.key_packer.pending_fits() {
            // continue in this key-block.
            self.key_packer.accept();
        } else {
            // Start a new leaf block for this key.
            self._flush_current_block()?;
            self._start_leaf_block()?;

            // Write (key, start, len):
            write_vbyte(self.last_pair.id, &mut self.key_packer)?; // 5 + 9 + 9
            write_vbyte_u64(self.last_pair.addr, &mut self.key_packer)?;
            write_vbyte_u64(val_length, &mut self.key_packer)?;

            // Put that key into the page; not pending.
            debug_assert!(self.key_packer.pending_fits());
            self.key_packer.accept();
        }

        Ok(())
    }

    fn _val_tell(&mut self) -> io::Result<u64> {
        Ok(self.vals_file.tell())
    }

    fn _key_tell(&mut self) -> io::Result<u64> {
        Ok(self.keys_file.tell())
    }

    fn _flush_current_block(&mut self) -> io::Result<()> {
        // write the number of keys in this block at the front:
        // TODO: fixed-u16? This is USUALLY and AT MOST two bytes with page_item limit 512.
        write_vbyte(self.page_item_count, &mut self.keys_file)?;
        // reset the number of keys for the next block.
        self.page_item_count = 0;

        // Round off current block's alignment.
        // TODO: save four bits of address.
        self.key_packer.pad_alignment(16);

        // Write the rest of the page.
        self.keys_file.write_all(&self.key_packer.current_page)?;

        // Throw away current page and scratch (can't be delta-gapped on new page!).
        self.key_packer.current_page.clear();
        self.key_packer.scratch.clear();
        Ok(())
    }

    pub(crate) fn finish_file(&mut self) -> io::Result<()> {
        // finish off the last LEAF_BLOCK.
        self._flush_current_block()?;
        let max_id = self.last_pair.id + 1;
        println!("Maximum-ID: {}", max_id);

        let mut _level = 0;
        // serialize all multi-block levels:
        while self.block_starts.len() > 1 {
            _level += 1;
            // grab the current level;
            let current_level: Vec<IdAndValueAddr> = self.block_starts.drain(..).collect();
            // generate at least one node from this level by starting our node block.
            self._start_node_block(current_level[0].id)?;

            // reset the 'last_pair' used for linking to the first from this level.
            // we won't start the loop without at least one to write about.
            let mut last_pair = current_level[0].clone();
            let _here = self._key_tell()?;
            /*
            println!(
                " - Write node level={}, block id={}.. addr={}, entries={}",
                _level,
                last_pair.id,
                _here,
                current_level.len()
            );
            */

            for block_ref in current_level {
                // try delta-gapped:
                write_vbyte(block_ref.id - last_pair.id, &mut self.key_packer)?;
                write_vbyte_u64(block_ref.addr - last_pair.addr, &mut self.key_packer)?;
                // update delta-gapping:
                last_pair = block_ref;

                // alloc a new page if needed.
                if !self.key_packer.pending_fits() || self.page_item_count > MAX_PAGE_ITEM_COUNT {
                    self._flush_current_block()?;
                    self._start_node_block(last_pair.id)?;
                    write_vbyte(last_pair.id, &mut self.key_packer)?;
                    write_vbyte_u64(last_pair.addr, &mut self.key_packer)?;
                    let _here = self._key_tell()?;
                    /*
                    println!(
                        " - Write node level={}, block id={}.. addr={}",
                        _level, last_pair.id, _here
                    );
                    */
                }

                // add entry to current page:
                self.key_packer.accept();
                self.page_item_count += 1;
            }

            self._flush_current_block()?;
        }
        // Finish every level with an encoding of max_id.
        write_vbyte(max_id, &mut self.keys_file)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::mem::encoders::{Encoder, UTF8Encoder};

    use super::*;

    #[test]
    fn page_packer_flow() {
        let mut pages: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut packer = PagePacker::new(4096);
        for i in 0..10000 {
            // write-vbyte to scratch;
            write_vbyte(i, &mut packer).unwrap();
            // if it doesn't fit, done with that page.
            if !packer.pending_fits() {
                packer.pad_zeros();
                pages.push((i, packer.take_page()));
            }
            packer.accept();
        }
        packer.pad_zeros();
        pages.push((10001, packer.take_page()));

        let mut offset = 0;
        for (idx, p) in &pages {
            println!("page=(start:{} addr:{} len:{})", idx, offset, p.len());
            offset += p.len();
        }
    }

    #[test]
    fn test_key_value_writing() {
        let tmp_dir = TempDir::new().unwrap();
        let mut writer =
            KeyValueWriter::create(&tmp_dir.path().to_path_buf(), "num_to_hex").unwrap();

        let mut enc = UTF8Encoder;

        for i in 0u32..10_000 {
            let key = i * 3;
            let value = format!("0x{:x}", key * 7);
            writer.begin_pair(key).expect("Key write failure.");
            enc.write(&value, writer.value_writer())
                .expect("Value write failure.");
            writer.finish_pair().expect("Key finish failure.");
        }
        writer.finish_file().expect("Tree writing failure.");
    }

    #[test]
    fn test_key_value_writing_huge() {
        let tmp_dir = TempDir::new().unwrap();
        let mut writer =
            KeyValueWriter::create(&tmp_dir.path().to_path_buf(), "num_to_hex").unwrap();
        let mut enc = UTF8Encoder;

        for i in 0u32..1_000_000 {
            let key = i;
            let value = format!("0x{:x}", key * 37);
            writer.begin_pair(key).expect("Key write failure.");
            enc.write(&value, writer.value_writer())
                .expect("Value write failure.");
            writer.finish_pair().expect("Key finish failure.");
        }
        writer.finish_file().expect("Tree writing failure.");
    }
}
