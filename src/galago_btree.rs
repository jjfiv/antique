use crate::Error;
use crate::HashMap;
use memmap::{Mmap, MmapOptions};
use serde_json::{Map as Dict, Value};
use std::fs::File;
use std::{
    convert::TryInto,
    io::prelude::*,
    path::{Path, PathBuf},
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
pub struct Footer {
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
    #[serde(alias = "filename")]
    file_name: String,
    reader_class: String,
    writer_class: Option<String>,
    merger_class: Option<String>,
    key_count: u64,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

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

    // Last u64 in file should be:
    let maybe_magic = u64::from_be_bytes(
        (&mmap[file_length - 8..file_length])
            .try_into()
            .map_err(|_| Error::SliceErr)?,
    );
    Ok(maybe_magic == MAGIC_NUMBER)
}

fn read_u64(input: &[u8]) -> Result<(u64, &[u8]), Error> {
    let (long_bytes, rest) = input.split_at(8);
    let long = u64::from_be_bytes(long_bytes.try_into().map_err(|_| Error::SliceErr)?);
    Ok((long, rest))
}

fn read_u32(input: &[u8]) -> Result<(u32, &[u8]), Error> {
    let (long_bytes, rest) = input.split_at(4);
    let long = u32::from_be_bytes(long_bytes.try_into().map_err(|_| Error::SliceErr)?);
    Ok((long, rest))
}

/// Read footer:
pub fn read_footer(path: &Path) -> Result<Footer, Error> {
    let location = TreeLocation::new(path)?;

    // Use Memory-Mapped I/O:
    let file = File::open(location.keys_path())?;
    let opts = MmapOptions::new();
    let mmap: Mmap = unsafe { opts.map(&file)? };
    let file_length = mmap.len();

    let footer_start = file_length - FOOTER_SIZE;
    let rest = &mmap[footer_start..];

    let (vocabulary_offset, rest) = read_u64(rest)?;
    let (manifest_offset, rest) = read_u64(rest)?;
    let (block_size, rest) = read_u32(rest)?;
    let (magic_number, rest) = read_u64(rest)?;
    debug_assert_eq!(0, rest.len());

    if magic_number != MAGIC_NUMBER {
        return Err(Error::BadGalagoMagic(magic_number));
    }

    let manifest = serde_json::from_slice(&mmap[(manifest_offset as usize)..footer_start])
        .map_err(|details| Error::BadManifest(details))?;

    Ok(Footer {
        vocabulary_offset,
        manifest_offset,
        block_size,
        magic_number,
        manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_size() {
        assert_eq!(FOOTER_SIZE, std::mem::size_of::<Footer>());
    }
}
