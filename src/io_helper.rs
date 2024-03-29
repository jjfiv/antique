use crate::Error;
use io::Seek;
use memmap::{Mmap, MmapOptions};
use std::path::Path;
use std::sync::Arc;
use std::{cmp::Ordering, str};
use std::{convert::TryInto, fs::File};
use std::{fmt, io};
use std::{fs, io::SeekFrom};

pub fn open_mmap_file(path: &Path) -> Result<Arc<Mmap>, Error> {
    let file = fs::File::open(path)?;
    let opts = MmapOptions::new();
    let mmap: Mmap = unsafe { opts.map(&file)? };
    Ok(Arc::new(mmap))
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
    pub fn as_bytes(&self) -> &[u8] {
        &self.source[self.start..self.end]
    }
    pub fn to_str(&self) -> Result<&str, Error> {
        Ok(std::str::from_utf8(self.as_bytes())?)
    }
    pub fn as_le_u64(&self) -> Result<u64, Error> {
        if self.len() == 8 {
            Ok(u64::from_le_bytes(self.as_bytes().try_into().unwrap()))
        } else {
            Err(Error::InternalSizeErr)
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct Bytes {
    pub data: Box<[u8]>,
}
impl Bytes {
    pub fn len(&self) -> usize {
        return self.data.len();
    }
    /// I think this is the only way to get a boxed slice...
    /// Someday, bumpalo these?
    pub fn from_slice(input: &[u8]) -> Self {
        let mut tmp = Vec::new();
        tmp.reserve_exact(input.len());
        tmp.extend_from_slice(input);
        Self {
            data: tmp.into_boxed_slice(),
        }
    }

    /// Ditch the Box for reading.
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    /// Compare to another byte slice somewhere else.
    pub fn cmp(&self, rhs: &[u8]) -> Ordering {
        self.data.as_ref().cmp(rhs)
    }

    pub fn stream(&self) -> SliceInputStream {
        SliceInputStream::new(self.as_bytes())
    }
}
impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(readable) = str::from_utf8(&self.data) {
            write!(f, "{}", readable)
        } else {
            write!(f, "{:?}", &self.data)
        }
    }
}
pub trait InputStream {
    fn tell(&self) -> usize;
    fn eof(&self) -> bool;
    fn advance(&mut self, n: usize) -> Result<&[u8], Error>;
    fn get(&mut self) -> Result<u8, Error>;
}

pub trait DataInputStream {
    fn read_vbyte(&mut self) -> Result<u64, Error>;
    fn read_signed_vbyte(&mut self) -> Result<i64, Error>;
    fn read_lemur_vbyte(&mut self) -> Result<u64, Error>;
    fn read_u64(&mut self) -> Result<u64, Error>;
    fn read_u32(&mut self) -> Result<u32, Error>;
    fn read_u16(&mut self) -> Result<u16, Error>;
}

impl<I> DataInputStream for I
where
    I: InputStream,
{
    /// Indri & Galago's vbyte: highest-bit set means stop.
    fn read_vbyte(&mut self) -> Result<u64, Error> {
        let mut result: u64 = 0;
        let mut bit_p: u8 = 0;
        while !self.eof() {
            // read_byte:
            let byte = self.get()? as u64;

            // if highest bit set we're done!
            if byte & 0x80 > 0 {
                result |= (byte & 0x7f) << bit_p;
                return Ok(result);
            }
            result |= byte << bit_p;
            bit_p += 7;
        }
        Err(Error::InternalSizeErr)
    }
    /// Lemur's Keyfile vbyte: highest-bit set means continue.
    fn read_lemur_vbyte(&mut self) -> Result<u64, Error> {
        let mut result: u64 = 0;
        while !self.eof() {
            // read_byte:
            let byte = self.get()? as u64;

            result |= byte & 0x7f;
            // if highest bit NOT set we're done!
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            result <<= 7;
        }
        Err(Error::InternalSizeErr)
    }
    /// Indri-Style signed vbytes (lowest bit as sign).
    fn read_signed_vbyte(&mut self) -> Result<i64, Error> {
        let raw = self.read_vbyte()?;
        // RVLCompress::unfoldNegatives
        let keep_bits = (raw / 2) as i64;
        if raw & 1 > 0 {
            Ok(-keep_bits)
        } else {
            Ok(keep_bits)
        }
    }
    fn read_u64(&mut self) -> Result<u64, Error> {
        let exact = self.advance(8)?;
        Ok(u64::from_be_bytes(exact.try_into().unwrap()))
    }
    fn read_u32(&mut self) -> Result<u32, Error> {
        let exact = self.advance(4)?;
        Ok(u32::from_be_bytes(exact.try_into().unwrap()))
    }
    fn read_u16(&mut self) -> Result<u16, Error> {
        let exact = self.advance(2)?;
        Ok(u16::from_be_bytes(exact.try_into().unwrap()))
    }
}

// Zero-Copy InputStream
#[derive(Clone)]
pub struct SliceInputStream<'src> {
    data: &'src [u8],
    /// This supports rewinding and "telling" how far we've read.
    position: usize,
}

impl fmt::Debug for SliceInputStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SliceInputStream[@{}..{}]",
            self.position,
            self.data.len()
        )
    }
}

impl<'src> InputStream for SliceInputStream<'src> {
    fn tell(&self) -> usize {
        self.position
    }
    fn eof(&self) -> bool {
        self.position >= self.data.len()
    }
    fn advance(&mut self, n: usize) -> Result<&[u8], Error> {
        self.consume(n)
    }
    fn get(&mut self) -> Result<u8, Error> {
        if self.position >= self.data.len() {
            Err(Error::InternalSizeErr)
        } else {
            let result = Ok(self.data[self.position]);
            self.position += 1;
            result
        }
    }
}

impl<'src> SliceInputStream<'src> {
    pub fn new(data: &'src [u8]) -> Self {
        Self { data, position: 0 }
    }
    pub fn peek(&self) -> Option<u8> {
        if self.position < self.data.len() {
            Some(self.data[self.position])
        } else {
            None
        }
    }
    pub fn seek(&mut self, position: usize) -> Result<(), Error> {
        self.position = position;
        if self.position < self.data.len() {
            Ok(())
        } else {
            Err(Error::InternalSizeErr)
        }
    }
    #[inline]
    pub fn consume(&mut self, n: usize) -> Result<&'src [u8], Error> {
        let end = self.position + n;
        if end > self.data.len() {
            return Err(Error::InternalSizeErr);
        }
        let found = &self.data[self.position..end];
        self.position = end;
        Ok(found)
    }
    pub fn read_bytes(&mut self, n: usize) -> Result<&'src [u8], Error> {
        Ok(self.consume(n)?)
    }
}

#[derive(Debug, Clone)]
pub struct ArcInputStream {
    source: Arc<Mmap>,
    start: usize,
    end: usize,
    offset: usize,
}

impl ArcInputStream {
    pub fn from_mmap(source: Arc<Mmap>) -> Self {
        let end = source.len();
        Self {
            source,
            start: 0,
            end,
            offset: 0,
        }
    }
    pub fn new(source: Arc<Mmap>, start: usize, end: usize) -> Self {
        Self {
            source,
            start,
            end,
            offset: 0,
        }
    }
}

impl InputStream for ArcInputStream {
    fn tell(&self) -> usize {
        self.offset
    }
    fn eof(&self) -> bool {
        self.offset + self.start >= self.end
    }
    fn advance(&mut self, n: usize) -> Result<&[u8], Error> {
        let lhs = self.start + self.offset;
        let rhs = lhs + n;
        self.offset += n;
        if rhs > self.end {
            return Err(Error::InternalSizeErr);
        }
        Ok(&self.source[lhs..rhs])
    }
    fn get(&mut self) -> Result<u8, Error> {
        if self.eof() {
            Err(Error::InternalSizeErr)
        } else {
            let b = self.source[self.start + self.offset];
            self.offset += 1;
            Ok(b)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Galago's VByte compression (trevor, jfoley)
    fn compress_u32(i: u32, out: &mut Vec<u8>) {
        if i < 1 << 7 {
            out.push((i | 0x80) as u8);
        } else if i < 1 << 14 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) | 0x80) as u8);
        } else if i < 1 << 21 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) | 0x80) as u8);
        } else if i < 1 << 28 {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) & 0x7f) as u8);
            out.push(((i >> 21) | 0x80) as u8);
        } else {
            out.push((i & 0x7f) as u8);
            out.push(((i >> 7) & 0x7f) as u8);
            out.push(((i >> 14) & 0x7f) as u8);
            out.push(((i >> 21) & 0x7f) as u8);
            out.push(((i >> 28) | 0x80) as u8);
        }
    }
    #[test]
    fn test_vbytes() {
        let expected = &[
            0, 0xf, 0xef, 0xeef, 0xbeef, 0xdbeef, 0xadbeef, 0xeadbeef, 0xdeadbeef,
        ];
        let mut buf = Vec::new();
        for x in expected {
            compress_u32(*x, &mut buf)
        }

        let mut rdr = SliceInputStream::new(&buf[0..]);
        for x in expected {
            let x = *x as u64;
            assert_eq!(x, rdr.read_vbyte().unwrap());
        }
        assert!(rdr.eof());
    }

    #[test]
    fn test_read_u32() {
        let expected = &[0x11, 0x22, 0x33, 0x44];
        let mut rdr = SliceInputStream::new(&expected[0..]);
        assert_eq!(0x11223344, rdr.read_u32().unwrap());
        assert!(rdr.eof());
    }
}
