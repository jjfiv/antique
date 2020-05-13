use crate::Error;
use std::convert::TryInto;
use std::fmt;
use std::{cmp::Ordering, str};

#[derive(Clone)]
pub struct Bytes {
    pub data: Box<[u8]>,
}
impl Bytes {
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

#[derive(Clone)]
pub struct SliceInputStream<'src> {
    data: &'src [u8],
    /// This supports rewinding and "telling" how far we've read.
    position: usize,
}

impl fmt::Debug for SliceInputStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SliceInputStream[@{}..{}]", self.position, self.data.len())
    }
}

impl<'src> SliceInputStream<'src> {
    pub fn new(data: &'src [u8]) -> Self {
        Self { data, position: 0 }
    }
    pub fn tell(&self) -> usize {
        self.position
    }
    pub fn eof(&self) -> bool {
        self.position >= self.data.len()
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
    pub fn read_vbyte(&mut self) -> Result<u64, Error> {
        let mut result: u64 = 0;
        let mut bit_p: u8 = 0;
        while self.position < self.data.len() {
            // read_byte:
            let byte = self.data[self.position] as u64;
            self.position += 1;

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
    pub fn read_bytes(&mut self, n: usize) -> Result<&'src [u8], Error> {
        Ok(self.consume(n)?)
    }
    pub fn read_u64(&mut self) -> Result<u64, Error> {
        let exact = self.consume(8)?;
        Ok(u64::from_be_bytes(exact.try_into().unwrap()))
    }
    pub fn read_u32(&mut self) -> Result<u32, Error> {
        let exact = self.consume(4)?;
        Ok(u32::from_be_bytes(exact.try_into().unwrap()))
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
