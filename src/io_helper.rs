use crate::Error;
use std::convert::TryInto;
use std::fmt;
use std::str;

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Bytes<'src> {
    data: &'src [u8],
}
impl fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(readable) = str::from_utf8(self.data) {
            write!(f, "{}", readable)
        } else {
            write!(f, "{:?}", &self.data)
        }
    }
}
pub struct SliceInputStream<'src> {
    data: &'src [u8],
    // TODO: keeping this separate in case we need to rewind...
    position: usize,
}

impl<'src> SliceInputStream<'src> {
    pub fn new(data: &'src [u8]) -> Self {
        Self { data, position: 0 }
    }
    pub fn eof(&self) -> bool {
        self.position >= self.data.len()
    }
    #[inline]
    fn consume(&mut self, n: usize) -> Result<&'src [u8], Error> {
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
    pub fn read_bytes(&mut self, n: usize) -> Result<Bytes<'src>, Error> {
        Ok(Bytes {
            data: self.consume(n)?,
        })
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