use io::Write;
use std::io;

pub(crate) trait Encoder<V, W>
where
    W: io::Write,
{
    fn write(&mut self, item: &V, out: &mut W) -> io::Result<()>;
}

pub(crate) fn write_vbyte<W>(i: u32, out: &mut W) -> io::Result<usize>
where
    W: io::Write,
{
    // TODO: stack-vec
    let mut buf = Vec::with_capacity(5);

    if i < 1 << 7 {
        buf.push((i | 0x80) as u8);
    } else if i < 1 << 14 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) | 0x80) as u8);
    } else if i < 1 << 21 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) | 0x80) as u8);
    } else if i < 1 << 28 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) | 0x80) as u8);
    } else {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x80) as u8);
    }

    out.write_all(&buf)?;
    Ok(buf.len())
}

pub(crate) fn write_vbyte_u64<W>(i: u64, out: &mut W) -> io::Result<usize>
where
    W: io::Write,
{
    // TODO: stack-vec
    let mut buf = Vec::with_capacity(9);

    if i < 1 << 7 {
        buf.push((i | 0x80) as u8);
    } else if i < 1 << 14 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) | 0x80) as u8);
    } else if i < 1 << 21 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) | 0x80) as u8);
    } else if i < 1 << 28 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) | 0x80) as u8);
    } else if i < 1 << 35 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x80) as u8);
    } else if i < 1 << 42 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x7f) as u8);
        buf.push(((i >> 35) | 0x80) as u8);
    } else if i < 1 << 49 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x7f) as u8);
        buf.push(((i >> 35) | 0x7f) as u8);
        buf.push(((i >> 42) | 0x80) as u8);
    } else if i < 1 << 56 {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x7f) as u8);
        buf.push(((i >> 35) | 0x7f) as u8);
        buf.push(((i >> 42) | 0x7f) as u8);
        buf.push(((i >> 49) | 0x80) as u8);
    } else {
        buf.push((i & 0x7f) as u8);
        buf.push(((i >> 7) & 0x7f) as u8);
        buf.push(((i >> 14) & 0x7f) as u8);
        buf.push(((i >> 21) & 0x7f) as u8);
        buf.push(((i >> 28) | 0x7f) as u8);
        buf.push(((i >> 35) | 0x7f) as u8);
        buf.push(((i >> 42) | 0x7f) as u8);
        buf.push(((i >> 49) | 0x7f) as u8);
        buf.push(((i >> 56) | 0x80) as u8);
    }

    out.write_all(&buf)?;

    Ok(buf.len())
}
pub(crate) struct GalagoU32VByte;
impl<W> Encoder<u32, W> for GalagoU32VByte
where
    W: io::Write,
{
    fn write(&mut self, item: &u32, out: &mut W) -> io::Result<()> {
        let _ = write_vbyte(*item, out)?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct LZ4StringEncoder {
    buffer: Vec<u8>,
}
impl<S, W> Encoder<S, W> for LZ4StringEncoder
where
    S: AsRef<str>,
    W: io::Write,
{
    fn write(&mut self, item: &S, out: &mut W) -> io::Result<()> {
        // TODO: check!
        let item: &str = item.as_ref();
        // clear internal buffer; write compressed temporarily there:
        self.buffer.clear();
        lz4_flex::compress_into(item.as_bytes(), &mut self.buffer);

        // vbyte length; blob.
        let length = self.buffer.len() as u32;
        write_vbyte(length, out)?;

        let _ = out.write(&self.buffer)?;
        Ok(())
    }
}

pub(crate) struct UTF8Encoder;
impl<S, W> Encoder<S, W> for UTF8Encoder
where
    S: AsRef<str>,
    W: io::Write,
{
    fn write(&mut self, item: &S, out: &mut W) -> io::Result<()> {
        let item: &str = item.as_ref();
        let length = item.len() as u32;
        write_vbyte(length, out)?;
        let _ = out.write(item.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use crate::{
        io_helper::{self, DataInputStream, InputStream},
        mem::key_val_files::CountingFileWriter,
    };
    use io_helper::ArcInputStream;
    use tempfile::TempDir;

    use super::{Encoder, GalagoU32VByte, UTF8Encoder};

    #[test]
    fn write_nums() -> Result<(), crate::Error> {
        let tmp_dir = TempDir::new()?;
        let path = tmp_dir.path().join("write_nums.tmp");
        let mut offsets = Vec::new();
        {
            let mut file = CountingFileWriter::create(&path)?;
            let mut writer = GalagoU32VByte;

            for i in 0..10000 {
                writer.write(&i, &mut file)?;
                offsets.push(file.tell());
            }
        }

        let mmap = io_helper::open_mmap_file(&path)?;
        let mut stream = ArcInputStream::from_mmap(mmap);

        for i in 0..10000 {
            assert_eq!(i, stream.read_vbyte()?);
            assert_eq!(stream.tell() as u64, offsets[i as usize]);
        }
        assert!(stream.eof());

        Ok(())
    }

    #[test]
    fn write_strs() -> Result<(), crate::Error> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("write_strs.tmp");
        let mut offsets = Vec::new();
        {
            let mut file = CountingFileWriter::create(&path)?;
            let mut writer = UTF8Encoder;

            for i in 0..10000 {
                writer.write(&format!("{:08x}", i), &mut file)?;
                offsets.push(file.tell());
            }
        }

        let mmap = io_helper::open_mmap_file(&path)?;
        let mut stream = ArcInputStream::from_mmap(mmap);

        for i in 0..10000 {
            let expected = format!("{:08x}", i);
            assert_eq!(expected.len() as u64, stream.read_vbyte()?);
            assert_eq!(
                expected,
                String::from_utf8(stream.advance(8)?.iter().cloned().collect()).unwrap()
            );
            assert_eq!(stream.tell() as u64, offsets[i as usize]);
        }
        assert!(stream.eof());

        Ok(())
    }
}
