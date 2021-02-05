use std::{
    fs::File,
    io::{self, Seek, SeekFrom},
    marker::PhantomData,
};

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

trait Encoder<V, W>
where
    W: io::Write,
{
    fn write(&mut self, item: &V, out: &mut W) -> io::Result<()>;
}

fn write_vbyte<W>(i: u32, out: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    // TODO: stack-vec
    let mut buf = Vec::with_capacity(8);

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

    out.write(&buf);

    Ok(())
}
struct GalagoU32VByte;
impl<W> Encoder<u32, W> for GalagoU32VByte
where
    W: io::Write,
{
    fn write(&mut self, item: &u32, out: &mut W) -> io::Result<()> {
        write_vbyte(*item, out)
    }
}

#[derive(Default)]
struct LZ4StringEncoder {
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

struct UTF8Encoder;
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

struct SeqVarLenValuesWriter<V, E>
where
    E: Encoder<V, File>,
{
    output: File,
    encoder: E,
    _phantom: PhantomData<V>,
}

impl<V, E> SeqVarLenValuesWriter<V, E>
where
    E: Encoder<V, File>,
{
    fn new(output: File, encoder: E) -> Self {
        Self {
            output,
            encoder,
            _phantom: PhantomData::default(),
        }
    }

    fn write(&mut self, value: &V) -> io::Result<()> {
        self.encoder.write(value, &mut self.output)
    }

    fn tell(&mut self) -> io::Result<u64> {
        self.output.seek(SeekFrom::Current(0))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::io_helper::{self, DataInputStream, InputStream};
    use io_helper::ArcInputStream;
    use tempfile::TempDir;

    use super::{GalagoU32VByte, SeqVarLenValuesWriter, UTF8Encoder};

    #[test]
    fn write_nums() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("write_nums.tmp");
        let mut offsets = Vec::new();
        {
            let file = File::create(&path).unwrap();
            let mut writer = SeqVarLenValuesWriter::new(file, GalagoU32VByte);

            for i in 0..10000 {
                writer.write(&i).unwrap();
                offsets.push(writer.tell().unwrap());
            }
        }

        let mmap = io_helper::open_mmap_file(&path).unwrap();
        let mut stream = ArcInputStream::from_mmap(mmap);

        for i in 0..10000 {
            assert_eq!(i, stream.read_vbyte().unwrap());
            assert_eq!(stream.tell() as u64, offsets[i as usize]);
        }
        assert!(stream.eof());
    }

    #[test]
    fn write_strs() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("write_strs.tmp");
        let mut offsets = Vec::new();
        {
            let file = File::create(&path).unwrap();
            let mut writer = SeqVarLenValuesWriter::new(file, UTF8Encoder);

            for i in 0..10000 {
                writer.write(&format!("{:08x}", i)).unwrap();
                offsets.push(writer.tell().unwrap());
            }
        }

        let mmap = io_helper::open_mmap_file(&path).unwrap();
        let mut stream = ArcInputStream::from_mmap(mmap);

        for i in 0..10000 {
            let expected = format!("{:08x}", i);
            assert_eq!(expected.len() as u64, stream.read_vbyte().unwrap());
            assert_eq!(
                expected,
                String::from_utf8(stream.advance(8).unwrap().iter().cloned().collect()).unwrap()
            );
            assert_eq!(stream.tell() as u64, offsets[i as usize]);
        }
        assert!(stream.eof());
    }
}
