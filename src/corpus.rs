use crate::galago_tokenizer::{Document, State as Tokenizer};
use crate::io_helper::*;
use crate::Error;
use crate::HashSet;
use snap::raw::Decoder;
use std::convert::TryInto;

/// Java's Snappy Header; I'm just putting the versions in here.
/// Non-standard snappy header of some kind.
/// https://github.com/xerial/snappy-java/blob/master/src/main/java/org/xerial/snappy/SnappyCodec.java
const SNAPPY_HEADER: &[u8] = &[
    // SNAPPY
    130, b'S', b'N', b'A', b'P', b'P', b'Y', 0, //
    // Version info:
    0, 0, 0, 1, 0, 0, 0, 1,
];

pub fn decompress_document(value: ValueEntry) -> Result<Document, Error> {
    let compressed = &value.source[value.start..value.end];
    if !compressed.starts_with(SNAPPY_HEADER) {
        return Err(Error::CompressionError.with_context("Missing Xerial Snappy Header"));
    }
    let uw = u32::from_be_bytes(
        compressed[SNAPPY_HEADER.len()..SNAPPY_HEADER.len() + 4]
            .try_into()
            .unwrap(),
    );

    let mut snappy = Decoder::new();
    let decompressed = snappy
        .decompress_vec(&compressed[SNAPPY_HEADER.len() + 4..])
        .map_err(|e| {
            Error::CompressionError.with_context(format!("{:?} {}", e, compressed.len()))
        })?;

    debug_assert_eq!(uw as usize, compressed.len() - SNAPPY_HEADER.len() - 4);

    let mut reader = SliceInputStream::new(&decompressed);

    let _metadata_size = reader.read_u32()? as usize;
    let text_size = reader.read_u32()? as usize;

    let _identifier = reader.read_u64()?;
    let _name = read_string(&mut reader)?;

    let metadata_count = reader.read_u32()?;
    let mut metadata: Vec<(&str, &str)> = Vec::new();
    for _ in 0..metadata_count {
        let key = read_string(&mut reader)?;
        let val = read_string(&mut reader)?;
        metadata.push((key, val));
    }

    let text = read_string(&mut reader)?;
    // text should be skippable: so it's size + byte-length should be encoded correctly.
    debug_assert_eq!(text_size, text.as_bytes().len() + 4);

    let mut tok = Tokenizer::new(text);
    tok.parse();
    let tags: HashSet<String> = HashSet::default();
    Ok(tok.into_document(tags))
}

fn read_string<'src>(target: &mut SliceInputStream<'src>) -> Result<&'src str, Error> {
    let length = target.read_u32()? as usize;
    let buf = target.consume(length)?;
    Ok(std::str::from_utf8(&buf)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress() {
        let raw_data: &[u8] = &[
            // header
            130, 83, 78, 65, 80, 80, 89, 0, //
            // version
            0, 0, 0, 1, //
            // compat-version
            0, 0, 0, 1, //
            // compressed-length (minus header!)
            0, 0, 0, 198, //
            // actual raw-snappy bytes!
            200, 1, 32, 0, 0, 0, 17, 0, 0, 0, 120, 0, 9, 1, 240, 60, 3, 0, 0, 0, 43, 47, 104, 111,
            109, 101, 47, 106, 102, 111, 108, 101, 121, 47, 97, 110, 116, 105, 113, 117, 101, 47,
            100, 97, 116, 97, 47, 105, 110, 112, 117, 116, 115, 47, 82, 69, 65, 68, 77, 69, 46,
            116, 120, 116, 0, 0, 0, 1, 0, 0, 0, 5, 116, 105, 116, 108, 101, 9, 67, 8, 0, 116, 84,
            1, 13, 240, 110, 58, 32, 80, 114, 105, 100, 101, 32, 97, 110, 100, 32, 80, 114, 101,
            106, 117, 100, 105, 99, 101, 10, 65, 117, 116, 104, 111, 114, 58, 32, 74, 97, 110, 101,
            32, 65, 117, 115, 116, 101, 110, 10, 67, 104, 97, 112, 116, 101, 114, 115, 58, 32, 49,
            45, 53, 10, 83, 111, 117, 114, 99, 101, 58, 32, 104, 116, 116, 112, 58, 47, 47, 119,
            119, 119, 46, 103, 117, 116, 101, 110, 98, 101, 114, 103, 46, 111, 114, 103, 47, 102,
            105, 108, 101, 115, 47, 49, 51, 52, 50, 47, 49, 51, 52, 50, 45, 48, 46, 116, 120, 116,
            10,
        ];
        assert!(raw_data.starts_with(SNAPPY_HEADER));

        let mut snappy = Decoder::new();
        for x in 0..15 {
            if let Ok(decompressed) = snappy.decompress_vec(&raw_data[SNAPPY_HEADER.len() + x..]) {
                println!("Safe-offset: {}", x);
                println!("Decompressed-length: {}", decompressed.len());
            }
        }
    }
}
