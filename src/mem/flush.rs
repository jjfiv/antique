use std::{
    io::{self, Write},
    path::PathBuf,
};

use stream_vbyte::Scalar;

use super::{
    document::{FieldId, FieldMetadata, FieldType, TextOptions},
    encoders::{write_vbyte, write_vbyte_u64, Encoder, LZ4StringEncoder},
    index::{Indexer, PostingListBuilder},
    key_val_files::{CountingFileWriter, KeyValueWriter},
};

// Let's deal with indexing.

// 1. Field-Schemas
// 2. max-id

struct SegmentFieldInfo {
    id: FieldId,
    name: String,
    metadata: FieldMetadata,
    vocab_size: u64,
}

impl SegmentFieldInfo {
    fn new(id: FieldId, name: String, metadata: FieldMetadata, vocab_size: u64) -> Self {
        Self {
            id,
            name,
            metadata,
            vocab_size,
        }
    }
}

struct SegmentMetadata {
    maximum_document: u32,
    fields: Vec<SegmentFieldInfo>,
}

impl SegmentMetadata {
    fn from(indexer: &Indexer) -> Self {
        let mut fields = vec![];

        for (name, id) in indexer.fields.iter() {
            let meta = indexer.schema.get(id).unwrap().clone();
            let vocab_size = indexer.vocab.get(id).unwrap().len() as u64;
            fields.push(SegmentFieldInfo::new(*id, name.clone(), meta, vocab_size));
        }

        Self {
            maximum_document: indexer.next_id,
            fields,
        }
    }
}

pub fn flush_segment(segment: u32, dir: &PathBuf, indexer: &mut Indexer) -> io::Result<()> {
    flush_direct_indexes(segment, dir, indexer)?;
    indexer.stored_fields.clear();
    flush_postings(segment, dir, indexer)?;
    indexer.postings.clear();
    Ok(())
}

struct DocCountSkipKeyInfo {
    has_counts: bool,
    skips_addr: u64,
}

fn delta_gap(input: &[u32], output: &mut Vec<u32>) {
    output.clear();
    output.reserve(input.len());
    let mut prev = input[0];
    for it in input {
        output.push(it - prev);
        prev = *it;
    }
}
struct SkipInfo {
    id: u32,
    doc_addr: u64,
    pos_addr: u64,
}
impl SkipInfo {
    fn create(
        id: u32,
        docs_writer: &mut CountingFileWriter,
        pos_writer: &Option<&mut CountingFileWriter>,
    ) -> Self {
        let pos_addr = if let Some(pw) = pos_writer {
            pw.tell()
        } else {
            0
        };
        let doc_addr = docs_writer.tell();
        SkipInfo {
            id,
            doc_addr,
            pos_addr,
        }
    }
}

struct PostingsWriter {
    terms_writer: CountingFileWriter,
    docs_writer: CountingFileWriter,
    pos_writer: Option<CountingFileWriter>,
}

/// Returns skip-addr from within docs.
fn write_docs_counts_skips(
    postings: &PostingListBuilder,
    docs_writer: &mut CountingFileWriter,
    mut pos_writer: Option<&mut CountingFileWriter>,
) -> io::Result<u64> {
    let doc_frequency = postings.docs.len();
    let has_counts = postings.counts.len() > 0;
    let has_positions = postings.positions.len() > 0;
    debug_assert_eq!(has_positions, pos_writer.is_some());

    // buffers for encoding 128-chunks of ints:
    let mut buffer = Vec::with_capacity(INDEX_CHUNK_SIZE);
    let mut encoded_docs = [0u8; INDEX_CHUNK_SIZE * 5];
    let mut encoded_counts = [0u8; INDEX_CHUNK_SIZE * 5];

    let mut skips = Vec::new();

    // write blocked (docs, counts?)*
    for (i, docs) in postings.docs.buffers.iter().enumerate() {
        if docs[0] > 0 {
            // hold onto the start of each block in RAM, except the first; we know where that is.
            skips.push(SkipInfo::create(docs[0], docs_writer, &pos_writer));
        }
        // delta-gap blocks of documents:
        delta_gap(&docs, &mut buffer);

        // encode docs:
        let byte_len = stream_vbyte::encode::<Scalar>(&buffer, &mut encoded_docs);

        // encoded-block-size:
        write_vbyte(byte_len as u32, docs_writer)?;
        // encoded-block:
        docs_writer.write_all(&encoded_docs[..byte_len])?;

        if has_counts {
            let counts = postings.counts.buffers[i].as_slice();
            debug_assert_eq!(counts.len(), docs.len());
            let byte_len = stream_vbyte::encode::<Scalar>(counts, &mut encoded_counts);
            // encoded-block-size:
            write_vbyte(byte_len as u32, docs_writer)?;
            // encoded-block:
            docs_writer.write_all(&encoded_counts[..byte_len])?;
        }
        if has_positions {
            let pos_writer = pos_writer.as_mut().unwrap();
            let start = i * INDEX_CHUNK_SIZE;
            let end = start + INDEX_CHUNK_SIZE;
            let end = if end > doc_frequency {
                doc_frequency
            } else {
                end
            };
            for buf in &postings.positions[start..end] {
                pos_writer.write_all(buf)?;
            }
        }
    }
    // now prepare to write skips:
    let skips_addr = docs_writer.tell();
    let num_skips = skips.len() as u32;

    // TODO: compression opportunity here: delta-gap each array.
    write_vbyte(num_skips, docs_writer)?;
    for skip in skips {
        write_vbyte(skip.id, docs_writer)?;
        write_vbyte_u64(skip.doc_addr, docs_writer)?;
        if has_positions {
            write_vbyte_u64(skip.pos_addr, docs_writer)?;
        }
    }

    Ok(skips_addr)
}

pub(crate) const INDEX_CHUNK_SIZE: usize = 128;

pub fn flush_postings(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    for (field, contents) in &indexer.postings {
        let schema = indexer.schema.get(&field).unwrap().clone();
        let file_name = format!("{}.{}.post", segment, field.0);
        match schema.kind {
            FieldType::Categorical => todo! {},
            FieldType::Textual(_opts, _tok) => {
                let mut writer = KeyValueWriter::create(dir, &file_name)?;
                for (term_id, val) in contents {
                    writer.begin_pair(term_id.0)?;
                    let w = writer.value_writer();
                    let skip_addr = write_docs_counts_skips(val, w, None)?;
                    println!("skip_addr! {}", skip_addr);
                    writer.finish_pair()?;
                    todo!("write posting lists");
                }
                writer.finish_file()?;
            }
            FieldType::Boolean | FieldType::DenseInt | FieldType::DenseFloat => {
                panic!("Dense fields should not have postings entries...")
            }
            FieldType::SparseInt | FieldType::SparseFloat => todo! {},
        }
    }
    Ok(())
}

pub fn flush_direct_indexes(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    for (field, contents) in &indexer.stored_fields {
        let schema = indexer.schema.get(&field).unwrap().clone();
        let file_name = format!("{}.{}.fwd", segment, field.0);
        match schema.kind {
            // Only textual fields should be separated, CLOB/BLOB style...
            // Should really be a value-size branch...? Different writer for that.
            FieldType::Textual(_, _) => {
                let mut encoder = LZ4StringEncoder::default();
                let mut writer = KeyValueWriter::create(dir, &file_name)?;
                for (doc_id, val) in contents {
                    let data = val.as_str().unwrap();
                    writer.begin_pair(doc_id.0)?;
                    encoder.write(&data, writer.value_writer())?;
                    writer.finish_pair()?;
                }
                writer.finish_file()?;
            }
            // Small fields belong intermixed in the keys format.
            FieldType::Boolean
            | FieldType::DenseInt
            | FieldType::DenseFloat
            | FieldType::SparseInt
            | FieldType::SparseFloat
            | FieldType::Categorical => todo! {},
        }
    }
    Ok(())
}
