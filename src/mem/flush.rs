use std::{
    io::{self, Write},
    path::PathBuf,
};

use stream_vbyte::Scalar;

use super::{
    document::{FieldId, FieldMetadata, FieldType, TextOptions},
    encoders::{write_vbyte, write_vbyte_u64, Encoder, LZ4StringEncoder},
    index::Indexer,
    key_val_files::KeyValueWriter,
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

pub fn flush_postings(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    for (field, contents) in &indexer.postings {
        let schema = indexer.schema.get(&field).unwrap().clone();
        let file_name = format!("{}.{}.post", segment, field.0);
        match schema.kind {
            FieldType::Categorical => todo! {},
            FieldType::Textual(opts, _tok) => {
                let mut writer = KeyValueWriter::create(dir, &file_name)?;
                for (term_id, val) in contents {
                    writer.begin_pair(term_id.0)?;
                    let w = writer.value_writer();

                    write_vbyte_u64(val.docs.len() as u64, w)?;
                    write_vbyte_u64(val.total_term_frequency, w)?;
                    match opts {
                        TextOptions::Docs => {
                            write_vbyte(0b1, w)?;
                            let mut docs = Vec::with_capacity(128);
                            let mut encoded = vec![0u8; 128 * 5];

                            let mut skips = Vec::new();

                            let mut last_doc = 0;
                            let mut offset = 0;
                            for doc in val.docs.iter() {
                                docs.push(doc - last_doc);
                                last_doc = doc;
                                if docs.len() == 128 {
                                    skips.push((docs[0], offset));
                                    let byte_len =
                                        stream_vbyte::encode::<Scalar>(&docs, &mut encoded);
                                    offset += byte_len;
                                    write_vbyte(byte_len as u32, w)?;
                                    w.write_all(&encoded[..byte_len])?;
                                }
                            }

                            skips.push((docs[0], offset));
                            let byte_len = stream_vbyte::encode::<Scalar>(&docs, &mut encoded);
                            offset += byte_len;
                            write_vbyte(byte_len as u32, w)?;
                            w.write_all(&encoded[..byte_len])?;
                        }
                        TextOptions::Counts => write_vbyte(0b11, w)?,
                        TextOptions::Positions => write_vbyte(0b111, w)?,
                    }

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
