use std::{
    io::{self, Write},
    path::PathBuf,
};

use stream_vbyte::Scalar;

use super::{
    document::{FieldId, FieldMetadata, FieldType, TextOptions},
    encoders::{write_vbyte, write_vbyte_u64, Encoder, LZ4StringEncoder},
    index::{BTreeMapChunkedIter, Indexer, PostingListBuilder},
    key_val_files::{CountingFileWriter, StrKeyWriter, U32KeyWriter},
};

// Let's deal with indexing.

// 1. Field-Schemas
// 2. max-id

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
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
    let field_info = SegmentMetadata::from(indexer);
    std::fs::write(
        dir.join(format!("{}.fields.json", segment)),
        serde_json::to_string(&field_info)?,
    )?;

    println!("flush_lengths");
    flush_lengths(segment, dir, indexer)?;

    println!("flush_vocabularies");
    flush_vocabularies(segment, dir, indexer)?;
    println!(".flush_vocabularies");
    println!("flush_direct_indexes");
    flush_direct_indexes(segment, dir, indexer)?;
    indexer.stored_fields.clear();
    println!(".flush_direct_indexes");
    println!("flush_postings");
    flush_postings(segment, dir, indexer)?;
    indexer.postings.clear();
    println!(".flush_postings");
    println!("ok");
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct LengthsMetadata {
    field: u16,
    version: u32,
    num_documents: u32,
    total_positions: u64,
}

pub fn flush_lengths(segment: u32, dir: &PathBuf, indexer: &mut Indexer) -> io::Result<()> {
    for (field, entries) in &indexer.lengths {
        let path = dir.join(&format!("{}.{}.len", segment, field.0));
        let page_size = TERMS_PER_VOCAB_BLOCK as u32;

        let metadata = LengthsMetadata {
            field: field.0,
            version: 1,
            num_documents: entries.num_docs(),
            total_positions: entries.total,
        };

        let mut writer = U32KeyWriter::create(&path, entries.num_docs(), page_size)?;
        let mut start = 0;
        let mut encoded_buf = vec![0u8; 5 * TERMS_PER_VOCAB_BLOCK];
        for lengths in entries.as_slice().chunks(INDEX_CHUNK_SIZE) {
            let count = lengths.len() as u32;
            writer.start_dense_key_block(start, count)?;

            let encoded_len = stream_vbyte::encode::<Scalar>(lengths, &mut encoded_buf);
            writer.write_v32(encoded_len as u32)?;
            writer.write_bytes(&encoded_buf[..encoded_len])?;
            start += count;
        }
        writer.finish(&metadata)?;
    }
    Ok(())
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
pub(crate) const KEY_TERMS_PER_BLOCK: usize = 64;

#[derive(Serialize, Deserialize)]
struct PostingsMetadata {
    field: u16,
    field_type: FieldType,
    value_file: String,
    positions_file: Option<String>,
}

pub fn flush_postings(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    for (field, contents) in &indexer.postings {
        let schema = indexer.schema.get(&field).unwrap().clone();
        let file_name = format!("{}.{}.inv", segment, field.0);
        match &schema.kind {
            FieldType::Categorical => {
                let page_size = KEY_TERMS_PER_BLOCK as u32;
                let mut key_writer = U32KeyWriter::create(
                    dir.join(&file_name).as_ref(),
                    contents.len() as u32,
                    page_size,
                )?;
                let metadata = PostingsMetadata {
                    field: field.0,
                    field_type: schema.kind.clone(),
                    value_file: format!("{}.dv", &file_name),
                    positions_file: None,
                };
                let mut docs_writer =
                    CountingFileWriter::create(dir.join(&metadata.value_file).as_ref())?;

                let mut iter = BTreeMapChunkedIter::new(contents, KEY_TERMS_PER_BLOCK);
                let mut key_buffer = Vec::with_capacity(KEY_TERMS_PER_BLOCK);

                while let Some(_first_id) = iter.next() {
                    key_buffer.clear();
                    for key in iter.keys() {
                        key_buffer.push(key.0);
                    }
                    let vals = iter.vals();
                    key_writer.start_key_block(&key_buffer)?;

                    for (_term_id, val) in key_buffer.iter().cloned().zip(vals) {
                        // grab stats!
                        let df = val.docs.len() as u64;

                        // now write actual key-data:
                        key_writer.write_v64(df)?;
                        if df < 5 {
                            for doc in &val.docs.buffers[0] {
                                key_writer.write_v32(*doc)?;
                            }
                        } else {
                            let docs_addr = docs_writer.tell();
                            let skip_addr = write_docs_counts_skips(val, &mut docs_writer, None)?;
                            key_writer.write_v64(docs_addr)?;
                            // write skip-offset rather than absolute address for vbyte savings.
                            key_writer.write_v64(skip_addr - docs_addr)?;
                        }
                    }
                }
                key_writer.finish(&metadata)?;
            }
            FieldType::Textual(opts, _tok) => {
                let page_size = KEY_TERMS_PER_BLOCK as u32;
                let mut key_writer = U32KeyWriter::create(
                    dir.join(&file_name).as_ref(),
                    contents.len() as u32,
                    page_size,
                )?;
                let metadata = PostingsMetadata {
                    field: field.0,
                    field_type: schema.kind.clone(),
                    value_file: format!("{}.dv", &file_name),
                    positions_file: match opts {
                        TextOptions::Docs | TextOptions::Counts => None,
                        TextOptions::Positions => Some(format!("{}.pos", file_name)),
                    },
                };
                let mut docs_writer =
                    CountingFileWriter::create(dir.join(&metadata.value_file).as_ref())?;
                let mut pos_writer = if let Some(name) = metadata.positions_file.as_ref() {
                    Some(CountingFileWriter::create(dir.join(name).as_ref())?)
                } else {
                    None
                };
                let mut iter = BTreeMapChunkedIter::new(contents, KEY_TERMS_PER_BLOCK);
                let mut key_buffer = Vec::with_capacity(KEY_TERMS_PER_BLOCK);

                while let Some(_first_id) = iter.next() {
                    key_buffer.clear();
                    for key in iter.keys() {
                        key_buffer.push(key.0);
                    }
                    let vals = iter.vals();

                    key_writer.start_key_block(&key_buffer)?;

                    for (_term_id, val) in key_buffer.iter().cloned().zip(vals) {
                        // grab stats!
                        let df = val.docs.len() as u64;
                        let cf = val.total_term_frequency;
                        let docs_addr = docs_writer.tell();
                        let pos_addr = pos_writer.as_ref().map(|w| w.tell()).unwrap_or_default();
                        let skip_addr =
                            write_docs_counts_skips(val, &mut docs_writer, pos_writer.as_mut())?;

                        // now write actual key-data:
                        // worst-case: 45 bytes.
                        key_writer.write_v64(df)?;
                        if val.counts.len() != 0 {
                            key_writer.write_v64(cf)?;
                        }
                        key_writer.write_v64(docs_addr)?;
                        // write skip-offset rather than absolute address for vbyte savings.
                        key_writer.write_v64(skip_addr - docs_addr)?;
                        if pos_writer.is_some() {
                            key_writer.write_v64(pos_addr)?;
                        }
                    }
                }
                key_writer.finish(&metadata)?;
            }
            FieldType::Boolean | FieldType::DenseInt | FieldType::DenseFloat => {
                panic!("Dense fields should not have postings entries...")
            }
            FieldType::SparseInt | FieldType::SparseFloat => todo! {},
        }
    }
    Ok(())
}

pub(crate) const DOC_IDS_PER_CORPUS_BLOCK: usize = 64;

#[derive(Serialize, Deserialize)]
pub struct DirectIndexMetadata {
    field: u16,
    val_file: String,
    val_file_len: u64,
}

pub fn flush_direct_indexes(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    let mut lz4 = LZ4StringEncoder::default();
    for (field, contents) in &indexer.stored_fields {
        let schema = indexer.schema.get(&field).unwrap().clone();
        let file_name = format!("{}.{}.fwd", segment, field.0);
        println!(
            "field = {:?}, schema={:?}, file={}",
            field, schema, file_name
        );
        let mut metadata = DirectIndexMetadata {
            field: field.0,
            val_file: format!("{}.v", &file_name),
            val_file_len: 0,
        };
        let mut key_writer = U32KeyWriter::create(
            dir.join(&file_name).as_ref(),
            contents.len() as u32,
            DOC_IDS_PER_CORPUS_BLOCK as u32,
        )?;
        let mut val_writer = CountingFileWriter::create(dir.join(&metadata.val_file).as_ref())?;

        match schema.kind {
            // Only textual fields should be separated, CLOB/BLOB style...
            // Should really be a value-size branch...? Different writer for that.
            FieldType::Textual(_, _) | FieldType::Categorical => {
                let mut scratch = String::new();
                println!("{:?}", contents.keys().collect::<Vec<_>>());

                let mut iter = BTreeMapChunkedIter::new(contents, KEY_TERMS_PER_BLOCK);
                let mut key_buffer = Vec::with_capacity(KEY_TERMS_PER_BLOCK);

                while let Some(_first_id) = iter.next() {
                    key_buffer.clear();
                    for key in iter.keys() {
                        key_buffer.push(key.0);
                    }
                    let vals = iter.vals();

                    key_writer.start_key_block(&key_buffer)?;

                    for (_doc_id, val) in key_buffer.iter().cloned().zip(vals) {
                        // 0b001xxxxx (small value inline with keys!)
                        let data = val
                            .as_str()
                            .expect("data value expected for Textual/Categorical field");

                        if data.len() < 32 {
                            let byte_len = data.len() as u8;
                            key_writer.put(0b0010_0000u8 | byte_len)?;
                            key_writer.write_bytes(&data.as_bytes())?;
                        } else {
                            key_writer.put(0x00)?;
                            key_writer.write_v64(val_writer.tell())?;
                            scratch.clear();
                            scratch.push_str(data);
                            lz4.write(&scratch, &mut val_writer)?;
                        }
                    }
                }
            }
            // Small fields belong intermixed in the keys format.
            FieldType::Boolean
            | FieldType::DenseInt
            | FieldType::DenseFloat
            | FieldType::SparseInt
            | FieldType::SparseFloat => todo! {},
        } // match
        println!("key_writer.finish()");

        metadata.val_file_len = val_writer.tell();
        key_writer.finish(&metadata)?;
    } //field-loop.
    Ok(())
}

pub(crate) const TERMS_PER_VOCAB_BLOCK: usize = 64;

#[derive(Serialize, Deserialize)]
pub struct VocabularyMetadata {
    field: u16,
    first_key: String,
    last_key: String,
}

pub fn flush_vocabularies(segment: u32, dir: &PathBuf, indexer: &Indexer) -> io::Result<()> {
    for (field, vocab) in &indexer.vocab {
        /* Debugging
        std::fs::write(
            dir.join(format!("{}.{}.vocab.json", segment, field.0)),
            serde_json::to_string(&vocab)?,
        )?;
        */

        let mut metadata = VocabularyMetadata {
            field: field.0,
            first_key: String::new(),
            last_key: String::new(),
        };

        let num_keys = vocab.len();
        let path = dir.join(&format!("{}.{}.vocab", segment, field.0));
        let page_size = TERMS_PER_VOCAB_BLOCK as u32;
        let mut writer = StrKeyWriter::create(&path, num_keys as u32, page_size)?;
        let mut iter = BTreeMapChunkedIter::new(vocab, page_size as usize);
        let mut keys_written = 0;

        let mut terms_buffer = Vec::with_capacity(TERMS_PER_VOCAB_BLOCK);
        while let Some(first_key) = iter.next() {
            if metadata.first_key.len() == 0 {
                metadata.first_key = first_key;
            }
            let keys = iter.keys();
            terms_buffer.clear();
            terms_buffer.extend(iter.vals().iter().map(|ti| ti.0));
            writer.write_leaf_block(keys, &terms_buffer)?;
            keys_written += keys.len();
            if keys_written == num_keys {
                metadata.last_key = keys[keys.len() - 1].clone();
            }
        }
        writer.finish(&metadata)?;
    }

    Ok(())
}
