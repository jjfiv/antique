use std::{fs::File, marker::PhantomData};

use super::{
    document::{FieldId, FieldMetadata},
    index::Indexer,
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
