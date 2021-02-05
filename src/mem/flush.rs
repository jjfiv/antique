use super::{
    document::{FieldId, FieldMetadata},
    index::Indexer,
};

// Let's deal with indexing.

// 1. Field-Schemas
// 2. max-id

struct SegmentMetadata {
    maximum_document: u32,
    fields: Vec<(FieldId, String, FieldMetadata)>,
}

impl SegmentMetadata {
    fn from(indexer: &Indexer) -> Self {
        let mut fields = vec![];

        for (name, id) in indexer.fields.iter() {
            let meta = indexer.schema.get(id).unwrap().clone();
            fields.push((*id, name.clone(), meta));
        }

        Self {
            maximum_document: indexer.next_id,
            fields,
        }
    }
}
