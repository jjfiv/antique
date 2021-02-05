use super::{
    document::{DocField, DocFields, FieldId, FieldMetadata, FieldValue, TermId, TextOptions},
    CompressedSortedIntSet,
};
use crate::DocId;
use crate::HashMap;
use std::collections::BTreeMap;

#[derive(Default)]
struct PostingListBuilder {
    /// index-paired with counts.
    docs: CompressedSortedIntSet,
    /// index-paired with docs.
    counts: Vec<u32>,
    /// encoded & d-gapped positions buffers, only.
    positions: Vec<Vec<u8>>,
}

#[derive(Default)]
struct DenseU32FieldBuilder {
    /// Every doc must have an entry for every T.
    blob: Vec<u32>,
}

impl DenseU32FieldBuilder {
    fn insert(&mut self, doc_id: DocId, x: u32) {
        let doc_index = doc_id.0 as usize;
        // pad-zeros
        while self.blob.len() < doc_index {
            self.blob.push(0);
        }
        // should be equivalent now:
        // TODO: should we just increment in this case?
        debug_assert!(self.blob.len() == doc_index);
        self.blob.push(x)
    }
}
/// An in-memory index / indexer.
#[derive(Default)]
pub struct Indexer {
    next_id: u32,
    vocab: BTreeMap<FieldId, BTreeMap<String, TermId>>,
    fields: BTreeMap<String, FieldId>,
    schema: BTreeMap<FieldId, FieldMetadata>,
    /// Textual and categorical features end up here.
    postings: BTreeMap<FieldId, BTreeMap<TermId, PostingListBuilder>>,
    /// Additional integer-valued fields may end up here.
    dense_fields: BTreeMap<FieldId, DenseU32FieldBuilder>,
    // TODO: corpus-structure:
    stored_fields: BTreeMap<u32, Vec<DocFields>>,
    /// Each field stores a 'length' for normalizing.
    lengths: BTreeMap<FieldId, DenseU32FieldBuilder>,
}

impl Indexer {
    fn next_docid(&mut self) -> DocId {
        let n = DocId(self.next_id);
        self.next_id += 1;
        n
    }
    fn token_to_id(&mut self, field: FieldId, token: &str) -> TermId {
        let vocab = self.vocab.entry(field).or_default();
        // assume term already exists; fast-path.
        if let Some(id) = vocab.get(token) {
            return *id;
        }
        let next_term_id = TermId(vocab.len() as u32);
        vocab.insert(token.to_string(), next_term_id);
        next_term_id
    }
    pub fn field_to_id(&mut self, field: &str) -> FieldId {
        if let Some(id) = self.fields.get(field) {
            return *id;
        }
        let next_field_id = FieldId(self.fields.len() as u16);
        self.fields.insert(field.to_string(), next_field_id);
        next_field_id
    }
    fn insert_text_field(
        &mut self,
        doc_id: DocId,
        field: FieldId,
        tokens: &[&str],
        options: TextOptions,
    ) {
        // Ensure index exists for this field.
        self.postings.entry(field).or_default();

        match options {
            TextOptions::Docs => {
                for token in tokens.iter() {
                    let token = self.token_to_id(field, token);
                    let postings = self
                        .postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(token)
                        .or_default();
                    postings.docs.push(doc_id.0);
                }
            }
            TextOptions::Counts => {
                // incr lengths.
                self.lengths
                    .entry(field)
                    .or_default()
                    .insert(doc_id, tokens.len() as u32);

                let mut counts = HashMap::<TermId, u32>::default();
                for token in tokens.iter() {
                    let token = self.token_to_id(field, token);
                    let count: &mut u32 = counts.entry(token).or_default();
                    *count += 1;
                }
                for (term_id, count) in counts.into_iter() {
                    let postings = self
                        .postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(term_id)
                        .or_default();
                    postings.docs.push(doc_id.0);
                    postings.counts.push(count);
                }
            }
            TextOptions::Positions => {
                // incr lengths.
                self.lengths
                    .entry(field)
                    .or_default()
                    .insert(doc_id, tokens.len() as u32);

                let mut positions = HashMap::<TermId, CompressedSortedIntSet>::default();
                for (index, token) in tokens.iter().enumerate() {
                    let token = self.token_to_id(field, token);
                    if let Some(pos) = positions.get_mut(&token) {
                        pos.push(index as u32)
                    } else {
                        positions.insert(token, CompressedSortedIntSet::new(index as u32));
                    }
                }

                for (term_id, positions) in positions.into_iter() {
                    let postings = self
                        .postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(term_id)
                        .or_default();
                    postings.docs.push(doc_id.0);
                    postings.counts.push(positions.len() as u32);
                    postings.positions.push(positions.encode_vbyte())
                }
            }
        }
    }
    pub fn insert_document(&mut self, document: &[DocField]) {
        let doc_id = self.next_docid();

        for field in document {
            match &field.value {
                FieldValue::Categorical(term) => {
                    self.insert_text_field(doc_id, field.field, &[term], TextOptions::Docs)
                }
                FieldValue::Textual(text) => {
                    let tokens: Vec<_> = text.split_whitespace().collect();
                    self.insert_text_field(doc_id, field.field, &tokens, TextOptions::Positions)
                }
                FieldValue::Integer(num) => {
                    self.dense_fields
                        .entry(field.field)
                        .or_default()
                        .insert(doc_id, *num);
                }
                FieldValue::Floating(_) => todo! {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_indexer() {
        let mut indexer = Indexer::default();
        let id_field = indexer.field_to_id("id");
        let body_field = indexer.field_to_id("body");

        let mut doc0 = DocFields::default();
        doc0.categorical(id_field, "doc0".into());
        doc0.textual(body_field, "hello world hello".into());
        indexer.insert_document(doc0.as_ref());

        let mut doc1 = DocFields::default();
        doc1.categorical(id_field, "doc1".into());
        doc1.textual(body_field, "hello yolo yolo yolo".into());
        indexer.insert_document(doc1.as_ref());

        println!("vocab: {:?}", indexer.vocab)
    }
}
