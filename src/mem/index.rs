use super::{
    document::{DocField, FieldId, FieldMetadata, FieldValue, TermId, TextOptions},
    int_set::ChunkedIntList,
    CompressedSortedIntSet,
};
use crate::mem::document::FieldType;
use crate::HashMap;
use crate::{stats::CountStats, DocId};
use std::collections::BTreeMap;

#[derive(Default)]
pub(crate) struct PostingListBuilder {
    /// index-paired with counts.
    pub(crate) docs: ChunkedIntList,
    /// index-paired with docs.
    pub(crate) counts: ChunkedIntList,
    /// encoded & d-gapped positions buffers, only.
    pub(crate) positions: Vec<Vec<u8>>,
    /// Total # of counts across all documents.
    pub(crate) total_term_frequency: u64,
}

impl PostingListBuilder {
    /// a.k.a. Document Frequency!
    fn num_docs(&self) -> usize {
        self.docs.len()
    }
    fn push_doc(&mut self, doc_id: DocId) {
        self.docs.push(doc_id.0);
    }
    fn push_counts(&mut self, doc_id: DocId, count: u32) {
        self.docs.push(doc_id.0);
        self.counts.push(count);
        self.total_term_frequency += count as u64;
    }
    fn push_positions(&mut self, doc_id: DocId, positions: CompressedSortedIntSet) {
        self.docs.push(doc_id.0);
        let count = positions.len() as u32;
        self.counts.push(count);
        self.positions.push(positions.encode_vbyte());
        self.total_term_frequency += count as u64;
    }
}

#[derive(Default)]
pub(crate) struct DenseU32FieldBuilder {
    total: u64,
    /// Every doc must have an entry for every T.
    blob: Vec<u32>,
}

impl DenseU32FieldBuilder {
    fn num_docs(&self) -> u32 {
        return self.blob.len() as u32;
    }
    fn insert(&mut self, doc_id: DocId, x: u32) {
        let doc_index = doc_id.0 as usize;
        // pad-zeros
        while self.blob.len() < doc_index {
            self.blob.push(0);
        }
        // should be equivalent now:
        // TODO: should we just increment in this case?
        debug_assert!(self.blob.len() == doc_index);
        self.blob.push(x);
        self.total += x as u64;
    }
}
/// An in-memory index / indexer.
#[derive(Default)]
pub struct Indexer {
    /// What document id will we assign next? Also max docs.
    pub(crate) next_id: u32,
    /// Each field has its own vocabulary.
    pub(crate) vocab: BTreeMap<FieldId, BTreeMap<String, TermId>>,
    /// Each field has a name.
    pub(crate) fields: BTreeMap<String, FieldId>,
    /// Each field has a schema.
    pub(crate) schema: BTreeMap<FieldId, FieldMetadata>,
    /// Textual and categorical features end up here.
    pub(crate) postings: BTreeMap<FieldId, BTreeMap<TermId, PostingListBuilder>>,
    /// Additional integer-valued fields may end up here.
    pub(crate) dense_fields: BTreeMap<FieldId, DenseU32FieldBuilder>,
    // TODO: corpus-structure:
    pub(crate) stored_fields: BTreeMap<FieldId, BTreeMap<DocId, FieldValue>>,
    /// Each field stores a 'length' for normalizing.
    pub(crate) lengths: BTreeMap<FieldId, DenseU32FieldBuilder>,
}

impl Indexer {
    pub fn get_stats(&self, field: FieldId, term: TermId) -> Option<CountStats> {
        let mut out = CountStats::default();
        if let Some(field_lengths) = self.lengths.get(&field) {
            out.document_count = field_lengths.num_docs() as u64;
            out.collection_length = field_lengths.total;

            // missing ok:
            if let Some(term_postings) = self
                .postings
                .get(&field)
                .expect("Lengths -> Postings")
                .get(&term)
            {
                out.document_frequency = term_postings.num_docs() as u64;
                out.collection_frequency = term_postings.total_term_frequency;
            }
            return Some(out);
        }
        None
    }
    pub fn declare_field(&mut self, name: &str, metadata: FieldMetadata) -> FieldId {
        let id = self.field_to_id(name);
        self.schema.insert(id, metadata);
        id
    }
    fn next_docid(&mut self) -> DocId {
        let n = DocId(self.next_id);
        self.next_id += 1;
        n
    }
    pub fn find_term_id(&self, field: FieldId, token: &str) -> Option<TermId> {
        self.vocab.get(&field)?.get(token).cloned()
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
    fn insert_text_field<S>(
        &mut self,
        doc_id: DocId,
        field: FieldId,
        tokens: &[S],
        options: TextOptions,
    ) where
        S: AsRef<str>,
    {
        // Ensure index exists for this field.
        self.postings.entry(field).or_default();

        match options {
            TextOptions::Docs => {
                for token in tokens.iter() {
                    let token = token.as_ref();
                    let token = self.token_to_id(field, token);
                    self.postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(token)
                        .or_default()
                        .push_doc(doc_id);
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
                    let token = token.as_ref();
                    let token = self.token_to_id(field, token);
                    let count: &mut u32 = counts.entry(token).or_default();
                    *count += 1;
                }
                for (term_id, count) in counts.into_iter() {
                    self.postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(term_id)
                        .or_default()
                        .push_counts(doc_id, count);
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
                    let token = token.as_ref();
                    let token = self.token_to_id(field, token);
                    positions.entry(token).or_default().push(index as u32);
                }

                for (term_id, positions) in positions.into_iter() {
                    self.postings
                        .get_mut(&field)
                        .unwrap()
                        .entry(term_id)
                        .or_default()
                        .push_positions(doc_id, positions);
                }
            }
        }
    }
    pub fn insert_document(&mut self, document: &[DocField]) -> Result<DocId, ()> {
        let doc_id = self.next_docid();

        let mut stored = Vec::new();
        for field in document {
            let schema = self.schema.get(&field.field).ok_or(())?.clone();
            if schema.stored {
                stored.push(field.clone())
            }

            match &field.value {
                FieldValue::Categorical(term) => {
                    self.insert_text_field(doc_id, field.field, &[term], TextOptions::Docs)
                }
                FieldValue::Textual(text) => {
                    let (opts, tok) = match &schema.kind {
                        FieldType::Textual(opts, tok) => (opts, tok),
                        _ => return Err(()),
                    };
                    let tokens: Vec<_> = tok.process(text);
                    self.insert_text_field(doc_id, field.field, &tokens, *opts)
                }
                FieldValue::Integer(num) => {
                    if schema.is_dense() {
                        self.dense_fields
                            .entry(field.field)
                            .or_default()
                            .insert(doc_id, *num);
                    } else {
                        todo!()
                    }
                }
                FieldValue::Floating(num) => {
                    let bytes = num.to_le_bytes();
                    let word = u32::from_le_bytes(bytes);
                    if schema.is_dense() {
                        self.dense_fields
                            .entry(field.field)
                            .or_default()
                            .insert(doc_id, word);
                    } else {
                        todo!()
                    }
                }
            }
        }

        if stored.len() > 0 {
            for field in stored.into_iter() {
                let column = self.stored_fields.entry(field.field).or_default();
                column.insert(doc_id, field.value);
            }
        }

        Ok(doc_id)
    }
}

pub struct BTreeMapChunkedIter<'src, K, V>
where
    K: Clone,
{
    iter: std::collections::btree_map::Iter<'src, K, V>,
    pub key_buffer: Vec<&'src K>,
    pub val_buffer: Vec<&'src V>,
    pub page_size: usize,
}

impl<'src, K, V> BTreeMapChunkedIter<'src, K, V>
where
    K: Clone,
{
    pub fn new(tree: &'src BTreeMap<K, V>, page_size: usize) -> Self {
        Self {
            iter: tree.iter(),
            key_buffer: Vec::with_capacity(page_size),
            val_buffer: Vec::with_capacity(page_size),
            page_size: page_size,
        }
    }
    pub fn keys(&self) -> &[&'src K] {
        &self.key_buffer
    }
    pub fn vals(&self) -> &[&'src V] {
        &self.val_buffer
    }
}

impl<'tree, K, V> Iterator for BTreeMapChunkedIter<'tree, K, V>
where
    K: Clone,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.key_buffer.clear();
        self.val_buffer.clear();
        for _ in 0..self.page_size {
            if let Some((k, v)) = self.iter.next() {
                self.key_buffer.push(k);
                self.val_buffer.push(v);
            } else {
                break;
            }
        }
        self.key_buffer.get(0).cloned().cloned()
    }
}

pub fn is_contiguous(ids: &[u32]) -> bool {
    if ids.len() == 0 {
        return true;
    }
    let mut prev = ids[0];
    for current in ids[1..].iter().cloned() {
        if prev + 1 != current {
            return false;
        }
        prev = current;
    }
    true
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::mem::{
        document::{DocFields, TokenizerStyle},
        flush_segment,
    };

    use super::*;
    use std::io::Read;
    use std::{fs::File, path::Path};

    #[test]
    fn test_btreemap_chunks() {
        let mut map = BTreeMap::new();
        let mut vec: Vec<u32> = Vec::new();
        for i in 0..1000 {
            map.insert(i, i * 3);
            vec.push(i * 3);
        }
        let mut map_iter = BTreeMapChunkedIter::new(&map, 75);
        let mut vec_iter = vec.chunks(75);

        loop {
            let m = map_iter.next();
            let v = vec_iter.next();
            if v.is_none() {
                assert!(m.is_none());
                break;
            }
            let m = m.unwrap();
            let vs = v.unwrap();
            let keyspace = map_iter
                .keys()
                .iter()
                .cloned()
                .cloned()
                .collect::<Vec<u32>>();
            assert!(is_contiguous(&keyspace));
            assert_eq!(m * 3, vs[0]);
            assert_eq!(
                map_iter
                    .vals()
                    .iter()
                    .cloned()
                    .cloned()
                    .collect::<Vec<u32>>(),
                vs
            );
        }
    }

    #[test]
    fn test_indexer() {
        let mut indexer = Indexer::default();
        let id_field =
            indexer.declare_field("id", FieldMetadata::new(FieldType::Categorical, true));
        let body_field = indexer.declare_field(
            "body",
            FieldMetadata::new(
                FieldType::Textual(TextOptions::Positions, TokenizerStyle::Galago),
                true,
            ),
        );

        let mut doc0 = DocFields::default();
        doc0.categorical(id_field, "doc0".into());
        doc0.textual(body_field, "hello world hello".into());
        let _ = indexer.insert_document(doc0.as_ref()).expect("Schema OK!");

        let mut doc1 = DocFields::default();
        doc1.categorical(id_field, "doc1".into());
        doc1.textual(body_field, "hello yolo yolo yolo".into());
        let _ = indexer.insert_document(doc1.as_ref()).expect("Schema OK!");

        println!("vocab: {:?}", indexer.vocab)
    }

    #[test]
    fn index_sample_data() {
        let mut tmpdir = TempDir::new().unwrap();
        let mut indexer = Indexer::default();
        let id_field =
            indexer.declare_field("id", FieldMetadata::new(FieldType::Categorical, true));
        let body_field = indexer.declare_field(
            "body",
            FieldMetadata::new(
                FieldType::Textual(TextOptions::Positions, TokenizerStyle::Galago),
                true,
            ),
        );

        for i in 1..=5 {
            let path = format!("data/inputs/ch{}.txt", i);
            let mut body = String::new();
            let mut fp = File::open(path).unwrap();
            let _ = File::read_to_string(&mut fp, &mut body).unwrap();
            let mut doc = DocFields::default();
            doc.categorical(id_field, format!("ch{}", i));
            doc.textual(body_field, body.to_lowercase());
            let _ = indexer.insert_document(doc.as_ref()).expect("Schema OK!");
        }

        println!(
            "the.stats: {:?}",
            indexer.get_stats(
                body_field,
                indexer
                    .find_term_id(body_field, "the")
                    .expect("'the' exists as a token!")
            )
        );

        let path = tmpdir.path().to_path_buf();
        flush_segment(0, &path, &mut indexer).unwrap();
    }
}
