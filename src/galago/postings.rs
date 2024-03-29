use crate::io_helper::{
    ArcInputStream, DataInputStream, InputStream, SliceInputStream, ValueEntry,
};
use crate::scoring::{EvalNode, Explanation, Movement};
use crate::{stats::CountStats, DocId, Error};
use std::convert::TryInto;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy)]
pub enum IndexPartType {
    Names,
    NamesReverse,
    Corpus,
    Positions,
    Lengths,
}

impl IndexPartType {
    #[cfg(test)]
    fn from_file(path: &str) -> Result<IndexPartType, Error> {
        use crate::galago::btree::TreeReader;
        use std::path::Path;
        let reader = TreeReader::new(Path::new(path))?;
        Self::from_reader_class(&reader.manifest.reader_class)
    }
    pub fn from_reader_class(class_name: &str) -> Result<IndexPartType, Error> {
        Ok(match class_name {
            "org.lemurproject.galago.core.index.disk.DiskNameReader" => IndexPartType::Names,
            "org.lemurproject.galago.core.index.disk.DiskNameReverseReader" => {
                IndexPartType::NamesReverse
            }
            "org.lemurproject.galago.core.index.corpus.CorpusReader" => IndexPartType::Corpus,
            "org.lemurproject.galago.core.index.disk.DiskLengthsReader" => IndexPartType::Lengths,
            "org.lemurproject.galago.core.index.disk.PositionIndexReader" => {
                IndexPartType::Positions
            }
            _ => return Err(Error::MissingGalagoReader(class_name.to_string())),
        })
    }
}

impl ValueEntry {
    pub(crate) fn stream(&self) -> SliceInputStream {
        SliceInputStream::new(&self.source[self.start..self.end])
    }
    pub(crate) fn substream(&self, start_end: (usize, usize)) -> ArcInputStream {
        let (start, end) = start_end;
        let sub_start = self.start + start;
        let sub_end = self.start + end;
        // OK to each other:
        debug_assert!(sub_start <= sub_end);

        // OK to outer stream:
        debug_assert!(sub_start >= self.start);
        debug_assert!(sub_start < self.end);
        debug_assert!(sub_end > self.start);
        debug_assert!(sub_end <= self.end);
        ArcInputStream::new(self.source.clone(), sub_start, sub_end)
    }
}

#[derive(Debug)]
pub struct LengthsPostings {
    source: ValueEntry,
    pub total_document_count: u64,
    pub non_zero_document_count: u64,
    pub collection_length: u64,
    pub avg_length: f64, // TODO: to-double?
    pub max_length: u64,
    pub min_length: u64,
    pub first_doc: DocId,
    pub last_doc: DocId,
    values_offset: usize,
    current_document: DocId,
}

impl LengthsPostings {
    pub fn num_entries(&self) -> usize {
        (self.last_doc.0 - self.first_doc.0 + 1) as usize
    }
    /// What can this posting list tell us about the CountStats?
    pub fn get_stats(&self, stats: &mut CountStats) {
        stats.collection_length = self.collection_length;
        stats.document_count = self.total_document_count;
    }
    pub fn to_vec(&self) -> Vec<u32> {
        let begin = self.values_offset + self.source.start;
        let end = begin + (4 * self.num_entries());
        self.source.source[begin..end]
            .chunks_exact(4)
            .map(|word| u32::from_be_bytes(word.try_into().unwrap()))
            .collect()
    }
    pub fn new(source: ValueEntry) -> Result<LengthsPostings, Error> {
        let mut stream = source.stream();
        let total_document_count = stream.read_u64()?;
        let non_zero_document_count = stream.read_u64()?;
        let collection_length = stream.read_u64()?;
        let avg_length = f64::from_bits(stream.read_u64()?);
        let max_length = stream.read_u64()?;
        let min_length = stream.read_u64()?;
        let first_doc = DocId(stream.read_u64()? as u32);
        let last_doc = DocId(stream.read_u64()? as u32);
        let values_offset = stream.tell();

        Ok(LengthsPostings {
            source,
            total_document_count,
            non_zero_document_count,
            collection_length,
            avg_length,
            max_length,
            min_length,
            first_doc,
            last_doc,
            current_document: first_doc,
            values_offset,
        })
    }
}
impl EvalNode for LengthsPostings {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = "lengths".into();
        if self.matches(doc) {
            Explanation::Match(self.count(doc) as f32, info, vec![])
        } else {
            Explanation::Miss(info, vec![])
        }
    }
    fn current_document(&self) -> DocId {
        // We're basically never done?
        self.current_document
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        self.current_document = document;
        Ok(document)
    }
    fn count(&mut self, doc: DocId) -> u32 {
        if doc < self.first_doc || doc > self.last_doc {
            return 0;
        }
        let offset = ((doc.0 - self.first_doc.0) * 4) as usize;
        let begin = self.values_offset + offset + self.source.start;
        self.source.source[begin..begin + 4]
            .try_into()
            .ok()
            .map(|it| u32::from_be_bytes(it))
            .unwrap_or(0)
    }
    fn score(&mut self, _doc: DocId) -> f32 {
        todo!()
    }
    fn matches(&mut self, _doc: DocId) -> bool {
        // simplification, but fast
        true
    }
    fn estimate_df(&self) -> u64 {
        self.total_document_count
    }
}

/// Note that this resembles: PositionIndexExtentSource.java from Galago, but we don't support skips.
/// I couldn't find any indexes in-the-wild (on CIIR servers) that actually had them for testing.
/// So I decided to ditch the un-tested code rather than pursue generating an index with them.
#[derive(Debug)]
pub struct PositionsPostings {
    source: ValueEntry,
    pub document_count: u64,
    pub total_position_count: u64,
    pub maximum_position_count: Option<u32>,
    inline_minimum: Option<u32>,
    documents: (usize, usize),
    counts: (usize, usize),
    positions: (usize, usize),
}

/// Represent a positions iterator.
#[derive(Debug)]
pub struct PositionsPostingsIter {
    postings: PositionsPostings,
    documents: ArcInputStream,
    counts: ArcInputStream,
    positions: ArcInputStream,
    document_index: u64,
    pub current_document: DocId,
    current_count: u32,
    positions_buffer: Vec<u32>,
    positions_loaded: bool,
    positions_byte_size: usize,
}

/// Note we detect skips, and ignore them.
const HAS_SKIPS: u8 = 0b1;
/// In practice, most indexes have MAXTF set:
const HAS_MAXTF: u8 = 0b10;
/// In practice, most indexes have HAS_INLINING set with a threshold of 2:
const HAS_INLINING: u8 = 0b100;

impl PositionsPostings {
    pub fn new(source: ValueEntry) -> Result<PositionsPostings, Error> {
        let mut reader = source.stream();

        let options = reader.read_vbyte()? as u8;
        let has_inlining = options & HAS_INLINING > 0;
        let has_skips = options & HAS_SKIPS > 0;
        let has_maxtf = options & HAS_MAXTF > 0;

        let inline_minimum = if has_inlining {
            Some(reader.read_vbyte()? as u32)
        } else {
            None
        };
        let document_count = reader.read_vbyte()?;
        let total_position_count = reader.read_vbyte()?;
        let maximum_position_count = if has_maxtf {
            Some(reader.read_vbyte()? as u32)
        } else {
            None
        };

        // We don't support skips, but we can support ignoring them fairly easily.
        if has_skips {
            let _distance = reader.read_vbyte()?;
            let _reset_distance = reader.read_vbyte()?;
            let _total = reader.read_vbyte()?;
        }

        let documents_length = reader.read_vbyte()? as usize;
        let counts_length = reader.read_vbyte()? as usize;
        let positions_length = reader.read_vbyte()? as usize;
        // Again, we don't support skips, bug ignore them.
        if has_skips {
            let _skips_length = reader.read_vbyte()?;
            let _skip_positions_length = reader.read_vbyte()?;
        }

        let documents_start = reader.tell();
        let counts_start = documents_start + documents_length;
        let positions_start = counts_start + counts_length;
        let positions_end = positions_start + positions_length;

        // Keep slices ready-to-go:
        let documents = (documents_start, counts_start);
        let counts = (counts_start, positions_start);
        let positions = (positions_start, positions_end);

        Ok(PositionsPostings {
            source,
            total_position_count,
            document_count,
            inline_minimum,
            maximum_position_count,
            documents,
            counts,
            positions,
        })
    }
    pub fn get_stats(&self, stats: &mut CountStats) {
        stats.collection_frequency = self.total_position_count;
        stats.document_frequency = self.document_count;
    }
    pub fn docs(self) -> Result<DocsIter, Error> {
        let postings = self;
        let mut documents = postings.source.substream(postings.documents);
        let start = documents.read_vbyte()? as u32;
        Ok(DocsIter {
            documents,
            postings,
            current_document: DocId(start),
            document_index: 0,
        })
    }
    pub fn counts(self) -> Result<CountsIter, Error> {
        let postings = self;
        let mut documents = postings.source.substream(postings.documents);
        let mut counts = postings.source.substream(postings.counts);
        let start = documents.read_vbyte()? as u32;
        let current_count = counts.read_vbyte()? as u32;
        Ok(CountsIter {
            documents,
            counts,
            postings,
            current_document: DocId(start),
            current_count,
            document_index: 0,
        })
    }
    pub fn iterator(self) -> Result<PositionsPostingsIter, Error> {
        let postings = self;
        let mut iter = PositionsPostingsIter {
            documents: postings.source.substream(postings.documents),
            counts: postings.source.substream(postings.counts),
            positions: postings.source.substream(postings.positions),
            postings,
            positions_byte_size: 0,
            current_count: 0,
            current_document: DocId(0),
            positions_buffer: Vec::new(),
            // These two values are basically invalid; but tricks to init correctly...
            positions_loaded: true,
            document_index: 0,
        };
        iter.load_next_posting()?;
        Ok(iter)
    }
}

impl PositionsPostingsIter {
    pub fn new(value: ValueEntry) -> Result<Self, Error> {
        PositionsPostings::new(value)?.iterator()
    }
    /// Some positions arrays are prefixed with their length, but it depends on their size.
    /// If "inlining" was turned of while writing, they're all NOT prefixed with their length, even if many many positions to load/skip.
    fn current_positions_has_length(&self) -> bool {
        if let Some(inline_minimum) = self.postings.inline_minimum {
            self.current_count > inline_minimum
        } else {
            false
        }
    }
    fn load_next_posting(&mut self) -> Result<(), Error> {
        if self.document_index >= self.postings.document_count {
            self.positions_buffer.clear();
            self.current_count = 0;
            self.current_document = DocId::no_more();
            return Ok(());
        }

        if !self.positions_loaded {
            if self.current_positions_has_length() {
                let _ = self.positions.advance(self.positions_byte_size)?;
            } else {
                // skip positions, the hard way.
                for _ in 0..self.current_count {
                    let _ = self.positions.read_vbyte()?;
                }
            }
        }

        // Step forward:
        self.current_document.0 += self.documents.read_vbyte()? as u32;
        self.current_count = self.counts.read_vbyte()? as u32;

        // prepare the array of positions:
        self.positions_loaded = false;

        if self.current_positions_has_length() {
            // lazy-load, since we can.
            self.positions_byte_size = self.positions.read_vbyte()? as usize;
        } else {
            self.load_positions()?;
        }

        Ok(())
    }
    pub fn get_positions(&mut self) -> Result<&[u32], Error> {
        if self.is_done() {
            return Ok(&[]);
        }
        self.load_positions()?;
        Ok(&self.positions_buffer)
    }
    fn load_positions(&mut self) -> Result<(), Error> {
        if self.positions_loaded {
            return Ok(());
        }

        // Delta-coded positions:
        let mut position = 0;
        for _ in 0..self.current_count {
            position += self.positions.read_vbyte()? as u32;
            self.positions_buffer.push(position);
        }
        self.positions_loaded = true;

        Ok(())
    }
}

impl EvalNode for PositionsPostingsIter {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = "positions TODO".into();
        if self.matches(doc) {
            Explanation::Match(self.count(doc) as f32, info, vec![])
        } else {
            Explanation::Miss(info, vec![])
        }
    }
    fn current_document(&self) -> DocId {
        self.current_document
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        // Linear search through the postings-list:
        // Don't have to check for done here because of u64::max trick.
        while document > self.current_document && self.document_index < self.postings.document_count
        {
            self.document_index += 1;
            self.load_next_posting()
                .map_err(|e| e.with_context("load_next_posting"))?;
        }

        Ok(self.current_document)
    }
    fn count(&mut self, doc: DocId) -> u32 {
        if doc != self.current_document {
            0
        } else {
            self.current_count
        }
    }
    fn score(&mut self, _doc: DocId) -> f32 {
        todo!()
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.sync_to(doc).unwrap() == doc
    }
    fn estimate_df(&self) -> u64 {
        self.postings.document_count
    }
    // TODO: come back to this...
    //fn positions(&mut self, doc: DocId) -> &[u32] {
    //    if doc != self.current_document {
    //        &[]
    //    } else {
    //        self.get_positions().unwrap()
    //    }
    //}
}

pub struct CountsIter {
    postings: PositionsPostings,
    documents: ArcInputStream,
    counts: ArcInputStream,
    document_index: u64,
    current_document: DocId,
    current_count: u32,
}

impl EvalNode for CountsIter {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = "counts".into();
        if self.matches(doc) {
            Explanation::Match(self.count(doc) as f32, info, vec![])
        } else {
            Explanation::Miss(info, vec![])
        }
    }
    fn current_document(&self) -> DocId {
        self.current_document
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        // Linear search through the postings-list:
        // Don't have to check for done here because of u64::max trick.
        while document > self.current_document && self.document_index < self.postings.document_count
        {
            self.document_index += 1;
            if self.document_index >= self.postings.document_count {
                self.current_document = DocId::no_more();
                break;
            }

            // Step forward:
            self.current_document.0 += self.documents.read_vbyte()? as u32;
            self.current_count = self.counts.read_vbyte()? as u32;
        }

        Ok(self.current_document)
    }
    fn count(&mut self, doc: DocId) -> u32 {
        if self.matches(doc) {
            self.current_count
        } else {
            0
        }
    }
    fn score(&mut self, _doc: DocId) -> f32 {
        todo!()
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.sync_to(doc).unwrap() == doc
    }
    fn estimate_df(&self) -> u64 {
        self.postings.document_count
    }
}

pub struct DocsIter {
    postings: PositionsPostings,
    documents: ArcInputStream,
    document_index: u64,
    current_document: DocId,
}
impl DocsIter {
    pub fn new(value: ValueEntry) -> Result<Self, Error> {
        PositionsPostings::new(value)?.docs()
    }
}

impl EvalNode for DocsIter {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = "docs".into();
        if self.matches(doc) {
            Explanation::Match(1.0, info, vec![])
        } else {
            Explanation::Miss(info, vec![])
        }
    }
    fn current_document(&self) -> DocId {
        self.current_document
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        // Linear search through the postings-list:
        // Don't have to check for done here because of u64::max trick.
        while document > self.current_document && self.document_index < self.postings.document_count
        {
            self.document_index += 1;
            if self.document_index >= self.postings.document_count {
                self.current_document = DocId::no_more();
                break;
            }

            // Step forward:
            self.current_document.0 += self.documents.read_vbyte()? as u32;
        }

        Ok(self.current_document)
    }
    fn count(&mut self, _doc: DocId) -> u32 {
        todo!()
    }
    fn score(&mut self, _doc: DocId) -> f32 {
        todo!()
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.sync_to(doc).unwrap() == doc
    }
    fn estimate_df(&self) -> u64 {
        self.postings.document_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::galago::btree;
    use crate::scoring::Movement;
    use std::path::Path;

    #[test]
    fn test_index_parts() {
        assert_eq!(
            IndexPartType::Lengths,
            IndexPartType::from_file("data/index.galago/lengths").unwrap()
        );
        assert_eq!(
            IndexPartType::Positions,
            IndexPartType::from_file("data/index.galago/postings").unwrap()
        );
        assert_eq!(
            IndexPartType::Positions,
            IndexPartType::from_file("data/index.galago/postings.krovetz").unwrap()
        );
        assert_eq!(
            IndexPartType::Names,
            IndexPartType::from_file("data/index.galago/names").unwrap()
        );
        assert_eq!(
            IndexPartType::NamesReverse,
            IndexPartType::from_file("data/index.galago/names.reverse").unwrap()
        );
        assert_eq!(
            IndexPartType::Corpus,
            IndexPartType::from_file("data/index.galago/corpus").unwrap()
        );
    }

    #[test]
    fn test_load_lengths() {
        const TRUE_LENGTHS: &[u32] = &[1071, 887, 991, 19, 831, 1717];
        let reader = btree::read_info(&Path::new("data/index.galago/lengths")).unwrap();
        let lengths_entry = reader.find_str("document").unwrap().unwrap();
        let mut lengths = LengthsPostings::new(lengths_entry).unwrap();
        assert_eq!(lengths.to_vec(), TRUE_LENGTHS);
        assert_eq!(lengths.count(DocId(3)), 19);
        assert_eq!(lengths.count(DocId(0)), 1071);
        assert_eq!(lengths.count(DocId(5)), 1717);
        assert_eq!(lengths.count(DocId(6)), 0);
        assert_eq!(lengths.max_length, 1717);
        assert_eq!(lengths.min_length, 19);
        let sum = TRUE_LENGTHS.iter().map(|l| *l as u64).sum::<u64>();
        assert_eq!(lengths.collection_length, sum);
        assert_eq!(lengths.non_zero_document_count as usize, TRUE_LENGTHS.len());
        assert_eq!(lengths.total_document_count as usize, TRUE_LENGTHS.len());
        assert_eq!(lengths.first_doc, DocId(0));
        assert_eq!(lengths.last_doc.0 as usize, TRUE_LENGTHS.len() - 1);

        assert_ne!(0, TRUE_LENGTHS.len());
        let avg_length = sum as f64 / (TRUE_LENGTHS.len() as f64);

        let diff = (avg_length - lengths.avg_length).abs();
        assert!(diff < 0.001);
    }

    #[test]
    fn test_load_positions() {
        let reader = btree::read_info(&Path::new("data/index.galago/postings")).unwrap();
        let the_entry = reader.find_str("the").unwrap().unwrap();
        let positions = PositionsPostings::new(the_entry).unwrap();
        println!("positions {:?}", positions);
        let mut iter = positions.iterator().unwrap();
        while !iter.is_done() {
            iter.sync_to(iter.current_document).unwrap();
            println!(
                "the[{:?}] = {} .. ",
                iter.current_document, iter.current_count
            );
            println!("   {:?}", iter.get_positions().unwrap());
            iter.move_past().unwrap();
        }
    }

    #[test]
    fn test_load_all_field_names() {
        let reader = btree::read_info(&Path::new("data/index.galago/lengths")).unwrap();
        let fields = reader.collect_string_keys().unwrap();
        assert_eq!(fields, &["document".to_string()])
    }

    #[test]
    fn test_load_all_doc_names() {
        let reader = btree::read_info(&Path::new("data/index.galago/names.reverse")).unwrap();
        let fields = reader.collect_string_keys().unwrap();
        let mut names: Vec<String> = fields
            .into_iter()
            .map(|path| {
                Path::new(&path)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        names.sort();
        assert_eq!(
            names,
            &[
                "README.txt",
                "ch1.txt",
                "ch2.txt",
                "ch3.txt",
                "ch4.txt",
                "ch5.txt"
            ]
        )
    }
}
