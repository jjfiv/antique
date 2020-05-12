use crate::galago_btree::ValueEntry;
use crate::io_helper::SliceInputStream;
use crate::Error;
use std::convert::TryInto;

impl ValueEntry {
    fn stream(&self) -> SliceInputStream {
        SliceInputStream::new(&self.source[self.start..self.end])
    }
    fn substream(&self, start_end: (usize, usize)) -> SliceInputStream {
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
        SliceInputStream::new(&self.source[sub_start..sub_end])
    }
}

pub struct LengthsPostings {
    source: ValueEntry,
    pub total_document_count: u64,
    pub non_zero_document_count: u64,
    pub collection_length: u64,
    pub avg_length: f64, // TODO: to-double?
    pub max_length: u64,
    pub min_length: u64,
    pub first_doc: u64,
    pub last_doc: u64,
    values_offset: usize,
}

impl LengthsPostings {
    pub fn is_appropriate(reader_class: &str) -> bool {
        "org.lemurproject.galago.core.index.disk.DiskLengthsReader" == reader_class
    }
    pub fn num_entries(&self) -> usize {
        (self.last_doc - self.first_doc + 1) as usize
    }
    pub fn length(&self, docid: u64) -> Option<u32> {
        if docid < self.first_doc || docid > self.last_doc {
            return None;
        }
        let offset = ((docid - self.first_doc) * 4) as usize;
        let begin = self.values_offset + offset + self.source.start;
        self.source.source[begin..begin + 4]
            .try_into()
            .ok()
            .map(|it| u32::from_be_bytes(it))
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
        let first_doc = stream.read_u64()?;
        let last_doc = stream.read_u64()?;
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
            values_offset,
        })
    }
}

pub struct PositionsPostings {
    source: ValueEntry,
    document_count: u64,
    total_position_count: u64,
    maximum_position_count: Option<u32>,
    inline_minimum: Option<u32>,
    documents: (usize, usize),
    counts: (usize, usize),
    positions: (usize, usize),
    skip_data: Option<(usize, usize)>,
    skip_positions: Option<(usize, usize)>,
    first_skip: Option<SkipState>,
}

#[derive(Default, Debug, Clone)]
struct SkipState {
    distance: u64,
    reset_distance: u64,
    total: u64,
    read: u64,
    next_document: u64,
    next_position: u64,
    documents_byte_floor: u64,
    counts_byte_floor: u64,
    positions_byte_floor: u64,
}

pub struct PositionsPostingsIter<'p> {
    postings: &'p PositionsPostings,

    documents: SliceInputStream<'p>,
    counts: SliceInputStream<'p>,
    positions: SliceInputStream<'p>,
    skip_data: Option<SliceInputStream<'p>>,
    skip_positions: Option<SliceInputStream<'p>>,

    skip: Option<SkipState>,
    document_index: u64,
    current_document: u64,
    current_count: u32,
    done: bool,
    positions_buffer: Vec<u32>,
    extents_loaded: bool,
    extents_byte_size: u32,
}

const HAS_SKIPS: u8 = 0b1;
const HAS_MAXTF: u8 = 0b10;
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
        } else { None };
        let document_count = reader.read_vbyte()?;
        let total_position_count = reader.read_vbyte()?;
        let maximum_position_count = if has_maxtf { Some(reader.read_vbyte()? as u32) } else { None };
        let mut skip = if has_skips {
            let distance = reader.read_vbyte()?;
            let reset_distance = reader.read_vbyte()?;
            let total = reader.read_vbyte()?;
            Some(SkipState { 
                distance,
                reset_distance,
                total,
                ..Default::default() })
        } else {
            None
        };

        let documents_length = reader.read_vbyte()? as usize;
        let counts_length = reader.read_vbyte()? as usize;
        let positions_length = reader.read_vbyte()? as usize;
        let skips_length  = if has_skips { reader.read_vbyte()? as usize } else { 0 };
        let skip_positions_length = if has_skips { reader.read_vbyte()? as usize } else { 0 };

        let documents_start = reader.tell();
        let counts_start = documents_start + documents_length;
        let positions_start = counts_start + counts_length;
        let positions_end = positions_start + positions_length;

        // Keep slices ready-to-go:
        let documents = (documents_start, counts_start);
        let counts = (counts_start, positions_start);
        let positions = (positions_start, positions_end);

        let mut skip_data = None;
        let mut skip_positions = None;
        if let Some(skip) = skip.as_mut() {
            let skips_start = positions_end;
            let skip_positions_start = skips_start + skips_length;
            let skip_positions_end = skip_positions_start + skip_positions_length;
            skip_data = Some((skips_start, skip_positions_start));
            skip_positions = Some((skip_positions_start, skip_positions_end));
        };

        Ok(PositionsPostings {
            source,
            first_skip: skip,
            total_position_count,
            document_count, inline_minimum, maximum_position_count, 
            documents, counts, positions, skip_data, skip_positions,
        })
    }
    pub fn iterator(&self) -> PositionsPostingsIter {
        let postings = self;
        PositionsPostingsIter {
            postings,
            skip: postings.first_skip.clone(),
            documents: postings.source.substream(postings.documents),
            counts: postings.source.substream(postings.counts),
            positions: postings.source.substream(postings.positions),
            skip_data: postings.skip_data.map(|it| postings.source.substream(it)),
            skip_positions: postings.skip_positions.map(|it| postings.source.substream(it)),
            done: false,
            extents_loaded: false,
            document_index: 0,
            extents_byte_size: 0,
            current_count: 0,
            current_document: 0,
            positions_buffer: Vec::new(), 
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::galago_btree as btree;
    use std::path::Path;

    #[test]
    fn test_load_lengths() {
        const TRUE_LENGTHS: &[u32] = &[1071, 887, 991, 19, 831, 1717];
        let reader = btree::read_info(&Path::new("data/index.galago/lengths")).unwrap();
        assert!(LengthsPostings::is_appropriate(
            &reader.manifest.reader_class
        ));
        let lengths_entry = reader.find_str("document").unwrap().unwrap();
        let lengths = LengthsPostings::new(lengths_entry).unwrap();
        assert_eq!(lengths.to_vec(), TRUE_LENGTHS);
        assert_eq!(lengths.length(3), Some(19));
        assert_eq!(lengths.max_length, 1717);
        assert_eq!(lengths.min_length, 19);
        let sum = TRUE_LENGTHS.iter().map(|l| *l as u64).sum::<u64>();
        assert_eq!(lengths.collection_length, sum);
        assert_eq!(lengths.non_zero_document_count as usize, TRUE_LENGTHS.len());
        assert_eq!(lengths.total_document_count as usize, TRUE_LENGTHS.len());
        assert_eq!(lengths.first_doc, 0);
        assert_eq!(lengths.last_doc as usize, TRUE_LENGTHS.len() - 1);

        assert_ne!(0, TRUE_LENGTHS.len());
        let avg_length = sum as f64 / (TRUE_LENGTHS.len() as f64);

        let diff = (avg_length - lengths.avg_length).abs();
        assert!(diff < 0.001);
    }
}
