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

#[derive(Debug)]
pub struct PositionsPostings {
    source: ValueEntry,
    document_count: u64,
    total_position_count: u64,
    maximum_position_count: Option<u32>,
    inline_minimum: Option<u32>,
    documents: (usize, usize),
    counts: (usize, usize),
    positions: (usize, usize),
    skips: Option<SkipConfig>,
}

#[derive(Default, Debug, Clone)]
struct SkipConfig {
    distance: u64,
    reset_distance: u64,
    total: u64,
    data: (usize, usize),
    positions: (usize, usize),
}

#[derive(Debug, Clone)]
struct SkipState<'p> {
    config: SkipConfig,
    data: SliceInputStream<'p>,
    positions: SliceInputStream<'p>,
    read: u64,
    next_document: u64,
    next_position: u64,
    documents_byte_floor: usize,
    counts_byte_floor: usize,
    positions_byte_floor: usize,
}

impl<'p> SkipState<'p> {
    fn new(postings: &'p PositionsPostings, config: SkipConfig) -> Result<SkipState<'p>, Error> {
        let mut data = postings.source.substream(config.data);
        let positions = postings.source.substream(config.positions);
        let next_document = data.read_vbyte()?;

        Ok(SkipState {
            config,
            data,
            positions,
            next_document,
            read: 0,
            next_position: 0,
            documents_byte_floor: 0,
            counts_byte_floor: 0,
            positions_byte_floor: 0,
        })
    }
}

pub struct PositionsPostingsIter<'p> {
    postings: &'p PositionsPostings,
    documents: SliceInputStream<'p>,
    counts: SliceInputStream<'p>,
    positions: SliceInputStream<'p>,
    skips: Option<SkipState<'p>>,
    document_index: u64,
    current_document: u64,
    current_count: u32,
    positions_buffer: Vec<u32>,
    positions_loaded: bool,
    positions_byte_size: usize,
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
            Some(SkipConfig { 
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

        // Now read skips slices if we have skips.
        if let Some(skip) = skip.as_mut() {
            let skips_start = positions_end;
            let skip_positions_start = skips_start + skips_length;
            let skip_positions_end = skip_positions_start + skip_positions_length;
            skip.data = (skips_start, skip_positions_start);
            skip.positions = (skip_positions_start, skip_positions_end);
        };

        Ok(PositionsPostings {
            source,
            skips: skip,
            total_position_count,
            document_count, inline_minimum, maximum_position_count, 
            documents, counts, positions,
        })
    }
    pub fn iterator(&self) -> Result<PositionsPostingsIter, Error> {
        let postings = self;
        let skips = if let Some(skip_cfg) = &postings.skips {
            Some(SkipState::new(postings, skip_cfg.clone())?)
        } else {
            None
        };
        let mut iter = PositionsPostingsIter {
            postings,
            skips,
            documents: postings.source.substream(postings.documents),
            counts: postings.source.substream(postings.counts),
            positions: postings.source.substream(postings.positions),
            positions_byte_size: 0,
            current_count: 0,
            current_document: 0,
            positions_buffer: Vec::new(), 
            // These two values are basically invalid; but tricks to init correctly...
            positions_loaded: true,
            document_index: 0,
        };
        iter.load_next_posting()?;
        Ok(iter)
    }
}

impl<'p> SkipState<'p> {
    fn sync_to(&mut self, current_document: u64) -> Result<(), Error> {
        // Make sure skips is caught up to a current document:
        while self.next_document <= current_document {
            let _ = self.skip_once()?;
        }
        Ok(())
    }
    fn skip_once(&mut self) -> Result<u64, Error> {
        if self.next_document == std::u64::MAX {
            return Ok(self.next_document);
        }

        debug_assert!(self.read < self.config.total);

        // data = (delta-positions, delta_document_id)
        let current_skip_position = self.next_position + self.data.read_vbyte()?;
        if self.read % self.config.reset_distance == 0 {
            self.positions.seek(current_skip_position as usize)?;

            // positions = (docs_ptr, counts_ptr, positions_ptr)*
            self.documents_byte_floor = self.positions.read_vbyte()? as usize;
            self.counts_byte_floor = self.positions.read_vbyte()? as usize;
            self.positions_byte_floor = self.positions.read_vbyte()? as usize;
        }
        let current_document = self.next_document;

        if self.read + 1 == self.config.total {
            self.next_document = std::u64::MAX;
        } else {
            self.next_document += self.data.read_vbyte()?;
        }
        self.read += 1;
        self.next_position = current_skip_position;

        Ok(current_document)
    }
}

impl<'p> PositionsPostingsIter<'p> {
    /// Some positions arrays are prefixed with their length, but it depends on their size.
    /// If "inlining" was turned of while writing, they're all NOT prefixed with their length, even if many many positions to load/skip.
    fn current_positions_has_length(&self) -> bool {
        if let Some(inline_minimum) = self.postings.inline_minimum {
            self.current_count > inline_minimum
        } else {
            false
        }
    }
    fn is_done(&self) -> bool {
        self.current_document == std::u64::MAX
    }
    fn load_next_posting(&mut self) -> Result<(), Error> {
        if self.document_index >= self.postings.document_count {
            // Free memory:
            self.positions_buffer.clear();
            self.positions_buffer.shrink_to_fit();

            self.current_count = 0;
            self.current_document = std::u64::MAX;
            return Ok(());
        }

        if !self.positions_loaded {
            if self.current_positions_has_length() {
                let _ = self.positions.consume(self.positions_byte_size)?;
            } else {
                // skip positions, the hard way.
                for _ in 0..self.current_count {
                    let _ = self.positions.read_vbyte()?;
                }
            }
        }

        // Step forward:
        self.current_document += self.documents.read_vbyte()?;
        self.current_count = self.counts.read_vbyte()? as u32;

        // prepare the array of positions:
        self.positions_buffer.clear();
        self.positions_loaded = false;

        if self.current_positions_has_length() {
            // lazy-load, since we can.
            self.positions_byte_size = self.positions.read_vbyte()? as usize;
        } else {
            self.load_positions()?;
        }

        Ok(())
    }
    fn reposition_main_streams(&mut self) -> Result<(), Error> {
        let skips = self.skips.as_mut().unwrap();
        // Two-level skip; if we just reset-floors, no reading necessary...
        if (skips.read - 1) % skips.config.reset_distance == 0 {
            self.documents.seek(skips.documents_byte_floor)?;
            self.counts.seek(skips.counts_byte_floor)?;
            self.positions.seek(skips.positions_byte_floor)?;
        } else {
            skips.positions.seek(skips.next_position as usize)?;
            self.documents.seek(skips.documents_byte_floor + skips.positions.read_vbyte()? as usize)?;
            self.counts.seek(skips.counts_byte_floor + skips.positions.read_vbyte()? as usize)?;
            self.positions.seek(skips.positions_byte_floor + skips.positions.read_vbyte()? as usize)?;
        }
        self.document_index = (skips.config.distance * skips.read) - 1;
        Ok(())
    }
    pub fn sync_to(&mut self, document: u64) -> Result<u64, Error> {
        if self.is_done() {
            return Ok(self.current_document);
        }

        let needs_sync = if let Some(skips) = self.skips.as_mut() {
            skips.sync_to(self.current_document).map_err(|e| e.with_context("skips.sync_to"))?;

            // Can we use our skips?
            if document > skips.next_document {
                // Ditch extent state.
                self.positions_loaded = true;
                self.positions_byte_size = 0;

                while skips.read < skips.config.total && document > skips.next_document {
                    self.current_document = skips.skip_once().map_err(|e| e.with_context("skips.skip_once"))?;
                }
                true
            } else {
                false
            }
        } else {
            false
        };
        if needs_sync {
            self.reposition_main_streams().map_err(|e| e.with_context("reposition_main_streams"))?;
        }
        // Skips tapped out or never used; linear search from here:
        while document > self.current_document && self.document_index < self.postings.document_count {
            self.document_index += 1;
            self.load_next_posting().map_err(|e| e.with_context("load_next_posting"))?;
        }

        Ok(self.current_document)
    }
    fn get_positions(&mut self) -> Result<&[u32], Error> {
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

    #[test]
    fn test_load_positions() {
        let reader = btree::read_info(&Path::new("data/index.galago/postings")).unwrap();
        let the_entry = reader.find_str("the").unwrap().unwrap();
        let positions = PositionsPostings::new(the_entry).unwrap();
        println!("positions {:?}", positions);
        let mut iter = positions.iterator().unwrap();
        while !iter.is_done() {
            iter.sync_to(iter.current_document).unwrap();
            println!("the[{}] = {} .. ", iter.current_document, iter.current_count);
            println!("   {:?}", iter.get_positions().unwrap());
            iter.sync_to(iter.current_document+1).unwrap();
        }
    }
}
