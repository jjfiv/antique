use crate::DocId;
use std::{cmp::Ordering, collections::BinaryHeap};

#[derive(Debug, Copy, Clone)]
pub struct ScoreDoc {
    score: f32,
    doc: DocId,
}

impl ScoreDoc {
    pub fn new(score: f32, doc: DocId) -> Self {
        Self { score, doc }
    }
}

impl PartialEq for ScoreDoc {
    fn eq(&self, other: &ScoreDoc) -> bool {
        self.doc == other.doc
    }
}
impl Eq for ScoreDoc {}

impl PartialOrd for ScoreDoc {
    fn partial_cmp(&self, other: &ScoreDoc) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Rust has a MaxHeap, so we do reverse ordering here so we can always pop the min.
impl Ord for ScoreDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.score < other.score {
            Ordering::Greater
        } else if self.score > other.score {
            Ordering::Less
        } else {
            self.doc.cmp(&other.doc)
        }
    }
}

pub struct ScoringHeap {
    size: usize,
    heap: BinaryHeap<ScoreDoc>,
}

impl ScoringHeap {
    pub fn new(size: usize) -> ScoringHeap {
        ScoringHeap {
            size,
            heap: BinaryHeap::new(),
        }
    }
    pub fn offer(&mut self, score: f32, doc: DocId) {
        // Add when non-full:
        if self.heap.len() < self.size {
            self.heap.push(ScoreDoc::new(score, doc));
        } else if score > self.top().unwrap().score {
            // Otherwise, only if better than the worst of the best.
            self.heap.push(ScoreDoc::new(score, doc));
            self.heap.pop();
        }
    }
    pub fn top(&self) -> Option<&ScoreDoc> {
        self.heap.peek()
    }
    pub fn into_vec(self) -> Vec<ScoreDoc> {
        self.heap.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ord() {
        // NOT INTUITIVE
        assert!(ScoreDoc::new(0.6, DocId(1)) > ScoreDoc::new(0.7, DocId(1)));
    }

    #[test]
    fn test_min_at_top() {
        let mut heap = ScoringHeap::new(10);
        heap.offer(0.6, DocId(1));
        assert_eq!(heap.top().unwrap().doc, DocId(1));
        heap.offer(0.8, DocId(2));
        assert_eq!(heap.top().unwrap().doc, DocId(1));
        heap.offer(0.7, DocId(3));
        assert_eq!(heap.top().unwrap().doc, DocId(1));

        let output = heap.into_vec();
        assert_eq!(output[0].doc, DocId(2)); // 0.8
        assert_eq!(output[1].doc, DocId(3)); // 0.7
        assert_eq!(output[2].doc, DocId(1)); // 0.6
    }

    #[test]
    fn test_overflow() {
        let mut heap = ScoringHeap::new(2);
        heap.offer(0.6, DocId(1));
        assert_eq!(heap.top().unwrap().doc, DocId(1));
        heap.offer(0.8, DocId(2));
        assert_eq!(heap.top().unwrap().doc, DocId(1));
        heap.offer(0.7, DocId(3));
        assert_eq!(heap.top().unwrap().doc, DocId(3));
    }
}
