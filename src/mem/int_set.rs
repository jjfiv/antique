/// Compressed, Sorted-Int-Set
#[derive(Default)]
pub struct CompressedSortedIntSet {
    deltas: Vec<u32>,
    prev: u32,
}

impl CompressedSortedIntSet {
    pub fn new(first: u32) -> Self {
        Self {
            deltas: vec![first],
            prev: first,
        }
    }
    pub fn len(&self) -> usize {
        self.deltas.len()
    }
    #[cfg(test)]
    fn into_deltas(self) -> Vec<u32> {
        self.deltas
    }
    pub fn push(&mut self, n: u32) {
        // make sure it's a set, logically; and no negatives are needed.
        debug_assert!(n == 0 || n > self.prev);
        self.deltas.push(n - self.prev);
        self.prev = n
    }
    #[cfg(test)]
    fn iter(&self) -> DeltaIterator<std::iter::Cloned<core::slice::Iter<'_, u32>>> {
        DeltaIterator::new(self.deltas.iter().cloned())
    }
    pub fn encode_vbyte(&self) -> Vec<u8> {
        let estimated_bytes = 5 * self.deltas.len(); // encoding bytes; 4-bytes each; leftover.
        let mut buffer = vec![0u8; estimated_bytes];
        let used = stream_vbyte::encode::<stream_vbyte::Scalar>(&self.deltas, &mut buffer);
        buffer.truncate(used);
        buffer.shrink_to_fit();
        buffer
    }
}

struct DeltaIterator<T>
where
    T: Iterator<Item = u32>,
{
    inner: T,
    prev: u32,
}

impl<T: Iterator<Item = u32>> DeltaIterator<T> {
    fn new(inner: T) -> Self {
        Self { inner, prev: 0 }
    }
}

impl<T: Iterator<Item = u32>> Iterator for DeltaIterator<T> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(delta) = self.inner.next() {
            let out = delta + self.prev;
            self.prev = out;
            return Some(out);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta_gap(monotonic: &[u32]) -> Vec<u32> {
        if monotonic.len() == 0 {
            return vec![];
        }
        let mut out = CompressedSortedIntSet::new(monotonic[0]);
        for x in monotonic.iter().skip(1).cloned() {
            out.push(x);
        }
        out.into_deltas()
    }

    fn undelta_gap(gaps: &[u32]) -> Vec<u32> {
        DeltaIterator::new(gaps.iter().cloned()).collect()
    }

    #[test]
    fn delta_gap_ok() {
        let sequence = vec![1, 2, 3, 4];
        assert_eq!(vec![1, 1, 1, 1], delta_gap(&sequence));
        assert_eq!(undelta_gap(&[1, 1, 1, 1]), sequence);
    }
    #[test]
    #[should_panic]
    fn panic_unsorted_deltas() {
        // should panic in debug-mode because 7 > 4; it's not sorted!
        let sequence = vec![1, 2, 7, 4];
        assert_eq!(vec![0], delta_gap(&sequence));
    }
}
