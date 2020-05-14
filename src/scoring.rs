use crate::{DocId, Error};

pub trait Movement {
    fn is_done(&self) -> bool;
    fn move_past(&mut self) -> Result<DocId, Error>;
}

pub trait EvalNode {
    fn current_document(&self) -> DocId;
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error>;
    fn count(&mut self, doc: DocId) -> u32;
    fn score(&mut self, doc: DocId) -> f32;
    fn matches(&mut self, doc: DocId) -> bool;
    fn estimate_df(&self) -> u64;
}

struct BM25Eval {
    b: f32,
    k: f32,
    average_dl: f32,
    idf: f32,
    child: Box<dyn EvalNode>,
    lengths: Box<dyn EvalNode>,
}

impl EvalNode for BM25Eval {
    fn current_document(&self) -> DocId {
        self.child.current_document()
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        self.child.sync_to(document)
    }
    fn count(&mut self, _doc: DocId) -> u32 {
        todo!()
    }
    fn score(&mut self, doc: DocId) -> f32 {
        let b = self.b;
        let k = self.k;
        let count = self.child.count(doc) as f32;
        let length = self.lengths.count(doc) as f32;
        let num = count * (k + 1.0);
        let denom = count + (k * (1.0 - b + (b * length / self.average_dl)));
        self.idf * (num / denom)
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.child.matches(doc)
    }
    fn estimate_df(&self) -> u64 {
        self.child.estimate_df()
    }
}

struct WeightedSumEval {
    children: Vec<Box<dyn EvalNode>>,
    weights: Vec<f32>,
}

impl EvalNode for WeightedSumEval {
    fn current_document(&self) -> DocId {
        self.children
            .iter()
            .map(|c| c.current_document())
            .min()
            .unwrap()
    }
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
        let mut min = DocId::no_more();
        for c in self.children.iter_mut() {
            min = std::cmp::min(c.sync_to(document)?, min);
        }
        Ok(min)
    }
    fn count(&mut self, _doc: DocId) -> u32 {
        todo!()
    }
    fn score(&mut self, doc: DocId) -> f32 {
        self.children
            .iter_mut()
            .map(|c| c.score(doc))
            .zip(self.weights.iter())
            .map(|(c, w)| c * w)
            .sum()
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.children.iter_mut().any(|c| c.matches(doc))
    }
    fn estimate_df(&self) -> u64 {
        self.children.iter().map(|c| c.estimate_df()).max().unwrap()
    }
}

impl<T> Movement for T
where
    T: EvalNode,
{
    fn is_done(&self) -> bool {
        self.current_document().is_done()
    }
    fn move_past(&mut self) -> Result<DocId, Error> {
        self.sync_to(DocId(self.current_document().0 + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct VecMovement {
        position: usize,
        docs: Vec<u32>,
    }
}
