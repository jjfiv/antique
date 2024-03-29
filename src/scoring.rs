use crate::{stats::CountStats, DocId, Error};

#[derive(Debug)]
pub enum Explanation {
    Miss(String, Vec<Explanation>),
    Match(f32, String, Vec<Explanation>),
}

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
    fn explain(&mut self, doc: DocId) -> Explanation;
}

pub struct BM25Eval {
    b: f32,
    k: f32,
    average_dl: f32,
    idf: f32,
    child: Box<dyn EvalNode>,
    lengths: Box<dyn EvalNode>,
}

impl BM25Eval {
    pub fn new(
        child: Box<dyn EvalNode>,
        lengths: Box<dyn EvalNode>,
        b: f32,
        k: f32,
        stats: CountStats,
    ) -> Self {
        let idf = (stats.document_count as f64) / (stats.document_frequency as f64 + 0.5);
        Self {
            b,
            k,
            child,
            lengths,
            average_dl: stats.average_doc_length(),
            // Matching Galago, though log2 is probs faster:
            idf: idf.ln() as f32,
        }
    }
}

impl EvalNode for BM25Eval {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = format!(
            "b: {}, k: {}, idf: {} len: {} avgdl: {}",
            self.b,
            self.k,
            self.idf,
            self.lengths.count(doc),
            self.average_dl,
        );
        if self.matches(doc) {
            Explanation::Match(self.score(doc), info, vec![self.child.explain(doc)])
        } else {
            Explanation::Miss(info, vec![self.child.explain(doc)])
        }
    }
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
        self.idf * num / denom
    }
    fn matches(&mut self, doc: DocId) -> bool {
        self.child.matches(doc)
    }
    fn estimate_df(&self) -> u64 {
        self.child.estimate_df()
    }
}

pub struct WeightedSumEval {
    children: Vec<Box<dyn EvalNode>>,
    weights: Vec<f32>,
}
impl WeightedSumEval {
    pub fn new(children: Vec<Box<dyn EvalNode>>, weights: Vec<f32>) -> WeightedSumEval {
        Self { children, weights }
    }
}

impl EvalNode for WeightedSumEval {
    fn explain(&mut self, doc: DocId) -> Explanation {
        let info = format!("weights: {:?}", self.weights);
        let children = self.children.iter_mut().map(|c| c.explain(doc)).collect();
        if self.matches(doc) {
            Explanation::Match(self.score(doc), info, children)
        } else {
            Explanation::Miss(info, children)
        }
    }
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

impl Movement for &mut dyn EvalNode {
    fn is_done(&self) -> bool {
        self.current_document().is_done()
    }
    fn move_past(&mut self) -> Result<DocId, Error> {
        self.sync_to(DocId(self.current_document().0 + 1))
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

pub struct MissingTermEval;

impl EvalNode for MissingTermEval {
    fn explain(&mut self, _doc: DocId) -> Explanation {
        Explanation::Miss("MissingTermEval".into(), vec![])
    }
    fn current_document(&self) -> DocId {
        DocId::no_more()
    }
    fn sync_to(&mut self, _doc: DocId) -> Result<DocId, Error> {
        Ok(DocId::no_more())
    }
    fn count(&mut self, _doc: DocId) -> u32 {
        0
    }
    fn score(&mut self, _doc: DocId) -> f32 {
        0.0
    }
    fn matches(&mut self, _doc: DocId) -> bool {
        false
    }
    fn estimate_df(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {}
