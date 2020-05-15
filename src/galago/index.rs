use super::field::GalagoField;
use super::postings::PositionsPostings;
use super::stemmer::Stemmer;
use crate::galago::btree::*;
use crate::galago::corpus::CorpusDoc;
use crate::galago::lang::expr_to_eval;
use crate::galago::postings::IndexPartType;
use crate::galago::postings::LengthsPostings;
use crate::lang::*;
use crate::scoring::*;
use crate::DataNeeded;
use crate::{io_helper::DataInputStream, stats::CountStats, DocId, Error, HashMap};
use std::fs;
use std::path::Path;

// TODO: build an index with fields and update this appropriately.
pub struct Index {
    postings: HashMap<GalagoField, TreeReader>,
    corpus: Option<TreeReader>,
    lengths: TreeReader,
    /// id -> name
    names: TreeReader,
    /// name -> id
    names_reverse: TreeReader,
}

impl Index {
    pub fn open(path: &Path) -> Result<Index, Error> {
        // Collect different types:
        let mut postings = HashMap::default();
        let mut corpus = Vec::new();
        let mut names = Vec::new();
        let mut lengths = Vec::new();
        let mut names_reverse = Vec::new();

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if !is_btree(&entry.path()) {
                // Skip non-btree files for now.
                continue;
            }
            let reader = TreeReader::new(&entry.path())?;
            match reader.index_part_type()? {
                IndexPartType::Names => names.push(reader),
                IndexPartType::NamesReverse => names_reverse.push(reader),
                IndexPartType::Corpus => corpus.push(reader),
                IndexPartType::Positions => {
                    let stemmer = Stemmer::from_class_name(
                        reader.manifest.stemmer.as_ref().map(|x| x.as_str()),
                    )?;
                    let name = reader.file_name()?;
                    let field = GalagoField::from_file_name(name)?;
                    if field.stemmer() != stemmer {
                        return Err(Error::UnknownIndexPart(name.to_string())).map_err(|e| {
                            e.with_context(format!(
                                "Name implies {:?} but manifest has {:?}",
                                field.stemmer(),
                                stemmer
                            ))
                        });
                    }
                    postings.insert(field, reader);
                }
                IndexPartType::Lengths => lengths.push(reader),
            }
        }

        assert!(corpus.len() <= 1);
        assert!(lengths.len() == 1);
        assert!(names.len() == 1);
        assert!(names_reverse.len() == 1);
        let corpus = corpus.drain(0..).nth(0);
        let lengths = lengths.drain(0..).nth(0).unwrap();
        let names = names.drain(0..).nth(0).unwrap();
        let names_reverse = names_reverse.drain(0..).nth(0).unwrap();

        Ok(Index {
            postings,
            corpus,
            lengths,
            names,
            names_reverse,
        })
    }

    pub fn eval(&mut self, expr: &QExpr) -> Result<Box<dyn EvalNode>, Error> {
        let errors = expr.check();
        if !errors.is_empty() {
            return Err(Error::QueryErrors(errors));
        }
        expr_to_eval(expr, self)
    }

    pub fn lookup_document(&mut self, doc: DocId) -> Result<Option<CorpusDoc>, Error> {
        if let Some(corpus) = self.corpus.as_ref() {
            if let Some(entry) = corpus.find_bytes(&doc.to_be_bytes())? {
                return Ok(Some(CorpusDoc::extract(entry)?));
            }
        }
        Ok(None)
    }

    pub fn lookup_id_for_name(&mut self, name: &str) -> Result<Option<DocId>, Error> {
        if let Some(result) = self.names_reverse.find_str(name)? {
            return Ok(Some(DocId(result.stream().read_u64()?)));
        }
        Ok(None)
    }

    pub fn lookup_name_for_id(&mut self, id: DocId) -> Result<Option<String>, Error> {
        if let Some(result) = self.names.find_bytes(&id.to_be_bytes())? {
            return Ok(Some(result.to_str()?.into()));
        }
        Ok(None)
    }

    pub fn count_stats(&mut self, expr: &QExpr) -> Result<CountStats, Error> {
        match expr {
            QExpr::Text(TextExpr {
                term,
                field,
                stats_field,
                ..
            }) => {
                let mut stats = CountStats::default();
                let field = stats_field.as_deref().or(field.as_deref());
                let lengths = self.lengths_for_field(field)?;
                lengths.get_stats(&mut stats);

                // If we can't find the term, its frequencies stay at zero.
                let part = self.postings_for_field(field)?;
                if let Some(value) = part.find_str(term)? {
                    PositionsPostings::new(value)?.get_stats(&mut stats);
                }

                Ok(stats)
            }
            other => panic!("TODO: implement stats computation: {:?}", other),
        }
    }

    fn postings_for_field(&self, field: Option<&str>) -> Result<&TreeReader, Error> {
        let actual = GalagoField::from_str(field)?;
        if let Some(tree) = self.postings.get(&actual) {
            Ok(tree)
        } else {
            Err(Error::MissingField).map_err(|e| {
                e.with_context(format!("Requested: {:?}, Attempted: {:?}", field, actual))
            })
        }
    }

    pub fn lengths_for_field(&self, field: Option<&str>) -> Result<LengthsPostings, Error> {
        let actual = GalagoField::from_str(field)?;
        if let Some(value) = self.lengths.find_str(actual.name())? {
            Ok(LengthsPostings::new(value)?)
        } else {
            Err(Error::MissingField).map_err(|e| {
                e.with_context(format!("Requested: {:?}, Attempted: {:?}", field, actual))
            })
        }
    }

    pub fn get_postings(
        &mut self,
        term: &str,
        field: Option<&str>,
        needed: DataNeeded,
    ) -> Result<Option<Box<dyn EvalNode>>, Error> {
        let part = self.postings_for_field(field)?;
        if let Some(value) = part.find_str(term)? {
            let postings = PositionsPostings::new(value)?;
            match needed {
                DataNeeded::Positions => Ok(Some(Box::new(postings.iterator()?))),
                DataNeeded::Counts => Ok(Some(Box::new(postings.counts()?))),
                DataNeeded::Docs => Ok(Some(Box::new(postings.docs()?))),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let mut index = Index::open(Path::new("data/index.galago")).unwrap();
        assert!(index.corpus.is_some());

        // Isn't nice that listing directories has no inherent order?
        assert_eq!(
            Some("/home/jfoley/antique/data/inputs/ch4.txt"),
            index.lookup_name_for_id(DocId(0)).unwrap().as_deref()
        );
        assert_eq!(
            Some(DocId(0)),
            index
                .lookup_id_for_name("/home/jfoley/antique/data/inputs/ch4.txt")
                .unwrap()
        );
        assert_eq!(None, index.lookup_id_for_name("missing").unwrap());
    }

    #[test]
    fn test_stats() {
        // cmp to: galago stats --index= data/index.galago/ --node+ "#counts:the()"
        // also: galago stats --index= data/index.galago/
        let mut index = Index::open(Path::new("data/index.galago")).unwrap();
        let the_term: QExpr = TextExpr::new("the").into();
        let stats = index.count_stats(&the_term).unwrap();
        assert_eq!(stats.document_frequency, 5);
        assert_eq!(stats.document_count, 6);
        assert_eq!(stats.collection_frequency, 179);
        assert_eq!(stats.collection_length, 5516);
    }

    #[test]
    fn test_bm25() {
        // see: galago debug-query --index= data/index.galago/ --query= '#bm25:b=0.75:k=1.2(#counts:part=postings:the())' --docid=/home/jfoley/antique/data/inputs/ch1.txt
        let mut index = Index::open(Path::new("data/index.galago")).unwrap();
        let q: QExpr = BM25Expr {
            b: Some(0.75),
            k: Some(1.2),
            stats: None,
            child: Box::new(TextExpr::new("the").into()),
        }
        .into();

        // Create a scorer:
        let mut scorer = index.eval(&q).unwrap();

        let ch1 = DocId(1);
        // Go to "ch1.txt"
        assert_eq!(ch1, scorer.sync_to(ch1).unwrap());

        let score = scorer.score(ch1);
        println!("actual-score: {}", score);

        //Match(0.07647033, "b: 0.75, k: 1.2, idf: 0.087011375 length: 887", [Match(18.0, "counts", [])])
        println!("{:?}", scorer.explain(ch1));
        let expected = 0.17975731531052844;
        let diff = (expected - score).abs();
        assert!(diff < 0.00001);
    }
}
