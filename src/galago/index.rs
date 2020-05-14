use crate::galago::btree::*;
use crate::galago::postings::{DocsIter, IndexPartType, PositionsPostingsIter};
use crate::lang::*;
use crate::movement::MoverType;
use crate::scoring::{EvalNode, Movement};
use crate::DataNeeded;
use crate::{Error, HashMap};
use std::fs;
use std::path::Path;

// TODO: build an index with fields and update this appropriately.
pub struct Index {
    postings: HashMap<GalagoField, TreeReader>,
    corpus: Option<TreeReader>,
    lengths: TreeReader,
    names: TreeReader,
    names_reverse: TreeReader,
}

/// Galago defines a field as a stemmer across a field name.
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
struct GalagoField(Stemmer, String);

impl Default for GalagoField {
    fn default() -> Self {
        GalagoField(Stemmer::Krovetz, "document".into())
    }
}
impl GalagoField {
    fn from_file_name(name: &str) -> Result<GalagoField, Error> {
        Ok(if name.starts_with("field") {
            let parts: Vec<&str> = name.split(".").collect();
            match parts.len() {
                2 => GalagoField(Stemmer::Null, parts[1].to_string()),
                3 => GalagoField(
                    match parts[1] {
                        "krovetz" => Stemmer::Krovetz,
                        "porter" => Stemmer::Porter2,
                        _ => return Err(Error::UnknownIndexPart(name.into())),
                    },
                    parts[2].to_string(),
                ),
                _ => return Err(Error::UnknownIndexPart(name.into())),
            }
        } else {
            let field = "document".to_string();
            match name {
                "postings" => GalagoField(Stemmer::Null, field),
                "postings.porter" => GalagoField(Stemmer::Porter2, field),
                "postings.krovetz" => GalagoField(Stemmer::Krovetz, field),
                _ => return Err(Error::UnknownIndexPart(name.into())),
            }
        })
    }
}

#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy)]
enum Stemmer {
    Krovetz,
    Porter2,
    Null,
}
impl Stemmer {
    fn from_class_name(class_name: Option<&str>) -> Result<Stemmer, Error> {
        Ok(match class_name {
            Some("org.lemurproject.galago.core.parse.stem.KrovetzStemmer") => Stemmer::Krovetz,
            Some("org.lemurproject.galago.core.parse.stem.Porter2Stemmer") => Stemmer::Porter2,
            Some("org.lemurproject.galago.core.parse.stem.NullStemmer") => Stemmer::Null,
            None => Stemmer::Null,
            Some(other) => return Err(Error::UnknownStemmer(other.into())),
        })
    }
}

fn is_btree(path: &Path) -> bool {
    match open_file_magic(path, MAGIC_NUMBER) {
        Ok(_) => true,
        Err(_) => false,
    }
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
                    if field.0 != stemmer {
                        return Err(Error::UnknownIndexPart(name.to_string())).map_err(|e| {
                            e.with_context(format!(
                                "Name implies {:?} but manifest has {:?}",
                                field.0, stemmer
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

    fn postings_for_field(&self, field: Option<&str>) -> Option<&TreeReader> {
        match field {
            Some("document") | None => Some(&self.postings[&GalagoField::default()]),
            Some(name) => match GalagoField::from_file_name(name) {
                Ok(ff) => Some(&self.postings[&&ff]),
                Err(_) => None,
            },
        }
    }

    fn get_postings(
        &mut self,
        term: &str,
        field: Option<&str>,
        needed: DataNeeded,
    ) -> Result<Option<Box<dyn EvalNode>>, Error> {
        if let Some(part) = self.postings_for_field(field) {
            if let Some(value) = part.find_str(term)? {
                match needed {
                    DataNeeded::Counts | DataNeeded::Positions => {
                        Ok(Some(Box::new(PositionsPostingsIter::new(value)?)))
                    }
                    DataNeeded::Docs => Ok(Some(Box::new(DocsIter::new(value)?))),
                }
            } else {
                Ok(None)
            }
        } else {
            Err(Error::UnknownIndexPart(field.unwrap().to_string()))
        }
    }
}

pub fn expr_to_mover(e: &QExpr, context: &mut Index) -> Result<MoverType, Error> {
    match e {
        QExpr::Require(RequireExpr { cond, .. }) => expr_to_mover(cond, context),
        QExpr::Must(MustExpr { cond, value }) => {
            let cond = expr_to_mover(cond, context)?;
            let value = expr_to_mover(value, context)?;
            Ok(MoverType::create_and(vec![cond, value]))
        }
        QExpr::Reject(_) | QExpr::Not(_) | QExpr::LongParam(_) | QExpr::FloatParam(_) => {
            todo!("{:?}", e)
        }

        QExpr::UnorderedWindow(UnorderedWindowExpr { children, .. })
        | QExpr::OrderedWindow(OrderedWindowExpr { children, .. })
        | QExpr::And(AndExpr { children, .. }) => {
            let child_movers: Vec<MoverType> = children
                .iter()
                .map(|c| expr_to_mover(c, context))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MoverType::create_and(child_movers))
        }

        QExpr::BM25(BM25Expr { child, .. })
        | QExpr::LinearQL(LinearQLExpr { child, .. })
        | QExpr::Weighted(WeightedExpr { child, .. })
        | QExpr::DirQL(DirQLExpr { child, .. }) => expr_to_mover(child, context),

        QExpr::Lengths(_) | QExpr::AlwaysMatch => Ok(MoverType::AllMover),
        QExpr::NeverMatch => Ok(MoverType::EmptyMover),

        QExpr::Sum(SumExpr { children, .. })
        | QExpr::Mult(MultExpr { children, .. })
        | QExpr::Max(MaxExpr { children, .. })
        | QExpr::Or(OrExpr { children, .. })
        | QExpr::Synonym(SynonymExpr { children, .. })
        | QExpr::Combine(CombineExpr { children, .. }) => {
            let child_movers: Vec<MoverType> = children
                .iter()
                .map(|c| expr_to_mover(c, context))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MoverType::create_or(child_movers))
        }
        QExpr::Text(TextExpr {
            term,
            field,
            data_needed,
            ..
        }) => Ok(
            match context.get_postings(
                term.as_str(),
                field.as_ref().map(|f| f.as_str()),
                // Assume the worst here:
                data_needed.unwrap_or(DataNeeded::Positions),
            )? {
                Some(postings) => MoverType::RealMover(postings),
                None => MoverType::EmptyMover,
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let index = Index::open(Path::new("data/index.galago")).unwrap();
        assert!(index.corpus.is_some());
    }
}
