use super::field::GalagoField;
use super::postings::PositionsPostings;
use super::stemmer::Stemmer;
use crate::galago::btree::*;
use crate::galago::postings::IndexPartType;
use crate::galago::postings::LengthsPostings;
use crate::lang::*;
use crate::movement::MoverType;
use crate::scoring::*;
use crate::DataNeeded;
use crate::{stats::CountStats, Error, HashMap};
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

    fn count_stats(&mut self, expr: &QExpr) -> Result<CountStats, Error> {
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

    fn lengths_for_field(&self, field: Option<&str>) -> Result<LengthsPostings, Error> {
        let actual = GalagoField::from_str(field)?;
        if let Some(value) = self.lengths.find_str(actual.name())? {
            Ok(LengthsPostings::new(value)?)
        } else {
            Err(Error::MissingField).map_err(|e| {
                e.with_context(format!("Requested: {:?}, Attempted: {:?}", field, actual))
            })
        }
    }

    fn get_postings(
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

pub fn expr_to_eval(e: &QExpr, context: &mut Index) -> Result<Box<dyn EvalNode>, Error> {
    match e {
        QExpr::Text(TextExpr {
            term,
            field,
            data_needed,
            ..
        }) => {
            match context.get_postings(
                term.as_str(),
                field.as_ref().map(|f| f.as_str()),
                // Assume the worst here:
                data_needed.unwrap_or(DataNeeded::Counts),
            )? {
                Some(postings) => Ok(postings),
                None => Ok(Box::new(MissingTermEval)),
            }
        }
        QExpr::Lengths(LengthsExpr { field }) => {
            Ok(Box::new(context.lengths_for_field(Some(&field))?))
        }
        QExpr::Combine(CombineExpr { children, weights }) => {
            let children: Result<Vec<_>, _> =
                children.iter().map(|c| expr_to_eval(c, context)).collect();
            let children = children?;
            Ok(Box::new(WeightedSumEval::new(
                children,
                weights.into_iter().map(|w| *w as f32).collect(),
            )))
        }
        QExpr::BM25(BM25Expr { b, k, child, stats }) => {
            let fields = child.find_fields();
            if fields.len() > 1 {
                return Err(Error::QueryInit).map_err(|e| {
                    e.with_context(format!("Too many fields in sub-query: {:?}", child))
                });
            }
            let stats = match stats.as_ref() {
                Some(prev) => prev.clone(),
                None => context.count_stats(child)?,
            };
            let child = expr_to_eval(child, context)?;
            let field = fields.iter().map(|s| s.as_str()).nth(0);
            let lengths = Box::new(context.lengths_for_field(field)?);
            Ok(Box::new(BM25Eval::new(
                child,
                lengths,
                b.unwrap_or(0.75) as f32,
                k.unwrap_or(1.2) as f32,
                stats,
            )))
        }
        other => panic!("expr_to_eval. TODO: {:?}", other),
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
                data_needed.unwrap_or(DataNeeded::Docs),
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
