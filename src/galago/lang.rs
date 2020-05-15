use crate::galago::index::Index;
use crate::lang::*;
use crate::movement::MoverType;
use crate::scoring::*;
use crate::DataNeeded;
use crate::Error;

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
