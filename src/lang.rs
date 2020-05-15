use crate::stats::CountStats;
use crate::DataNeeded;
use crate::HashSet;

#[derive(Debug, Clone)]
pub enum QErr {
    OrderedWindowBadStep(u32),
    UnorderedWindowBadWidth(u32),
    NegativeWeight(f64),
    NaNWeight,
    InfiniteWeight,
    EmptyTerm,
    BadFrequencies(CountStats),
    BadLengths(CountStats),
    BadDocProb(CountStats),
    BadTermProb(CountStats),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QExpr {
    Require(RequireExpr),
    Reject(RejectExpr),
    Must(MustExpr),
    And(AndExpr),
    Or(OrExpr),
    Not(NotExpr),
    AlwaysMatch,
    NeverMatch,
    Sum(SumExpr),
    Combine(CombineExpr),
    Mult(MultExpr),
    Max(MaxExpr),
    Weighted(WeightedExpr),
    Text(TextExpr),
    Lengths(LengthsExpr),
    LongParam(LongParamExpr),
    FloatParam(FloatParamExpr),
    OrderedWindow(OrderedWindowExpr),
    UnorderedWindow(UnorderedWindowExpr),
    Synonym(SynonymExpr),
    BM25(BM25Expr),
    LinearQL(LinearQLExpr),
    DirQL(DirQLExpr),
}

/// #filreq, #require
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequireExpr {
    pub cond: Box<QExpr>,
    pub value: Box<QExpr>,
}
/// #filrej, #reject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectExpr {
    pub cond: Box<QExpr>,
    pub value: Box<QExpr>,
}
/// #scoreif
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MustExpr {
    pub cond: Box<QExpr>,
    pub value: Box<QExpr>,
}
/// #all, #band, #and
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndExpr {
    pub children: Vec<QExpr>,
}
/// #any, #bor, #or
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrExpr {
    pub children: Vec<QExpr>,
}
/// #not
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotExpr {
    pub child: Box<QExpr>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumExpr {
    pub children: Vec<QExpr>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombineExpr {
    pub children: Vec<QExpr>,
    pub weights: Vec<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultExpr {
    pub children: Vec<QExpr>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxExpr {
    pub children: Vec<QExpr>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightedExpr {
    pub weight: f64,
    pub child: Box<QExpr>,
}
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct TextExpr {
    pub term: String,
    pub field: Option<String>,
    pub stats_field: Option<String>,
    pub data_needed: Option<DataNeeded>,
}
impl TextExpr {
    pub fn new<S>(term: S) -> TextExpr
    where
        S: Into<String>,
    {
        TextExpr {
            term: term.into(),
            field: None,
            stats_field: None,
            data_needed: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LengthsExpr {
    pub field: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LongParamExpr {
    pub field: String,
    pub missing: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FloatParamExpr {
    pub field: String,
    pub missing: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedWindowExpr {
    pub children: Vec<QExpr>,
    pub step: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnorderedWindowExpr {
    pub children: Vec<QExpr>,
    pub width: Option<u32>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynonymExpr {
    pub children: Vec<QExpr>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Expr {
    pub child: Box<QExpr>,
    pub b: Option<f64>,
    pub k: Option<f64>,
    pub stats: Option<CountStats>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinearQLExpr {
    pub child: Box<QExpr>,
    pub lambda: Option<f64>,
    pub stats: Option<CountStats>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirQLExpr {
    pub child: Box<QExpr>,
    pub mu: Option<f64>,
    pub stats: Option<CountStats>,
}

pub fn term<S: Into<String>>(term: S) -> QExpr {
    QExpr::Text(TextExpr {
        term: term.into(),
        field: None,
        stats_field: None,
        data_needed: None,
    })
}
pub fn phrase(terms: Vec<QExpr>) -> QExpr {
    QExpr::OrderedWindow(OrderedWindowExpr {
        children: terms,
        step: 1,
    })
}

fn check_weight(x: f64, errors: &mut Vec<QErr>) {
    if x.is_nan() {
        errors.push(QErr::NaNWeight)
    } else if x.is_infinite() {
        errors.push(QErr::InfiniteWeight)
    }
}
fn opt_check_stats(stats: &Option<CountStats>, errors: &mut Vec<QErr>) {
    if let Some(stats) = stats {
        if stats.collection_frequency < stats.document_frequency {
            errors.push(QErr::BadFrequencies(stats.clone()))
        }
        if stats.collection_length < stats.document_count {
            errors.push(QErr::BadLengths(stats.clone()))
        }
        if stats.document_frequency > stats.document_count {
            errors.push(QErr::BadDocProb(stats.clone()))
        }
        if stats.collection_frequency > stats.collection_length {
            errors.push(QErr::BadTermProb(stats.clone()))
        }
    }
}

impl QExpr {
    pub fn weighted(self, weight: f64) -> QExpr {
        QExpr::Weighted(WeightedExpr {
            child: Box::new(self),
            weight,
        })
    }
    /// Check expression for argument-bounds errors...
    pub fn check(&self) -> Vec<QErr> {
        let mut errors = Vec::new();
        self.check_rec(&mut errors);
        errors
    }
    fn check_rec(&self, errors: &mut Vec<QErr>) {
        match self {
            Self::Require(RequireExpr { cond, value })
            | Self::Reject(RejectExpr { cond, value })
            | Self::Must(MustExpr { cond, value }) => {
                cond.check_rec(errors);
                value.check_rec(errors);
            }
            Self::Not(NotExpr { child }) => child.check_rec(errors),
            Self::Synonym(SynonymExpr { children })
            | Self::Sum(SumExpr { children })
            | Self::Mult(MultExpr { children })
            | Self::Max(MaxExpr { children })
            | Self::Or(OrExpr { children })
            | Self::And(AndExpr { children }) => {
                for c in children.iter() {
                    c.check_rec(errors);
                }
            }
            Self::Combine(CombineExpr {
                children, weights, ..
            }) => {
                for w in weights.iter() {
                    check_weight(*w, errors);
                }
                for c in children.iter() {
                    c.check_rec(errors);
                }
            }
            Self::LongParam(_)
            | Self::FloatParam(_)
            | Self::Lengths(_)
            | Self::AlwaysMatch
            | Self::NeverMatch => {}
            Self::Weighted(WeightedExpr { child, weight }) => {
                let weight = *weight;
                if weight < 0.0 {
                    errors.push(QErr::NegativeWeight(weight));
                }
                check_weight(weight, errors);
                child.check_rec(errors);
            }
            Self::Text(TextExpr { term, .. }) => {
                if term.is_empty() {
                    errors.push(QErr::EmptyTerm);
                }
            }
            Self::OrderedWindow(OrderedWindowExpr { children, step, .. }) => {
                if *step <= 0 {
                    errors.push(QErr::OrderedWindowBadStep(*step));
                }
                for c in children {
                    c.check_rec(errors);
                }
            }
            Self::UnorderedWindow(UnorderedWindowExpr {
                children, width, ..
            }) => {
                if let Some(width) = *width {
                    if width <= 1 {
                        errors.push(QErr::UnorderedWindowBadWidth(width));
                    }
                }
                for c in children {
                    c.check_rec(errors);
                }
            }
            Self::BM25(BM25Expr { child, b, k, stats }) => {
                if let Some(b) = *b {
                    check_weight(b, errors);
                }
                if let Some(k) = *k {
                    check_weight(k, errors);
                }
                opt_check_stats(stats, errors);
                child.check_rec(errors)
            }
            Self::LinearQL(LinearQLExpr {
                child,
                lambda,
                stats,
            }) => {
                if let Some(lambda) = *lambda {
                    check_weight(lambda, errors);
                }
                opt_check_stats(stats, errors);
                child.check_rec(errors);
            }
            Self::DirQL(DirQLExpr { child, mu, stats }) => {
                if let Some(mu) = *mu {
                    check_weight(mu, errors);
                }
                opt_check_stats(stats, errors);
                child.check_rec(errors);
            }
        }
    }

    fn visit<F>(&self, visitor: &mut F)
    where
        F: FnMut(&QExpr) -> (),
    {
        visitor(&self);
        match self {
            Self::Require(RequireExpr { cond, value })
            | Self::Reject(RejectExpr { cond, value })
            | Self::Must(MustExpr { cond, value }) => {
                cond.visit(visitor);
                value.visit(visitor);
            }
            Self::Not(NotExpr { child }) => child.visit(visitor),
            Self::OrderedWindow(OrderedWindowExpr { children, .. })
            | Self::UnorderedWindow(UnorderedWindowExpr { children, .. })
            | Self::Combine(CombineExpr { children, .. })
            | Self::Synonym(SynonymExpr { children })
            | Self::Sum(SumExpr { children })
            | Self::Mult(MultExpr { children })
            | Self::Max(MaxExpr { children })
            | Self::Or(OrExpr { children })
            | Self::And(AndExpr { children }) => {
                for c in children.iter() {
                    c.visit(visitor);
                }
            }
            Self::Text(_)
            | Self::LongParam(_)
            | Self::FloatParam(_)
            | Self::Lengths(_)
            | Self::AlwaysMatch
            | Self::NeverMatch => {}

            Self::DirQL(DirQLExpr { child, .. })
            | Self::LinearQL(LinearQLExpr { child, .. })
            | Self::BM25(BM25Expr { child, .. })
            | Self::Weighted(WeightedExpr { child, .. }) => {
                child.visit(visitor);
            }
        }
    }

    pub fn find_fields(&self) -> HashSet<String> {
        let mut out = HashSet::default();
        self.find_fields_rec(&mut out);
        out
    }
    fn find_fields_rec(&self, out: &mut HashSet<String>) {
        self.visit(&mut |q| match q {
            QExpr::Text(TextExpr {
                field, stats_field, ..
            }) => {
                if let Some(stats_field) = stats_field.as_ref().or(field.as_ref()) {
                    out.insert(stats_field.to_string());
                }
            }
            _ => {}
        })
    }
    pub fn bm25(self) -> QExpr {
        QExpr::BM25(BM25Expr {
            child: Box::new(self),
            b: None,
            k: None,
            stats: None,
        })
    }
}

// Adding these as-needed. TODO: a macro?
impl From<TextExpr> for QExpr {
    fn from(e: TextExpr) -> Self {
        QExpr::Text(e)
    }
}
impl From<BM25Expr> for QExpr {
    fn from(e: BM25Expr) -> Self {
        QExpr::BM25(e)
    }
}
