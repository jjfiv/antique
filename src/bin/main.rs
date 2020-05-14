use antique::Error;
use std::env;
use std::io;
use std::path::Path;
use std::time::Instant;

use antique::galago::index::{expr_to_eval, expr_to_mover, Index};
use antique::galago::tokenizer::tokenize_to_terms;
use antique::heap_collection::*;
use antique::lang::*;
use antique::{scoring::Movement, DocId};
use io::Write;

fn main() -> Result<(), Error> {
    let input = env::args()
        .nth(1)
        .unwrap_or_else(|| "data/index.galago".to_string());
    let path = Path::new(&input);
    let mut index = Index::open(path)?;

    loop {
        let mut line = String::new();
        print!("query> ");
        io::stdout().flush()?;
        let n = io::stdin().read_line(&mut line)?;
        if n == 0 {
            // EOF
            break;
        } else if line.trim().is_empty() {
            // Blank line
            continue;
        }
        let terms = tokenize_to_terms(&line);
        println!("tokenized: {:?}", terms);

        let weights: Vec<f64> = (0..terms.len()).map(|_| 1.0f64).collect();
        let children: Vec<QExpr> = terms
            .into_iter()
            .map(|t| TextExpr {
                term: t,
                ..Default::default()
            })
            .map(|te| {
                QExpr::BM25(BM25Expr {
                    child: Box::new(QExpr::Text(te)),
                    b: None,
                    k: None,
                    stats: None,
                })
            })
            .collect();

        let query = QExpr::Combine(CombineExpr { weights, children });
        // evaluation parts:
        //let mut mover = expr_to_mover(&query, &mut index)?;
        let start = Instant::now();
        let mut eval = expr_to_eval(&query, &mut index)?;

        let mut results = ScoringHeap::new(1000);
        let mut here = eval.current_document();
        let mut total: usize = 0;
        while !here.is_done() {
            eval.sync_to(here)?;
            let score = eval.score(here);
            results.offer(score, here);
            total += 1;
            here = eval.sync_to(here.next())?;
        }
        let finish = start.elapsed();
        println!("Scored {} results in {:?}", total, finish);
        let results: Vec<ScoreDoc> = results.into_vec();
        println!("{:?}", &results[..10])
    }

    Ok(())
}
