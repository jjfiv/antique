use antique::Error;
use std::env;
use std::io;
use std::path::Path;

use antique::scoring::*;
use antique::galago_btree as btree;
use antique::galago_postings::*;
use io::Write;

fn main() -> Result<(), Error> {
    let input = env::args()
        .nth(1)
        .unwrap_or_else(|| "data/index.galago/lengths".to_string());
    let path = Path::new(&input);

    if !btree::file_matches(&path)? {
        println!("{} is NOT a galago btree!", input);
        return Ok(());
    }
    println!("{} is a galago_btree!", input);

    let reader = btree::read_info(&path)?;
    println!("Location: {:?}", reader.location);
    println!("Manifest: {:?}", reader.manifest);

    for block in reader.vocabulary.blocks.iter().take(20) {
        println!("block: {:?} ..", block.first_key);
    }
    println!("num_blocks: {}.", reader.vocabulary.blocks.len());

    println!("the: {:?}", reader.find_str("the")?);

    let part_type = IndexPartType::from_reader_class(&reader.manifest.reader_class)?;
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
        let value = reader.find_str(line.trim())?;
        if value.is_none() {
            println!("Not Found");
            continue;
        }
        let value = value.unwrap();
        match part_type {
            IndexPartType::Positions => {
                let postings = PositionsPostings::new(value)?;
                println!("Postings: {:?}", postings);
                let mut iter = postings.iterator()?;
                iter.sync_to(iter.current_document)?;
                println!("Iterator: {:?}", iter);
            }
            IndexPartType::Lengths => {
                let lengths = LengthsPostings::new(value)?;
                println!("Lengths: {:?}", lengths);
            }
            _ => {
                println!("Reader TODO for: {:?}", part_type);
                break;
            }
        }
    }


    Ok(())
}
