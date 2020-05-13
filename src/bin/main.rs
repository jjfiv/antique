use antique::Error;
use std::env;
use std::io;
use std::path::Path;

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

    let reader_class = &reader.manifest.reader_class;
    loop {
        let mut line = String::new();
        print!("query> ");
        io::stdout().flush()?;
        let n = io::stdin().read_line(&mut line)?;
        if n == 0 {
            continue;
        }
        let value = reader.find_str(line.trim())?;
        if value.is_none() {
            println!("Not Found");
            continue;
        }
        let value = value.unwrap();
        if PositionsPostings::is_appropriate(reader_class) {
            let postings = PositionsPostings::new(value)?;
            println!("Postings: {:?}", postings);
            let mut iter = postings.iterator()?;
            iter.sync_to(iter.current_document)?;
            println!("Iterator: {:?}", iter);
        } else if LengthsPostings::is_appropriate(reader_class) {
            let lengths = LengthsPostings::new(value)?;
            println!("Lengths: {:?}", lengths);
        } else {
            println!("Reader TODO for: {}", reader_class);
            break;
        }

    }


    Ok(())
}
