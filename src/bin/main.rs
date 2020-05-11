use antique::Error;
use std::env;
use std::path::Path;

use antique::galago_btree as btree;

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

    let footer = btree::read_info(&path)?;
    println!("Footer: {:?}", footer);

    let vocab = btree::read_vocabulary(&footer)?;
    for block in vocab.blocks {
        println!("block: {:?} .. {:?}", block.first_key, block.next_block_key);
    }

    Ok(())
}
