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

    let reader = btree::read_info(&path)?;
    println!("Location: {:?}", reader.location);
    println!("Manifest: {:?}", reader.manifest);

    for block in &reader.vocabulary.blocks {
        println!("block: {:?} ..", block.first_key);
    }

    println!("the: {:?}", reader.find_str("the")?);

    Ok(())
}
