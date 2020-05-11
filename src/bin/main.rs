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

    let footer = btree::read_footer(&path)?;
    println!("Footer: {:?}", footer);

    Ok(())
}