use crate::galago::btree::*;
use crate::galago::postings::IndexPartType;
use crate::Error;
use std::fs;
use std::path::Path;

pub struct Index {
    postings: Vec<TreeReader>,
    corpus: Option<TreeReader>,
    lengths: TreeReader,
    names: TreeReader,
    names_reverse: TreeReader,
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
        let mut postings = Vec::new();
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
                IndexPartType::Positions => postings.push(reader),
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
