use crate::{DocId, Error};

pub trait SyncTo {
    fn current_document(&self) -> DocId;
    fn sync_to(&mut self, document: DocId) -> Result<DocId, Error>;
}

pub trait Movement {
    fn is_done(&self) -> bool;
    fn move_past(&mut self) -> Result<DocId, Error>;
}

impl<T> Movement for T
where
    T: SyncTo,
{
    fn is_done(&self) -> bool {
        self.current_document().is_done()
    }
    fn move_past(&mut self) -> Result<DocId, Error> {
        self.sync_to(DocId(self.current_document().0 + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct VecMovement {
        position: usize,
        docs: Vec<u32>,
    }
    impl SyncTo for VecMovement {
        fn current_document(&self) -> DocId {
            if self.position < self.docs.len() {
                DocId(self.docs[self.position] as u64)
            } else {
                DocId::no_more()
            }
        }
        fn sync_to(&mut self, document: DocId) -> Result<DocId, Error> {
            while self.current_document() < document {
                self.position += 1;
            }
            Ok(self.current_document())
        }
    }
}
