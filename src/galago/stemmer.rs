use crate::Error;

#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy)]
pub enum Stemmer {
    Krovetz,
    Porter2,
    Null,
}
impl Default for Stemmer {
    fn default() -> Self {
        // Until we have a stemmer...
        Self::Null
    }
}
impl Stemmer {
    pub fn from_str(name: &str) -> Result<Stemmer, Error> {
        Ok(match name {
            "krovetz" | "org.lemurproject.galago.core.parse.stem.KrovetzStemmer" => {
                Stemmer::Krovetz
            }
            "porter" | "org.lemurproject.galago.core.parse.stem.Porter2Stemmer" => Stemmer::Porter2,
            "" | "org.lemurproject.galago.core.parse.stem.NullStemmer" => Stemmer::Null,
            other => return Err(Error::UnknownStemmer(other.into())),
        })
    }
    pub fn from_class_name(class_name: Option<&str>) -> Result<Stemmer, Error> {
        Ok(match class_name {
            Some("org.lemurproject.galago.core.parse.stem.KrovetzStemmer") => Stemmer::Krovetz,
            Some("org.lemurproject.galago.core.parse.stem.Porter2Stemmer") => Stemmer::Porter2,
            Some("org.lemurproject.galago.core.parse.stem.NullStemmer") => Stemmer::Null,
            None => Stemmer::Null,
            Some(other) => return Err(Error::UnknownStemmer(other.into())),
        })
    }
}
