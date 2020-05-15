use super::stemmer::Stemmer;
use crate::Error;

/// Galago defines a field as a stemmer across a field name.
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct GalagoField(Stemmer, String);

impl Default for GalagoField {
    fn default() -> Self {
        GalagoField(Stemmer::default(), "document".into())
    }
}
impl GalagoField {
    pub fn stemmer(&self) -> Stemmer {
        self.0
    }
    pub fn name(&self) -> &str {
        &self.1
    }
    pub fn from_str(field: Option<&str>) -> Result<GalagoField, Error> {
        if field.is_none() || field == Some("document") {
            return Ok(GalagoField::default());
        }
        let field = field.unwrap();
        if field.starts_with("field.") || field.starts_with("postings") {
            return GalagoField::from_file_name(field);
        }
        if !field.contains('.') {
            return Ok(GalagoField(Stemmer::default(), field.into()));
        }
        let parts: Vec<&str> = field.split('.').collect();
        match parts.len() {
            2 => Ok(GalagoField(Stemmer::from_str(parts[1])?, parts[0].into())),
            _ => Err(Error::UnknownIndexPart(field.into()))
                .map_err(|e| e.with_context("GalagoField::from_str")),
        }
    }
    pub fn from_file_name(name: &str) -> Result<GalagoField, Error> {
        Ok(if name.starts_with("field") {
            let parts: Vec<&str> = name.split(".").collect();
            match parts.len() {
                2 => GalagoField(Stemmer::Null, parts[1].to_string()),
                3 => GalagoField(
                    match parts[1] {
                        "krovetz" => Stemmer::Krovetz,
                        "porter" => Stemmer::Porter2,
                        _ => return Err(Error::UnknownIndexPart(name.into())),
                    },
                    parts[2].to_string(),
                ),
                _ => return Err(Error::UnknownIndexPart(name.into())),
            }
        } else {
            let field = "document".to_string();
            match name {
                "postings" => GalagoField(Stemmer::Null, field),
                "postings.porter" => GalagoField(Stemmer::Porter2, field),
                "postings.krovetz" => GalagoField(Stemmer::Krovetz, field),
                _ => return Err(Error::UnknownIndexPart(name.into())),
            }
        })
    }
}
