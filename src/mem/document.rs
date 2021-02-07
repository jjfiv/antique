use crate::galago::tokenizer::tokenize_to_terms;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TermId(pub u32);

/// Example: <body>, <head>, <title>, etc.
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FieldId(pub u16);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum TextOptions {
    Docs,
    Counts,
    Positions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    /// Sparse boolean as well as other categorical.
    Categorical,
    /// Words are considered sparse features.
    Textual(TextOptions, TokenizerStyle),
    /// Bitmap (expect dense!)
    Boolean,
    /// One int for every document.
    DenseInt,
    /// One float for every document.
    DenseFloat,
    /// An int for some documents; keys in a posting-list.
    SparseInt,
    /// A float for some documents; skippable.
    SparseFloat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TokenizerStyle {
    Whitespace,
    Galago,
    Unicode,
}
impl TokenizerStyle {
    pub fn process(&self, input: &str) -> Vec<String> {
        match self {
            TokenizerStyle::Whitespace => input
                .to_lowercase()
                .split_whitespace()
                .map(|str| str.to_owned())
                .collect(),
            TokenizerStyle::Galago => tokenize_to_terms(input),
            TokenizerStyle::Unicode => todo!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub kind: FieldType,
    pub stored: bool,
}
impl FieldMetadata {
    pub fn new(kind: FieldType, stored: bool) -> Self {
        Self { kind, stored }
    }

    pub(crate) fn is_dense(&self) -> bool {
        match self.kind {
            FieldType::Categorical | FieldType::Textual(_, _) => false,
            FieldType::Boolean | FieldType::DenseInt | FieldType::DenseFloat => true,
            FieldType::SparseInt | FieldType::SparseFloat => false,
        }
    }
}

// TODO: make these Cows?
#[derive(Clone)]
pub enum FieldValue {
    Categorical(String),
    Textual(String),
    Integer(u32),
    Floating(f32),
}

impl FieldValue {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::Categorical(x) | FieldValue::Textual(x) => Some(x.as_ref()),
            FieldValue::Integer(_) | FieldValue::Floating(_) => None,
        }
    }
}
#[derive(Clone)]
pub struct DocField {
    pub field: FieldId,
    pub value: FieldValue,
}
impl DocField {
    pub fn new(field: FieldId, value: FieldValue) -> Self {
        Self { field, value }
    }
}

#[derive(Default)]
pub struct DocFields {
    fields: Vec<DocField>,
}
impl DocFields {
    pub fn as_ref(&self) -> &[DocField] {
        self.fields.as_ref()
    }
    /// Add a 'categorical' field to this document; text is considered atomic; not split to words.
    /// This factory is written to support chaining.
    pub fn categorical(&mut self, field: FieldId, text: String) -> &mut Self {
        self.fields
            .push(DocField::new(field, FieldValue::Categorical(text)));
        self
    }
    /// Add a 'textual' field to this document; text is considered to be prose; split to words somehow.
    /// This factory is written to support chaining.
    pub fn textual(&mut self, field: FieldId, text: String) -> &mut Self {
        self.fields
            .push(DocField::new(field, FieldValue::Textual(text)));
        self
    }
}
