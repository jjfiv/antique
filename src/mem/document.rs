use crate::{DocId, HashMap};
use std::collections::BTreeMap;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TermId(pub u32);

/// Example: <body>, <head>, <title>, etc.
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(pub u16);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TextOptions {
    Docs,
    Counts,
    Positions,
}

pub enum FieldType {
    /// Sparse boolean as well as other categorical.
    Categorical,
    /// Words are considered sparse features.
    Textual(TextOptions),
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
pub struct FieldMetadata {
    index: FieldId,
    kind: FieldType,
    stored: bool,
}
impl FieldMetadata {
    fn dense(&self) -> bool {
        match self.kind {
            FieldType::Categorical | FieldType::Textual(_) => false,
            FieldType::Boolean | FieldType::DenseInt | FieldType::DenseFloat => true,
            FieldType::SparseInt | FieldType::SparseFloat => false,
        }
    }
}

// TODO: make these Cows?
pub enum FieldValue {
    Categorical(String),
    Textual(String),
    Integer(u32),
    Floating(f32),
}
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
