#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct CountStats {
    pub collection_frequency: u64,
    pub document_frequency: u64,
    pub collection_length: u64,
    pub document_count: u64,
}

impl CountStats {
    pub fn average_doc_length(&self) -> f32 {
        if self.document_count == 0 {
            0.0
        } else {
            let cf = self.collection_length as f64;
            let dc = self.document_count as f64;
            (cf / dc) as f32
        }
    }
}
