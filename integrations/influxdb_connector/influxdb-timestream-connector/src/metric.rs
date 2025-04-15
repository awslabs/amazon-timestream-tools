#[derive(Debug)]
pub struct Metric {
    name: String,
    tags: Option<Vec<(String, String)>>,
    fields: Vec<(String, FieldValue)>,
    timestamp: i64,
}

#[derive(Debug, PartialEq)]
pub enum FieldValue {
    Boolean(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
}

impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FieldValue::Boolean(v) => v.fmt(f),
            FieldValue::I64(v) => v.fmt(f),
            FieldValue::U64(v) => v.fmt(f),
            FieldValue::F64(v) => v.fmt(f),
            FieldValue::String(v) => v.fmt(f),
        }
    }
}

impl Metric {
    pub fn new(
        name: String,
        tags: Option<Vec<(String, String)>>,
        fields: Vec<(String, FieldValue)>,
        timestamp: i64,
    ) -> Self {
        Metric {
            name,
            tags,
            fields,
            timestamp,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tags(&self) -> &Option<Vec<(String, String)>> {
        &self.tags
    }

    pub fn fields(&self) -> &Vec<(String, FieldValue)> {
        &self.fields
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
}
