use core::fmt;

use crate::dtypes::schema::{Schema, TypedValue};

pub mod avro_reader;
pub mod csv_reader;

#[derive(Debug)]
pub enum ReaderError {
    Io(std::io::Error),
    Csv(csv::Error),
    Avro(avro_rs::Error),
    InvalidFormat(String),
}

impl From<std::io::Error> for ReaderError {
    fn from(e: std::io::Error) -> Self {
        ReaderError::Io(e)
    }
}

impl From<csv::Error> for ReaderError {
    fn from(e: csv::Error) -> Self {
        ReaderError::Csv(e)
    }
}

pub trait Reader {
    fn schema(&self) -> &Schema;
    fn data(&self) -> &[u8];
    fn bytes_read(&self) -> u64;
    fn total_rows(&self) -> u128;
    fn column_names(&self) -> Vec<&str>;
    fn column_types(&self) -> Vec<String>;
    fn total_columns(&self) -> usize;
}

pub trait ReaderConstructor<'a> {
    type ReaderType: Reader;
    fn try_new(data: &'a [u8], max_infer_rows: usize) -> Result<Self::ReaderType, ReaderError>;
}

pub struct Row(Vec<TypedValue>);

impl std::ops::Deref for Row {
    type Target = [TypedValue];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        write!(f, "{}", rendered.join(" | "))
    }
}

pub trait IterableReader<'a>: Reader {
    // TODO: Item should be a struct packing Type + Value
    fn iter_rows(&'a self) -> Box<dyn Iterator<Item = Result<Row, ReaderError>> + 'a>;
}
