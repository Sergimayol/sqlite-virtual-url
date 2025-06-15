use crate::dtypes::schema::Schema;

pub mod csv_reader;
// pub mod avro;
// pub mod parquet;

#[derive(Debug)]
pub enum ReaderError {
    Io(std::io::Error),
    Csv(csv::Error),
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

pub trait IterableReader<'a>: Reader {
    fn iter_rows(&'a self) -> Box<dyn Iterator<Item = Result<Vec<String>, ReaderError>> + 'a>;
}
