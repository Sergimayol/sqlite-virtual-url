use csv::{ReaderBuilder, StringRecord};
use std::io::Cursor;

use super::{Reader, ReaderConstructor, ReaderError};
use crate::dtypes::inference::InferredType;
use crate::dtypes::schema::{DataType, Schema, SchemaField, TypedValue, ValueLiteral};

pub struct CsvReader<'a> {
    pub data: &'a [u8],
    pub schema: Schema,
    pub bytes_read: u64,
    pub total_rows: u128,
}

impl<'a> Reader for CsvReader<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    fn total_rows(&self) -> u128 {
        self.total_rows
    }

    fn column_names(&self) -> Vec<&str> {
        self.schema.fields.iter().map(|f| f.name.as_str()).collect()
    }

    fn column_types(&self) -> Vec<String> {
        self.schema
            .fields
            .iter()
            .map(|f| format!("{:?}", f.dtype))
            .collect()
    }

    fn total_columns(&self) -> usize {
        self.schema.fields.len()
    }
}

impl<'a> ReaderConstructor<'a> for CsvReader<'a> {
    type ReaderType = CsvReader<'a>;

    fn try_new(data: &'a [u8], max_infer_rows: usize) -> Result<Self::ReaderType, ReaderError> {
        let cursor = Cursor::new(data);
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(cursor);

        let headers = reader.headers()?.clone();
        let column_count = headers.len();

        let mut inferred_types = vec![InferredType::Null; column_count];
        let mut has_nulls = vec![false; column_count];

        let mut total_rows = 0u128;
        let mut bytes_read = 0u64;

        for (i, result) in reader.records().enumerate() {
            let record = result?;
            total_rows += 1;
            bytes_read += record.as_byte_record().len() as u64;

            for (j, field) in record.iter().enumerate() {
                if field.trim().is_empty() {
                    has_nulls[j] = true;
                } else {
                    inferred_types[j].update(field);
                }
            }

            if i + 1 >= max_infer_rows {
                break;
            }
        }

        let fields = headers
            .iter()
            .enumerate()
            .map(|(i, name)| SchemaField {
                name: name.to_string(),
                dtype: inferred_types[i].to_data_type(),
                nullable: has_nulls[i],
            })
            .collect();

        Ok(CsvReader {
            data,
            schema: Schema { fields },
            bytes_read,
            total_rows,
        })
    }
}

pub struct CsvRowIterator<'a> {
    reader: csv::Reader<Cursor<&'a [u8]>>,
}

impl<'a> Iterator for CsvRowIterator<'a> {
    type Item = Result<Vec<TypedValue>, super::ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = StringRecord::new();
        match self.reader.read_record(&mut buf) {
            Ok(true) => {
                let row = buf
                    .iter()
                    .map(|s| parse_str_value(s))
                    .collect::<Vec<TypedValue>>();
                Some(Ok(row))
            }
            Ok(false) => None,
            Err(e) => Some(Err(super::ReaderError::from(e))),
        }
    }
}

fn parse_str_value(s: &str) -> TypedValue {
    if s.is_empty() {
        TypedValue {
            dtype: DataType::Null,
            value: ValueLiteral::Null,
        }
    } else if let Ok(v) = s.parse::<i64>() {
        TypedValue {
            dtype: DataType::Int,
            value: ValueLiteral::Int(v),
        }
    } else if let Ok(v) = s.parse::<f64>() {
        TypedValue {
            dtype: DataType::Real,
            value: ValueLiteral::Float(v),
        }
    } else if let Ok(v) = s.parse::<bool>() {
        TypedValue {
            dtype: DataType::Numeric,
            value: ValueLiteral::Boolean(v),
        }
    } else {
        TypedValue {
            dtype: DataType::Text,
            value: ValueLiteral::Text(s.to_string()),
        }
    }
}

impl<'a> super::IterableReader<'a> for CsvReader<'a> {
    fn iter_rows(
        &'a self,
    ) -> Box<dyn Iterator<Item = Result<Vec<TypedValue>, super::ReaderError>> + 'a> {
        let cursor = Cursor::new(self.data);
        let reader = csv::Reader::from_reader(cursor);
        Box::new(CsvRowIterator { reader })
    }
}

use std::fmt::{self, Display, Formatter};

impl<'a> Display for CsvReader<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "root")?;
        for field in &self.schema.fields {
            writeln!(
                f,
                " |-- {}: {:?} (nullable = {})",
                field.name, field.dtype, field.nullable
            )?;
        }
        Ok(())
    }
}
