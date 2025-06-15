use super::{IterableReader, Reader, ReaderConstructor, ReaderError};
use crate::dtypes::inference::dtype_from_avro;
use crate::dtypes::schema::{Schema, SchemaField, TypedValue, ValueLiteral};
use avro_rs::{types::Value, Error as AvroError, Reader as AvroRsReader};
use std::io::Cursor;

pub struct AvroReader<'a> {
    data: &'a [u8],
    schema: Schema,
    bytes_read: u64,
    total_rows: u128,
    records: Vec<Value>,
}

impl<'a> Reader for AvroReader<'a> {
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
            .map(|f| f.dtype.to_string())
            .collect()
    }

    fn total_columns(&self) -> usize {
        self.schema.fields.len()
    }
}

impl<'a> ReaderConstructor<'a> for AvroReader<'a> {
    type ReaderType = AvroReader<'a>;

    fn try_new(data: &'a [u8], _max_infer_rows: usize) -> Result<Self::ReaderType, ReaderError> {
        let cursor = Cursor::new(data);
        let mut reader =
            AvroRsReader::new(cursor).map_err(|e| ReaderError::InvalidFormat(e.to_string()))?;

        let mut records = vec![];
        let mut total_rows = 0u128;
        let mut bytes_read = 0u64;

        for value in reader.by_ref() {
            let val = value.map_err(|e| ReaderError::InvalidFormat(e.to_string()))?;
            bytes_read += std::mem::size_of_val(&val) as u64; // approximate
            records.push(val);
            total_rows += 1;
        }

        let schema = if let Some(Value::Record(fields)) = records.first() {
            let schema_fields = fields
                .iter()
                .map(|(name, value)| SchemaField {
                    name: name.clone(),
                    dtype: dtype_from_avro(value),
                    nullable: matches!(value, Value::Null),
                })
                .collect();
            Schema {
                fields: schema_fields,
            }
        } else {
            return Err(ReaderError::InvalidFormat(
                "Empty or invalid AVRO file".into(),
            ));
        };

        Ok(AvroReader {
            data,
            schema,
            bytes_read,
            total_rows,
            records,
        })
    }
}

pub struct AvroRowIterator {
    records: std::vec::IntoIter<Value>,
}

impl Iterator for AvroRowIterator {
    type Item = Result<Vec<TypedValue>, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next().map(|value| {
            if let Value::Record(fields) = value {
                fields
                    .into_iter()
                    .map(|(_, val)| {
                        let dtype = dtype_from_avro(&val);
                        let literal = convert_avro_value(&val)?;
                        Ok(TypedValue {
                            dtype,
                            value: literal,
                        })
                    })
                    .collect()
            } else {
                Err(ReaderError::InvalidFormat("Expected record".to_string()))
            }
        })
    }
}

fn convert_avro_value(value: &Value) -> Result<ValueLiteral, ReaderError> {
    match value {
        Value::Null => Ok(ValueLiteral::Null),
        Value::Boolean(b) => Ok(ValueLiteral::Boolean(*b)),
        Value::Int(i) => Ok(ValueLiteral::Int(*i as i64)),
        Value::Long(i) => Ok(ValueLiteral::Int(*i)),
        Value::TimeMillis(i) => Ok(ValueLiteral::Int(*i as i64)),
        Value::TimeMicros(i) => Ok(ValueLiteral::Int(*i)),
        Value::Date(i) => Ok(ValueLiteral::Int(*i as i64)),
        Value::TimestampMillis(i) => Ok(ValueLiteral::Int(*i)),
        Value::TimestampMicros(i) => Ok(ValueLiteral::Int(*i)),
        Value::Duration(d) => {
            let bytes: Vec<u8> = Vec::from(<[u8; 12]>::from(*d));
            Ok(ValueLiteral::Blob(bytes))
        }
        Value::Float(f) => Ok(ValueLiteral::Float(*f as f64)),
        Value::Double(f) => Ok(ValueLiteral::Float(*f)),
        Value::Decimal(decimal) => {
            let bytes: Vec<u8> = decimal
                .try_into()
                .map_err(|e| ReaderError::InvalidFormat(AvroError::to_string(&e)))?;
            Ok(ValueLiteral::Blob(bytes))
        }
        Value::Bytes(b) => Ok(ValueLiteral::Blob(b.clone())),
        Value::Fixed(_, b) => Ok(ValueLiteral::Blob(b.clone())),
        Value::String(s) => Ok(ValueLiteral::Text(s.clone())),
        Value::Enum(_, s) => Ok(ValueLiteral::Text(s.clone())),
        Value::Uuid(u) => Ok(ValueLiteral::Text(u.to_string())),
        Value::Union(inner) => convert_avro_value(inner),
        Value::Array(_) | Value::Map(_) | Value::Record(_) => Err(ReaderError::InvalidFormat(
            "Complex types not supported".into(),
        )),
    }
}

impl<'a> IterableReader<'a> for AvroReader<'a> {
    fn iter_rows(&'a self) -> Box<dyn Iterator<Item = Result<Vec<TypedValue>, ReaderError>> + 'a> {
        Box::new(AvroRowIterator {
            records: self.records.clone().into_iter(),
        })
    }
}

use std::fmt::{self, Display, Formatter};

impl<'a> Display for AvroReader<'a> {
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
