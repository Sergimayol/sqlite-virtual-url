use avro_rs::{types::Value, Reader};
use polars::prelude::*;
use std::collections::HashMap;

pub struct AvroReader<'a> {
    data: &'a [u8],
}

impl<'a> AvroReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn finish(self) -> PolarsResult<DataFrame> {
        let reader = Reader::new(self.data).unwrap();

        let mut col_data: HashMap<String, Vec<AnyValue>> = HashMap::new();

        for record in reader {
            let value = record.unwrap();

            if let Value::Record(fields) = value {
                for (k, v) in fields {
                    col_data
                        .entry(k.clone())
                        .or_insert_with(Vec::new)
                        .push(Self::map_value_to_any(v));
                }
            }
        }

        let columns = col_data
            .into_iter()
            .map(|(col, values)| Series::new(col.into(), values))
            .map(|s| Column::new(s.name().clone(), s))
            .collect::<Vec<_>>();

        DataFrame::new(columns)
    }

    fn map_value_to_any(value: Value) -> AnyValue<'a> {
        match value {
            Value::String(s) => AnyValue::StringOwned(s.into()),
            Value::Int(i) => AnyValue::Int32(i),
            Value::Long(l) => AnyValue::Int64(l),
            Value::Float(f) => AnyValue::Float32(f),
            Value::Double(d) => AnyValue::Float64(d),
            Value::Boolean(b) => AnyValue::Boolean(b),
            Value::Null => AnyValue::Null,
            Value::Bytes(b) => AnyValue::BinaryOwned(b.into()),

            Value::Date(days) => {
                let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_days(chrono::Days::new(days as u64));
                match date {
                    Some(d) => {
                        let epoch_days = d
                            .signed_duration_since(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                            )
                            .num_days() as i32;
                        AnyValue::Date(epoch_days)
                    }
                    None => AnyValue::Null,
                }
            }

            Value::TimeMillis(ms) => AnyValue::Int32(ms),
            Value::TimeMicros(us) => AnyValue::Int64(us),
            Value::TimestampMillis(ms) => AnyValue::Datetime(ms, TimeUnit::Milliseconds, None),
            Value::TimestampMicros(us) => AnyValue::Datetime(us, TimeUnit::Microseconds, None),

            Value::Uuid(s) => AnyValue::StringOwned(s.to_string().into()),
            Value::Fixed(_, bytes) => AnyValue::BinaryOwned(bytes.into()),
            Value::Enum(_, symbol) => AnyValue::StringOwned(symbol.into()),

            Value::Decimal(decimal) => AnyValue::StringOwned(format!("{:?}", decimal).into()),

            Value::Array(arr) => {
                let repr = format!("{:?}", arr);
                AnyValue::StringOwned(repr.into())
            }

            Value::Map(map) => {
                let repr = format!("{:?}", map);
                AnyValue::StringOwned(repr.into())
            }

            Value::Record(fields) => {
                let repr = format!("{:?}", fields);
                AnyValue::StringOwned(repr.into())
            }

            Value::Duration(duration) => {
                let repr = format!("{:?}", duration.millis());
                AnyValue::StringOwned(repr.into())
            }

            Value::Union(boxed_value) => Self::map_value_to_any(*boxed_value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use avro_rs::{Schema, Writer};

    #[test]
    fn test_avroreader_single_record() {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        let records = vec![Value::Record(vec![
            ("id".to_string(), Value::Int(1)),
            ("name".to_string(), Value::String("Bob".to_string())),
        ])];

        let mut writer = Writer::new(&schema, Vec::new());
        for r in records {
            writer.append(r).unwrap();
        }
        let avro_data = writer.into_inner().unwrap();

        let reader = AvroReader::new(&avro_data);
        let df = reader.finish().unwrap();

        assert_eq!(df.shape(), (1, 2)); // 1 row, 2 columns

        let names = df.column("name").unwrap().str().unwrap();
        let ids = df.column("id").unwrap().i32().unwrap();

        assert_eq!(names.get(0), Some("Bob"));
        assert_eq!(ids.get(0), Some(1));
    }

    #[test]
    fn test_avroreader_multiple_records() {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        let records = vec![
            Value::Record(vec![
                ("id".to_string(), Value::Int(1)),
                ("name".to_string(), Value::String("Bob".to_string())),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(2)),
                ("name".to_string(), Value::String("Carol".to_string())),
            ]),
        ];

        let mut writer = Writer::new(&schema, Vec::new());
        for r in records {
            writer.append(r).unwrap();
        }
        let avro_data = writer.into_inner().unwrap();

        let reader = AvroReader::new(&avro_data);
        let df = reader.finish().unwrap();

        assert_eq!(df.shape(), (2, 2)); // 2 rows, 2 columns

        let names = df.column("name").unwrap().str().unwrap();
        let ids = df.column("id").unwrap().i32().unwrap();

        assert_eq!(names.get(0), Some("Bob"));
        assert_eq!(names.get(1), Some("Carol"));
        assert_eq!(ids.get(0), Some(1));
        assert_eq!(ids.get(1), Some(2));
    }
}
