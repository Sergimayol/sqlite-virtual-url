use avro_rs::{types::Value, Reader};
use polars::prelude::*;
use reqwest::blocking::get;
use sqlite_loadable::{
    api, define_virtual_table,
    table::{BestIndexError, ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    Result,
};
use sqlite_loadable::{prelude::*, Error};
use std::{collections::HashMap, mem, os::raw::c_int};

#[derive(Debug)]
struct ParsedArgs {
    named: HashMap<String, String>,
    positional: Vec<String>,
}

fn parse_args(args: Vec<String>) -> ParsedArgs {
    let mut named = HashMap::new();
    let mut positional = Vec::new();

    for arg in args {
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].trim().to_string().to_uppercase();
            let value = arg[eq_pos + 1..]
                .trim_matches(|c| c == '\'' || c == '"')
                .to_string();
            named.insert(key, value);
        } else {
            positional.push(arg.trim_matches(|c| c == '\'' || c == '"').to_string());
        }
    }

    ParsedArgs { named, positional }
}

#[derive(Debug)]
enum VTabDataFormats {
    CSV,
    AVRO,
    PARQUET,
    JSON,
    JSONL,
}

fn get_format(fmt: &str) -> Result<VTabDataFormats> {
    match fmt.to_uppercase().as_str() {
        "CSV" => Ok(VTabDataFormats::CSV),
        "AVRO" => Ok(VTabDataFormats::AVRO),
        "PARQUET" => Ok(VTabDataFormats::PARQUET),
        "JSON" => Ok(VTabDataFormats::JSON),
        "JSONL" => Ok(VTabDataFormats::JSONL),
        "NDJSON" => Ok(VTabDataFormats::JSONL),
        _ => Err(Error::new_message(&format!("Unknown data format: {}", fmt))),
    }
}

struct AvroReader<'a> {
    data: &'a [u8],
    series: Option<Vec<Series>>,
}

impl<'a> AvroReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, series: None }
    }

    fn finish(mut self) -> PolarsResult<DataFrame> {
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

        let series = col_data
            .into_iter()
            .map(|(col, values)| Series::new(&col, values))
            .collect();

        self.series = Some(series);

        DataFrame::new(self.series.unwrap())
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
            Value::TimestampMillis(ms) => AnyValue::Datetime(ms, TimeUnit::Milliseconds, &None),
            Value::TimestampMicros(us) => AnyValue::Datetime(us, TimeUnit::Microseconds, &None),

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

#[repr(C)]
struct UrlTable {
    base: sqlite3_vtab,
    df: DataFrame,
    headers: Vec<String>,
}

impl<'vtab> VTab<'vtab> for UrlTable {
    type Aux = ();
    type Cursor = UrlCursor;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        vt_args: VTabArguments,
    ) -> Result<(String, Self)> {
        let args = vt_args.arguments;
        if args.len() < 1 {
            return Err(Error::new_message("URL argument missing"));
        }

        let parsed_args = parse_args(args);
        let url = parsed_args
            .named
            .get("URL")
            .cloned()
            .or_else(|| parsed_args.positional.get(0).cloned())
            .ok_or_else(|| Error::new_message("No URL provided"))?;

        let format = parsed_args
            .named
            .get("FORMAT")
            .cloned()
            .or_else(|| parsed_args.positional.get(1).cloned())
            .ok_or_else(|| Error::new_message("No data format specified"))
            .and_then(|f| get_format(&f))?;

        let resp = get(&url)
            .map_err(|e| Error::new_message(&format!("HTTP error: {}", e)))?
            .bytes()
            .map_err(|e| Error::new_message(&format!("Read error: {}", e)))?;

        let df = match format {
            VTabDataFormats::CSV => CsvReader::new(std::io::Cursor::new(resp))
                .finish()
                .map_err(|e| Error::new_message(&format!("CSV parse error: {}", e)))?,
            VTabDataFormats::PARQUET => ParquetReader::new(std::io::Cursor::new(resp))
                .finish()
                .map_err(|e| Error::new_message(&format!("Parquet parse error: {}", e)))?,
            VTabDataFormats::AVRO => AvroReader::new(resp.as_ref())
                .finish()
                .map_err(|e| Error::new_message(&format!("Avro build error: {}", e)))?,
            VTabDataFormats::JSON => JsonReader::new(std::io::Cursor::new(resp))
                .with_json_format(JsonFormat::Json)
                .finish()
                .map_err(|e| Error::new_message(&format!("JSON build error: {}", e)))?,
            VTabDataFormats::JSONL => JsonReader::new(std::io::Cursor::new(resp))
                .with_json_format(JsonFormat::JsonLines)
                .finish()
                .map_err(|e| Error::new_message(&format!("JSON build error: {}", e)))?,
        };

        let headers = df
            .get_column_names_owned()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let schema = format!(
            "CREATE TABLE x({});",
            df.get_column_names()
                .iter()
                .map(|h| format!("\"{}\"", h))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        Ok((schema, UrlTable { base, df, headers }))
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut used_cols = Vec::new();
        let mut used_ops = Vec::new();

        for (_i, constraint) in info.constraints().iter_mut().enumerate() {
            if constraint.usable() {
                let op = match constraint.op() {
                    Some(ConstraintOperator::EQ) => "=",
                    Some(ConstraintOperator::GT) => ">",
                    Some(ConstraintOperator::LT) => "<",
                    Some(ConstraintOperator::GE) => ">=",
                    Some(ConstraintOperator::LE) => "<=",
                    Some(ConstraintOperator::NE) => "!=",
                    _ => continue,
                };

                constraint.set_argv_index((used_cols.len() + 1) as i32); // 1-based
                used_cols.push(constraint.column_idx());
                used_ops.push(op);
            }
        }

        let idx_str = used_cols
            .iter()
            .zip(used_ops.iter())
            .map(|(col, op)| format!("{}{}", col, op))
            .collect::<Vec<String>>()
            .join(",");

        let _ = info.set_idxstr(&idx_str);
        info.set_idxnum(used_cols.len() as i32);

        Ok(())
    }

    fn open(&mut self) -> Result<UrlCursor> {
        Ok(UrlCursor::new(self.df.clone()))
    }
}

#[repr(C)]
struct UrlCursor {
    base: sqlite3_vtab_cursor,
    row_idx: usize,
    filtered_df: DataFrame,
}

impl UrlCursor {
    fn new(df: DataFrame) -> UrlCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        UrlCursor {
            base,
            row_idx: 0,
            filtered_df: df,
        }
    }
}

impl VTabCursor for UrlCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        idx_str: Option<&str>,
        args: &[*mut sqlite3_value],
    ) -> Result<()> {
        let vtab: &UrlTable = unsafe { &*(self.base.pVtab as *mut UrlTable) };
        let mut lf = vtab.df.clone().lazy();

        if let Some(idx_str) = idx_str {
            for (i, part) in idx_str.split(',').enumerate() {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let (col_str, op) = if trimmed.ends_with('=') {
                    trimmed.split_at(trimmed.len() - 1)
                } else {
                    trimmed.split_at(trimmed.len())
                };

                if col_str.is_empty() {
                    continue;
                }

                let col_idx: usize = match col_str.parse::<usize>() {
                    Ok(idx) => idx,
                    Err(_) => continue,
                };

                let col_name = &vtab.headers[col_idx];
                let col_type: &DataType = &vtab.df.dtypes()[col_idx];
                let arg: *mut sqlite3_value = args[i];

                let filter_value = match col_type {
                    DataType::Boolean => {
                        let val = api::value_int(&arg);
                        lit(val != 0)
                    }
                    DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64 => {
                        let val = api::value_int64(&arg);
                        lit(val)
                    }
                    DataType::Float32 | DataType::Float64 => {
                        let val = api::value_double(&arg);
                        lit(val)
                    }
                    DataType::String => {
                        let val = api::value_text(&arg)?;
                        lit(val.to_string())
                    }
                    _ => {
                        let val = api::value_text(&arg)?;
                        lit(val.to_string())
                    }
                };

                let filter_expr = match op {
                    "=" => col(col_name).eq(filter_value),
                    ">" => col(col_name).gt(filter_value),
                    "<" => col(col_name).lt(filter_value),
                    ">=" => col(col_name).gt_eq(filter_value),
                    "<=" => col(col_name).lt_eq(filter_value),
                    "!" => col(col_name).neq(filter_value),
                    _ => continue,
                };

                lf = lf.filter(filter_expr);
            }
        }

        self.filtered_df = lf
            .collect()
            .map_err(|e| Error::new_message(&format!("Polars collect error: {}", e)))?;
        self.row_idx = 0;

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_idx += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_idx >= self.filtered_df.height()
    }

    fn column(&self, ctx: *mut sqlite3_context, i: c_int) -> Result<()> {
        let col = self
            .filtered_df
            .select_at_idx(i as usize)
            .ok_or_else(|| Error::new_message("Invalid column index"))?;
        let val = col.get(self.row_idx);

        match val {
            Ok(AnyValue::Int64(v)) => api::result_int64(ctx, v),
            Ok(AnyValue::Int32(v)) => api::result_int64(ctx, v as i64),
            Ok(AnyValue::Float64(v)) => api::result_double(ctx, v),
            Ok(AnyValue::Float32(v)) => api::result_double(ctx, v as f64),
            Ok(AnyValue::Boolean(v)) => api::result_int(ctx, if v { 1 } else { 0 }),
            Ok(AnyValue::String(v)) => api::result_text(ctx, v)?,
            Ok(AnyValue::StringOwned(v)) => api::result_text(ctx, &v)?,
            Ok(AnyValue::Null) => api::result_null(ctx),
            Ok(v) => api::result_text(ctx, &v.to_string())?,
            Err(_) => api::result_null(ctx),
        }

        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_idx as i64)
    }
}

#[sqlite_entrypoint]
pub fn sqlite3_url_init(db: *mut sqlite3) -> Result<()> {
    define_virtual_table::<UrlTable>(db, "url", None)?;
    Ok(())
}
