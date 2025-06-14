use std::{
    error::Error,
    ffi::{CStr, CString},
};

use chrono::{DateTime, NaiveDate};
use libsqlite3_sys::sqlite3_errstr;
use polars::{
    frame::DataFrame,
    prelude::{AnyValue, DataType},
};
use sqlite_loadable::{
    ext::{
        sqlite3, sqlite3_stmt, sqlite3ext_column_text, sqlite3ext_finalize, sqlite3ext_prepare_v2,
        sqlite3ext_step,
    },
    SQLITE_DONE, SQLITE_ROW,
};

#[derive(Debug, PartialEq)]
pub enum StorageOpts {
    TEMP,
    DISK,
}

pub fn get_storage(storage: &str) -> Result<StorageOpts, Box<dyn Error>> {
    match storage.trim().to_uppercase().as_str() {
        "TEMP" => Ok(StorageOpts::TEMP),
        "DISK" => Ok(StorageOpts::DISK),
        _ => Err(format!("Not a valid storage option: {}", storage).into()),
    }
}

/// https://sqlite.org/c3ref/column_blob.html
#[derive(Debug, PartialEq)]
pub enum SQLiteDataTypes {
    BLOB,
    REAL,
    INT,
    NUMERIC,
    TEXT,
    NULL,
}

impl SQLiteDataTypes {
    /// Reference [here](https://www.sqlite.org/datatype3.html#:~:text=will%20be%20INTEGER.-,3.1.1.%20Affinity%20Name%20Examples,-The%20following%20table).
    pub fn as_str(&self) -> &'static str {
        match self {
            SQLiteDataTypes::BLOB => "BLOB",
            SQLiteDataTypes::REAL => "REAL",
            SQLiteDataTypes::INT => "INTEGER",
            SQLiteDataTypes::TEXT => "TEXT",
            SQLiteDataTypes::NULL => "NULL",
            SQLiteDataTypes::NUMERIC => "NUMERIC",
        }
    }
}

pub fn df_dtype_to_sqlite_dtype(df_dtype: &DataType) -> SQLiteDataTypes {
    match df_dtype {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Int128 => SQLiteDataTypes::INT,
        DataType::Float32 | DataType::Float64 => SQLiteDataTypes::REAL,
        DataType::String => SQLiteDataTypes::TEXT,
        DataType::Null => SQLiteDataTypes::NULL,
        DataType::Binary => SQLiteDataTypes::BLOB,
        DataType::Boolean => SQLiteDataTypes::NUMERIC,
        DataType::Datetime(_, _) => SQLiteDataTypes::NUMERIC,
        DataType::Date => SQLiteDataTypes::NUMERIC,
        _ => SQLiteDataTypes::TEXT,
    }
}

fn df_value_to_sqlite_value(value: AnyValue) -> String {
    match value {
        AnyValue::Null => "NULL".to_string(),
        AnyValue::String(s) => format!("'{}'", escape_sql_string(s)),
        AnyValue::Boolean(b) => (if b { "1" } else { "0" }).to_string(),
        AnyValue::Int8(i) => i.to_string(),
        AnyValue::Int16(i) => i.to_string(),
        AnyValue::Int32(i) => i.to_string(),
        AnyValue::Int64(i) => i.to_string(),
        AnyValue::UInt8(i) => i.to_string(),
        AnyValue::UInt16(i) => i.to_string(),
        AnyValue::UInt32(i) => i.to_string(),
        AnyValue::UInt64(i) => i.to_string(),
        AnyValue::Float32(f) => f.to_string(),
        AnyValue::Float64(f) => f.to_string(),
        AnyValue::Date(i) => {
            let date = NaiveDate::from_num_days_from_ce_opt(i)
                .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            format!("'{}'", date)
        }
        AnyValue::Datetime(ms, _, _) => {
            let dt = DateTime::from_timestamp_millis(ms)
                .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());
            format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S"))
        }
        other => format!("'{}'", escape_sql_string(&other.to_string())),
    }
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

pub fn generate_inserts_from_dataframe(
    df: &DataFrame,
    module_name: &str,
    table_name: &str,
    columns_def: String,
    batch_size: usize,
) -> Vec<String> {
    let total_rows = df.height();
    let mut inserts = Vec::new();

    for batch_start in (0..total_rows).step_by(batch_size) {
        let batch_end = usize::min(batch_start + batch_size, total_rows);
        let mut values_sql = Vec::new();

        for row_idx in batch_start..batch_end {
            let row_values: Vec<String> = df
                .get_columns()
                .iter()
                .map(|series| {
                    match series.get(row_idx) {
                        Ok(val) => df_value_to_sqlite_value(val),
                        Err(_) => "NULL".to_string(), // fallback
                    }
                })
                .collect();

            values_sql.push(format!("({})", row_values.join(", ")));
        }

        let values_clause = values_sql.join(",\n");

        let insert_statement = format!(
            "INSERT INTO \"{}.{}_data\" ({}) VALUES\n{};",
            module_name, table_name, columns_def, values_clause
        );

        inserts.push(insert_statement);
    }

    inserts
}

type SqliteResult<T> = Result<T, Box<dyn std::error::Error>>;

pub struct Statement {
    raw: *mut sqlite3_stmt,
    finalized: bool,
}
impl Statement {
    pub fn build(db: *mut sqlite3, sql: &str) -> SqliteResult<Self> {
        let sql_c = CString::new(sql)?;
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3ext_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut())
        };
        if rc != 0 {
            let err_msg = unsafe {
                let c_str = sqlite3_errstr(rc);
                CStr::from_ptr(c_str).to_string_lossy().into_owned()
            };
            Err(format!("Error building statement. (code: {rc}): {err_msg}").into())
        } else {
            Ok(Self {
                raw: stmt,
                finalized: false,
            })
        }
    }

    pub fn execute(self) -> SqliteResult<Self> {
        let rc = unsafe { sqlite3ext_step(self.raw) };
        if rc != SQLITE_DONE && rc != SQLITE_ROW {
            let err_msg = unsafe {
                let c_str = sqlite3_errstr(rc);
                CStr::from_ptr(c_str).to_string_lossy().into_owned()
            };
            Err(format!("Error executing statement (code: {rc}): {err_msg}").into())
        } else {
            Ok(self)
        }
    }

    pub fn fetch(self, col_count: i32) -> SqliteResult<Vec<Vec<String>>> {
        let mut results = Vec::new();

        loop {
            let rc = unsafe { sqlite3ext_step(self.raw) };

            if rc == SQLITE_DONE {
                break;
            } else if rc != SQLITE_ROW {
                let err_msg = unsafe {
                    let c_str = sqlite3_errstr(rc);
                    CStr::from_ptr(c_str).to_string_lossy().into_owned()
                };
                return Err(format!("Error fetching row (code: {rc}): {err_msg}").into());
            }

            let mut row = Vec::new();
            for i in 0..col_count {
                let text_ptr = unsafe { sqlite3ext_column_text(self.raw, i) };
                if text_ptr.is_null() {
                    row.push("NULL".to_string());
                } else {
                    let c_str = unsafe { CStr::from_ptr(text_ptr as *const i8) };
                    row.push(c_str.to_string_lossy().into_owned());
                }
            }

            results.push(row);
        }

        Ok(results)
    }

    pub fn finalize(mut self) -> SqliteResult<()> {
        if self.finalized {
            return Ok(());
        }
        let rc = unsafe { sqlite3ext_finalize(self.raw) };
        self.finalized = true;
        std::mem::forget(self);
        if rc != 0 {
            let err_msg = unsafe {
                let c_str = sqlite3_errstr(rc);
                CStr::from_ptr(c_str).to_string_lossy().into_owned()
            };
            Err(format!("Error finalizing statement(code: {rc}): {err_msg}").into())
        } else {
            Ok(())
        }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.finalized {
            unsafe {
                sqlite3ext_finalize(self.raw);
            }
            self.finalized = true;
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum VTabDataFormats {
    CSV,
    AVRO,
    PARQUET,
    JSON,
    JSONL,
}

impl VTabDataFormats {
    pub fn as_str(&self) -> &'static str {
        match self {
            VTabDataFormats::CSV => "CSV",
            VTabDataFormats::AVRO => "AVRO",
            VTabDataFormats::PARQUET => "PARQUET",
            VTabDataFormats::JSON => "JSON",
            VTabDataFormats::JSONL => "JSONL",
        }
    }
}

pub fn get_format(fmt: &str) -> Result<VTabDataFormats, Box<dyn Error>> {
    match fmt.to_uppercase().as_str() {
        "CSV" => Ok(VTabDataFormats::CSV),
        "AVRO" => Ok(VTabDataFormats::AVRO),
        "PARQUET" => Ok(VTabDataFormats::PARQUET),
        "JSON" => Ok(VTabDataFormats::JSON),
        "JSONL" => Ok(VTabDataFormats::JSONL),
        "NDJSON" => Ok(VTabDataFormats::JSONL),
        _ => Err(format!("Unknown data format: {}", fmt).into()),
    }
}

#[cfg(test)]
mod types_tests {
    use polars::prelude::TimeUnit;

    use super::*;

    #[test]
    fn test_get_storage_temp_uppercase() {
        let result = get_storage("TEMP");
        assert_eq!(result.unwrap(), StorageOpts::TEMP);
    }

    #[test]
    fn test_get_storage_temp_lowercase() {
        let result = get_storage("temp");
        assert_eq!(result.unwrap(), StorageOpts::TEMP);
    }

    #[test]
    fn test_get_storage_disk_uppercase() {
        let result = get_storage("DISK");
        assert_eq!(result.unwrap(), StorageOpts::DISK);
    }

    #[test]
    fn test_get_storage_disk_mixed_case() {
        let result = get_storage("DisK");
        assert_eq!(result.unwrap(), StorageOpts::DISK);
    }

    #[test]
    fn test_get_storage_invalid() {
        let result = get_storage("mem");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Not a valid storage option: mem"
        );
    }

    #[test]
    fn test_get_storage_empty() {
        let result = get_storage("");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Not a valid storage option: "
        );
    }

    #[test]
    fn test_get_storage_with_whitespace() {
        let result = get_storage(" DISK ");
        assert_eq!(result.unwrap(), StorageOpts::DISK);
    }

    #[test]
    fn test_get_storage_with_newline() {
        let result = get_storage("TEMP\n");
        assert_eq!(result.unwrap(), StorageOpts::TEMP);
    }

    #[test]
    fn test_get_storage_with_tab() {
        let result = get_storage("\tDISK\t");
        assert_eq!(result.unwrap(), StorageOpts::DISK);
    }

    #[test]
    fn test_get_storage_with_carriage_return() {
        let result = get_storage("TEMP\r\n");
        assert_eq!(result.unwrap(), StorageOpts::TEMP);
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_int() {
        let int_types = vec![
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Int128,
        ];
        for dt in int_types {
            assert_eq!(df_dtype_to_sqlite_dtype(&dt), SQLiteDataTypes::INT);
        }
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_real() {
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Float32),
            SQLiteDataTypes::REAL
        );
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Float64),
            SQLiteDataTypes::REAL
        );
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_text() {
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::String),
            SQLiteDataTypes::TEXT
        );
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_null() {
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Null),
            SQLiteDataTypes::NULL
        );
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_blob() {
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Binary),
            SQLiteDataTypes::BLOB
        );
    }

    #[test]
    fn test_df_dtype_to_sqlite_dtype_numeric() {
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Boolean),
            SQLiteDataTypes::NUMERIC
        );
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Datetime(TimeUnit::Milliseconds, None)),
            SQLiteDataTypes::NUMERIC
        );
        assert_eq!(
            df_dtype_to_sqlite_dtype(&DataType::Date),
            SQLiteDataTypes::NUMERIC
        );
    }

    #[test]
    fn test_sqlite_data_type_as_str() {
        assert_eq!(SQLiteDataTypes::BLOB.as_str(), "BLOB");
        assert_eq!(SQLiteDataTypes::REAL.as_str(), "REAL");
        assert_eq!(SQLiteDataTypes::INT.as_str(), "INTEGER");
        assert_eq!(SQLiteDataTypes::TEXT.as_str(), "TEXT");
        assert_eq!(SQLiteDataTypes::NULL.as_str(), "NULL");
        assert_eq!(SQLiteDataTypes::NUMERIC.as_str(), "NUMERIC");
    }

    #[test]
    fn test_null() {
        assert_eq!(df_value_to_sqlite_value(AnyValue::Null), "NULL");
    }

    #[test]
    fn test_string() {
        assert_eq!(
            df_value_to_sqlite_value(AnyValue::String("hello".into())),
            "'hello'"
        );
        assert_eq!(
            df_value_to_sqlite_value(AnyValue::String("O'Reilly".into())),
            "'O''Reilly'"
        );
    }

    #[test]
    fn test_boolean() {
        assert_eq!(df_value_to_sqlite_value(AnyValue::Boolean(true)), "1");
        assert_eq!(df_value_to_sqlite_value(AnyValue::Boolean(false)), "0");
    }

    #[test]
    fn test_integers() {
        assert_eq!(df_value_to_sqlite_value(AnyValue::Int8(-8)), "-8");
        assert_eq!(df_value_to_sqlite_value(AnyValue::Int16(-16)), "-16");
        assert_eq!(df_value_to_sqlite_value(AnyValue::Int32(-32)), "-32");
        assert_eq!(df_value_to_sqlite_value(AnyValue::Int64(-64)), "-64");
        assert_eq!(df_value_to_sqlite_value(AnyValue::UInt8(8)), "8");
        assert_eq!(df_value_to_sqlite_value(AnyValue::UInt16(16)), "16");
        assert_eq!(df_value_to_sqlite_value(AnyValue::UInt32(32)), "32");
        assert_eq!(df_value_to_sqlite_value(AnyValue::UInt64(64)), "64");
    }

    #[test]
    fn test_floats() {
        assert_eq!(df_value_to_sqlite_value(AnyValue::Float32(1.23)), "1.23");
        assert_eq!(df_value_to_sqlite_value(AnyValue::Float64(4.56)), "4.56");
    }

    #[test]
    fn test_date() {
        let value = AnyValue::Date(739040);
        assert_eq!(df_value_to_sqlite_value(value), "'2024-06-03'");

        let invalid_date = AnyValue::Date(i32::MAX);
        assert_eq!(df_value_to_sqlite_value(invalid_date), "'1970-01-01'");
    }

    #[test]
    fn test_datetime() {
        let ms = 1_577_836_800_000; // 2020-01-01 00:00:00 UTC
        let value = AnyValue::Datetime(ms, TimeUnit::Milliseconds, None);
        assert_eq!(df_value_to_sqlite_value(value), "'2020-01-01 00:00:00'");

        let invalid = AnyValue::Datetime(i64::MAX, TimeUnit::Milliseconds, None);
        assert_eq!(df_value_to_sqlite_value(invalid), "'1970-01-01 00:00:00'");
    }

    #[test]
    fn test_fallback() {
        let other = AnyValue::String("some'value");
        assert_eq!(df_value_to_sqlite_value(other), "'some''value'");
    }
}

#[cfg(test)]
mod vtab_data_format_tests {
    use super::*;

    #[test]
    fn test_get_format_valid() {
        assert_eq!(get_format("csv").unwrap(), VTabDataFormats::CSV);
        assert_eq!(get_format("AVRO").unwrap(), VTabDataFormats::AVRO);
        assert_eq!(get_format("parquet").unwrap(), VTabDataFormats::PARQUET);
        assert_eq!(get_format("JSON").unwrap(), VTabDataFormats::JSON);
        assert_eq!(get_format("jsonl").unwrap(), VTabDataFormats::JSONL);
        assert_eq!(get_format("NDJSON").unwrap(), VTabDataFormats::JSONL);
    }

    #[test]
    fn test_get_format_invalid() {
        let result = get_format("xml");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.to_string(), "Unknown data format: xml");
        }
    }
}
