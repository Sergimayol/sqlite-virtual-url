use std::{
    error::Error,
    ffi::{CStr, CString},
};

use libsqlite3_sys::sqlite3_errstr;
use polars::prelude::DataType;
use sqlite_loadable::{
    ext::{
        sqlite3, sqlite3_stmt, sqlite3ext_column_text, sqlite3ext_finalize, sqlite3ext_prepare_v2,
        sqlite3ext_step,
    },
    SQLITE_DONE, SQLITE_ROW,
};

#[derive(Debug, PartialEq)]
pub enum StorageOpts {
    MEM,
    SQLITE,
}

pub fn get_storage(storage: &str) -> Result<StorageOpts, Box<dyn Error>> {
    match storage.trim().to_uppercase().as_str() {
        "MEM" => Ok(StorageOpts::MEM),
        "SQLITE" => Ok(StorageOpts::SQLITE),
        _ => Err(format!("Not a valid storage option: {}", storage).into()),
    }
}

/// https://sqlite.org/c3ref/column_blob.html
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_storage_mem_uppercase() {
        let result = get_storage("MEM");
        assert_eq!(result.unwrap(), StorageOpts::MEM);
    }

    #[test]
    fn test_get_storage_mem_lowercase() {
        let result = get_storage("mem");
        assert_eq!(result.unwrap(), StorageOpts::MEM);
    }

    #[test]
    fn test_get_storage_sqlite_uppercase() {
        let result = get_storage("SQLITE");
        assert_eq!(result.unwrap(), StorageOpts::SQLITE);
    }

    #[test]
    fn test_get_storage_sqlite_mixed_case() {
        let result = get_storage("Sqlite");
        assert_eq!(result.unwrap(), StorageOpts::SQLITE);
    }

    #[test]
    fn test_get_storage_invalid() {
        let result = get_storage("disk");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Not a valid storage option: disk"
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
        let result = get_storage(" SQLITE ");
        assert_eq!(result.unwrap(), StorageOpts::SQLITE);
    }

    #[test]
    fn test_get_storage_with_newline() {
        let result = get_storage("mem\n");
        assert_eq!(result.unwrap(), StorageOpts::MEM);
    }

    #[test]
    fn test_get_storage_with_tab() {
        let result = get_storage("\tSQLITE\t");
        assert_eq!(result.unwrap(), StorageOpts::SQLITE);
    }

    #[test]
    fn test_get_storage_with_carriage_return() {
        let result = get_storage("MEM\r\n");
        assert_eq!(result.unwrap(), StorageOpts::MEM);
    }
}
