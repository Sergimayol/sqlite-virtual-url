mod args;
mod avro;
mod storage;

use args::parse_args;
use avro::AvroReader;
use polars::prelude::*;
use reqwest::blocking::get;
use sqlite_loadable::{
    api, define_virtual_table,
    table::{BestIndexError, ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    Result,
};
use sqlite_loadable::{prelude::*, Error};
use std::{mem, os::raw::c_int};

use storage::{
    df_dtype_to_sqlite_dtype, generate_inserts_from_dataframe, get_format, get_storage, Statement,
    StorageOpts, VTabDataFormats,
};

#[repr(C)]
struct UrlTable {
    base: sqlite3_vtab,
    df: DataFrame,
    headers: Vec<String>,
    columns_types: Vec<String>,
}

impl UrlTable {
    fn init(
        db: *mut sqlite3,
        _aux: Option<&<UrlTable as VTab>::Aux>,
        vt_args: VTabArguments,
        is_created: bool,
    ) -> Result<(String, Self)> {
        let args = vt_args.arguments;
        if args.len() < 2 {
            return Err(Error::new_message("URL and FORMAT args must be provided"));
        }

        let parsed_args = parse_args(args);
        let url = parsed_args
            .named
            .get("URL")
            .or_else(|| parsed_args.positional.get(0))
            .ok_or_else(|| Error::new_message("No URL provided"))?;

        let format = parsed_args
            .named
            .get("FORMAT")
            .or_else(|| parsed_args.positional.get(1))
            .ok_or_else(|| Error::new_message("No data format specified"))
            .and_then(|f| get_format(&f).map_err(|err| Error::new_message(format!("{}", err))))?;

        let storage = parsed_args
            .named
            .get("STORAGE")
            .or_else(|| parsed_args.positional.get(2))
            .map_or_else(
                || Ok(StorageOpts::DISK),
                |opt| get_storage(opt).map_err(|err| Error::new_message(format!("{}", err))),
            )?;

        let t_name = format!(
            "\"{}.{}_metadata\"",
            vt_args.module_name, vt_args.table_name
        );
        let fetch_data = is_created && !Self::has_metadata(db, &t_name)?;
        let df = if fetch_data {
            let resp = get(url)
                .map_err(|e| Error::new_message(&format!("HTTP error: {}", e)))?
                .bytes()
                .map_err(|e| Error::new_message(&format!("Read error: {}", e)))?;

            match format {
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
            }
        } else {
            let metadata_sql = format!(
                "SELECT HEADERS FROM \"{}.{}_metadata\";",
                vt_args.module_name, vt_args.table_name
            );
            let stmt = Statement::build(db, &metadata_sql)
                .map_err(|e| Error::new_message(e.to_string()))?;
            let results = stmt
                .fetch(1)
                .map_err(|e| Error::new_message(e.to_string()))?;
            let raw_headers = results.get(0).and_then(|row| row.get(0));
            let headers: Vec<&str> = match raw_headers {
                Some(h) => Self::split_headers_line(h),
                None => todo!("Internal bug"),
            };

            let data_sql = format!(
                "SELECT * FROM  \"{}.{}_data\";",
                vt_args.module_name, vt_args.table_name
            );
            let stmt =
                Statement::build(db, &data_sql).map_err(|e| Error::new_message(e.to_string()))?;
            let results = stmt
                .fetch(headers.len().try_into().unwrap())
                .map_err(|e| Error::new_message(e.to_string()))?;

            Self::dataframe_from_rows(results, Some(headers))
                .map_err(|e| Error::new_message(e.to_string()))?
        };

        let headers = df
            .get_column_names_owned()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let columns_types = df
            .dtypes()
            .into_iter()
            .map(|col_dtype| df_dtype_to_sqlite_dtype(&col_dtype).as_str().to_string())
            .collect::<Vec<String>>();

        let columns_def = df
            .get_column_names()
            .iter()
            .zip(columns_types.iter())
            .map(|(name, ty)| format!("\"{}\" {}", name, ty))
            .collect::<Vec<_>>()
            .join(", ");

        // TODO: Maybe table naming should be "module_name.table_name.{data,metadata}"
        // TODO: Evaluate saving metadata into multiple rows
        // TODO: If StorageOpts::TEMP create temp tables
        if storage == StorageOpts::DISK && is_created {
            let data_schema = format!(
                "CREATE TABLE \"{}.{}_data\" ({});",
                vt_args.module_name, vt_args.table_name, columns_def
            );
            Statement::build(db, &data_schema)
                .map_err(|e| Error::new_message(e.to_string()))?
                .execute()
                .map_err(|e| Error::new_message(e.to_string()))?
                .finalize()
                .map_err(|e| Error::new_message(e.to_string()))?;

            let parsed_headers = headers
                .clone()
                .into_iter()
                .map(|name| format!("\"{}\"", name))
                .collect::<Vec<_>>()
                .join(", ");

            let batch_size = 1_000;
            let data_data = generate_inserts_from_dataframe(
                &df,
                &vt_args.module_name,
                &vt_args.table_name,
                parsed_headers.clone(),
                batch_size,
            );

            for data in data_data {
                Statement::build(db, &data)
                    .map_err(|e| Error::new_message(e.to_string()))?
                    .execute()
                    .map_err(|e| Error::new_message(e.to_string()))?
                    .finalize()
                    .map_err(|e| Error::new_message(e.to_string()))?;
            }

            let metadata_schema = format!(
                "CREATE TABLE \"{}.{}_metadata\" (URL TEXT, FORMAT TEXT, HEADERS TEXT, COLUMN_TYPES TEXT);",
                vt_args.module_name, vt_args.table_name
            );
            Statement::build(db, &metadata_schema)
                .map_err(|e| Error::new_message(e.to_string()))?
                .execute()
                .map_err(|e| Error::new_message(e.to_string()))?
                .finalize()
                .map_err(|e| Error::new_message(e.to_string()))?;

            let metadata_data = format!(
                "INSERT INTO\"{}.{}_metadata\" (URL, FORMAT, HEADERS, COLUMN_TYPES) VALUES ('{}', '{}', '{}', '{}');",
                vt_args.module_name,
                vt_args.table_name,
                url,
                format.as_str(),
                parsed_headers.clone(),
                columns_types.join(", ")
            );

            Statement::build(db, &metadata_data)
                .map_err(|e| Error::new_message(e.to_string()))?
                .execute()
                .map_err(|e| Error::new_message(e.to_string()))?
                .finalize()
                .map_err(|e| Error::new_message(e.to_string()))?;
        }

        let schema = format!("CREATE TABLE x({});", columns_def);
        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        Ok((
            schema,
            UrlTable {
                base,
                df,
                headers,
                columns_types,
            },
        ))
    }

    fn dataframe_from_rows(
        data: Vec<Vec<String>>,
        headers: Option<Vec<&str>>,
    ) -> PolarsResult<DataFrame> {
        if data.is_empty() {
            return Err(PolarsError::NoData("No rows provided".into()));
        }

        let num_cols = data[0].len();

        if !data.iter().all(|row| row.len() == num_cols) {
            return Err(PolarsError::ShapeMismatch(
                "Inconsistent row lengths".into(),
            ));
        }

        let columns: Vec<Vec<String>> = (0..num_cols)
            .map(|i| data.iter().map(|row| row[i].clone()).collect())
            .collect();

        let columns: Vec<Column> = columns
            .into_iter()
            .enumerate()
            .map(|(i, col_values)| {
                let name: String = headers
                    .as_ref()
                    .and_then(|h| h.get(i).copied())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("column_{}", i));

                let series = Series::new((&name).into(), col_values);
                Column::new(series.name().clone(), series)
            })
            .collect();

        DataFrame::new(columns)
    }

    fn has_metadata(db: *mut sqlite3, table_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{}';",
            table_name
        );
        let stmt = Statement::build(db, &sql).map_err(|e| Error::new_message(e.to_string()))?;
        let results = stmt
            .fetch(1)
            .map_err(|e| Error::new_message(e.to_string()))?;

        Ok(results.len() > 0)
    }

    fn split_headers_line<'a>(line: &'a str) -> Vec<&'a str> {
        let mut result = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;
        let mut i = 0;
        let bytes = line.as_bytes();

        while i < bytes.len() {
            match bytes[i] {
                b'"' => {
                    in_quotes = !in_quotes;
                    i += 1;
                }
                b',' if !in_quotes => {
                    let field = &line[start..i].trim();
                    result.push(Self::trim_quotes(field));
                    i += 1;
                    start = i;
                }
                _ => {
                    i += 1;
                }
            }
        }

        if start < line.len() {
            let field = &line[start..].trim();
            result.push(Self::trim_quotes(field));
        }

        result
    }

    fn trim_quotes(s: &str) -> &str {
        let s = s.trim();
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            &s[1..s.len() - 1]
        } else {
            s
        }
    }
}

impl<'vtab> VTab<'vtab> for UrlTable {
    type Aux = ();
    type Cursor = UrlCursor;

    fn create(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        args: VTabArguments,
    ) -> Result<(String, Self)> {
        UrlTable::init(db, aux, args, true)
    }

    fn connect(
        db: *mut sqlite3,
        aux: Option<&Self::Aux>,
        vt_args: VTabArguments,
    ) -> Result<(String, Self)> {
        UrlTable::init(db, aux, vt_args, false)
    }

    // TODO: Improve this by getting data from sqlite tables
    // Big tables won't fit in a single polars df in mem
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
    // TODO: This with SQLite tables will be easier, maybe?
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
pub fn sqlite3_httpfs_init(db: *mut sqlite3) -> Result<()> {
    define_virtual_table::<UrlTable>(db, "httpfs", None)?;
    Ok(())
}
