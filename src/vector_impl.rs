use avro_rs::Reader;
use csv::ReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
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
}

fn get_format(fmt: &str) -> Result<VTabDataFormats> {
    match fmt.to_uppercase().as_str() {
        "CSV" => Ok(VTabDataFormats::CSV),
        "AVRO" => Ok(VTabDataFormats::AVRO),
        "PARQUET" => Ok(VTabDataFormats::PARQUET),
        _ => Err(Error::new_message(&format!("Unknown data format: {}", fmt))),
    }
}

#[repr(C)]
struct UrlTable {
    base: sqlite3_vtab,
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
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
        let url = match parsed_args.named.get("URL") {
            Some(url_val) => url_val.to_string(),
            None => match parsed_args.positional.get(0) {
                Some(pos_val) => pos_val.to_string(),
                None => return Err(Error::new_message("No URL provided")),
            },
        };

        let format = match parsed_args.named.get("FORMAT") {
            Some(t_fmt) => get_format(t_fmt),
            None => match parsed_args.positional.get(1) {
                Some(pos_val) => get_format(pos_val),
                None => return Err(Error::new_message("No data format specified")),
            },
        };

        let resp = get(url)
            .map_err(|e| Error::new_message(&format!("HTTP error: {}", e)))?
            .bytes()
            .map_err(|e| Error::new_message(&format!("Read error: {}", e)))?;

        let (data_headers, data_rows) = match format {
            Ok(fmt) => match fmt {
                VTabDataFormats::CSV => {
                    let mut rdr = ReaderBuilder::new()
                        .has_headers(true)
                        .from_reader(resp.as_ref());

                    let headers = rdr
                        .headers()
                        .map_err(|e| Error::new_message(&format!("CSV header error: {}", e)))?
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>();

                    let mut rows = Vec::new();
                    for result in rdr.records() {
                        let record = result
                            .map_err(|e| Error::new_message(&format!("CSV parse error: {}", e)))?;
                        rows.push(record.iter().map(|s| s.trim().to_string()).collect());
                    }

                    (headers, rows)
                }
                VTabDataFormats::AVRO => {
                    let reader = Reader::new(resp.as_ref())
                        .map_err(|e| Error::new_message(&format!("AVRO parse error: {}", e)))?;

                    let mut headers: Vec<String> = Vec::new();
                    let mut rows: Vec<Vec<String>> = Vec::new();

                    for record in reader {
                        let value = record
                            .map_err(|e| Error::new_message(&format!("AVRO read error: {}", e)))?;

                        if let avro_rs::types::Value::Record(fields) = &value {
                            if headers.is_empty() {
                                headers = fields.iter().map(|(k, _)| k.clone()).collect();
                            }
                        }

                        if let avro_rs::types::Value::Record(fields) = value {
                            let mut row = Vec::new();
                            for header in &headers {
                                let val_str = fields
                                    .iter()
                                    .find(|(k, _)| k == header)
                                    .map(|(_, v)| format!("{:?}", v))
                                    .unwrap_or_default();
                                row.push(val_str);
                            }
                            rows.push(row);
                        }
                    }

                    (headers, rows)
                }
                VTabDataFormats::PARQUET => {
                    let reader = SerializedFileReader::new(resp)
                        .map_err(|e| Error::new_message(&format!("Parquet error: {e}")))?;

                    let iter = reader
                        .get_row_iter(None)
                        .map_err(|e| Error::new_message(&format!("Parquet row iter error: {e}")))?;

                    let mut headers: Vec<String> = Vec::new();
                    let mut rows: Vec<Vec<String>> = Vec::new();

                    for row_result in iter {
                        let row = row_result
                            .map_err(|e| Error::new_message(&format!("Row error: {e}")))?;

                        if headers.is_empty() {
                            headers = row
                                .get_column_iter()
                                .map(|(name, _)| name.to_string())
                                .collect();
                        }

                        let row_values = row
                            .get_column_iter()
                            .map(|(_, val)| format!("{val}"))
                            .collect::<Vec<String>>();

                        rows.push(row_values);
                    }

                    (headers, rows)
                }
            },
            Err(err) => return Err(err),
        };

        let schema = format!(
            "CREATE TABLE x({});",
            data_headers
                .iter()
                .map(|h| format!("\"{}\"", h))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        Ok((
            schema,
            UrlTable {
                base,
                headers: data_headers,
                rows: data_rows,
            },
        ))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut used_cols = Vec::new();
        let mut used_ops = Vec::new();

        for (i, constraint) in info.constraints().iter_mut().enumerate() {
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

                info.constraints()[i].set_argv_index((used_cols.len() + 1) as i32); // 1-based
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
        Ok(UrlCursor::new(self.rows.clone()))
    }
}

#[repr(C)]
struct UrlCursor {
    base: sqlite3_vtab_cursor,
    row_idx: usize,
    filtered_rows: Vec<Vec<String>>,
}

impl UrlCursor {
    fn new(all_rows: Vec<Vec<String>>) -> UrlCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        UrlCursor {
            base,
            row_idx: 0,
            filtered_rows: all_rows,
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
        let mut filtered_rows = vtab.rows.clone();

        if !args.is_empty() && idx_str.is_some() {
            let col_ops: Vec<(usize, &str)> = idx_str
                .unwrap()
                .split(',')
                .filter_map(|part| {
                    let (col_str, op) = if part.ends_with('=') {
                        part.split_at(part.len() - 1)
                    } else {
                        part.split_at(part.len())
                    };
                    col_str.parse::<usize>().ok().map(|col| (col, op))
                })
                .collect();

            for (i, (col_idx, op)) in col_ops.iter().enumerate() {
                let filter_value = api::value_text(&args[i])?;

                filtered_rows = filtered_rows
                    .into_iter()
                    .filter(|row| {
                        let cell_value = row[*col_idx].trim();

                        let cell_num = cell_value.parse::<f64>();
                        let filter_num = filter_value.parse::<f64>();

                        let comparison = if cell_num.is_ok() && filter_num.is_ok() {
                            let c = cell_num.unwrap();
                            let f = filter_num.unwrap();

                            // Num comp.
                            match *op {
                                "=" => c == f,
                                ">" => c > f,
                                "<" => c < f,
                                ">=" => c >= f,
                                "<=" => c <= f,
                                "!" => c != f,
                                _ => false,
                            }
                        } else {
                            // Text comp.
                            match *op {
                                "=" => cell_value == filter_value,
                                ">" => cell_value > filter_value,
                                "<" => cell_value < filter_value,
                                ">=" => cell_value >= filter_value,
                                "<=" => cell_value <= filter_value,
                                "!" => cell_value != filter_value,
                                _ => false,
                            }
                        };

                        comparison
                    })
                    .collect();
            }
        }

        self.filtered_rows = filtered_rows;
        self.row_idx = 0;

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_idx += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_idx >= self.filtered_rows.len()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        if let Some(row) = self.filtered_rows.get(self.row_idx) {
            if let Some(value) = row.get(i as usize) {
                api::result_text(context, value)?;
            } else {
                api::result_null(context);
            }
        } else {
            api::result_null(context);
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
