use csv::ReaderBuilder;
use reqwest::blocking::get;
use sqlite_loadable::{
    api, define_virtual_table,
    table::{BestIndexError, ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    Result,
};
use sqlite_loadable::{prelude::*, Error};
use std::{mem, os::raw::c_int};

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

        let url = args[0].trim_matches(|c| c == '\'' || c == '"');

        let resp = get(url)
            .map_err(|e| Error::new_message(&format!("HTTP error: {}", e)))?
            .text()
            .map_err(|e| Error::new_message(&format!("Read error: {}", e)))?;

        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(resp.as_bytes());

        let headers = rdr
            .headers()
            .map_err(|e| Error::new_message(&format!("CSV header error: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let mut rows = Vec::new();
        for result in rdr.records() {
            let record =
                result.map_err(|e| Error::new_message(&format!("CSV parse error: {}", e)))?;
            rows.push(record.iter().map(|s| s.to_string()).collect());
        }

        let schema = format!(
            "CREATE TABLE x({});",
            headers
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
                headers,
                rows,
            },
        ))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        for inf in info.constraints().iter() {
            println!("{:#?}", inf)
        }

        let mut used_cols = Vec::new();

        for (i, constraint) in info.constraints().iter_mut().enumerate() {
            if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                info.constraints()[i].set_argv_index((used_cols.len() + 1) as i32); // 1-based
                used_cols.push(constraint.column_idx());
            }
        }

        let _ = info.set_idxstr(
            &used_cols
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        info.set_idxnum(used_cols.len().try_into().unwrap());

        Ok(())
    }

    fn open(&mut self) -> Result<UrlCursor> {
        Ok(UrlCursor::new())
    }
}

#[repr(C)]
struct UrlCursor {
    base: sqlite3_vtab_cursor,
    row_idx: usize,
    filtered_rows: Vec<Vec<String>>,
}

impl UrlCursor {
    fn new() -> UrlCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        UrlCursor {
            base,
            row_idx: 0,
            filtered_rows: Vec::<Vec<String>>::new(),
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
        println!("idx_str {:?}", idx_str);
        println!("args.len {:?}", args.len());

        let vtab: &mut UrlTable = unsafe { &mut *(self.base.pVtab as *mut UrlTable) };
        self.row_idx = 0;

        // Si no hay filtros, dejamos todas las filas
        if args.is_empty() || idx_str.is_none() {
            return Ok(());
        }

        // Obtenemos los índices de columna desde idx_str (ej. "0,2,4")
        let col_indices: Vec<usize> = idx_str
            .unwrap()
            .split(',')
            .filter_map(|s| s.parse::<usize>().ok())
            .collect();

        // Empezamos con todas las filas
        let mut filtered_rows = vtab.rows.clone();

        // Aplicamos cada filtro
        for (i, col_idx) in col_indices.iter().enumerate() {
            let filter_value = api::value_text(&args[i])?;
            println!(
                "Filtrando columna {} ({}) == {}",
                col_idx, vtab.headers[*col_idx], filter_value
            );

            filtered_rows = filtered_rows
                .into_iter()
                .filter(|row| {
                    let cell_value = row[*col_idx].trim();
                    if cell_value == filter_value {
                        println!(
                            "Comparando '{}' con '{}' -> {}",
                            cell_value,
                            filter_value,
                            cell_value == filter_value
                        );
                    }
                    cell_value == filter_value
                })
                .collect();
        }

        println!("{:#?}", filtered_rows);
        self.filtered_rows = filtered_rows;

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_idx += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        let vtab = unsafe { &*(self.base.pVtab as *mut UrlTable) };
        self.row_idx >= vtab.rows.len()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let vtab = unsafe { &*(self.base.pVtab as *mut UrlTable) };
        if let Some(value) = vtab.rows[self.row_idx].get(i as usize) {
            let _ = api::result_text(context, value);
        } else {
            api::result_null(context);
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_idx.try_into().unwrap())
    }
}

#[sqlite_entrypoint]
pub fn sqlite3_url_init(db: *mut sqlite3) -> Result<()> {
    define_virtual_table::<UrlTable>(db, "url", None)?;
    Ok(())
}
