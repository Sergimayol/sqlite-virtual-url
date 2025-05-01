use csv::ReaderBuilder;
use reqwest::blocking::get;
use sqlite_loadable::{
    api, define_table_function, define_virtual_table,
    table::{BestIndexError, IndexInfo, VTab, VTabArguments, VTabCursor},
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

#[repr(C)]
struct UrlCursor {
    base: sqlite3_vtab_cursor,
    row_idx: usize,
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

    fn best_index(&self, mut _info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        Ok(())
    }

    fn open(&mut self) -> Result<UrlCursor> {
        Ok(UrlCursor::new())
    }
}

impl UrlCursor {
    fn new() -> UrlCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        UrlCursor { base, row_idx: 0 }
    }
}

impl VTabCursor for UrlCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &[*mut sqlite3_value],
    ) -> Result<()> {
        self.row_idx = 0;
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
