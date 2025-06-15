use sqlite_httpfs::io::{
    csv_reader::CsvReader, IterableReader, Reader, ReaderConstructor, ReaderError,
};
use std::fs;

fn main() -> Result<(), ReaderError> {
    let data = fs::read("benches/data/2014_us_cities.csv")?;

    let mut csv_reader = CsvReader::try_new(&data, 100)?;
    println!("{}", csv_reader);

    let mut total_rows = 0u128;
    let mut bytes_read = 0u64;

    for row_result in csv_reader.iter_rows() {
        let row = row_result?;
        total_rows += 1;

        bytes_read += row.iter().map(|field| field.len()).sum::<usize>() as u64;
    }

    csv_reader.total_rows = total_rows;
    csv_reader.bytes_read = bytes_read;

    println!("Total number of rows: {}", csv_reader.total_rows());
    println!("Bytes read: {}", csv_reader.bytes_read());

    Ok(())
}
