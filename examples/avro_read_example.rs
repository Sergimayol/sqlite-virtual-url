use sqlite_httpfs::{
    io::{avro_reader::AvroReader, IterableReader, Reader, ReaderConstructor, ReaderError},
};
use std::fs;

fn main() -> Result<(), ReaderError> {
    let data = fs::read("benches/data/userdata1.avro")?;

    let reader = AvroReader::try_new(&data, 100)?;
    println!("{}", reader);
    println!("Rows: {}", reader.total_rows());
    println!("Columns: {}", reader.total_columns());

    println!("\nFirst few rows:");
    for row in reader.iter_rows().take(5) {
        println!("{}", &row?);
    }

    Ok(())
}
