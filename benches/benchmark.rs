use std::{
    fs,
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Criterion};
use sqlite_httpfs::{
    dtypes::inference::InferredType,
    io::{csv_reader::CsvReader, IterableReader, Reader, ReaderConstructor},
};

fn benchmark_update_inferred_type(c: &mut Criterion) {
    let samples = vec![
        "42",
        "3.14",
        "true",
        "false",
        "hello",
        "",
        "   ",
        "123456789",
        "-100",
        "0.0001",
        "TRUE",
        "FALSE",
        "not_a_number",
        "9999999999999999999999999", // overflow for i64
    ];

    c.bench_function("InferredType::update on sample inputs", |b| {
        b.iter(|| {
            for s in &samples {
                let mut t = InferredType::Null;
                t.update(black_box(s));
                black_box(&t); // prevent optimization
            }
        });
    });
}

fn benchmark_csvreader_infer(c: &mut Criterion) {
    let sample_csv = fs::read("benches/data/2014_us_cities.csv").expect("Failed to read CSV file");

    let mut group = c.benchmark_group("csvreader");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(1000);

    group.bench_function("CsvReader::new from file", |b| {
        b.iter(|| {
            let reader = CsvReader::try_new(black_box(&sample_csv), black_box(100))
                .expect("Failed to construct CsvReader");

            black_box(reader.total_rows());
            black_box(reader.schema());
        });
    });

    group.finish();
}

fn benchmark_csvreader_iter_rows(c: &mut Criterion) {
    let sample_csv = fs::read("benches/data/2014_us_cities.csv").expect("Failed to read CSV file");

    let reader = CsvReader::try_new(&sample_csv, 100).expect("Failed to construct CsvReader");

    let mut group = c.benchmark_group("csvreader_iter");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    group.bench_function("CsvReader::iter_rows with rows/sec", |b| {
        b.iter_custom(|iters| {
            let mut total_rows = 0u64;
            let start = Instant::now();

            for _ in 0..iters {
                for row in reader.iter_rows() {
                    match row {
                        Ok(fields) => {
                            total_rows += 1;
                            let _ = black_box(fields);
                        }
                        Err(e) => panic!("Error reading row: {:?}", e),
                    }
                }
            }

            let elapsed = start.elapsed();

            let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
            println!(
                "Processed {} rows in {:?} (~{:.2} rows/sec)",
                total_rows, elapsed, rows_per_sec
            );

            elapsed
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_update_inferred_type,
    benchmark_csvreader_infer,
    benchmark_csvreader_iter_rows
);
criterion_main!(benches);
