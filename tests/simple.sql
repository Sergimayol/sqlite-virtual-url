SELECT
    load_extension (
        './target/release/libsqlite_httpfs',
        'sqlite3_httpfs_init'
    );

CREATE VIRTUAL TABLE IF NOT EXISTS simple_demo USING HTTPFS (
    URL = 'https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/2014_us_cities.csv',
    FORMAT = 'CSV',
    STORAGE = 'SQLITE'
);

.bail on
.header on
.mode box
.timer on
.echo on

SELECT * FROM simple_demo LIMIT 2;

.schema

SELECT * FROM "HTTPFS.demo_data" LIMIT 2;
SELECT * FROM "HTTPFS.demo_metadata";