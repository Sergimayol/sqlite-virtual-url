.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_httpfs_init');

CREATE VIRTUAL TABLE IF NOT EXISTS parquet_demo USING HTTPFS(
    url='https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/oil-and-gas.parquet',
    format='parquet'
);
.timer on
SELECT * FROM parquet_demo LIMIT 10;
