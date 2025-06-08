.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_httpfs_init');

CREATE VIRTUAL TABLE demo2 USING HTTPFS(
    url='https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/carshare_data.json',
    format='JSON'
);
.timer on
SELECT * FROM demo2 LIMIT 10;
SELECT * FROM demo2 WHERE peak_hour = 2 LIMIT 10;
SELECT * FROM demo2 WHERE peak_hour < 2 LIMIT 10;
