.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_virtual_url', 'sqlite3_url_init');

CREATE VIRTUAL TABLE demo USING URL('https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/2014_us_cities.csv');

.timer on
SELECT * FROM demo LIMIT 10;

SELECT * FROM demo WHERE name = 'Chicago';