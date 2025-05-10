.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_virtual_url', 'sqlite3_url_init');

CREATE VIRTUAL TABLE demo2 USING URL(
    url='https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/carshare_data.json',
    format='JSON'
);
.timer on
SELECT * FROM demo2 LIMIT 10;
SELECT * FROM demo2 WHERE peak_hour = 2 LIMIT 10;
SELECT * FROM demo2 WHERE peak_hour < 2 LIMIT 10;
