.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_httpfs_init');

CREATE VIRTUAL TABLE demo2 USING HTTPFS(url='https://raw.githubusercontent.com/Teradata/kylo/refs/heads/master/samples/sample-data/avro/userdata1.avro', format='avro');
.timer on
SELECT * FROM demo2 LIMIT 10;
