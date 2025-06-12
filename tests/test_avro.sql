.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_httpfs_init');

CREATE VIRTUAL TABLE IF NOT EXISTS avro_demo USING HTTPFS(
    url='https://raw.githubusercontent.com/Teradata/kylo/refs/heads/master/samples/sample-data/avro/userdata1.avro',
    format='avro'
);
.timer on
SELECT * FROM avro_demo LIMIT 10;
