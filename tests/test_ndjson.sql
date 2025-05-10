.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_virtual_url', 'sqlite3_url_init');

CREATE VIRTUAL TABLE demo2 USING URL(
    url='https://raw.githubusercontent.com/raphaelstolt/json-lines/refs/heads/main/tests/fixtures/metadata_catalogue.jsonl',
    format='JSONL'
);
.timer on
SELECT * FROM demo2 LIMIT 10;
