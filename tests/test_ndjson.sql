.bail on

.header on
.mode box

SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_httpfs_init');

CREATE VIRTUAL TABLE IF NOT EXISTS jsonl_demo USING HTTPFS(
    url='https://raw.githubusercontent.com/raphaelstolt/json-lines/refs/heads/main/tests/fixtures/metadata_catalogue.jsonl',
    format='JSONL'
);
.timer on
SELECT * FROM jsonl_demo LIMIT 10;
