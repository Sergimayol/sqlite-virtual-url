# sqlite-httpfs

SQLite3 extension for querying data directly from a URL.

## Run Demo

1. Compile:

```bash
cargo build --release
```

2. Run:

```bash
sqlite3 :memory: '.read tests/test.sql'
```

3. Output:

```bash
┌───────────────┬─────────┬────────────┬──────────────┐
│     name      │   pop   │    lat     │     lon      │
├───────────────┼─────────┼────────────┼──────────────┤
│ New York      │ 8287238 │ 40.7305991 │ -73.9865812  │
│ Los Angeles   │ 3826423 │ 34.053717  │ -118.2427266 │
│ Chicago       │ 2705627 │ 41.8755546 │ -87.6244212  │
│ Houston       │ 2129784 │ 29.7589382 │ -95.3676974  │
│ Philadelphia  │ 1539313 │ 39.952335  │ -75.163789   │
│ Phoenix       │ 1465114 │ 33.4467681 │ -112.0756724 │
│ San Antonio   │ 1359174 │ 29.4246002 │ -98.4951405  │
│ San Diego     │ 1321016 │ 32.7174209 │ -117.1627714 │
│ Dallas        │ 1219399 │ 32.7761963 │ -96.7968994  │
│ San Jose      │ 971495  │ 37.3438502 │ -121.8831349 │
└───────────────┴─────────┴────────────┴──────────────┘
Run Time: real 0.000 user 0.000106 sys 0.000008
```

## Usage

### CSV

1. **Load the extension**

```sql
SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_url_init');
-- or
.load target/release/libsqlite_httpfs sqlite3_url_init
```

2. **Create a virtual table using `url`**

```sql
CREATE VIRTUAL TABLE us_cities USING URL (
    'https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/2014_us_cities.csv',
    'csv'
);
-- or
CREATE VIRTUAL TABLE us_cities USING URL (
    url = 'https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/2014_us_cities.csv',
    format = 'csv'
);
```

3. **Query the table**

```sql
SELECT * FROM us_cities WHERE lat >= 33.0 AND name != 'Logan' LIMIT 5;
```

```bash
┌───────────────┬─────────┬────────────┬──────────────┐
│     name      │   pop   │    lat     │     lon      │
├───────────────┼─────────┼────────────┼──────────────┤
│ New York      │ 8287238 │ 40.7305991 │ -73.9865812  │
│ Los Angeles   │ 3826423 │ 34.053717  │ -118.2427266 │
│ Chicago       │ 2705627 │ 41.8755546 │ -87.6244212  │
│ Houston       │ 2129784 │ 29.7589382 │ -95.3676974  │
│ Philadelphia  │ 1539313 │ 39.952335  │ -75.163789   │
│ Phoenix       │ 1465114 │ 33.4467681 │ -112.0756724 │
│ San Antonio   │ 1359174 │ 29.4246002 │ -98.4951405  │
│ San Diego     │ 1321016 │ 32.7174209 │ -117.1627714 │
│ Dallas        │ 1219399 │ 32.7761963 │ -96.7968994  │
│ San Jose      │ 971495  │ 37.3438502 │ -121.8831349 │
└───────────────┴─────────┴────────────┴──────────────┘
Run Time: real 0.000 user 0.000106 sys 0.000008
```

### AVRO

1. **Load the extension**

```sql
SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_url_init');
-- or
.load target/release/libsqlite_httpfs sqlite3_url_init
```

2. **Create a virtual table using `url`**

```sql
CREATE VIRTUAL TABLE avro_demo USING URL (
    url = 'https://raw.githubusercontent.com/Teradata/kylo/refs/heads/master/samples/sample-data/avro/userdata1.avro',
    format = 'avro'
);
```

3. **Query the table**

```sql
SELECT * FROM avro_demo LIMIT 5;
```

### PARQUET

1. **Load the extension**

```sql
SELECT load_extension('./target/release/libsqlite_httpfs', 'sqlite3_url_init');
-- or
.load target/release/libsqlite_httpfs sqlite3_url_init
```

2. **Create a virtual table using `url`**

```sql
CREATE VIRTUAL TABLE parquet_demo USING URL(
    url='https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/oil-and-gas.parquet',
    format='parquet'
);
```

3. **Query the table**

```sql
SELECT * FROM parquet_demo LIMIT 5;
```
