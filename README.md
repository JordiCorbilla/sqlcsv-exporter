# sqlcsv-exporter

`sqlcsv-exporter` is a publishable CLI package for streaming SQL Server query results into CSV files without loading the full result set into memory.

## Features

- Reads SQL from a `.sql` file
- Rewrites a declared `@InAsOfDate` value when present
- Supports trusted or SQL authentication
- Streams rows with chunked `fetchmany()` and chunked CSV writes
- Shows live Rich progress while the export runs
- Ships with pytest coverage for core behavior

## Install

```bash
poetry install
```

## Run

```bash
poetry run sqlcsv-exporter \
  --sql ./queries/report.sql \
  --output ./exports/report.csv \
  --server my-sql-server \
  --database Reporting
```

With SQL authentication:

```bash
poetry run sqlcsv-exporter \
  --sql ./queries/report.sql \
  --output ./exports/report.csv \
  --server my-sql-server \
  --database Reporting \
  --sql-auth \
  --username reporting_user \
  --password secret
```

Override the as-of date used in a declared `@InAsOfDate` variable:

```bash
poetry run sqlcsv-exporter \
  --sql ./queries/report.sql \
  --output ./exports/report.csv \
  --server my-sql-server \
  --database Reporting \
  --date 2026-03-26
```

## Publish

```bash
poetry build
poetry publish
```
