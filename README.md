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

For local development without Poetry:

```powershell
python -m pip install -e .
```

If you want to run directly from the repo without installing the package, use the `src` directory on `PYTHONPATH`:

```powershell
$env:PYTHONPATH = "src"
python -m sqlcsv_exporter --help
```

Poetry still works if you prefer it:

```bash
poetry install
```

## Run

If you installed the package with `pip install -e .`:

```powershell
python -m sqlcsv_exporter `
  --sql .\queries\report.sql `
  --output .\exports\report.csv `
  --server my-sql-server `
  --database Reporting
```

From the repo without installing:

```powershell
$env:PYTHONPATH = "src"
python -m sqlcsv_exporter `
  --sql .\queries\report.sql `
  --output .\exports\report.csv `
  --server my-sql-server `
  --database Reporting
```

With SQL authentication:

```powershell
python -m sqlcsv_exporter `
  --sql .\queries\report.sql `
  --output .\exports\report.csv `
  --server my-sql-server `
  --database Reporting `
  --sql-auth `
  --username reporting_user `
  --password secret
```

Override the as-of date used in a declared `@InAsOfDate` variable:

```powershell
python -m sqlcsv_exporter `
  --sql .\queries\report.sql `
  --output .\exports\report.csv `
  --server my-sql-server `
  --database Reporting `
  --date 2026-03-26
```

Using the real test query in this repo:

```powershell
$env:PYTHONPATH = "src"
python -m sqlcsv_exporter `
  --sql .\queries\stock_metrics_top_1000.sql `
  --output .\exports\stock_metrics_top_1000.csv `
  --server "DESKTOP-TTUSQLJ\SQLEXPRESS" `
  --database QuantDevTest
```

## Publish

```bash
poetry build
poetry publish
```
