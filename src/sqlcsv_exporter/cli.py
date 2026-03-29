from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from rich.console import Console

from sqlcsv_exporter import __version__
from sqlcsv_exporter.config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DATE_PARAMETER,
    DEFAULT_DRIVER,
    DEFAULT_ENCODING,
    DEFAULT_LOGIN_TIMEOUT_SECONDS,
    DEFAULT_QUERY_TIMEOUT_SECONDS,
    ConfigError,
    ExportConfig,
    resolve_as_of_date,
)
from sqlcsv_exporter.exporter import ExportError, execute_query_to_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqlcsv-exporter",
        description="Execute a SQL Server query from a file and stream the result into CSV.",
    )
    parser.add_argument("--sql", required=True, help="Path to the .sql file to execute.")
    parser.add_argument("--output", required=True, help="Destination CSV file path.")
    parser.add_argument("--server", required=True, help="SQL Server hostname or instance.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument(
        "--date",
        help="As-of date injected into a declared @InAsOfDate parameter. Defaults to yesterday.",
    )
    parser.add_argument(
        "--date-parameter",
        default=DEFAULT_DATE_PARAMETER,
        help=f"Declared SQL variable to rewrite. Default: {DEFAULT_DATE_PARAMETER}.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Rows to fetch and write per batch. Default: {DEFAULT_CHUNK_SIZE}.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Single-character CSV delimiter. Default: ','.",
    )
    parser.add_argument(
        "--encoding",
        default=DEFAULT_ENCODING,
        help=f"Output file encoding. Default: {DEFAULT_ENCODING}.",
    )
    parser.add_argument("--no-header", action="store_true", help="Do not write CSV column headers.")
    parser.add_argument(
        "--driver",
        default=DEFAULT_DRIVER,
        help=f"ODBC driver name. Default: {DEFAULT_DRIVER}.",
    )
    parser.add_argument("--sql-auth", action="store_true", help="Use SQL authentication instead of trusted connection.")
    parser.add_argument("--username", help="SQL username for --sql-auth.")
    parser.add_argument("--password", help="SQL password for --sql-auth.")
    parser.add_argument(
        "--login-timeout",
        type=int,
        default=DEFAULT_LOGIN_TIMEOUT_SECONDS,
        help=f"Connection timeout in seconds. Default: {DEFAULT_LOGIN_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--query-timeout",
        type=int,
        default=DEFAULT_QUERY_TIMEOUT_SECONDS,
        help=f"Query timeout in seconds. Default: {DEFAULT_QUERY_TIMEOUT_SECONDS}.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def build_config(args: argparse.Namespace) -> ExportConfig:
    return ExportConfig(
        sql_file=Path(args.sql),
        output_csv=Path(args.output),
        server=args.server,
        database=args.database,
        as_of_date=resolve_as_of_date(args.date),
        chunk_size=args.chunk_size,
        delimiter=args.delimiter,
        encoding=args.encoding,
        include_header=not args.no_header,
        trusted_connection=not args.sql_auth,
        username=args.username,
        password=args.password,
        driver=args.driver,
        date_parameter_name=args.date_parameter,
        login_timeout_seconds=args.login_timeout,
        query_timeout_seconds=args.query_timeout,
    )


def main(argv: Sequence[str] | None = None) -> int:
    console = Console(stderr=True, legacy_windows=False)
    parser = build_parser()

    try:
        args = parser.parse_args(argv)
        config = build_config(args)
        execute_query_to_csv(config, console=console)
        return 0
    except (ConfigError, ExportError, FileNotFoundError, RuntimeError) as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        return 1
    except KeyboardInterrupt:
        console.print("[bold red]Error:[/bold red] Export cancelled.")
        return 130
