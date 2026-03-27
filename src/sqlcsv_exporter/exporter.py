from __future__ import annotations

import csv
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from sqlcsv_exporter.config import ExportConfig
from sqlcsv_exporter.connection import open_connection
from sqlcsv_exporter.sql_rewriter import read_sql_file, replace_declared_date_parameter

ProgressCallback = Callable[[int, int], None]


class ExportError(RuntimeError):
    """Raised when the export pipeline cannot complete successfully."""


@dataclass(frozen=True)
class ExportResult:
    output_csv: Path
    row_count: int
    column_count: int
    file_size_bytes: int
    duration_seconds: float
    date_parameter_replaced: bool


def iter_row_chunks(cursor, chunk_size: int) -> Iterator[Sequence[Sequence[object]]]:
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        yield rows


def write_rows_to_csv(
    output_path: Path,
    columns: Sequence[str],
    row_chunks: Iterable[Sequence[Sequence[object]]],
    *,
    delimiter: str,
    encoding: str,
    include_header: bool,
    progress_callback: ProgressCallback | None = None,
) -> tuple[int, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.writer(handle, delimiter=delimiter)

        if include_header:
            writer.writerow(columns)

        for chunk in row_chunks:
            writer.writerows(chunk)
            total_rows += len(chunk)

            if progress_callback is not None:
                progress_callback(total_rows, handle.tell())

    return total_rows, output_path.stat().st_size


def _format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KiB"
    return f"{num_bytes / (1024 * 1024):.1f} MiB"


def _render_run_summary(console: Console, config: ExportConfig, date_replaced: bool) -> None:
    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan")
    summary.add_column()
    summary.add_row("SQL file", str(config.sql_file))
    summary.add_row("Output", str(config.output_csv))
    summary.add_row("Server", config.server)
    summary.add_row("Database", config.database)
    summary.add_row("As-of date", config.as_of_date)
    summary.add_row("Chunk size", f"{config.chunk_size:,}")
    summary.add_row("Date rewritten", "yes" if date_replaced else "no")
    console.print(summary)


def _apply_query_timeout(connection: object, cursor: object, timeout_seconds: int) -> None:
    if hasattr(cursor, "timeout"):
        with suppress(Exception):
            setattr(cursor, "timeout", timeout_seconds)
            return

    if hasattr(connection, "timeout"):
        with suppress(Exception):
            setattr(connection, "timeout", timeout_seconds)


def execute_query_to_csv(config: ExportConfig, *, console: Console | None = None) -> ExportResult:
    console = console or Console(stderr=True)
    sql_text = read_sql_file(config.sql_file)
    sql_text, date_replaced = replace_declared_date_parameter(
        sql_text,
        config.as_of_date,
        parameter_name=config.date_parameter_name,
    )

    _render_run_summary(console, config, date_replaced)

    started_at = time.perf_counter()
    connection = None
    cursor = None

    try:
        with console.status("Connecting to SQL Server..."):
            connection = open_connection(config)
            cursor = connection.cursor()
            cursor.arraysize = config.chunk_size
            _apply_query_timeout(connection, cursor, config.query_timeout_seconds)

        with console.status("Executing query..."):
            cursor.execute(sql_text)

        if cursor.description is None:
            raise ExportError(
                "The SQL completed without returning a result set. Ensure the script ends with a SELECT query."
            )

        columns = [column[0] or f"column_{index + 1}" for index, column in enumerate(cursor.description)]
        console.print(f"Columns detected: [bold]{len(columns)}[/bold]")

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[row_text]}"),
            TextColumn("{task.fields[size_text]}"),
            TimeElapsedColumn(),
            console=console,
        )

        with progress:
            task_id = progress.add_task(
                "Writing CSV",
                total=None,
                row_text="0 rows",
                size_text="0 B",
            )
            completed_rows = 0

            def on_progress(total_rows: int, size_bytes: int) -> None:
                nonlocal completed_rows
                progress.update(
                    task_id,
                    advance=0,
                    row_text=f"{total_rows:,} rows",
                    size_text=_format_size(size_bytes),
                )
                progress.advance(task_id, total_rows - completed_rows)
                completed_rows = total_rows

            row_count, file_size_bytes = write_rows_to_csv(
                config.output_csv,
                columns,
                iter_row_chunks(cursor, config.chunk_size),
                delimiter=config.delimiter,
                encoding=config.encoding,
                include_header=config.include_header,
                progress_callback=on_progress,
            )

        duration_seconds = time.perf_counter() - started_at
        console.print(
            f"Export complete: [bold]{row_count:,}[/bold] rows, "
            f"[bold]{len(columns)}[/bold] columns, {_format_size(file_size_bytes)} written."
        )
        return ExportResult(
            output_csv=config.output_csv,
            row_count=row_count,
            column_count=len(columns),
            file_size_bytes=file_size_bytes,
            duration_seconds=duration_seconds,
            date_parameter_replaced=date_replaced,
        )
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        if connection is not None:
            with suppress(Exception):
                connection.close()
