from __future__ import annotations

import csv
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from sqlcsv_exporter import __version__
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
    columns: tuple[str, ...]
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


def _format_duration(duration_seconds: float) -> str:
    return f"{duration_seconds:.2f}s"


def render_export_report(console: Console, config: ExportConfig, result: ExportResult) -> None:
    summary = Table.grid(expand=True, padding=(0, 2))
    summary.add_column(style="bold cyan", no_wrap=True, width=15)
    summary.add_column()
    summary.add_row("sql", str(config.sql_file))
    summary.add_row("server", config.server)
    summary.add_row("database", config.database)
    summary.add_row("delimiter", repr(config.delimiter))
    summary.add_row("header", "yes" if config.include_header else "no")
    summary.add_row("date_rewrite", "yes" if result.date_parameter_replaced else "no")
    summary.add_row("chunk_size", f"{config.chunk_size:,}")
    summary.add_row("rows_exported", f"{result.row_count:,}")
    summary.add_row("column_count", f"{result.column_count:,}")
    summary.add_row("file_size", _format_size(result.file_size_bytes))
    summary.add_row("elapsed", _format_duration(result.duration_seconds))
    summary.add_row("output", str(result.output_csv))

    columns_table = Table(
        box=box.SIMPLE_HEAVY,
        expand=True,
        show_header=True,
        header_style="bold",
        safe_box=False,
    )
    columns_table.add_column("#", justify="right", style="dim", width=3)
    columns_table.add_column("Column", style="bold")
    for index, column_name in enumerate(result.columns, start=1):
        columns_table.add_row(str(index), column_name)

    panel_table = Table.grid(expand=True, padding=(0, 0))
    panel_table.add_row(summary)
    if result.columns:
        panel_table.add_row(Text(""))
        panel_table.add_row(columns_table)

    console.print(
        Panel(
            panel_table,
            title=f" sqlcsv-exporter v{__version__} ",
            border_style="cyan",
            box=box.ROUNDED,
            safe_box=False,
            expand=True,
        )
    )


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
        result = ExportResult(
            output_csv=config.output_csv,
            row_count=row_count,
            column_count=len(columns),
            columns=tuple(columns),
            file_size_bytes=file_size_bytes,
            duration_seconds=duration_seconds,
            date_parameter_replaced=date_replaced,
        )
        render_export_report(console, config, result)
        return result
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        if connection is not None:
            with suppress(Exception):
                connection.close()
