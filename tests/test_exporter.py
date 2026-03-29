from __future__ import annotations

from pathlib import Path

from rich.console import Console

from sqlcsv_exporter.config import ExportConfig
from sqlcsv_exporter.exporter import ExportResult, execute_query_to_csv, render_export_report, write_rows_to_csv


class FakeCursor:
    def __init__(self, chunks: list[list[tuple[object, ...]]]) -> None:
        self._chunks = list(chunks)
        self.description = [("id",), ("name",)]
        self.arraysize = 0
        self.timeout = 0
        self.executed_sql: str | None = None
        self.closed = False

    def execute(self, sql_text: str) -> None:
        self.executed_sql = sql_text

    def fetchmany(self, chunk_size: int):
        if not self._chunks:
            return []
        return self._chunks.pop(0)

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False
        self.timeout = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def test_write_rows_to_csv_writes_header_and_rows(tmp_path: Path) -> None:
    output_path = tmp_path / "export.csv"
    seen_progress: list[tuple[int, int]] = []

    row_count, file_size = write_rows_to_csv(
        output_path,
        ["id", "name"],
        [[(1, "alpha"), (2, "beta")], [(3, "gamma")]],
        delimiter=",",
        encoding="utf-8",
        include_header=True,
        progress_callback=lambda rows, size: seen_progress.append((rows, size)),
    )

    assert row_count == 3
    assert file_size > 0
    assert seen_progress[-1][0] == 3
    assert output_path.read_text(encoding="utf-8") == "id,name\n1,alpha\n2,beta\n3,gamma\n"


def test_execute_query_to_csv_streams_results(monkeypatch: object, tmp_path: Path) -> None:
    sql_file = tmp_path / "query.sql"
    sql_file.write_text(
        "DECLARE @InAsOfDate DATE = '2026-03-01';\nSELECT id, name FROM dbo.ExportSource;",
        encoding="utf-8",
    )
    output_path = tmp_path / "export.csv"

    cursor = FakeCursor(chunks=[[(1, "alpha"), (2, "beta")], [(3, "gamma")]])
    connection = FakeConnection(cursor)

    monkeypatch.setattr("sqlcsv_exporter.exporter.open_connection", lambda config: connection)

    config = ExportConfig(
        sql_file=sql_file,
        output_csv=output_path,
        server="sql01",
        database="Reporting",
        as_of_date="2026-03-26",
        chunk_size=2,
    )

    result = execute_query_to_csv(config, console=Console(file=None, stderr=True, color_system=None))

    assert result.row_count == 3
    assert result.column_count == 2
    assert result.columns == ("id", "name")
    assert result.date_parameter_replaced is True
    assert "2026-03-26" in (cursor.executed_sql or "")
    assert cursor.closed is True
    assert connection.closed is True
    assert output_path.exists()


def test_execute_query_to_csv_sets_connection_timeout_when_cursor_lacks_timeout(
    monkeypatch: object, tmp_path: Path
) -> None:
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT id, name FROM dbo.ExportSource;", encoding="utf-8")
    output_path = tmp_path / "export.csv"

    cursor = FakeCursor(chunks=[[(1, "alpha")]])
    del cursor.timeout
    connection = FakeConnection(cursor)

    monkeypatch.setattr("sqlcsv_exporter.exporter.open_connection", lambda config: connection)

    config = ExportConfig(
        sql_file=sql_file,
        output_csv=output_path,
        server="sql01",
        database="Reporting",
        as_of_date="2026-03-26",
        query_timeout_seconds=123,
    )

    execute_query_to_csv(config, console=Console(file=None, stderr=True, color_system=None))

    assert connection.timeout == 123


def test_render_export_report_includes_panel_fields() -> None:
    console = Console(record=True, width=120, color_system=None)
    config = ExportConfig(
        sql_file=Path("query.sql"),
        output_csv=Path("output.csv"),
        server="sql01",
        database="Reporting",
        as_of_date="2026-03-26",
    )
    result = ExportResult(
        output_csv=Path("output.csv"),
        row_count=42,
        column_count=2,
        columns=("id", "name"),
        file_size_bytes=4096,
        duration_seconds=2.51,
        date_parameter_replaced=False,
    )

    render_export_report(console, config, result)
    output = console.export_text()

    assert "sqlcsv-exporter v1.0.1" in output
    assert "rows_exported" in output
    assert "42" in output
    assert "Column" in output
    assert "name" in output
