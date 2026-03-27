from __future__ import annotations

from pathlib import Path

from sqlcsv_exporter.config import ExportConfig
from sqlcsv_exporter.connection import build_connection_string


def test_build_connection_string_normalises_driver_braces() -> None:
    config = ExportConfig(
        sql_file=Path("query.sql"),
        output_csv=Path("output.csv"),
        server="sql01",
        database="Reporting",
        as_of_date="2026-03-26",
        driver="{ODBC Driver 18 for SQL Server}",
    )

    connection_string = build_connection_string(config)

    assert "DRIVER={ODBC Driver 18 for SQL Server}" in connection_string
    assert "{{" not in connection_string
