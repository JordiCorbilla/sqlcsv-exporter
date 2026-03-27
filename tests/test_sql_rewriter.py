from __future__ import annotations

from pathlib import Path

import pytest

from sqlcsv_exporter.sql_rewriter import read_sql_file, replace_declared_date_parameter


def test_read_sql_file_returns_contents(tmp_path: Path) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("SELECT 1", encoding="utf-8")

    assert read_sql_file(sql_path) == "SELECT 1"


def test_read_sql_file_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_sql_file(tmp_path / "missing.sql")


def test_replace_declared_date_parameter_is_case_insensitive() -> None:
    sql_text = """
    DECLARE @InAsOfDate DATE = '2026-03-01';
    SELECT * FROM dbo.Snapshot WHERE SnapshotDate = @InAsOfDate;
    """

    rewritten, replaced = replace_declared_date_parameter(
        sql_text,
        "2026-03-26",
        parameter_name="@InAsOfDate",
    )

    assert replaced is True
    assert "'2026-03-26'" in rewritten


def test_replace_declared_date_parameter_leaves_unmatched_sql_alone() -> None:
    sql_text = "SELECT * FROM dbo.Snapshot;"

    rewritten, replaced = replace_declared_date_parameter(
        sql_text,
        "2026-03-26",
        parameter_name="@InAsOfDate",
    )

    assert replaced is False
    assert rewritten == sql_text


def test_replace_declared_date_parameter_supports_set_statements() -> None:
    sql_text = """
    DECLARE @InAsOfDate DATE = '2026-03-01';
    SET @InAsOfDate = '2026-03-02';
    """

    rewritten, replaced = replace_declared_date_parameter(
        sql_text,
        "2026-03-26",
        parameter_name="@InAsOfDate",
    )

    assert replaced is True
    assert rewritten.count("2026-03-26") == 2
