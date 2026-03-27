from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from sqlcsv_exporter.config import ConfigError, ExportConfig, resolve_as_of_date


def test_resolve_as_of_date_defaults_to_yesterday() -> None:
    value = resolve_as_of_date(None, now=datetime(2026, 3, 27, 10, 30, 0))
    assert value == "2026-03-26"


def test_resolve_as_of_date_rejects_invalid_format() -> None:
    with pytest.raises(ConfigError):
        resolve_as_of_date("03/27/2026")


def test_export_config_requires_sql_auth_credentials() -> None:
    with pytest.raises(ConfigError):
        ExportConfig(
            sql_file=Path("query.sql"),
            output_csv=Path("output.csv"),
            server="sql01",
            database="Reporting",
            as_of_date="2026-03-26",
            trusted_connection=False,
        )


def test_export_config_rejects_multicharacter_delimiter() -> None:
    with pytest.raises(ConfigError):
        ExportConfig(
            sql_file=Path("query.sql"),
            output_csv=Path("output.csv"),
            server="sql01",
            database="Reporting",
            as_of_date="2026-03-26",
            delimiter="||",
        )
