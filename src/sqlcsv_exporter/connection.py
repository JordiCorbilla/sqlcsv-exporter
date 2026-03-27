from __future__ import annotations

from types import ModuleType

from sqlcsv_exporter.config import ExportConfig


def load_pyodbc() -> ModuleType:
    try:
        import pyodbc  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is required to connect to SQL Server. Install system ODBC drivers and the pyodbc package."
        ) from exc

    return pyodbc


def build_connection_string(config: ExportConfig) -> str:
    driver_name = config.driver.strip().strip("{}")
    parts = [
        f"DRIVER={{{driver_name}}}",
        f"SERVER={config.server}",
        f"DATABASE={config.database}",
        "APP=sqlcsv-exporter",
    ]

    if config.trusted_connection:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={config.username}")
        parts.append(f"PWD={config.password}")

    return ";".join(parts) + ";"


def open_connection(config: ExportConfig):
    pyodbc = load_pyodbc()
    return pyodbc.connect(
        build_connection_string(config),
        timeout=config.login_timeout_seconds,
    )
