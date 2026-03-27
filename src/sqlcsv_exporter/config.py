from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_CHUNK_SIZE = 50_000
DEFAULT_DATE_PARAMETER = "@InAsOfDate"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
DEFAULT_ENCODING = "utf-8-sig"
DEFAULT_LOGIN_TIMEOUT_SECONDS = 15
DEFAULT_QUERY_TIMEOUT_SECONDS = 600


class ConfigError(ValueError):
    """Raised when CLI options cannot be turned into a valid config."""


def resolve_as_of_date(value: str | None, *, now: datetime | None = None) -> str:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ConfigError("Date must use YYYY-MM-DD format.") from exc

    current_time = now or datetime.now()
    return (current_time - timedelta(days=1)).strftime("%Y-%m-%d")


@dataclass(frozen=True)
class ExportConfig:
    sql_file: Path
    output_csv: Path
    server: str
    database: str
    as_of_date: str
    chunk_size: int = DEFAULT_CHUNK_SIZE
    delimiter: str = ","
    encoding: str = DEFAULT_ENCODING
    include_header: bool = True
    trusted_connection: bool = True
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    driver: str = DEFAULT_DRIVER
    date_parameter_name: str = DEFAULT_DATE_PARAMETER
    login_timeout_seconds: int = DEFAULT_LOGIN_TIMEOUT_SECONDS
    query_timeout_seconds: int = DEFAULT_QUERY_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if not self.server.strip():
            raise ConfigError("Server is required.")
        if not self.database.strip():
            raise ConfigError("Database is required.")
        if self.chunk_size <= 0:
            raise ConfigError("Chunk size must be greater than zero.")
        if len(self.delimiter) != 1:
            raise ConfigError("Delimiter must be exactly one character.")
        if self.login_timeout_seconds <= 0:
            raise ConfigError("Login timeout must be greater than zero.")
        if self.query_timeout_seconds <= 0:
            raise ConfigError("Query timeout must be greater than zero.")
        if not self.date_parameter_name.startswith("@"):
            raise ConfigError("Date parameter name must start with '@'.")

        if not self.trusted_connection:
            if not self.username:
                raise ConfigError("Username is required when SQL authentication is enabled.")
            if not self.password:
                raise ConfigError("Password is required when SQL authentication is enabled.")
