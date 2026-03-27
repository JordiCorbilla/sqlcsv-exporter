from __future__ import annotations

import re
from pathlib import Path


def read_sql_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def replace_declared_date_parameter(
    sql_text: str,
    date_value: str,
    *,
    parameter_name: str,
) -> tuple[str, bool]:
    escaped_name = re.escape(parameter_name)
    pattern = (
        rf"((?:DECLARE\s+{escaped_name}\s+(?:DATE|DATETIME|DATETIME2)"
        rf"|SET\s+{escaped_name})\s*=\s*')([^']*)(')"
    )
    rewritten_sql, replacements = re.subn(
        pattern,
        rf"\g<1>{date_value}\g<3>",
        sql_text,
        flags=re.IGNORECASE,
    )
    return rewritten_sql, replacements > 0
