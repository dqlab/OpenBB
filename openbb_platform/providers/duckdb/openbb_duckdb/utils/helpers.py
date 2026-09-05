"""Helpers for querying historical data from DuckDB."""

import re
from datetime import date
from pathlib import Path
from typing import Any

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.errors import EmptyDataError

DATABASE_CREDENTIAL = "duckdb_database_path"
STANDARD_COLUMNS = {
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
}


def quote_identifier(identifier: str) -> str:
    """Quote a possibly-qualified DuckDB identifier."""
    parts = identifier.split(".")
    if not identifier.strip() or any(not part.strip() for part in parts):
        raise OpenBBError("DuckDB table must be a non-empty qualified identifier.")
    return ".".join(f'"{part.strip().replace(chr(34), chr(34) * 2)}"' for part in parts)


def _normalize_column(column: str) -> str:
    """Normalize a database column name for OpenBB model validation."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", column).strip("_").lower()


def _database_path(
    database_path: str | None,
    credentials: dict[str, str] | None,
) -> str:
    """Resolve and validate the configured local database path."""
    configured: Any = database_path
    if not configured and credentials:
        configured = credentials.get(DATABASE_CREDENTIAL)
    if hasattr(configured, "get_secret_value"):
        configured = configured.get_secret_value()
    if not configured:
        raise OpenBBError(
            "A DuckDB database path is required. Pass database_path or configure "
            "duckdb_database_path in OpenBB user settings."
        )

    path = Path(str(configured)).expanduser().resolve()
    if not path.is_file():
        raise OpenBBError(f"DuckDB database file was not found: {path}")
    return str(path)


def query_historical_data(
    *,
    database_path: str | None,
    table: str,
    symbol: str,
    start_date: date | None,
    end_date: date | None,
    credentials: dict[str, str] | None,
    required_columns: set[str],
) -> list[dict[str, Any]]:
    """Read standardized historical records from a DuckDB table or view."""
    # pylint: disable=import-outside-toplevel
    import duckdb

    path = _database_path(database_path, credentials)
    relation = quote_identifier(table)
    symbols = [item.strip().upper() for item in symbol.split(",") if item.strip()]
    if not symbols:
        raise OpenBBError("At least one symbol is required for a DuckDB query.")

    try:
        with duckdb.connect(database=path, read_only=True) as connection:
            described = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()  # noqa: S608
            columns = {str(row[0]).casefold(): str(row[0]) for row in described}
            missing = sorted((required_columns | {"symbol", "date"}) - columns.keys())
            if missing:
                raise OpenBBError(f"DuckDB relation '{table}' is missing required columns: {', '.join(missing)}.")

            symbol_column = quote_identifier(columns["symbol"])
            date_column = quote_identifier(columns["date"])
            placeholders = ", ".join("?" for _ in symbols)
            conditions = [f"UPPER(CAST({symbol_column} AS VARCHAR)) IN ({placeholders})"]
            parameters: list[Any] = list(symbols)

            if start_date is not None:
                conditions.append(f"CAST({date_column} AS DATE) >= ?")
                parameters.append(start_date)
            if end_date is not None:
                conditions.append(f"CAST({date_column} AS DATE) <= ?")
                parameters.append(end_date)

            cursor = connection.execute(
                f"SELECT * FROM {relation} WHERE {' AND '.join(conditions)} ORDER BY {date_column}, {symbol_column}",  # noqa: S608
                parameters,
            )
            result_columns = [_normalize_column(item[0]) for item in cursor.description]
            records = [dict(zip(result_columns, row)) for row in cursor.fetchall()]
    except OpenBBError:
        raise
    except duckdb.Error as exc:
        raise OpenBBError(f"DuckDB could not query relation '{table}' in '{path}': {exc}") from exc

    if not records:
        raise EmptyDataError(f"No DuckDB data found in '{table}' for: {', '.join(symbols)}.")

    for record in records:
        for column in STANDARD_COLUMNS:
            if column not in required_columns:
                record.setdefault(column, None)
    return records
