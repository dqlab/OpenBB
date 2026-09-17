"""Bounded read-only queries for derivative datasets."""

from typing import Any

import duckdb
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.query_params import QueryParams
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_duckdb.utils.helpers import _database_path, quote_identifier
from pydantic import Field


class DuckDBQueryParams(QueryParams):
    """Shared local connection settings and result bound."""

    database_path: str | None = Field(default=None, description="Local database path; overrides duckdb_database_path.")
    limit: int = Field(
        default=100000,
        ge=1,
        le=1000000,
        description="Maximum rows; exceeding this bound raises an error instead of truncating.",
    )


def read_derivatives(
    query: Any,
    credentials: dict[str, str] | None,
    *,
    required: set[str],
    symbol_column: str = "symbol",
    symbol_fallback: str | None = None,
    date_column: str = "date",
    snapshot_dates: list[Any] | None = None,
    snapshot: bool = False,
    filters: list[tuple[str, str, Any]] | None = None,
    order: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Filter before materialization; identifiers and values are handled separately."""
    symbols = [part.strip().upper() for part in query.symbol.split(",") if part.strip()]
    if not symbols or (snapshot and len(symbols) != 1):
        raise OpenBBError("Supply one symbol for snapshots or one or more symbols for history.")
    path = _database_path(query.database_path, credentials)
    relation = quote_identifier(query.table)
    # All column names and operators are internal constants, not caller SQL.
    predicates = filters or []
    if any(op not in {"=", ">=", "<="} for _, op, _ in predicates):
        raise ValueError("Unsupported filter operator.")
    try:
        with duckdb.connect(path, read_only=True) as connection:
            description = connection.execute(
                f"SELECT * FROM {relation} LIMIT 0"  # noqa: S608
            ).description
            columns = [col[0].lower() for col in description]
            if len(set(columns)) != len(columns):
                raise OpenBBError("Duplicate column names in DuckDB derivative relation.")
            if symbol_column not in columns and symbol_fallback in columns:
                symbol_column = symbol_fallback
            required = required | {symbol_column, date_column} | {col for col, _, _ in predicates}
            missing = required - set(columns)
            if missing:
                raise OpenBBError(f"Missing required DuckDB columns: {', '.join(sorted(missing))}.")
            sym = quote_identifier(symbol_column)
            day = f"CAST({quote_identifier(date_column)} AS DATE)"
            where = [f"UPPER(CAST({sym} AS VARCHAR)) IN ({', '.join('?' for _ in symbols)})"]
            values: list[Any] = list(symbols)
            if snapshot:
                if snapshot_dates:
                    where.append(f"{day} IN ({', '.join('?' for _ in snapshot_dates)})")
                    values.extend(snapshot_dates)
                else:
                    # Select one complete snapshot for the underlying, before expiry/type filters.
                    where.append(f"{day} = (SELECT MAX({day}) FROM {relation} WHERE UPPER(CAST({sym} AS VARCHAR)) = ?)")  # noqa: S608
                    values.append(symbols[0])
            for column, operator, value in predicates:
                expression = quote_identifier(column)
                if column in {date_column, "expiration"}:
                    expression = f"CAST({expression} AS DATE)"
                if column == "option_type":
                    side = f"LOWER(TRIM(CAST({expression} AS VARCHAR)))"
                    expression = f"CASE {side} WHEN 'c' THEN 'call' WHEN 'p' THEN 'put' ELSE {side} END"
                where.append(f"{expression} {operator} ?")
                values.append(value)
            ordering = ", ".join(quote_identifier(col) for col in order)
            result = connection.execute(
                f"SELECT * FROM {relation} WHERE {' AND '.join(where)} ORDER BY {ordering} LIMIT ?",  # noqa: S608
                values + [query.limit + 1],
            )
            rows = result.fetchall()
    except duckdb.Error as exc:
        raise OpenBBError(f"Cannot read DuckDB derivative relation '{query.table}': {exc}") from exc
    if len(rows) > query.limit:
        raise OpenBBError("DuckDB result exceeds limit; narrow the symbols/dates or increase limit.")
    if not rows:
        raise EmptyDataError("No DuckDB derivative data matched the requested snapshot or filters.")
    return [dict(zip(columns, row)) for row in rows]
