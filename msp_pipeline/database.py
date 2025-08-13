"""msp_pipeline.database

Utility helpers for DuckDB persistence: connect, ensure table, load CSV.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb

__all__ = [
    "connect",
    "load_csv",
]


def connect(db_path: str | Path = "north_america_msp.duckdb") -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database located at *db_path*."""
    return duckdb.connect(str(db_path))


def load_csv(
    csv_path: str | Path,
    table: str = "msp",
    db_path: str | Path = "north_america_msp.duckdb",
    replace: bool = False,
    append: bool = False,
) -> int:
    """Load *csv_path* into *table* inside *db_path*.

    Parameters
    ----------
    csv_path: Path to the input CSV file.
    table: Destination table name.
    db_path: DuckDB database file.
    replace: If True, drops and recreates the table.
    append: If True, inserts rows into existing table.

    Returns
    -------
    int
        Number of rows now present in the destination table.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    con = connect(db_path)

    if replace:
        con.execute(f"DROP TABLE IF EXISTS {table};")

    exists = (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()[0]
        == 1
    )

    if exists and not append and not replace:
        raise RuntimeError(
            f"Table '{table}' already exists; use replace=True or append=True."
        )

    if exists and append:
        con.execute(
            f"INSERT INTO {table} SELECT * FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE);"
        )
    else:
        con.execute(
            f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE);"
        )

    count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    con.close()
    return count
