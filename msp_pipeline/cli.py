"""msp_pipeline.cli

Command-line entry-points for MSP pipeline tasks.

$ python -m msp_pipeline load-csv --csv north_america_msp_summaries.csv
"""
from __future__ import annotations

import typer

from pathlib import Path

from . import database as db

app = typer.Typer(add_completion=False, help="MSP data processing CLI")


@app.command("load-csv")
def load_csv(
    csv: Path = typer.Option("data/processed/north_america_msp_summaries.csv", exists=True, help="Input CSV path"),
    db_path: Path = typer.Option("north_america_msp.duckdb", help="DuckDB file"),
    table: str = typer.Option("msp", help="Destination table name"),
    replace: bool = typer.Option(False, "--replace", help="Drop and recreate table"),
    append: bool = typer.Option(False, "--append", help="Append to existing table"),
    show_count: bool = typer.Option(False, "--show-count", help="Print final row count"),
):
    """Load a CSV into DuckDB."""
    rows = db.load_csv(csv_path=csv, table=table, db_path=db_path, replace=replace, append=append)
    if show_count:
        typer.echo(f"[ok] Table '{table}' now contains {rows:,} rows in {db_path}")
    else:
        typer.echo("[ok] Load complete.")


def _run():  # helper for `python -m msp_pipeline`
    app()


if __name__ == "__main__":  # pragma: no cover
    _run()
