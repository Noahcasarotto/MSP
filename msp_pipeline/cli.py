"""msp_pipeline.cli

Command-line entry-points for MSP pipeline tasks.

$ python -m msp_pipeline load-csv --csv north_america_msp_summaries.csv
"""
from __future__ import annotations

import typer

from pathlib import Path

from . import database as db
from . import people as ppl
from . import clean as cln

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


# ------------------------------------------------------------------
# Discover LinkedIn profiles via Google CSE
# ------------------------------------------------------------------


@app.command("discover-people")
def discover_people(
    input_csv: Path = typer.Option(
        "data/processed/north_america_msp_summaries.csv",
        exists=True,
        help="Companies CSV (summaries with 'name' column)",
    ),
    output_csv: Path = typer.Option(
        "data/processed/linkedin_people.csv",
        help="Output CSV of discovered profile URLs",
    ),
    limit_companies: int = typer.Option(0, help="Process only first N companies"),
    per_company: int = typer.Option(25, help="Max profiles kept per company"),
    pause: float = typer.Option(0.2, help="Delay between Google queries (s)"),
    verbose: bool = typer.Option(False, '--verbose', '-v', help='Print progress for each company'),
):
    """Discover public LinkedIn profile URLs for employees."""

    rows = ppl.discover_people(
        input_csv=input_csv,
        output_csv=output_csv,
        limit_companies=limit_companies,
        per_company=per_company,
        pause_s=pause,
        verbose=verbose,
    )
    typer.echo(f"[ok] Discovered {rows} LinkedIn profile links → {output_csv}")


# ------------------------------------------------------------------
# Load database with companies & people
# ------------------------------------------------------------------


@app.command("load-db")
def load_db(
    db_path: Path = typer.Option("north_america_msp.duckdb", help="DuckDB file"),
    companies_csv: Path = typer.Option("data/processed/north_america_msp_summaries_clean.csv", exists=True),
    people_csv: Path = typer.Option("data/processed/linkedin_people.csv", exists=True),
):
    """Populate DuckDB with companies + people and link by name."""
    from . import database as dbmod

    comps, ppl = dbmod.populate_companies_people(
        db_path=db_path,
        companies_csv=companies_csv,
        people_csv=people_csv,
    )
    typer.echo(
        f"[ok] Loaded {comps:,} companies & {ppl:,} people into {db_path}"
    )


@app.command("dedupe-summaries")
def dedupe_summaries(
    input_csv: Path = typer.Option(
        "data/processed/north_america_msp_summaries.csv", exists=True, help="Input summaries CSV"
    ),
    output_csv: Path = typer.Option(
        "data/processed/north_america_msp_summaries_clean.csv", help="Output deduped CSV"
    ),
    keep: str = typer.Option("first", help="Keep 'first' or 'last' occurrence of duplicates"),
):
    total, unique = cln.dedupe_summaries(input_csv=input_csv, output_csv=output_csv, keep=keep)
    removed = total - unique
    typer.echo(
        f"[ok] Dedupe complete: total={total:,}, unique={unique:,}, removed={removed:,} -> {output_csv}"
    )


def _run():  # helper for `python -m msp_pipeline`
    app()


if __name__ == "__main__":  # pragma: no cover
    _run()
