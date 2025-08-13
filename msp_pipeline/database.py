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
    "create_schema",
    "populate_companies_people",
]


def connect(db_path: str | Path = "north_america_msp.duckdb") -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database located at *db_path*."""
    return duckdb.connect(str(db_path))


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create companies & people tables if absent."""

    con.execute(
        """
        create sequence if not exists company_id_seq;
        create sequence if not exists people_id_seq;

        create table if not exists companies (
            id          bigint primary key default nextval('company_id_seq'),
            name        text not null,
            name_norm   text unique,
            website     text,
            linkedin    text,
            phone       text,
            address     text,
            summary     text,
            top_urls    text
        );

        create table if not exists people (
            id          bigint primary key default nextval('people_id_seq'),
            company_id  bigint,
            profile_url text not null unique,
            title       text,
            snippet     text,
            query_used  text,
            crawled_at  timestamp default current_timestamp
        );
        """
    )


def _normalize_name(name: str) -> str:
    import re

    return re.sub(r"\s+", " ", (name or "").strip().lower())


def populate_companies_people(
    db_path: str | Path,
    companies_csv: Path,
    people_csv: Path,
):
    """Load companies and people CSVs into DuckDB, linking via normalized name."""

    con = connect(db_path)
    create_schema(con)

    # ------------------------------------------------------------
    # Companies
    # ------------------------------------------------------------

    con.execute(
        f"""
        create or replace temp view comp_src as
        select *, lower(regexp_replace(name,'\\s+',' ','g')) as name_norm
        from read_csv_auto('{companies_csv}', header=true, sample_size=-1);

        insert into companies (name, name_norm, website, linkedin, phone, address, summary, top_urls)
        select name, name_norm, website, linkedin, phone, address, summary, top_urls
        from comp_src
        on conflict(name_norm) do update set
            name_norm=excluded.name_norm,
            website=excluded.website,
            summary=excluded.summary,
            top_urls=excluded.top_urls;
        """
    )

    companies_loaded = con.execute("select count(*) from comp_src").fetchone()[0]

    # Map names to IDs for join
    con.execute("create or replace temp view comp_ids as select id, name_norm from companies;")

    # ------------------------------------------------------------
    # People
    # ------------------------------------------------------------

    con.execute(
        f"""
        create or replace temp view ppl_src as
        select *, lower(regexp_replace(company,'\\s+',' ','g')) as name_norm
        from read_csv_auto('{people_csv}', header=true, sample_size=-1);

        create or replace temp view ppl_join as
        select c.id as company_id, p.profile_url, p.title, p.snippet, null as query_used
        from ppl_src p
        join comp_ids c using(name_norm);

        insert into people (company_id, profile_url, title, snippet, query_used)
        select company_id, profile_url, title, snippet, query_used
        from (
            select *, row_number() over (partition by profile_url) as rn
            from ppl_join
        ) where rn = 1
        on conflict(profile_url) do nothing;
        """
    )

    people_loaded = con.execute("select count(*) from ppl_join").fetchone()[0]
    con.close()
    return companies_loaded, people_loaded


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
