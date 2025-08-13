# MSP Research Pipeline

This repository collects, enriches, and stores data about Managed Service Providers (MSPs).

## Folder layout

```
MSP's/
├── data/               # CSV inputs/outputs
│   ├── raw/            # Source lists (input)
│   └── processed/      # Enriched/summarised outputs
├── logs/               # Run-time logs
├── scripts/            # One-off or legacy Python scripts
├── msp_pipeline/       # Re-usable, importable Python package (CLI lives here)
└── requirements.txt    # Python deps
```

## Quick start

1.  Install deps (ideally inside a virtualenv):

    ```bash
    python3 -m pip install -r requirements.txt
    ```

2.  Load the latest summaries into DuckDB:

    ```bash
    python -m msp_pipeline load-csv \
        --csv data/processed/north_america_msp_summaries.csv \
        --db-path north_america_msp.duckdb \
        --replace --show-count
    ```

3.  Query inside Python or SQL CLI:

    ```python
    import duckdb
    con = duckdb.connect('north_america_msp.duckdb')
    con.execute('SELECT COUNT(*), country FROM msp GROUP BY country').show()
    ```

## Scripts vs Package

`scripts/` holds legacy standalone scripts (e.g. `msp_search_and_summarize.py`).
New work should go inside the `msp_pipeline` package to keep code modular.

## Environment variables

Put API keys in `.env` (or `env_content.txt`) at repo root:

```
GOOGLE_API_KEY=xxxxx
GOOGLE_CSE_ID=yyyyy
OPENAI_API_KEY=sk-zzz
```

## Contributing

1. Use `black` + `ruff` for formatting/linting.
2. PRs welcome for new commands under `msp_pipeline.cli`.
