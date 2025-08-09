# MSP

Google-only web research and summarization for Managed Service Providers.

- Input: `MSP - MSP.csv`
- Search: Google Programmable Search (cached under `.cache/msp_search`)
- Summary: OpenAI Chat Completions
- Output: `msp_summaries.csv`

## Run
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U requests python-dotenv
set -a; source env_content.txt; set +a
python msp_search_and_summarize.py --limit 10
python msp_search_and_summarize.py
```

## Notes
- No scraping of LinkedIn pages; only search APIs + titles.
- Cached queries reduce repeat costs.
- Secrets are ignored via `.gitignore`.
