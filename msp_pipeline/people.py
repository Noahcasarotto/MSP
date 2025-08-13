"""msp_pipeline.people

Discover public LinkedIn profile URLs for employees of given companies using
Google Programmable Search (CSE). We do *not* crawl LinkedIn directly: only
public search-result metadata is used, so this stays within LinkedIn ToS.
"""
from __future__ import annotations

import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Minimal .env loader (shared with other modules)
# ---------------------------------------------------------------------------

def _load_env() -> None:
    candidates = [Path(".env"), Path("env_content.txt")]
    for env_path in candidates:
        if not env_path.exists():
            continue
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv(dotenv_path=env_path)
            return
        except Exception:
            for _line in env_path.read_text().splitlines():
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
            return


_load_env()

# ---------------------------------------------------------------------------
# Config & helpers
# ---------------------------------------------------------------------------

CACHE_DIR = Path(".cache/people_search")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GOOGLE_CX = os.getenv("GOOGLE_CSE_ID", "").strip()

UA = "MSPResearch/1.2 (+no-scrape)"
HDR_JSON = {"User-Agent": UA, "Accept": "application/json"}

PROFILE_RE = re.compile(r"^https?://(?:www\.)?linkedin\.com/in/[^/?#]+", re.I)
EXCLUDE_RE = re.compile(
    r"/pub/|/jobs/|/posts/|/events/|/learning/|/pulse/|/company/|/school/", re.I
)


def cache_key(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", s)[:150]


def cache_load(key: str):
    p = CACHE_DIR / f"{key}.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def cache_save(key: str, data):
    try:
        (CACHE_DIR / f"{key}.json").write_text(json.dumps(data))
    except Exception:
        pass


def http_json(url: str, headers=None, timeout: int = 15):
    try:
        with urlopen(Request(url, headers=headers or HDR_JSON), timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core search routines
# ---------------------------------------------------------------------------

def google_cse(query: str) -> List[Dict[str, str]]:
    """Return at most 10 result dicts from Google CSE."""
    if not (GOOGLE_KEY and GOOGLE_CX):
        return []
    url = (
        "https://www.googleapis.com/customsearch/v1?q="
        + quote_plus(query)
        + f"&key={GOOGLE_KEY}&cx={GOOGLE_CX}&num=10"
    )
    data = http_json(url)
    return data.get("items", []) if data else []


def is_profile(url: str) -> bool:
    if not url or EXCLUDE_RE.search(url):
        return False
    return bool(PROFILE_RE.match(url))


def likely_employee(result: Dict[str, str], company: str) -> bool:
    blob = ((result.get("title") or "") + " " + (result.get("snippet") or "")).lower()
    return company.lower() in blob


def build_queries(company: str, domain: str) -> List[str]:
    q: List[str] = []
    if company:
        q.append(f'site:linkedin.com/in "{company}"')
    if domain:
        q.append(f'site:linkedin.com/in "{domain}"')
    if company:
        q.append(f'site:linkedin.com/in {company} -jobs -job -hiring')
    return q[:3]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def discover_people(
    input_csv: Path,
    output_csv: Path,
    limit_companies: int = 0,
    per_company: int = 25,
    pause_s: float = 0.2,
    verbose: bool = False,
) -> int:
    """Discover public LinkedIn profile URLs for employees of companies.

    Parameters
    ----------
    input_csv : Path
        CSV with at least a `name` and `website` column (our summaries file).
    output_csv : Path
        Where to write discovered profile rows.
    limit_companies : int, optional
        Process only the first N companies (for testing), by default 0 (all).
    per_company : int, optional
        Maximum profile URLs to keep per company, by default 25.
    pause_s : float, optional
        Sleep duration between Google requests to stay polite, by default 0.2.

    Returns
    -------
    int
        Number of profile rows written.
    """

    # Read companies
    companies: List[Dict[str, str]] = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            companies.append(row)
    if limit_companies:
        companies = companies[:limit_companies]

    out_rows: List[Dict[str, str]] = []

    for idx, row in enumerate(companies, 1):
        company = (row.get("name") or row.get("Company Name") or "").strip()
        website = (row.get("website") or row.get("Website") or "").strip()
        domain_match = re.search(r"https?://(?:www\.)?([^/]+)", website, re.I)
        domain = domain_match.group(1).lower() if domain_match else ""

        if verbose:
            print(f"[{idx}/{len(companies)}] {company} …", end="", flush=True)

        seen: set[str] = set()
        for q in build_queries(company, domain):
            key = cache_key(q)
            res = cache_load(key) or google_cse(q)
            cache_save(key, res)

            for item in res:
                url = item.get("link", "")
                if not is_profile(url) or url in seen:
                    continue
                if not likely_employee(item, company):
                    continue
                seen.add(url)
                out_rows.append(
                    {
                        "company": company,
                        "website": website,
                        "profile_url": url,
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                    }
                )
                if len(seen) >= per_company:
                    break
            time.sleep(pause_s)
        if verbose:
            print(f" {len(seen)} profiles")

    # Write CSV
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if not out_rows:
        output_csv.write_text("")
        return 0
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader(); w.writerows(out_rows)
    return len(out_rows)

