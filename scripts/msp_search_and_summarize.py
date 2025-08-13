#!/usr/bin/env python3
"""
MSP Web Research and Summarization (Google-only)
- Reads companies from "MSP - MSP.csv"
- Uses Google Programmable Search (GOOGLE_API_KEY + GOOGLE_CSE_ID)
- Summarizes top results with OpenAI (OPENAI_API_KEY)
- Writes results to msp_summaries.csv

This script does not modify anything outside the MSP's folder.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import quote_plus
from urllib.request import urlopen, Request

# -----------------------
# Environment auto-loader
# -----------------------

def _load_env() -> None:
    """Load environment vars from .env if present; if not, try env_content.txt.
    Falls back to a minimal KEY=VALUE parser if python-dotenv is unavailable.
    """
    candidates = [Path('.env'), Path('env_content.txt')]
    for env_path in candidates:
        if not env_path.exists():
            continue
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(dotenv_path=env_path)
            return
        except Exception:
            # Minimal fallback – only KEY=VALUE lines, ignores quotes & comments
            for _line in env_path.read_text().splitlines():
                _line = _line.strip()
                if not _line or _line.startswith('#') or '=' not in _line:
                    continue
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
            return


_load_env()

# -----------------------
# Config
# -----------------------

CACHE_DIR = Path('.cache/msp_search')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_KEY = os.getenv('GOOGLE_API_KEY', '').strip()
GOOGLE_CX  = os.getenv('GOOGLE_CSE_ID', '').strip()
OPENAI_KEY = os.getenv('OPENAI_API_KEY', '').strip()

UA = 'MSPResearch/1.0 (+no-scrape)'
HDR_JSON = {'User-Agent': UA, 'Accept': 'application/json'}
HDR_HTML = {'User-Agent': UA, 'Accept': 'text/html,application/xhtml+xml'}

# -----------------------
# Helpers
# -----------------------


def read_rows_no_header(path: str) -> List[List[str]]:
    with open(path, newline='', encoding='utf-8') as f:
        return [row for row in csv.reader(f)]


def write_csv(path: str, rows: List[Dict[str, str]]):
    if not rows:
        Path(path).write_text('')
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)


def website_domain(url: str) -> str:
    if not url:
        return ''
    m = re.search(r"https?://(?:www\.)?([^/]+)", (url or '').strip(), re.I)
    return m.group(1).lower() if m else ''


def cache_key(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]+', '-', s)[:120]


def cache_load(key: str):
    p = CACHE_DIR / f'{key}.json'
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def cache_save(key: str, data):
    try:
        (CACHE_DIR / f'{key}.json').write_text(json.dumps(data))
    except Exception:
        pass


def http_json(url: str, headers=None, timeout=15):
    try:
        with urlopen(Request(url, headers=headers or HDR_JSON), timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


def fetch_title(url: str) -> str:
    try:
        with urlopen(Request(url, headers=HDR_HTML), timeout=10) as r:
            txt = r.read(4000).decode('utf-8', 'ignore')
            m = re.search(r"<title[^>]*>([^<]+)</title>", txt, re.I)
            return m.group(1).strip() if m else ''
    except Exception:
        return ''

# -----------------------
# Google search only
# -----------------------


def search_google(query: str) -> List[Dict[str, str]]:
    if not (GOOGLE_KEY and GOOGLE_CX):
        return []
    url = (
        f"https://www.googleapis.com/customsearch/v1?q={quote_plus(query)}"
        f"&key={GOOGLE_KEY}&cx={GOOGLE_CX}&num=10"
    )
    data = http_json(url)
    items = data.get('items', []) if data else []
    return [
        {
            'url': it.get('link','') or '',
            'title': it.get('title','') or '',
            'snippet': it.get('snippet','') or ''
        }
        for it in items
    ]


def search_web(query: str) -> List[Dict[str, str]]:
    key = cache_key('q-' + query)
    cached = cache_load(key)
    if cached is not None:
        return cached
    res = search_google(query)
    cache_save(key, res)
    return res


def build_msp_queries(name: str, website: str) -> List[str]:
    name = (name or '').strip().strip('"').strip("'")
    dom = website_domain(website)
    queries: List[str] = []
    if name:
        queries.extend([
            f'"{name}" managed services',
            f'"{name}" IT services',
            f'"{name}" cloud services',
            f'"{name}" company profile',
        ])
    if dom:
        queries.extend([
            f'site:{dom} about',
            f'site:{dom} services',
            f'site:{dom} solutions',
        ])
    seen, uniq = set(), []
    for q in queries:
        if q not in seen:
            uniq.append(q); seen.add(q)
    return uniq[:6]

# -----------------------
# OpenAI summarization
# -----------------------


def summarize_with_openai(model: str, company: str, evidence: List[Dict[str, str]]) -> str:
    if not OPENAI_KEY:
        return "Missing OPENAI_API_KEY in environment; cannot summarize."

    items = []
    for it in evidence[:5]:
        title = (it.get('title') or '').strip()
        snippet = (it.get('snippet') or '').strip()
        url = (it.get('url') or '').strip()
        if not title:
            title = fetch_title(url)
        items.append({
            'title': title[:160],
            'snippet': snippet[:300],
            'url': url
        })

    system_msg = (
        "You are a precise research assistant. Summarize only from given evidence. "
        "Include focus areas, core services, notable technology/partner ecosystems (e.g., Azure/AWS/GCP), "
        "and typical customer segments/regions. Keep it concise (120-180 words)."
    )
    user_msg = json.dumps({'company': company, 'evidence': items}, ensure_ascii=False)

    payload = {
        'model': model,
        'messages': [
            {'role': 'system', 'content': system_msg},
            {'role': 'user', 'content': user_msg},
        ],
        'temperature': 0.2,
        'max_tokens': 300,
    }

    try:
        import requests  # lazy import
        resp = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENAI_KEY}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=60,
        )
        if resp.status_code != 200:
            return f"OpenAI error {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        return (data.get('choices', [{}])[0]
                    .get('message', {})
                    .get('content', '')
                    .strip()) or 'No summary generated.'
    except Exception as exc:
        return f"OpenAI request failed: {exc}"

# -----------------------
# Main flow
# -----------------------


def dedupe_results(results: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen_urls = set()
    uniq = []
    for r in results:
        url = (r.get('url') or '').strip()
        if not url or url in seen_urls:
            continue
        uniq.append(r)
        seen_urls.add(url)
    return uniq


def process_company(name: str, website: str, model: str) -> Tuple[str, List[Dict[str, str]]]:
    queries = build_msp_queries(name, website)
    collected: List[Dict[str, str]] = []
    for q in queries:
        results = search_web(q)
        if results:
            collected.extend(results[:5])
        time.sleep(0.15)
    collected = dedupe_results(collected)[:10]
    summary = summarize_with_openai(model=model, company=name, evidence=collected)
    return summary, collected


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', default='data/raw/MSP - MSP.csv')
    ap.add_argument('--output', default='data/processed/msp_summaries.csv')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--model', default='gpt-4o-mini')
    args = ap.parse_args()

    if not Path(args.input).exists():
        print(f"❌ Missing input: {args.input}")
        return 1
    if not (GOOGLE_KEY and GOOGLE_CX):
        print('❌ Missing Google search API keys. Set GOOGLE_API_KEY and GOOGLE_CSE_ID in .env or env_content.txt')
        return 1
    if not OPENAI_KEY:
        print('❌ Missing OPENAI_API_KEY in .env or env_content.txt')
        return 1

    rows = read_rows_no_header(args.input)
    if not rows:
        print('❌ Input CSV appears empty.')
        return 1

    out_rows: List[Dict[str, str]] = []
    total = len(rows)
    print(f'Processing {total} MSPs from {args.input} …')

    for idx, row in enumerate(rows, 1):
        if args.limit and idx > args.limit:
            break
        name = (row[0] if len(row) > 0 else '').strip()
        website = (row[1] if len(row) > 1 else '').strip()
        linkedin = (row[6] if len(row) > 6 else '').strip()
        phone = (row[4] if len(row) > 4 else '').strip()
        address = (row[5] if len(row) > 5 else '').strip()
        if not name:
            continue
        print(f"[{idx}/{total}] {name} …", end='', flush=True)
        summary, refs = process_company(name=name, website=website, model=args.model)
        top_urls = '; '.join(r.get('url','') for r in refs[:5])
        out_rows.append({
            'name': name,
            'website': website,
            'linkedin': linkedin,
            'phone': phone,
            'address': address,
            'summary': summary,
            'top_urls': top_urls,
        })
        print(' done')
        time.sleep(0.05)

    write_csv(args.output, out_rows)
    print('Saved:', args.output)
    return 0


if __name__ == '__main__':
    raise SystemExit(main()) 