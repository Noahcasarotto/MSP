#!/usr/bin/env python3
"""
North America MSP Web Research & Summarization (Google-only)
- Reads companies from "North America MSP.csv" (14-column schema with header)
- Uses Google Programmable Search (GOOGLE_API_KEY + GOOGLE_CSE_ID)
- Summarizes top evidence with OpenAI (OPENAI_API_KEY)
- Writes to north_america_msp_summaries.csv

This file is self-contained; it mirrors msp_search_and_summarize.py but parses
rows via DictReader instead of raw lists.
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
from urllib.request import Request, urlopen

# ----- env loader (imported verbatim) -----

def _load_env() -> None:
    candidates = [Path('.env'), Path('env_content.txt')]
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
                if not _line or _line.startswith('#') or '=' not in _line:
                    continue
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
            return


_load_env()

# -----------------------
# Config
# -----------------------

CACHE_DIR = Path('.cache/north_america_search')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

GOOGLE_KEY = os.getenv('GOOGLE_API_KEY', '').strip()
GOOGLE_CX = os.getenv('GOOGLE_CSE_ID', '').strip()
OPENAI_KEY = os.getenv('OPENAI_API_KEY', '').strip()

UA = 'MSPResearch/1.1 (+no-scrape)'
HDR_JSON = {'User-Agent': UA, 'Accept': 'application/json'}
HDR_HTML = {'User-Agent': UA, 'Accept': 'text/html,application/xhtml+xml'}

# -----------------------
# Helpers
# -----------------------

def read_rows_with_header(path: str) -> List[Dict[str, str]]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

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
    m = re.search(r"https?://(?:www\.)?([^/]+)", url.strip(), re.I)
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

# ----- Search -----

def search_google(query: str) -> List[Dict[str, str]]:
    if not (GOOGLE_KEY and GOOGLE_CX):
        return []
    url = (
        f"https://www.googleapis.com/customsearch/v1?q={quote_plus(query)}&key={GOOGLE_KEY}&cx={GOOGLE_CX}&num=10"
    )
    data = http_json(url)
    items = data.get('items', []) if data else []
    return [{
        'url': it.get('link', ''),
        'title': it.get('title', ''),
        'snippet': it.get('snippet', '')
    } for it in items]

def search_web(query: str) -> List[Dict[str, str]]:
    key = cache_key('q-' + query)
    cached = cache_load(key)
    if cached is not None:
        return cached
    res = search_google(query)
    cache_save(key, res)
    return res

def build_queries(name: str, website: str) -> List[str]:
    name = name.strip().strip('"').strip("'")
    dom = website_domain(website)
    q = [f'"{name}" managed services'] if name else []
    if dom:
        q.append(f'site:{dom} managed services')
    return q

# ----- OpenAI summarization -----

def summarize_with_openai(model: str, company: str, evidence: List[Dict[str, str]]) -> str:
    if not OPENAI_KEY:
        return 'Missing OPENAI_API_KEY; cannot summarize.'

    items = []
    for it in evidence[:5]:
        title = it.get('title', '').strip() or fetch_title(it.get('url', ''))
        items.append({
            'title': title[:160],
            'snippet': it.get('snippet', '')[:300],
            'url': it.get('url', '')
        })

    system_msg = (
        'You are a research assistant. Summarize the company based only on provided evidence. '
        'Highlight focus areas, core services, cloud/vendor partnerships, and customer segments in 120-180 words.'
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
        import requests
        resp = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
            json=payload, timeout=60,
        )
        if resp.status_code != 200:
            return f'OpenAI error {resp.status_code}: {resp.text[:200]}'
        return resp.json()['choices'][0]['message']['content'].strip()
    except Exception as exc:
        return f'OpenAI request failed: {exc}'

# ----- core -----

def process_company(row: Dict[str, str], model: str) -> Tuple[str, List[Dict[str, str]]]:
    name = row.get('Company Name', '').strip()
    website = row.get('Website', '').strip()
    queries = build_queries(name, website)
    results: List[Dict[str, str]] = []
    for q in queries:
        results.extend(search_web(q)[:5])
        time.sleep(0.15)
    # dedupe URLs
    seen = set(); uniq = []
    for r in results:
        url = r.get('url', '')
        if url and url not in seen:
            uniq.append(r); seen.add(url)
    summary = summarize_with_openai(model, name, uniq[:10])
    return summary, uniq

# ----- main -----

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', default='North America MSP.csv')
    ap.add_argument('--output', default='north_america_msp_summaries.csv')
    ap.add_argument('--limit', type=int, default=0, help='Process only first N rows for testing')
    ap.add_argument('--model', default='gpt-4o-mini')
    args = ap.parse_args()

    if not Path(args.input).exists():
        print(f'❌ Missing input {args.input}'); return 1
    if not (GOOGLE_KEY and GOOGLE_CX):
        print('❌ Missing Google API keys'); return 1
    if not OPENAI_KEY:
        print('❌ Missing OpenAI key'); return 1

    rows = read_rows_with_header(args.input)
    if not rows:
        print('❌ CSV empty'); return 1

    out: List[Dict[str, str]] = []
    total = len(rows)
    print(f'Processing {total} rows …')
    for idx, row in enumerate(rows, 1):
        if args.limit and idx > args.limit:
            break
        name = row.get('Company Name', '').strip()
        if not name:
            continue
        print(f'[{idx}/{total}] {name} …', end='', flush=True)
        summary, refs = process_company(row, args.model)
        top_urls = '; '.join(r.get('url','') for r in refs[:5])
        out.append({
            'name': name,
            'website': row.get('Website', '').strip(),
            'linkedin': '',
            'phone': '',
            'address': row.get('Location', '').strip(),
            'summary': summary,
            'top_urls': top_urls,
        })
        print(' done')
        time.sleep(0.05)

    write_csv(args.output, out)
    print('Saved', args.output)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())