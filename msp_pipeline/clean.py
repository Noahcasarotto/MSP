"""msp_pipeline.clean

Deduplicate summaries CSVs by company name.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple


def _normalize_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def dedupe_summaries(
    input_csv: Path,
    output_csv: Path,
    name_field: str = "name",
    keep: str = "first",
) -> Tuple[int, int]:
    """Deduplicate rows by normalized company name.

    Returns (total_rows, unique_rows)
    """
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames: List[str] = reader.fieldnames or [name_field]
        seen: Dict[str, Dict[str, str]] = {}
        total_rows = 0
        for row in reader:
            total_rows += 1
            key = _normalize_name(row.get(name_field, ""))
            if not key:
                continue
            if key in seen:
                if keep == "last":
                    seen[key] = row
                # else keep first
            else:
                seen[key] = row

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in seen.values():
            writer.writerow(row)

    return total_rows, len(seen)
