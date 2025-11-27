from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Iterable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.datasources.bundestag_dip import DIPClient
from app.services.ingestion_service import run_and_index
from app.services.search_service import ensure_index


def normalize_for_index(d: Dict) -> Dict:
    title = d.get("titel") or ""
    datum = d.get("datum") or ""
    url = d.get("pdf_url") or f"https://dip.bundestag.de/vorgang/{d['id']}"
    return {
        "id": str(d["id"]),
        "title": title,
        "source": "bundestag",
        "source_name": "German Bundestag",
        "publication_date": datum if datum.endswith("Z") else (datum + "T00:00:00Z" if len(datum) == 10 else datum),
        "url": url,
        "content": d.get("text") or "",
        "language": "de",
        "metadata": {},
    }


def iter_newest_for_day(target_date: str, max_docs: int = 2000) -> Iterable[Dict]:
    """Iterate newest-first across the feed and only yield items for target_date.
    Stop scanning as soon as we encounter an item older than target_date.
    A safety cap prevents overly long runs.
    """
    client = DIPClient()
    produced = 0

    def day_gate(d: Dict) -> int:
        dstr = (d.get("datum") or "")[:10]
        if not dstr:
            return 0  # neither older nor newer, treat as neutral
        if dstr > target_date:
            return 1  # newer than target
        if dstr < target_date:
            return -1  # older than target
        return 0  # equal to target

    # plenarprotokoll newest-first
    for doc in client.plenarprotokoll_text():
        cmp = day_gate(doc)
        if cmp > 0:
            continue  # skip newer than target
        if cmp < 0:
            break  # we've gone past the target day
        base = normalize_for_index(doc)
        base["metadata"]["document_type"] = "plenarprotokoll"
        yield base
        produced += 1
        if produced >= max_docs:
            return

    # drucksache newest-first
    for doc in client.drucksache_text():
        cmp = day_gate(doc)
        if cmp > 0:
            continue
        if cmp < 0:
            break
        base = normalize_for_index(doc)
        base["metadata"]["document_type"] = "drucksache"
        yield base
        produced += 1
        if produced >= max_docs:
            return


def main():
    ensure_index()
    daily_date = os.getenv("DAILY_DATE") or (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    max_docs = int(os.getenv("DAILY_MAX_DOCS", "2000"))
    res = run_and_index(iter_newest_for_day(daily_date, max_docs=max_docs), batch_size=200)
    print(f"Indexed: {res['indexed']}")


if __name__ == "__main__":
    main()
