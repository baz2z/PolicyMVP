from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Dict, Iterable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.datasources.bundestag_dip import DIPClient
from app.services.ingestion_service import run_and_index
from app.services.search_service import ensure_index
from app.settings import settings


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


def iter_range(date_from: str, date_to: str) -> Iterable[Dict]:
    client = DIPClient()
    for doc in client.plenarprotokoll_text(date_from=date_from, date_to=date_to):
        base = normalize_for_index(doc)
        base["metadata"]["document_type"] = "plenarprotokoll"
        yield base
    for doc in client.drucksache_text(date_from=date_from, date_to=date_to):
        base = normalize_for_index(doc)
        base["metadata"]["document_type"] = "drucksache"
        yield base


def main():
    # Backfill window can be controlled via env/.env (BACKFILL_START/BACKFILL_END)
    # Defaults: since 2025-08-01 until today
    start = settings.backfill_start or "2025-08-01"
    end = settings.backfill_end or datetime.utcnow().date().isoformat()
    ensure_index()
    res = run_and_index(iter_range(start, end), batch_size=200)
    print(f"Indexed: {res['indexed']}")


if __name__ == "__main__":
    main()
