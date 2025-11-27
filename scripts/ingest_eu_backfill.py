from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Dict

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.datasources.eu_client import run_eu_backfill
from app.services.ingestion_service import run_and_index
from app.services.search_service import ensure_index
from app.settings import settings


def main():
    # Backfill window via env or settings (EU_BACKFILL_START/END)
    start = os.getenv("EU_BACKFILL_START") or settings.backfill_start or (datetime.utcnow().date().isoformat())
    end = os.getenv("EU_BACKFILL_END") or settings.backfill_end or (datetime.utcnow().date().isoformat())
    term = int(os.getenv("EU_TERM") or 10)
    # In API-driven flow, 'term' optionally narrows scope; date window no longer used
    params: Dict = {"term": term}

    ensure_index()
    print(f"[EU][backfill] API-driven backfill; term={term if term else 'ALL'}")
    res = run_and_index(run_eu_backfill(params), batch_size=200)
    print(f"Indexed EU backfill: {res['indexed']}")


if __name__ == "__main__":
    main()
