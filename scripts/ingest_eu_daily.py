from __future__ import annotations

import os
import sys
from datetime import datetime

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.datasources.eu_client import run_eu_daily
from app.services.ingestion_service import run_and_index
from app.services.search_service import ensure_index


def main():
    ensure_index()
    term_env = os.getenv("EU_TERM")
    params = {"term": int(term_env)} if term_env else {}
    print(f"[EU][daily] starting small daily sample crawl/index term={params.get('term','10-default')}")
    res = run_and_index(run_eu_daily(params), batch_size=50)
    print(f"Indexed EU daily: {res['indexed']}")


if __name__ == "__main__":
    main()
