from __future__ import annotations

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.search_service import ensure_index, ping


def main():
    if not ping():
        print("OpenSearch not reachable. Ensure it's running at configured host:port.")
        return
    ensure_index()
    print("Index ensured.")


if __name__ == "__main__":
    main()
