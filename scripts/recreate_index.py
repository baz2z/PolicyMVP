from __future__ import annotations

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.search_service import get_client, ensure_index
from app.settings import settings


def main():
    client = get_client()
    index = settings.os_index
    if client.indices.exists(index=index):
        client.indices.delete(index=index)
        print(f"Deleted index: {index}")
    ensure_index()
    print(f"Recreated index: {index}")


if __name__ == "__main__":
    main()
