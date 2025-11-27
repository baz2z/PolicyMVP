from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Load variables from .env.example first (as defaults), then .env to override
project_root = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=project_root / ".env.example", override=False)
load_dotenv(dotenv_path=project_root / ".env", override=True)


def _getenv(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    return v


@dataclass
class Settings:
    app_env: str = _getenv("APP_ENV", "development") or "development"
    secret_key: str = _getenv("SECRET_KEY", "dev-secret") or "dev-secret"
    page_size: int = int(_getenv("PAGE_SIZE", "10") or 10)

    # OpenSearch
    os_host: str = _getenv("OPENSEARCH_HOST", "localhost") or "localhost"
    os_port: int = int(_getenv("OPENSEARCH_PORT", "9200") or 9200)
    os_user: str | None = _getenv("OPENSEARCH_USER")
    os_password: str | None = _getenv("OPENSEARCH_PASSWORD")
    os_index: str = _getenv("OPENSEARCH_INDEX", "protocols-v1") or "protocols-v1"

    # Bundestag DIP API
    dip_base_url: str = _getenv("DIP_BASE_URL", "https://search.dip.bundestag.de/api/v1") or "https://search.dip.bundestag.de/api/v1"
    dip_api_key: str | None = _getenv("DIP_API_KEY")

    # Ingestion backfill window (optional)
    backfill_start: str | None = _getenv("BACKFILL_START")
    backfill_end: str | None = _getenv("BACKFILL_END")


settings = Settings()
