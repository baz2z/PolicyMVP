# Data sources

Each data source is a simple function that yields dictionaries (protocol documents).

Function signature:

```python
def run(params: dict | None = None) -> Iterable[dict]:
    yield { ...protocol doc... }
```

Required fields per document:
- title (str)
- source (str) e.g., "bundestag"
- publication_date (ISO8601 str) e.g., "2025-08-01T10:30:00Z"
- url (str)
- content (str)

Optional fields:
- id (str) — default computed from url
- source_name (str)
- language (str) — default "de"
- metadata (dict) — keys like document_type, committee
- ingested_at (ISO8601 str) — default now

See `bundestag_dip.py` for a concrete implementation against the DIP API.
