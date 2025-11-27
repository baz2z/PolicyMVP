from __future__ import annotations

from typing import Iterable, Dict, Any, List, Set
from datetime import datetime

from .search_service import index_documents


def validate_doc_shape(doc: Dict[str, Any]) -> Dict[str, Any]:
    # Minimal normalization and defaults
    out = dict(doc)
    out.setdefault("language", "de")
    out.setdefault("ingested_at", datetime.utcnow().isoformat() + "Z")
    # Required minimal fields: source, url, content
    missing = [k for k in ["source", "url", "content"] if not out.get(k)]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    return out


def run_and_index(source_iter: Iterable[Dict[str, Any]], batch_size: int = 500) -> Dict[str, Any]:
    batch: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    total = 0
    for doc in source_iter:
        clean = validate_doc_shape(doc)
        doc_id = clean.get("id") or clean.get("url")
        if doc_id:
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
        batch.append(clean)
        if len(batch) >= batch_size:
            res = index_documents(batch)
            total += res.get("success", 0)
            batch.clear()
    if batch:
        res = index_documents(batch)
        total += res.get("success", 0)
    return {"indexed": total}
