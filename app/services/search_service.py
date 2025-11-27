from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, List, Optional

from opensearchpy import OpenSearch, helpers

from ..settings import settings


_client: Optional[OpenSearch] = None


def get_client() -> OpenSearch:
    global _client
    if _client is not None:
        return _client

    auth = None
    if settings.os_user and settings.os_password:
        auth = (settings.os_user, settings.os_password)

    _client = OpenSearch(
        hosts=[{"host": settings.os_host, "port": settings.os_port}],
        http_compress=True,
        http_auth=auth,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=20,
    )
    return _client


def ping() -> bool:
    try:
        return bool(get_client().ping())
    except Exception:
        return False


def ensure_index(index_name: str | None = None) -> None:
    index = index_name or settings.os_index
    client = get_client()
    if client.indices.exists(index=index):
        return

    body = {
        "settings": {
            "index": {"number_of_shards": 1, "number_of_replicas": 0},
            "analysis": {
                "analyzer": {
                    "german_custom": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "german_normalization",
                            "german_stem",
                        ],
                    }
                }
            },
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "german_custom",
                    "fields": {"raw": {"type": "keyword"}},
                },
                "content": {"type": "text", "analyzer": "german_custom"},
                "source": {"type": "keyword"},
                "source_name": {"type": "keyword"},
                "publication_date": {"type": "date"},
                "url": {"type": "keyword"},
                "language": {"type": "keyword"},
                "metadata": {
                    "properties": {
                        "document_type": {"type": "keyword"}
                    }
                },
                "ingested_at": {"type": "date"},
            }
        },
    }

    client.indices.create(index=index, body=body)


def _doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def index_documents(docs: List[Dict[str, Any]], index_name: str | None = None) -> dict:
    index = index_name or settings.os_index
    ensure_index(index)
    client = get_client()

    def gen_actions():
        for d in docs:
            if not d.get("id") and d.get("url"):
                d["id"] = _doc_id(d["url"])  # mutate in place
            yield {
                "_op_type": "index",
                "_index": index,
                "_id": d.get("id"),
                "_source": d,
            }

    success, errors = helpers.bulk(client, gen_actions(), stats_only=False, raise_on_error=False)
    # Make results visible for subsequent searches immediately (useful for tests and scripts)
    try:
        client.indices.refresh(index=index)
    except Exception:
        pass
    return {"success": success, "errors": errors}


def doc_exists(doc_id: str, index_name: str | None = None) -> bool:
    """Check if a document with given ID exists in the index."""
    index = index_name or settings.os_index
    client = get_client()
    try:
        return bool(client.exists(index=index, id=doc_id))
    except Exception:
        return False


def search_documents(
    query: Optional[str],
    sources: Optional[List[str]] = None,
    doc_types: Optional[List[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    size: int = 10,
    index_name: str | None = None,
) -> Dict[str, Any]:
    index = index_name or settings.os_index
    client = get_client()

    must: List[Dict[str, Any]] = []
    filter_clauses: List[Dict[str, Any]] = []

    if query:
        # Sicherstellen, dass zusätzliche Begriffe die Ergebnisse sichtbar beeinflussen:
        # 1) cross_fields mit AND (alle Terme), 2) Phrasen-Boosts, 3) schwacher fuzzy-OR-Fallback
        should_queries: List[Dict[str, Any]] = [
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^2", "content"],
                    "type": "cross_fields",
                    "operator": "and",
                    "boost": 3,
                }
            },
            {"match_phrase": {"title": {"query": query, "boost": 4, "slop": 2}}},
            {"match_phrase": {"content": {"query": query, "boost": 2, "slop": 2}}},
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^2", "content"],
                    "type": "best_fields",
                    "operator": "or",
                    "fuzziness": "AUTO",
                    "fuzzy_transpositions": True,
                    "boost": 0.2,
                }
            },
        ]
        must.append({"bool": {"should": should_queries, "minimum_should_match": 1}})

    if sources:
        filter_clauses.append({"terms": {"source": sources}})

    if doc_types:
        filter_clauses.append({"terms": {"metadata.document_type": doc_types}})

    if date_from or date_to:
        range_query: Dict[str, Any] = {"range": {"publication_date": {}}}
        if date_from:
            range_query["range"]["publication_date"]["gte"] = date_from
        if date_to:
            range_query["range"]["publication_date"]["lte"] = date_to
        filter_clauses.append(range_query)

    body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": must or [{"match_all": {}}],
                "filter": filter_clauses,
            }
        },
        "from": (page - 1) * size,
        "size": size,
        "aggs": {
            "sources": {"terms": {"field": "source"}},
            "doc_types": {"terms": {"field": "metadata.document_type"}},
        },
        "_source": ["id", "title", "source", "publication_date", "url", "content", "metadata.document_type"],
    }

    # Add highlighting when a query is provided
    if query:
        body["highlight"] = {
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
            "require_field_match": False,
            "fields": {
                "content": {
                    "fragment_size": 180,
                    "number_of_fragments": 1,
                    "no_match_size": 180,
                    "order": "score",
                },
                "title": {"number_of_fragments": 0},
            },
        }

    resp = client.search(index=index, body=body)

    hits_out: List[Dict[str, Any]] = []
    for h in resp.get("hits", {}).get("hits", []):
        src = h.get("_source", {})
        hl = h.get("highlight", {}) or {}
        # Prefer highlighted content fragment, then highlighted title, then plain truncated content
        snippet = None
        content_frags = hl.get("content") or []
        title_frags = hl.get("title") or []
        if content_frags:
            snippet = content_frags[0]
        elif title_frags:
            snippet = title_frags[0]
        elif src.get("content"):
            snippet = (src["content"][:300] + "…") if len(src["content"]) > 300 else src["content"]
        hits_out.append(
            {
                "id": src.get("id") or h.get("_id"),
                "title": src.get("title"),
                "source": src.get("source"),
                "publication_date": src.get("publication_date"),
                "url": src.get("url"),
                "snippet": snippet,
            }
        )

    total = resp.get("hits", {}).get("total", {}).get("value", 0)
    aggs_sources = resp.get("aggregations", {}).get("sources", {}).get("buckets", [])
    aggs_doc_types = resp.get("aggregations", {}).get("doc_types", {}).get("buckets", [])

    return {
        "total": total,
        "page": page,
        "size": size,
        "hits": hits_out,
    "aggs": {"sources": aggs_sources, "doc_types": aggs_doc_types},
    }
