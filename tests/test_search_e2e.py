from __future__ import annotations

import pytest

from app.services.search_service import ensure_index, index_documents, search_documents, ping
import uuid


@pytest.mark.skipif(not ping(), reason="OpenSearch not reachable")
def test_search_flow(tmp_path):
    test_index = f"test-{uuid.uuid4()}"
    ensure_index(test_index)
    docs = [
        {
            "title": "Wasserstoff Förderung im Bundestag",
            "source": "bundestag",
            "source_name": "German Bundestag",
            "publication_date": "2025-08-01T10:30:00Z",
            "url": "https://example.org/bt/1",
            "content": "Der Bundestag debattiert die Förderung von Wasserstoff.",
            "language": "de",
        },
        {
            "title": "Digitalisierung der Verwaltung",
            "source": "bmi",
            "source_name": "BMI",
            "publication_date": "2025-08-02T10:30:00Z",
            "url": "https://example.org/bmi/2",
            "content": "Das BMI veröffentlicht Maßnahmen zur Digitalisierung.",
            "language": "de",
        },
    ]
    index_documents(docs, index_name=test_index)

    res = search_documents("Wasserstoff", sources=["bundestag"], page=1, size=10, index_name=test_index)
    assert res["total"] >= 1
    assert any("Wasserstoff" in (h["title"] or "") for h in res["hits"]) 
