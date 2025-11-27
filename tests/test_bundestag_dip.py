from __future__ import annotations

import os
import pytest

from app.datasources.bundestag_dip import DIPClient


@pytest.mark.skipif(not os.getenv("DIP_API_KEY"), reason="DIP_API_KEY not set")
def test_dip_client_fetch_smoke():
    c = DIPClient()
    # Fetch a small page from each endpoint
    it1 = c.plenarprotokoll_text(max_docs=1)
    first_plenar = next(it1, None)
    assert first_plenar is None or set(first_plenar.keys()) == {"id", "titel", "datum", "text"}

    it2 = c.drucksache_text(max_docs=1)
    first_ds = next(it2, None)
    assert first_ds is None or set(first_ds.keys()) == {"id", "titel", "datum", "text"}
