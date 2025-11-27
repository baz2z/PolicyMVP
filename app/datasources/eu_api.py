from __future__ import annotations

import os
import time
import random
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

import requests


API_BASE = "https://data.europarl.europa.eu/api/v2"


def _http_timeout(default: int = 60) -> int:
    v = os.getenv("EU_HTTP_TIMEOUT")
    if not v:
        return default
    try:
        return max(10, int(float(v)))
    except ValueError:
        return default


def _delay() -> None:
    try:
        base = float(os.getenv("EU_REQUEST_DELAY_BASE", "0.5"))
        jitter = float(os.getenv("EU_REQUEST_DELAY_JITTER", "0.5"))
    except ValueError:
        base, jitter = 0.5, 0.5
    time.sleep(max(0.0, base) + random.uniform(0.0, max(0.0, jitter)))




@dataclass
class WorkStub:
    id: str                # e.g., "eli/dl/doc/A-10-2024-0001"
    work_type: str         # e.g., "def/ep-document-types/REPORT_PLENARY"
    identifier: str        # e.g., "A-10-2024-0001"
    label: Optional[str]


@dataclass
class WorkDetails:
    id: str
    work_type: str
    identifier: str
    term: Optional[int]
    title_en: Optional[str]
    issued: Optional[str]
    is_answer: bool


WORKTYPE_QUERY = {
    "TA": "TEXT_ADOPTED",
    "A": "REPORT_PLENARY",
    "E": "QUESTION_WRITTEN",
    "E-ASW": "QUESTION_WRITTEN_ANSWER",
    "CRE": "CRE_PLENARY",
}


def list_work_ids(kind: str, term: Optional[int] = None, page_limit: int = 5000, max_pages: Optional[int] = None) -> Iterator[WorkStub]:
    """Yield WorkStub for a given kind (A|TA|E|E-ASW|CRE). Client-side filter by term using identifier prefix."""
    query_kind = WORKTYPE_QUERY[kind]
    offset = 0
    pages = 0
    session = requests.Session()
    session.headers.update({
        "User-Agent": "PolicyRadarVibe-EU-API/0.1",
        "Accept": "application/ld+json, application/json;q=0.9, */*;q=0.1",
    })
    while True:
        url = f"{API_BASE}/documents"
        params = {
            "work-type": query_kind,
            "format": "application/ld+json",
            "offset": offset,
            "limit": page_limit,
        }
        resp = session.get(url, params=params, timeout=_http_timeout())
        try:
            data = resp.json()
        except Exception:
            data = {}
        _delay()
        items: List[Dict] = list(data.get("data") or [])
        if not items:
            break
        for it in items:
            wid = it.get("id") or ""
            wtype = it.get("work_type") or ""
            ident = it.get("identifier") or ""
            label = it.get("label")
            # client-side filter by term if requested
            if term is not None:
                if not (ident.startswith(f"A-{term}-") or ident.startswith(f"TA-{term}-") or ident.startswith(f"E-{term}-") or ident.startswith(f"CRE-{term}-")):
                    continue
            yield WorkStub(id=wid, work_type=wtype, identifier=ident, label=label)
        offset += page_limit
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break


def parse_identifier(identifier: str) -> Dict[str, Optional[str]]:
    # Patterns: A-10-2024-0001, TA-10-2024-0001, E-10-2024-001357, CRE-10-2025-01-20
    parts = identifier.split("-")
    if not parts or len(parts) < 2:
        return {"kind": None, "term": None, "year": None, "number": None, "date": None}
    kind = parts[0]
    term = parts[1]
    if kind == "CRE":
        # CRE-TERM-YYYY-MM-DD
        date_str = "-".join(parts[2:5]) if len(parts) >= 5 else None
        return {"kind": kind, "term": term, "year": None, "number": None, "date": date_str}
    # A/TA/E
    year = parts[2] if len(parts) > 2 else None
    number = parts[3] if len(parts) > 3 else None
    return {"kind": kind, "term": term, "year": year, "number": number, "date": None}


def get_work_details(work_id: str, lang: str = "en") -> WorkDetails:
    # Correct endpoint expects the plain identifier (e.g., A-10-2024-0011)
    ident_only = work_id.split("/")[-1]
    url = f"{API_BASE}/documents/{ident_only}"
    params = {"format": "application/ld+json", "language": lang}
    session = requests.Session()
    session.headers.update({
        "User-Agent": "PolicyRadarVibe-EU-API/0.1",
        "Accept": "application/ld+json, application/json;q=0.9, */*;q=0.1",
    })
    resp = session.get(url, params=params, timeout=_http_timeout())
    try:
        j = resp.json()
    except Exception:
        j = {}
    _delay()
    data = (j.get("data") or [None])[0] or {}
    identifier = data.get("identifier") or work_id.split("/")[-1]
    wtype = data.get("work_type") or ""
    term = None
    pt = data.get("parliamentary_term")
    if isinstance(pt, str) and pt.startswith("org/ep-"):
        try:
            term = int(pt.split("-")[-1])
        except Exception:
            term = None
    # Titles and issued date
    title_en = None
    issued = None
    is_answer = False
    if "CRE_PLENARY" in wtype or identifier.startswith("CRE-"):
        td = data.get("title_dcterms") or {}
        if isinstance(td, dict):
            title_en = (td.get("en") if isinstance(td.get("en"), str) else None) or None
    else:
        # pick English expression if available (id ends with /en or title has 'en'), else first
        exprs: List[Dict] = data.get("is_realized_by") or []
        chosen: Optional[Dict] = None
        for e in exprs:
            eid = e.get("id") or ""
            etitle = e.get("title") or {}
            if (isinstance(eid, str) and eid.endswith("/en")) or (isinstance(etitle, dict) and "en" in etitle):
                chosen = e
                break
        if chosen is None and exprs:
            chosen = exprs[0]
        if chosen:
            tit = chosen.get("title") or {}
            if isinstance(tit, dict) and isinstance(tit.get("en"), str):
                title_en = tit.get("en")
            # issued may be in nested is_embodied_by list
            emb_list = chosen.get("is_embodied_by") or []
            if emb_list and isinstance(emb_list, list):
                issued = emb_list[0].get("issued") or None
    # infer answer from type
    if "QUESTION_WRITTEN_ANSWER" in wtype:
        is_answer = True
    return WorkDetails(
        id=work_id,
        work_type=wtype,
        identifier=identifier,
        term=term,
        title_en=title_en,
        issued=issued,
        is_answer=is_answer,
    )


def build_download_url(details: WorkDetails, lang: str = "EN") -> Tuple[str, str]:
    """Return (url, source_name) from details. source_name in {A, TA, E, E-ASW, CRE}."""
    meta = parse_identifier(details.identifier)
    kind = (meta.get("kind") or "").upper()
    if kind == "CRE":
        term = meta.get("term")
        d = meta.get("date")
        # Link to the PDF version for display while we still crawl XML separately
        url = f"https://www.europarl.europa.eu/doceo/document/CRE-{term}-{d}_{lang}.pdf"
        return url, "CRE"
    # A/TA/E
    term = meta.get("term")
    year = meta.get("year")
    number = meta.get("number")
    if kind == "E" and details.is_answer:
        suffix = "-ASW"
        src = "E-ASW"
    else:
        suffix = ""
        src = kind
    url = f"https://www.europarl.europa.eu/doceo/document/{kind}-{term}-{year}-{number}{suffix}_{lang}.pdf"
    return url, src
