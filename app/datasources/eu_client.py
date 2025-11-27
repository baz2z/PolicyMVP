from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Dict, Iterable, Iterator, Optional

try:
    from ..services.search_service import _doc_id  # type: ignore
except Exception:
    # Fallback for script-style imports
    import sys
    from pathlib import Path
    PRJ = Path(__file__).resolve().parents[2]
    if str(PRJ) not in sys.path:
        sys.path.insert(0, str(PRJ))
    from app.services.search_service import _doc_id  # type: ignore

# Reuse the standalone extractor code via importlib to avoid duplication.
import importlib.util
import pathlib

EXPLORE_SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "explore" / "eu_ep_crawl_prototype.py"
if not EXPLORE_SCRIPT.exists():
    raise RuntimeError(f"EU crawler script not found at {EXPLORE_SCRIPT}")

spec = importlib.util.spec_from_file_location("eu_ep", str(EXPLORE_SCRIPT))
eu_ep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eu_ep)  # type: ignore

# EU Data API helpers
from .eu_api import list_work_ids, get_work_details, build_download_url


def _load_text(path_no_ext: str) -> Optional[str]:
    p = path_no_ext + ".txt"
    try:
        with open(p, "r", encoding="utf-8") as f:
            return f.read()
    except OSError:
        return None


def _publication_date_from_cre_filename(name: str) -> Optional[str]:
    # CRE-10-YYYY-MM-DD_EN.xml
    try:
        parts = name.split("-")
        yyyy, mm, dd = parts[2], parts[3], parts[4][:2]
        # safer: find the YYYY-MM-DD span
        for i in range(len(name)):
            if name[i:i+10].count('-') == 2:
                d = name[i:i+10]
                return d + "T00:00:00Z"
    except Exception:
        pass
    return None


class EUClient:
    """Yield normalized EU documents compatible with our index shape."""

    def __init__(self) -> None:
        pass

    def iter_cre(self, term: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Dict]:
        out_dir = eu_ep.os.path.join(eu_ep.BASE_OUT, "cre")
        eu_ep.ensure_dir(out_dir)
        processed = 0
        for stub in list_work_ids("CRE", term=term):
            details = get_work_details(stub.id, lang="en")
            # Get PDF link for display
            pdf_url, src_name = build_download_url(details, lang="EN")
            # But fetch the XML for text extraction
            fetch_url = pdf_url.replace(".pdf", ".xml")
            print(f"[EU][CRE] id={stub.identifier} -> fetch {fetch_url}")
            status, data = eu_ep.fetch(fetch_url)
            if status != 200 or not data:
                continue
            # Save XML
            fname = f"{stub.identifier}_EN.xml"
            fpath = eu_ep.os.path.join(out_dir, fname)
            with open(fpath, "wb") as f:
                f.write(data)
            print(f"[EU][CRE] saved {fname}")
            try:
                eu_ep._save_cre_derivatives(eu_ep.os.path.splitext(fpath)[0], data)
            except Exception:
                pass
            base = eu_ep.os.path.splitext(fpath)[0]
            text = _load_text(base) or ""
            # Title: use API title if present; otherwise explicit sentinel to avoid masking data gaps
            title = details.title_en or "title not found"
            pub_date = None
            # derive publication date from identifier CRE-TERM-YYYY-MM-DD
            parts = stub.identifier.split("-")
            if len(parts) >= 5:
                pub_date = f"{parts[2]}-{parts[3]}-{parts[4]}T00:00:00Z"
            yield {
                "id": _doc_id(pdf_url),
                "title": title,
                "source": "eu",
                "source_name": src_name,
                "publication_date": pub_date,
                "url": pdf_url,
                "content": text,
                "language": "mixed",
                "metadata": {"document_type": src_name},
            }
            processed += 1
            if limit is not None and processed >= limit:
                break

    def iter_pdf_kind(self, kind: str, term: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Dict]:
        # kind in {A, TA, E, E-ASW}
        out_sub = kind.lower().replace("-asw", "")
        out_dir = eu_ep.os.path.join(eu_ep.BASE_OUT, out_sub)
        eu_ep.ensure_dir(out_dir)
        processed = 0
        for stub in list_work_ids(kind, term=term):
            details = get_work_details(stub.id, lang="en")
            url, src_name = build_download_url(details, lang="EN")
            print(f"[EU][{src_name}] id={stub.identifier} -> fetch {url}")
            status, data = eu_ep.fetch(url)
            if status != 200 or not data:
                continue
            fname = f"{stub.identifier}{'-ASW' if src_name=='E-ASW' else ''}_EN.pdf" if src_name in ("E", "E-ASW") else f"{stub.identifier}_EN.pdf"
            fpath = eu_ep.os.path.join(out_dir, fname)
            with open(fpath, "wb") as f:
                f.write(data)
            print(f"[EU][{src_name}] saved {fname}")
            h_frac, f_frac = eu_ep._get_margin_fracs()
            eu_ep._save_pdf_derivatives(eu_ep.os.path.splitext(fpath)[0], data, h_frac, f_frac)
            text = _load_text(eu_ep.os.path.splitext(fpath)[0]) or ""
            # Title: use API title if present; otherwise explicit sentinel to avoid masking data gaps
            title = details.title_en or "title not found"
            # publication_date from details.issued normalized to UTC Z when possible
            pub_date = None
            pub = details.issued
            if pub:
                try:
                    dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    if dt.tzinfo is not None:
                        pub_date = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                    else:
                        # treat as date or naive datetime
                        if len(pub) == 10:
                            pub_date = pub + "T00:00:00Z"
                        else:
                            pub_date = pub + "Z"
                except Exception:
                    # fallback best-effort
                    if len(pub) == 10:
                        pub_date = pub + "T00:00:00Z"
                    else:
                        pub_date = pub
            yield {
                "id": _doc_id(url),
                "title": title,
                "source": "eu",
                "source_name": src_name,
                "publication_date": pub_date,
                "url": url,
                "content": text,
                "language": "en",
                "metadata": {"document_type": src_name},
            }
            processed += 1
            if limit is not None and processed >= limit:
                break

    # iter_e is now covered by iter_pdf_kind("E") and iter_pdf_kind("E-ASW")
    def iter_e(self, term: Optional[int], limit: Optional[int] = None, asw: bool = False) -> Iterator[Dict]:
        return self.iter_pdf_kind("E-ASW" if asw else "E", term=term, limit=limit)


def run_eu_backfill(params: Dict | None = None) -> Iterable[Dict]:
    """Backfill all documents (API-driven). If term provided, filter to that term."""
    p = params or {}
    term = int(p.get("term")) if p.get("term") is not None else None
    client = EUClient()
    # CRE (no API term filter; we filter client-side)
    yield from client.iter_cre(term=term)
    # A/TA/E/E-ASW
    yield from client.iter_pdf_kind("A", term=term)
    yield from client.iter_pdf_kind("TA", term=term)
    yield from client.iter_pdf_kind("E", term=term)
    yield from client.iter_pdf_kind("E-ASW", term=term)


def run_eu_daily(params: Dict | None = None) -> Iterable[Dict]:
    """Daily small sample: limit to 1 per type for the given term (or default 10)."""
    p = params or {}
    term = int(p.get("term", 10))
    client = EUClient()
    yield from client.iter_cre(term=term, limit=1)
    yield from client.iter_pdf_kind("A", term=term, limit=1)
    yield from client.iter_pdf_kind("TA", term=term, limit=1)
    yield from client.iter_pdf_kind("E", term=term, limit=1)
    yield from client.iter_pdf_kind("E-ASW", term=term, limit=1)
