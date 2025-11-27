from __future__ import annotations

"""
EU EP crawl prototype (standalone): fetch a few documents and SAVE LOCALLY only.

No indexing. Minimal, self-contained, hardcoded patterns.

Document types:
- CRE (verbatim plenary debates): XML (_EN.xml)
- A (committee reports): PDF (_EN.pdf)
- TA (texts adopted): PDF (_EN.pdf)

Rules:
- Terms: 9 (2019-07-01..2024-06-30), 10 (2024-07-01..today)
- CRE: iterate dates (skip weekends)
- A/TA: iterate increasing numbers per year until first 404
- Language: English (_EN)
- Crawl slowly, retry transient errors

Output:
- Files saved under explore/out/eu/{cre|a|ta}/
"""

import os
import time
import logging
import io
import json
import random
from datetime import date, timedelta
from typing import Optional, Tuple, List, Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry
import pdfplumber
import xml.etree.ElementTree as ET


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("eu_ep_explore")


BASE_OUT = os.path.join(os.path.dirname(__file__), "out", "eu")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "PolicyMVP-EU-Explore/0.1",
        "Accept": "*/*",
    })
    return s


SESSION = _session()
REQUEST_DELAY_BASE_SEC = 2.0  # base delay between requests (seconds)
REQUEST_DELAY_JITTER_SEC = 3.0  # random 0..jitter seconds added


def _http_timeout(default: int = 120) -> int:
    """Get HTTP timeout from env or default."""
    v = os.getenv("EU_HTTP_TIMEOUT")
    if not v:
        return default
    try:
        return max(10, int(float(v)))
    except ValueError:
        return default


def _delay_settings() -> Tuple[float, float]:
    """Get (base, jitter) seconds for request delays from env or defaults."""
    def _read_f(key: str, default: float) -> float:
        try:
            v = os.getenv(key)
            return float(v) if v is not None else default
        except ValueError:
            return default
    base = _read_f("EU_REQUEST_DELAY_BASE", REQUEST_DELAY_BASE_SEC)
    jitter = _read_f("EU_REQUEST_DELAY_JITTER", REQUEST_DELAY_JITTER_SEC)
    base = max(0.0, min(base, 30.0))
    jitter = max(0.0, min(jitter, 30.0))
    return base, jitter


def fetch(url: str, timeout: Optional[int] = None) -> Tuple[int, Optional[bytes]]:
    try:
        to = timeout if timeout is not None else _http_timeout()
        r = SESSION.get(url, timeout=to)
        status = r.status_code
        if status == 200:
            return status, r.content
        return status, None
    except requests.RequestException as e:
        log.warning("request error for %s: %s", url, e)
        return 0, None
    finally:
        base, jitter = _delay_settings()
        time.sleep(base + (random.uniform(0.0, jitter) if jitter > 0 else 0.0))


# ---- CRE (XML) ----


def cre_url(term: int, d: date) -> str:
    return f"https://www.europarl.europa.eu/doceo/document/CRE-{term}-{d.isoformat()}_EN.xml"


def crawl_cre(term: int, max_days: int = 3) -> None:
    term_ranges = {
        9: (date(2019, 7, 1), date(2024, 6, 30)),
        10: (date(2024, 7, 1), date.today()),
    }
    if term not in term_ranges:
        log.info("unknown term: %s", term)
        return
    start, end = term_ranges[term]
    out_dir = os.path.join(BASE_OUT, "cre")
    ensure_dir(out_dir)

    d = end
    saved = 0
    while d >= start and saved < max_days:
        if d.weekday() < 5:
            url = cre_url(term, d)
            status, data = fetch(url)
            if status == 200 and data:
                fname = f"CRE-{term}-{d.isoformat()}_EN.xml"
                fpath = os.path.join(out_dir, fname)
                with open(fpath, "wb") as f:
                    f.write(data)
                log.info("saved: %s", fpath)
                # Save derivatives (.txt, .json) for XML
                try:
                    _save_cre_derivatives(os.path.splitext(fpath)[0], data)
                except Exception as e:
                    log.warning("cre derivative error for %s: %s", fpath, e)
                saved += 1
            elif status in (404, 410):
                pass
        d -= timedelta(days=1)


# ---- PDF text extraction helpers (pdfplumber with margin cropping) ----


def _get_margin_fracs() -> Tuple[float, float]:
    """Read header/footer margin fractions from env or default."""
    def _read(key: str, default: float) -> float:
        try:
            v = os.getenv(key)
            return float(v) if v is not None else default
        except ValueError:
            return default
    # Slightly more aggressive defaults to avoid footer/page numbers/links
    header = _read("EU_PDF_HEADER_FRAC", 0.10)
    footer = _read("EU_PDF_FOOTER_FRAC", 0.14)
    # Clamp to sane bounds
    header = max(0.0, min(header, 0.3))
    footer = max(0.0, min(footer, 0.3))
    return header, footer


def _pdf_extract_text_and_pages(
    pdf_bytes: bytes,
    header_frac: Optional[float] = None,
    footer_frac: Optional[float] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Extract plain text and per-page text using pdfplumber, cropping out
    headers/footers by fractional margins. Returns (full_text, pages_meta)
    where pages_meta is a list of dicts:
      {"page": int, "text": str, "chars": int, "bbox": [x0,y0,x1,y1]}
    """
    if header_frac is None or footer_frac is None:
        df_h, df_f = _get_margin_fracs()
        header_frac = df_h if header_frac is None else header_frac
        footer_frac = df_f if footer_frac is None else footer_frac

    pages_meta: List[Dict[str, Any]] = []
    texts: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                width, height = page.width, page.height
                header_px = header_frac * height
                footer_px = footer_frac * height
                # Keep central content area (exclude header/footer)
                bbox = (0, footer_px, width, height - header_px)
                cropped = page.crop(bbox)
                ptxt = (cropped.extract_text(x_tolerance=1.5, y_tolerance=1.0) or "").strip()
                if ptxt:
                    pages_meta.append({
                        "page": i,
                        "text": ptxt,
                        "chars": len(ptxt),
                        "bbox": [0, round(footer_px, 2), round(width, 2), round(height - header_px, 2)],
                    })
                    texts.append(ptxt)
            except Exception as e:  # extraction per page can fail; continue others
                log.warning("pdf page extract error p%s: %s", i, e)
                continue
    full_text = "\n\n\f\n\n".join(texts)
    return full_text, pages_meta


def _save_pdf_derivatives(
    base_path_no_ext: str,
    pdf_bytes: bytes,
    header_frac: Optional[float] = None,
    footer_frac: Optional[float] = None,
) -> None:
    """Save .txt and .json derived from a PDF next to the PDF path base (without extension)."""
    full_text, pages = _pdf_extract_text_and_pages(pdf_bytes, header_frac, footer_frac)
    # Save raw text
    try:
        with open(base_path_no_ext + ".txt", "w", encoding="utf-8") as f:
            f.write(full_text)
    except OSError as e:
        log.warning("failed to write txt: %s", e)

    # Save light structured JSON (good for indexing or later processing)
    try:
        with open(base_path_no_ext + ".json", "w", encoding="utf-8") as f:
            json.dump({
                "pages": pages,
                "meta": {
                    "generator": "pdfplumber",
                    "chars_total": len(full_text),
                    "pages_count": len(pages),
                    "header_frac": header_frac,
                    "footer_frac": footer_frac,
                }
            }, f, ensure_ascii=False)
    except OSError as e:
        log.warning("failed to write json: %s", e)


"""
Note: numeric URL crawlers for A/TA/E were removed in favor of the API-driven
ingestion. This module retains only extraction and backfill helpers used by the
EU client (pdf text derivation, CRE XML parsing, and backfill of existing files).
"""


def backfill_pdf_texts(force: bool = False) -> None:
    """Process any PDFs already present under out/eu/{a,ta} and create .txt/.json if missing,
    or when the header/footer fractions changed, or when force=True."""
    cur_h, cur_f = _get_margin_fracs()
    for kind in ("a", "ta", "e"):
        d = os.path.join(BASE_OUT, kind)
        if not os.path.isdir(d):
            continue
        for name in os.listdir(d):
            if not name.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(d, name)
            base = os.path.splitext(pdf_path)[0]
            txt_path = base + ".txt"
            json_path = base + ".json"
            need = force or not (os.path.exists(txt_path) and os.path.exists(json_path))
            if not need:
                try:
                    with open(json_path, "r", encoding="utf-8") as jf:
                        j = json.load(jf)
                    meta = j.get("meta", {})
                    if meta.get("generator") != "pdfplumber":
                        need = True
                    else:
                        old_h = meta.get("header_frac")
                        old_f = meta.get("footer_frac")
                        if old_h is None or old_f is None:
                            need = True
                        else:
                            # Reprocess if fractions changed materially
                            if abs(float(old_h) - cur_h) > 1e-6 or abs(float(old_f) - cur_f) > 1e-6:
                                need = True
                except Exception:
                    need = True
            if not need:
                continue
            try:
                with open(pdf_path, "rb") as f:
                    pdf_bytes = f.read()
                _save_pdf_derivatives(base, pdf_bytes, cur_h, cur_f)
                log.info("derived text/json for: %s", pdf_path)
            except OSError as e:
                log.warning("failed backfill for %s: %s", pdf_path, e)


# ---- CRE (XML) extraction helpers ----


def _text_norm(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def _extract_orator_label(orateur: ET.Element) -> str:
    # Concatenate visible text under ORATEUR; this may include role/name markers
    parts: List[str] = []
    for t in orateur.itertext():
        parts.append(t)
    label = _text_norm(" ".join(parts))
    # If empty, fall back to LIB attribute
    if not label:
        label = _text_norm(orateur.get("LIB", ""))
    return label


def _parse_cre_xml(xml_bytes: bytes) -> Dict[str, Any]:
    root = ET.fromstring(xml_bytes)
    debats = root.find(".//DEBATS") if root is not None else None
    chapters_out: List[Dict[str, Any]] = []
    if debats is None:
        return {"chapters": chapters_out}
    for ch in debats.findall("CHAPTER"):
        number = ch.get("NUMBER")
        # Prefer English chapter title
        title_en = None
        for tl in ch.findall("TL-CHAP"):
            if tl.get("VL") == "EN":
                title_en = _text_norm("".join(tl.itertext()))
                break
        if not title_en:
            tl0 = ch.find("TL-CHAP")
            if tl0 is not None:
                title_en = _text_norm("".join(tl0.itertext()))
        interventions: List[Dict[str, Any]] = []
        for inv in ch.findall("INTERVENTION"):
            orateur = inv.find("ORATEUR")
            orator_label = _text_norm(orateur.get("LIB", "")) if orateur is not None else ""
            if orateur is not None:
                label = _extract_orator_label(orateur)
                if label:
                    orator_label = label
            mepid = orateur.get("MEPID") if orateur is not None else None
            lg = orateur.get("LG") if orateur is not None else None
            paras: List[str] = []
            for p in inv.findall("PARA"):
                pt = _text_norm("".join(p.itertext()))
                if pt:
                    paras.append(pt)
            if orator_label or paras:
                interventions.append({
                    "orator": orator_label,
                    "mepid": mepid,
                    "lg": lg,
                    "paragraphs": paras,
                })
        chapters_out.append({
            "number": number,
            "title_en": title_en,
            "interventions": interventions,
        })
    return {"chapters": chapters_out}


def _render_cre_text(doc: Dict[str, Any]) -> str:
    lines: List[str] = []
    for ch in doc.get("chapters", []):
        number = ch.get("number") or "?"
        title = ch.get("title_en") or ""
        lines.append(f"Chapter {number}: {title}".strip())
        for inv in ch.get("interventions", []):
            who = inv.get("orator") or "Unknown"
            lines.append(f"- {who}")
            for para in inv.get("paragraphs", []):
                lines.append(f"  {para}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _save_cre_derivatives(base_path_no_ext: str, xml_bytes: bytes) -> None:
    doc = _parse_cre_xml(xml_bytes)
    # Write text
    try:
        with open(base_path_no_ext + ".txt", "w", encoding="utf-8") as f:
            f.write(_render_cre_text(doc))
    except OSError as e:
        log.warning("failed to write cre txt: %s", e)
    # Write json
    try:
        with open(base_path_no_ext + ".json", "w", encoding="utf-8") as f:
            json.dump({
                "meta": {"generator": "cre-parser"},
                "doc": doc,
            }, f, ensure_ascii=False)
    except OSError as e:
        log.warning("failed to write cre json: %s", e)


def backfill_cre_texts() -> None:
    d = os.path.join(BASE_OUT, "cre")
    if not os.path.isdir(d):
        return
    for name in os.listdir(d):
        if not name.lower().endswith(".xml"):
            continue
        xml_path = os.path.join(d, name)
        base = os.path.splitext(xml_path)[0]
        txt_path = base + ".txt"
        json_path = base + ".json"
        # Reprocess if missing or generator differs
        need = False
        if not (os.path.exists(txt_path) and os.path.exists(json_path)):
            need = True
        else:
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    j = json.load(jf)
                if j.get("meta", {}).get("generator") != "cre-parser":
                    need = True
            except Exception:
                need = True
        if not need:
            continue
        try:
            with open(xml_path, "rb") as f:
                xml_bytes = f.read()
            _save_cre_derivatives(base, xml_bytes)
            log.info("derived text/json for: %s", xml_path)
        except OSError as e:
            log.warning("failed cre backfill for %s: %s", xml_path, e)


def main() -> None:
    # Process any existing PDFs to generate text/json once
    backfill_pdf_texts()
    # And process any existing CRE XMLs
    backfill_cre_texts()


if __name__ == "__main__":
    main()
