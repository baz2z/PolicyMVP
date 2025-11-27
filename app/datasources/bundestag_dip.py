from __future__ import annotations

from typing import Dict, Iterable, Iterator, List, Optional, Any
import requests

from ..settings import settings


class DIPClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self.base_url = base_url or settings.dip_base_url
        self.api_key = api_key or settings.dip_api_key
        if not self.api_key:
            raise RuntimeError("DIP API key missing. Set DIP_API_KEY in environment.")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"ApiKey {self.api_key}"})

    def _paginate_cursor(self, path: str, extra_params: Optional[Dict[str, Any]] = None) -> Iterator[Dict]:
        """Paginate using DIP cursor: repeat same params, pass 'cursor' from last response
        until it stops changing. Returns each document dict.
        """
        url = f"{self.base_url}{path}"
        params: Dict[str, Any] = {"format": "json"}
        if extra_params:
            params.update({k: v for k, v in extra_params.items() if v is not None})

        prev_cursor: Optional[str] = None
        while True:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            docs: List[Dict] = data.get("documents") or data.get("data") or (data if isinstance(data, list) else [])
            for d in docs:
                yield d
            next_cursor = data.get("cursor")
            if not next_cursor or next_cursor == prev_cursor:
                break
            prev_cursor = next_cursor
            params["cursor"] = next_cursor

    def plenarprotokoll_text(self, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Iterator[Dict]:
        params: Dict[str, Any] = {}
        if date_from:
            params["f.datum.start"] = date_from
            params["datum.start"] = date_from
        if date_to:
            params["f.datum.end"] = date_to
            params["datum.end"] = date_to
        for raw in self._paginate_cursor("/plenarprotokoll-text", extra_params=params):
            out = self._normalize_plenar(raw)
            # Only yield records with text
            if out and out.get("text"):
                yield out

    def drucksache_text(self, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Iterator[Dict]:
        params: Dict[str, Any] = {}
        if date_from:
            params["f.datum.start"] = date_from
            params["datum.start"] = date_from
            params["fundstelle.datum.start"] = date_from
        if date_to:
            params["f.datum.end"] = date_to
            params["datum.end"] = date_to
            params["fundstelle.datum.end"] = date_to
        for raw in self._paginate_cursor("/drucksache-text", extra_params=params):
            out = self._normalize_drucksache(raw)
            # Only yield records with text
            if out and out.get("text"):
                yield out

    @staticmethod
    def _normalize_plenar(d: Dict) -> Optional[Dict]:
        # Expected fields: id, titel, datum, text
        id_ = d.get("id") or d.get("documentId") or d.get("vorgangId")
        titel = d.get("titel") or d.get("title")
        datum = d.get("datum") or d.get("date")
        text = d.get("text") or d.get("inhalt")
        if not (id_ and titel and datum and text):
            return None
        pdf_url = None
        fs = d.get("fundstelle")
        if isinstance(fs, dict):
            pdf_url = fs.get("pdf_url")
        dokumentart = d.get("dokumentart") or d.get("Dokumentart") or "Plenarprotokoll"
        out = {
            "id": str(id_),
            "titel": str(titel),
            "datum": str(datum),
            "text": str(text),
        }
        if pdf_url:
            out["pdf_url"] = pdf_url
        if dokumentart:
            out["dokumentart"] = str(dokumentart)
        return out

    @staticmethod
    def _normalize_drucksache(d: Dict) -> Optional[Dict]:
        # Require inline text; use pdf_url for linking when available
        id_ = d.get("id") or d.get("drucksacheId")
        titel = d.get("titel") or d.get("title")
        datum = d.get("datum") or d.get("date")
        text = d.get("text") or d.get("inhalt")
        if not (id_ and titel and datum and text):
            return None
        pdf_url = None
        fs = d.get("fundstelle")
        if isinstance(fs, dict):
            pdf_url = fs.get("pdf_url")
        out = {
            "id": str(id_),
            "titel": str(titel),
            "datum": str(datum),
            "text": str(text),
        }
        if pdf_url:
            out["pdf_url"] = pdf_url
        return out


def run_plenar(params: Dict | None = None) -> Iterable[Dict]:
    client = DIPClient()
    start = (params or {}).get("date_from")
    end = (params or {}).get("date_to")
    return client.plenarprotokoll_text(date_from=start, date_to=end)


def run_drucksache(params: Dict | None = None) -> Iterable[Dict]:
    client = DIPClient()
    start = (params or {}).get("date_from")
    end = (params or {}).get("date_to")
    return client.drucksache_text(date_from=start, date_to=end)
