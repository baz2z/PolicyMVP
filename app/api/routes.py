from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.status import HTTP_200_OK

from .schemas import SearchRequest
from ..services.search_service import search_documents, ping
from ..settings import settings
from fastapi.templating import Jinja2Templates

router = APIRouter()

templates = Jinja2Templates(directory="app/templates")


@router.get("/healthz")
async def healthz():
    if not ping():
        return JSONResponse({"status": "down"}, status_code=503)
    return {"status": "ok"}


@router.post("/search", response_class=HTMLResponse)
async def search(request: Request):
    form = await request.form()
    payload = SearchRequest(
        search_terms=form.get("search_terms") or None,
        source=form.getlist("source") if hasattr(form, "getlist") else [],
        doc_type=form.getlist("doc_type") if hasattr(form, "getlist") else [],
        date_from=form.get("date_from") or None,
        date_to=form.get("date_to") or None,
        page=int(form.get("page") or 1),
        size=int(form.get("size") or settings.page_size),
    )

    result = search_documents(
        query=payload.search_terms,
    sources=payload.source,
    doc_types=payload.doc_type,
        date_from=payload.date_from,
        date_to=payload.date_to,
        page=payload.page,
        size=payload.size,
    )

    return templates.TemplateResponse(
        "partials/results.html",
        {
            "request": request,
            "result": result,
            "search_terms": payload.search_terms or "",
            "selected_sources": set(payload.source),
            "selected_doc_types": set(payload.doc_type),
            "date_from": payload.date_from or "",
            "date_to": payload.date_to or "",
        },
        status_code=HTTP_200_OK,
    )
