from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from .settings import settings
from .api.routes import router as api_router
from .services.search_service import ping

app = FastAPI(title="PolicyRadarVibe", debug=settings.app_env != "production")

# Static files and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


@app.on_event("startup")
async def _startup_check():
    if not ping():
        # Hard fail if OpenSearch is not reachable
        raise RuntimeError("OpenSearch is not reachable at startup. Check OPENSEARCH_* settings and service status.")


@app.get("/", response_class=None)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# Include API router (search endpoints and health)
app.include_router(api_router)
