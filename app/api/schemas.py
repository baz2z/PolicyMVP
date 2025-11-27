from __future__ import annotations

from pydantic import BaseModel, Field
from typing import List, Optional


class SearchRequest(BaseModel):
    search_terms: Optional[str] = Field(default=None, description="Free text query")
    source: List[str] = Field(default_factory=list, description="Source filters")
    doc_type: List[str] = Field(default_factory=list, description="Document type filters (plenarprotokoll, drucksache)")
    date_from: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    date_to: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    page: int = Field(default=1, ge=1)
    size: int = Field(default=10, ge=1, le=100)


class SearchResultHit(BaseModel):
    id: str
    title: str
    source: str
    publication_date: Optional[str] = None
    url: Optional[str] = None
    snippet: Optional[str] = None


class SearchResponse(BaseModel):
    total: int
    page: int
    size: int
    hits: List[SearchResultHit]
    aggs: dict = Field(default_factory=dict)
