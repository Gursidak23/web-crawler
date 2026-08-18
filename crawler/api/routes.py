"""Control-plane HTTP routes (mounted under ``/api/v1``).

Submit crawls, inspect aggregate stats, browse domains and documents, and read
the top of the link graph. Handlers stay thin by delegating to a
:class:`CrawlService`; the dependency is overridable for tests.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from .schemas import (
    CrawlActionOut,
    CrawlCreate,
    CrawlOut,
    CrawlSummary,
    DocumentOut,
    DocumentPage,
    DomainOut,
    GraphOut,
    StatsOut,
)
from .service import CrawlService, SqlCrawlService

router = APIRouter(prefix="/api/v1", tags=["crawl"])


def get_service() -> CrawlService:
    """Provide the crawl service. Overridden in tests via ``dependency_overrides``."""
    return SqlCrawlService()


@router.post("/crawls", response_model=CrawlOut, status_code=201)
async def create_crawl(
    req: CrawlCreate, service: CrawlService = Depends(get_service)
) -> CrawlOut:
    return await service.create_crawl(req)


@router.get("/crawls", response_model=list[CrawlSummary])
async def list_crawls(
    limit: int = Query(50, ge=1, le=200),
    service: CrawlService = Depends(get_service),
) -> list[CrawlSummary]:
    return await service.list_crawls(limit)


@router.post("/crawls/{crawl_id}/stop", response_model=CrawlActionOut)
async def stop_crawl(
    crawl_id: int, service: CrawlService = Depends(get_service)
) -> CrawlActionOut:
    result = await service.stop_crawl(crawl_id)
    if result is None:
        raise HTTPException(status_code=404, detail="crawl not found")
    return result


@router.get("/stats", response_model=StatsOut)
async def get_stats(service: CrawlService = Depends(get_service)) -> StatsOut:
    return await service.stats()


@router.get("/documents", response_model=DocumentPage)
async def list_documents(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    domain: str | None = Query(None, description="Filter by registered domain"),
    q: str | None = Query(None, description="Case-insensitive URL/title search"),
    service: CrawlService = Depends(get_service),
) -> DocumentPage:
    return await service.list_documents(limit, offset, domain, q)


@router.get("/domains", response_model=list[DomainOut])
async def list_domains(
    limit: int = Query(50, ge=1, le=500),
    service: CrawlService = Depends(get_service),
) -> list[DomainOut]:
    return await service.list_domains(limit)


@router.get("/documents/{doc_id}", response_model=DocumentOut)
async def get_document(
    doc_id: int, service: CrawlService = Depends(get_service)
) -> DocumentOut:
    doc = await service.get_document(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="document not found")
    return doc


@router.get("/graph", response_model=GraphOut)
async def get_graph(
    limit: int = Query(20, ge=1, le=200),
    service: CrawlService = Depends(get_service),
) -> GraphOut:
    return await service.graph(limit)
