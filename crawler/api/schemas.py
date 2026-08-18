"""Pydantic request/response models for the control-plane API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CrawlCreate(BaseModel):
    seeds: list[str] = Field(..., min_length=1, description="Seed URLs to start from")
    name: str = Field("crawl", description="Human-friendly crawl name")
    max_depth: int = Field(3, ge=0, le=20)
    max_pages: int = Field(1000, ge=1)
    allowed_domains: list[str] | None = Field(
        None, description="If set, restrict the crawl to these registered domains"
    )


class CrawlOut(BaseModel):
    id: int
    name: str
    status: str
    seeds: list[str]
    max_depth: int
    max_pages: int
    seeded: int = Field(0, description="Number of seed URLs enqueued onto the frontier")


class CrawlActionOut(BaseModel):
    """Result of an action on a crawl (e.g. stop)."""

    id: int
    status: str


class StatusCount(BaseModel):
    status: int | None
    count: int


class StatsOut(BaseModel):
    documents: int
    edges: int
    domains: int
    near_duplicates: int = Field(0, description="Documents flagged as near-duplicates")
    by_status: list[StatusCount]


class DomainOut(BaseModel):
    registered_domain: str
    documents: int


class LinkOut(BaseModel):
    url: str
    anchor: str | None = None


class DocumentOut(BaseModel):
    id: int
    url: str
    registered_domain: str
    http_status: int | None = None
    content_type: str | None = None
    title: str | None = None
    content_hash: str | None = None
    simhash: int | None = None
    depth: int = 0
    in_degree: int = 0
    out_degree: int = 0
    storage_key: str | None = None
    links: list[LinkOut] = Field(default_factory=list)


class GraphNode(BaseModel):
    url: str
    in_degree: int


class GraphOut(BaseModel):
    top_pages: list[GraphNode]


class CrawlSummary(BaseModel):
    id: int
    name: str
    status: str
    max_depth: int
    max_pages: int
    documents: int = Field(0, description="Documents stored for this crawl")
    created_at: str | None = None


class DocumentSummary(BaseModel):
    id: int
    url: str
    registered_domain: str
    http_status: int | None = None
    title: str | None = None
    depth: int = 0
    in_degree: int = 0
    out_degree: int = 0


class DocumentPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DocumentSummary]
