"""Control-plane API tests using a fake service (no Postgres/Kafka needed)."""

from fastapi.testclient import TestClient

from crawler.api.app import create_app
from crawler.api.routes import get_service
from crawler.api.schemas import (
    CrawlCreate,
    CrawlOut,
    DocumentOut,
    DomainOut,
    GraphNode,
    GraphOut,
    LinkOut,
    StatsOut,
    StatusCount,
)


class FakeService:
    def __init__(self) -> None:
        self.created: list[CrawlCreate] = []

    async def create_crawl(self, req: CrawlCreate) -> CrawlOut:
        self.created.append(req)
        return CrawlOut(
            id=1,
            name=req.name,
            status="running",
            seeds=req.seeds,
            max_depth=req.max_depth,
            max_pages=req.max_pages,
            seeded=len(req.seeds),
        )

    async def stats(self) -> StatsOut:
        return StatsOut(
            documents=10,
            edges=25,
            domains=3,
            near_duplicates=2,
            by_status=[StatusCount(status=200, count=9), StatusCount(status=404, count=1)],
        )

    async def list_domains(self, limit: int) -> list[DomainOut]:
        return [DomainOut(registered_domain="example.com", documents=7)][:limit]

    async def get_document(self, doc_id: int) -> DocumentOut | None:
        if doc_id != 1:
            return None
        return DocumentOut(
            id=1,
            url="https://example.com/",
            registered_domain="example.com",
            http_status=200,
            title="Home",
            in_degree=4,
            out_degree=2,
            links=[LinkOut(url="https://example.com/a", anchor="a")],
        )

    async def graph(self, limit: int) -> GraphOut:
        nodes = [GraphNode(url="https://example.com/", in_degree=4)]
        return GraphOut(top_pages=nodes[:limit])


def make_client() -> tuple[TestClient, FakeService]:
    app = create_app()
    fake = FakeService()
    app.dependency_overrides[get_service] = lambda: fake
    return TestClient(app), fake


def test_create_crawl_returns_201_and_seeds():
    client, fake = make_client()
    resp = client.post(
        "/api/v1/crawls",
        json={"seeds": ["https://example.com"], "max_depth": 2, "max_pages": 50},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] == 1
    assert body["seeded"] == 1
    assert body["status"] == "running"
    assert len(fake.created) == 1


def test_create_crawl_rejects_empty_seeds():
    client, _ = make_client()
    resp = client.post("/api/v1/crawls", json={"seeds": []})
    assert resp.status_code == 422  # pydantic validation (min_length=1)


def test_stats_endpoint():
    client, _ = make_client()
    resp = client.get("/api/v1/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["documents"] == 10
    assert body["edges"] == 25
    assert {s["status"] for s in body["by_status"]} == {200, 404}


def test_domains_endpoint_respects_limit():
    client, _ = make_client()
    resp = client.get("/api/v1/domains", params={"limit": 1})
    assert resp.status_code == 200
    assert resp.json()[0]["registered_domain"] == "example.com"


def test_document_found_and_not_found():
    client, _ = make_client()
    ok = client.get("/api/v1/documents/1")
    assert ok.status_code == 200
    assert ok.json()["links"][0]["url"] == "https://example.com/a"

    missing = client.get("/api/v1/documents/999")
    assert missing.status_code == 404


def test_graph_endpoint():
    client, _ = make_client()
    resp = client.get("/api/v1/graph", params={"limit": 5})
    assert resp.status_code == 200
    assert resp.json()["top_pages"][0]["in_degree"] == 4


def test_openapi_documents_routes():
    client, _ = make_client()
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/api/v1/crawls" in paths
    assert "/api/v1/stats" in paths
    assert "/api/v1/documents/{doc_id}" in paths
