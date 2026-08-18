"""Smoke test: the FastAPI app boots and exposes health + metrics."""

from fastapi.testclient import TestClient

from crawler.api.app import create_app


def test_health_ok():
    client = TestClient(create_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_metrics_exposed():
    client = TestClient(create_app())
    resp = client.get("/metrics")
    assert resp.status_code == 200
    # Unlabeled metrics register immediately; labeled ones only emit once a label
    # combination is observed, so assert on the frontier-depth gauge.
    assert "crawler_frontier_depth" in resp.text
