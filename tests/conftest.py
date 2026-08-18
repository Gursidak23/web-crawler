"""Pytest configuration: auto-skip ``integration`` tests when Docker is absent."""

from __future__ import annotations

import pytest


def _docker_available() -> bool:
    try:
        import docker
    except Exception:
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


DOCKER_AVAILABLE = _docker_available()


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if DOCKER_AVAILABLE:
        return
    skip_marker = pytest.mark.skip(reason="Docker not available; skipping integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
