"""Frontier interface shared by the in-memory and Kafka backends."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..models import FrontierItem


@runtime_checkable
class Frontier(Protocol):
    """A source/sink of URLs to crawl.

    ``add`` returns ``True`` when the URL was newly accepted (i.e. not already
    seen), enabling callers to count genuinely new work.
    """

    async def add(self, item: FrontierItem) -> bool: ...

    async def get(self) -> FrontierItem: ...

    def task_done(self) -> None: ...

    async def join(self) -> None: ...

    def qsize(self) -> int: ...

    async def close(self) -> None: ...
