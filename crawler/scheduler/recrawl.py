"""Adaptive recrawl scheduler.

Pages don't all change at the same rate, so a fixed recrawl interval either
wastes fetches on static pages or lets news pages go stale. We use a min-heap
keyed by "due time" (a priority queue ordered by freshness) and an adaptive
interval: every time a page comes back *unchanged* we multiply its interval by
``backoff_factor`` (up to a cap); when it *changes* we reset to the minimum.
This is the classic "adaptive crawl frequency" strategy.
"""

from __future__ import annotations

import heapq
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .. import metrics
from ..config import RecrawlSettings


def next_interval(
    current_interval: float | None,
    *,
    changed: bool,
    settings: RecrawlSettings,
) -> float:
    """Compute the next recrawl interval for a page.

    ``current_interval`` is the interval that was used for the just-completed
    crawl (None for a first crawl). Changed pages reset to ``min_interval``;
    unchanged pages back off geometrically up to ``max_interval``.
    """
    if changed or current_interval is None:
        return settings.min_interval_seconds
    grown = current_interval * settings.backoff_factor
    return min(grown, settings.max_interval_seconds)


@dataclass(order=True)
class RecrawlEntry:
    due_at: float
    url: str = field(compare=False)
    interval: float = field(compare=False, default=0.0)
    depth: int = field(compare=False, default=0)
    crawl_id: int | None = field(compare=False, default=None)


class RecrawlScheduler:
    """A min-heap of pending recrawls ordered by due time.

    Not thread-safe; intended to be driven from a single asyncio task. Use
    :meth:`schedule` to enqueue and :meth:`pop_due` to drain everything that is
    due at a given time.
    """

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self._heap: list[RecrawlEntry] = []
        self._clock = clock

    def __len__(self) -> int:
        return len(self._heap)

    def schedule(
        self,
        url: str,
        interval: float,
        *,
        depth: int = 0,
        crawl_id: int | None = None,
        now: float | None = None,
    ) -> RecrawlEntry:
        due_at = (now if now is not None else self._clock()) + interval
        entry = RecrawlEntry(
            due_at=due_at, url=url, interval=interval, depth=depth, crawl_id=crawl_id
        )
        heapq.heappush(self._heap, entry)
        metrics.RECRAWL_SCHEDULED.inc()
        return entry

    def peek(self) -> RecrawlEntry | None:
        return self._heap[0] if self._heap else None

    def pop_due(self, now: float | None = None) -> list[RecrawlEntry]:
        """Pop and return all entries whose due time has arrived, soonest first."""
        moment = now if now is not None else self._clock()
        due: list[RecrawlEntry] = []
        while self._heap and self._heap[0].due_at <= moment:
            due.append(heapq.heappop(self._heap))
        return due

    def seconds_until_next(self, now: float | None = None) -> float | None:
        """Seconds until the next entry is due (0 if one is ready, None if empty)."""
        head = self.peek()
        if head is None:
            return None
        moment = now if now is not None else self._clock()
        return max(0.0, head.due_at - moment)
