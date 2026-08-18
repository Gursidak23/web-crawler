"""Per-host circuit breaker.

A host that keeps failing (timeouts, 5xx, connection errors) shouldn't keep
consuming fetch slots and politeness budget. After ``failure_threshold``
consecutive failures the breaker *opens* and short-circuits further fetches to
that host. Once ``reset_seconds`` elapse it goes *half-open* and allows a single
probe; success *closes* it, another failure re-*opens* it.

Domains are pinned to a worker by the consistent-hash ring, so an in-process,
per-host breaker is sufficient and avoids a Redis round-trip on the hot path.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from .. import metrics


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _HostState:
    failures: int = 0
    opened_at: float = 0.0
    state: CircuitState = CircuitState.CLOSED


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_seconds: float = 30.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        self.failure_threshold = failure_threshold
        self.reset_seconds = reset_seconds
        self._clock = clock
        self._hosts: dict[str, _HostState] = {}

    def _get(self, host: str) -> _HostState:
        state = self._hosts.get(host)
        if state is None:
            state = _HostState()
            self._hosts[host] = state
        return state

    def state(self, host: str) -> CircuitState:
        return self._get(host).state

    def allow(self, host: str) -> bool:
        """Return True if a fetch to ``host`` may proceed right now."""
        st = self._get(host)
        if st.state is CircuitState.OPEN:
            if self._clock() - st.opened_at >= self.reset_seconds:
                # Cooldown elapsed: allow a single probe.
                st.state = CircuitState.HALF_OPEN
                return True
            return False
        # CLOSED or HALF_OPEN both permit a request.
        return True

    def record_success(self, host: str) -> None:
        st = self._get(host)
        was_open = st.state is not CircuitState.CLOSED
        st.failures = 0
        st.state = CircuitState.CLOSED
        if was_open:
            self._refresh_gauge()

    def record_failure(self, host: str) -> None:
        st = self._get(host)
        if st.state is CircuitState.HALF_OPEN:
            # Probe failed: re-open immediately.
            st.opened_at = self._clock()
            st.state = CircuitState.OPEN
            self._refresh_gauge()
            return
        st.failures += 1
        if st.failures >= self.failure_threshold and st.state is CircuitState.CLOSED:
            st.opened_at = self._clock()
            st.state = CircuitState.OPEN
            self._refresh_gauge()

    def open_count(self) -> int:
        return sum(1 for s in self._hosts.values() if s.state is CircuitState.OPEN)

    def _refresh_gauge(self) -> None:
        metrics.CIRCUIT_STATE.set(self.open_count())
