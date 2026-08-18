"""Tests for the global back-pressure semaphore."""

import asyncio

import pytest

from crawler.resilience.backpressure import Backpressure


async def test_caps_concurrent_holders():
    gate = Backpressure(limit=2)
    peak = 0
    current = 0
    lock = asyncio.Lock()

    async def task() -> None:
        nonlocal peak, current
        async with gate:
            async with lock:
                current += 1
                peak = max(peak, current)
            await asyncio.sleep(0.02)
            async with lock:
                current -= 1

    await asyncio.gather(*(task() for _ in range(10)))
    assert peak <= 2


async def test_in_flight_accounting():
    gate = Backpressure(limit=3)
    assert gate.in_flight == 0
    async with gate:
        assert gate.in_flight == 1
        async with gate:
            assert gate.in_flight == 2
    assert gate.in_flight == 0


def test_rejects_nonpositive_limit():
    with pytest.raises(ValueError):
        Backpressure(0)
