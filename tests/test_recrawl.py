"""Tests for the adaptive recrawl scheduler and interval policy."""

from crawler.config import RecrawlSettings
from crawler.scheduler import RecrawlScheduler, next_interval


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_next_interval_backs_off_when_unchanged():
    s = RecrawlSettings(min_interval_seconds=100, max_interval_seconds=1000, backoff_factor=2.0)
    assert next_interval(None, changed=False, settings=s) == 100  # first crawl
    assert next_interval(100, changed=False, settings=s) == 200
    assert next_interval(200, changed=False, settings=s) == 400
    assert next_interval(800, changed=False, settings=s) == 1000  # capped


def test_next_interval_resets_when_changed():
    s = RecrawlSettings(min_interval_seconds=100, max_interval_seconds=1000, backoff_factor=2.0)
    assert next_interval(800, changed=True, settings=s) == 100


def test_scheduler_orders_by_due_time():
    clock = FakeClock()
    sched = RecrawlScheduler(clock=clock)
    sched.schedule("slow", interval=500)
    sched.schedule("fast", interval=100)
    sched.schedule("mid", interval=300)

    assert len(sched) == 3
    assert sched.peek().url == "fast"
    assert sched.seconds_until_next() == 100


def test_pop_due_returns_only_ready_entries():
    clock = FakeClock()
    sched = RecrawlScheduler(clock=clock)
    sched.schedule("a", interval=100)
    sched.schedule("b", interval=300)

    # Nothing due yet.
    assert sched.pop_due() == []

    clock.advance(150)
    due = sched.pop_due()
    assert [e.url for e in due] == ["a"]
    assert len(sched) == 1  # "b" still pending

    clock.advance(200)
    due = sched.pop_due()
    assert [e.url for e in due] == ["b"]
    assert len(sched) == 0


def test_seconds_until_next_is_none_when_empty():
    sched = RecrawlScheduler()
    assert sched.seconds_until_next() is None
    assert sched.peek() is None
