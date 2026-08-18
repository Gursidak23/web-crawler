"""Tests for the per-host circuit breaker state machine."""

from crawler.resilience.circuit_breaker import CircuitBreaker, CircuitState


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_opens_after_threshold_failures():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=3, reset_seconds=10.0, clock=clock)

    assert cb.allow("a.example") is True
    cb.record_failure("a.example")
    cb.record_failure("a.example")
    assert cb.state("a.example") is CircuitState.CLOSED
    assert cb.allow("a.example") is True

    cb.record_failure("a.example")  # third failure trips it
    assert cb.state("a.example") is CircuitState.OPEN
    assert cb.allow("a.example") is False
    assert cb.open_count() == 1


def test_half_open_then_close_on_success():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=2, reset_seconds=10.0, clock=clock)
    cb.record_failure("h")
    cb.record_failure("h")
    assert cb.allow("h") is False

    clock.advance(10.0)
    # Cooldown elapsed: a single probe is permitted (half-open).
    assert cb.allow("h") is True
    assert cb.state("h") is CircuitState.HALF_OPEN

    cb.record_success("h")
    assert cb.state("h") is CircuitState.CLOSED
    assert cb.allow("h") is True
    assert cb.open_count() == 0


def test_half_open_reopens_on_failure():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, reset_seconds=5.0, clock=clock)
    cb.record_failure("h")
    assert cb.allow("h") is False

    clock.advance(5.0)
    assert cb.allow("h") is True  # half-open probe

    cb.record_failure("h")  # probe failed
    assert cb.state("h") is CircuitState.OPEN
    assert cb.allow("h") is False


def test_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=3, reset_seconds=10.0)
    cb.record_failure("h")
    cb.record_failure("h")
    cb.record_success("h")  # resets the streak
    cb.record_failure("h")
    cb.record_failure("h")
    # Only two consecutive failures since the reset -> still closed.
    assert cb.state("h") is CircuitState.CLOSED
