"""Resilience primitives: per-host circuit breaker, per-domain crawl budgets,
and global back-pressure."""

from .backpressure import Backpressure
from .budget import DomainBudget, InMemoryDomainBudget, RedisDomainBudget
from .circuit_breaker import CircuitBreaker, CircuitState

__all__ = [
    "Backpressure",
    "CircuitBreaker",
    "CircuitState",
    "DomainBudget",
    "InMemoryDomainBudget",
    "RedisDomainBudget",
]
