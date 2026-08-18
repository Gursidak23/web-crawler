"""Tests for the in-memory per-domain crawl budget."""

from crawler.resilience.budget import InMemoryDomainBudget


async def test_budget_allows_up_to_limit_then_blocks():
    budget = InMemoryDomainBudget(limit=3)
    assert await budget.try_consume("a.example") is True
    assert await budget.try_consume("a.example") is True
    assert await budget.try_consume("a.example") is True
    # Fourth request for the same domain is over budget.
    assert await budget.try_consume("a.example") is False
    assert await budget.remaining("a.example") == 0


async def test_budget_is_per_domain():
    budget = InMemoryDomainBudget(limit=1)
    assert await budget.try_consume("a.example") is True
    assert await budget.try_consume("a.example") is False
    # A different domain has its own independent budget.
    assert await budget.try_consume("b.example") is True


async def test_zero_limit_disables_budget():
    budget = InMemoryDomainBudget(limit=0)
    for _ in range(100):
        assert await budget.try_consume("a.example") is True
    assert await budget.remaining("a.example") == -1
