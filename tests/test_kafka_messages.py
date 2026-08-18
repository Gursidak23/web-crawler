"""Unit tests for frontier message encoding and domain partitioning.

These exercise the wire format and partition selection without a live broker.
"""

from crawler.config import Settings
from crawler.frontier.kafka_frontier import KafkaFrontier
from crawler.frontier.messages import FrontierMessage
from crawler.models import FrontierItem


def test_frontier_message_round_trip():
    message = FrontierMessage(url="http://example.com/a", depth=2, crawl_id=7, attempts=1)
    restored = FrontierMessage.from_bytes(message.to_bytes())
    assert restored == message


def test_message_to_item_and_back():
    item = FrontierItem(url="http://example.com/a", depth=3, crawl_id=9)
    message = FrontierMessage.from_item(item)
    assert message.to_item() == item


def test_partitioning_is_deterministic_and_in_range():
    settings = Settings()
    settings.kafka.partitions = 6
    frontier = KafkaFrontier(settings)

    p1 = frontier.partition_for("example.com")
    p2 = frontier.partition_for("example.com")
    assert p1 == p2
    assert 0 <= p1 < 6


def test_same_domain_distinct_paths_share_a_partition():
    # All URLs for one registered domain must map to a single partition so one
    # consumer owns the domain.
    settings = Settings()
    settings.kafka.partitions = 6
    frontier = KafkaFrontier(settings)

    from crawler.url_utils import registered_domain

    d1 = registered_domain("http://www.example.com/a")
    d2 = registered_domain("http://example.com/b?x=1")
    assert d1 == d2 == "example.com"
    assert frontier.partition_for(d1) == frontier.partition_for(d2)
