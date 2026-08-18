"""Integration test for the Kafka frontier (requires Docker).

Verifies round-trip delivery and that all URLs for a registered domain land on a
single partition (so one consumer owns the domain).
"""

import pytest

from crawler.config import Settings
from crawler.frontier.kafka_frontier import KafkaFrontier, ensure_topics, topic_names
from crawler.frontier.messages import FrontierMessage
from crawler.models import FrontierItem

pytestmark = pytest.mark.integration


async def test_frontier_round_trip_and_domain_partitioning():
    from aiokafka import AIOKafkaConsumer
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        settings = Settings()
        settings.kafka.bootstrap_servers = bootstrap
        settings.kafka.partitions = 4
        settings.kafka.topic_prefix = "testcrawler"

        await ensure_topics(settings)

        frontier = KafkaFrontier(settings)
        await frontier.start()
        try:
            urls = [f"http://alpha.example/{i}" for i in range(5)]
            urls += [f"http://beta.example/{i}" for i in range(5)]
            for url in urls:
                await frontier.add(FrontierItem(url=url, depth=0))
        finally:
            await frontier.stop()

        consumer = AIOKafkaConsumer(
            topic_names(settings)["frontier"],
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="test-reader",
        )
        await consumer.start()
        try:
            partitions_by_domain: dict[str, set[int]] = {}
            received = 0
            while received < 10:
                batch = await consumer.getmany(timeout_ms=3000, max_records=100)
                if not batch:
                    break
                for messages in batch.values():
                    for msg in messages:
                        fm = FrontierMessage.from_bytes(msg.value)
                        domain = "alpha.example" if "alpha" in fm.url else "beta.example"
                        partitions_by_domain.setdefault(domain, set()).add(msg.partition)
                        received += 1
        finally:
            await consumer.stop()

        assert received == 10
        assert len(partitions_by_domain["alpha.example"]) == 1
        assert len(partitions_by_domain["beta.example"]) == 1
