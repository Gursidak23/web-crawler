"""Kafka-backed distributed frontier.

The ``frontier`` topic is **partitioned by registered domain** using the
consistent-hash ring, so every URL for a domain lands on the same partition and
therefore the same consumer in the group - giving each domain a single owner
(natural politeness + robots/DNS cache locality). Sibling topics carry retries
and dead letters.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from ..config import Settings, get_settings
from ..logging_setup import get_logger
from ..metrics import URLS_ENQUEUED
from ..models import FrontierItem
from ..sharding import ConsistentHashRing
from ..url_utils import registered_domain
from .messages import FrontierMessage

log = get_logger(__name__)


def topic_names(settings: Settings) -> dict[str, str]:
    prefix = settings.kafka.topic_prefix
    return {
        "frontier": f"{prefix}.frontier",
        "parsed": f"{prefix}.parsed",
        "retry": f"{prefix}.retry",
        "dlq": f"{prefix}.dlq",
    }


async def ensure_topics(settings: Settings | None = None) -> None:
    """Create the crawler topics if absent (idempotent)."""
    s = settings or get_settings()
    names = topic_names(s)
    admin = AIOKafkaAdminClient(bootstrap_servers=s.kafka.bootstrap_servers)
    await admin.start()
    try:
        new_topics = [
            NewTopic(names["frontier"], s.kafka.partitions, s.kafka.replication_factor),
            NewTopic(names["parsed"], s.kafka.partitions, s.kafka.replication_factor),
            NewTopic(names["retry"], s.kafka.partitions, s.kafka.replication_factor),
            NewTopic(names["dlq"], 1, s.kafka.replication_factor),
        ]
        for topic in new_topics:
            try:
                await admin.create_topics([topic])
            except Exception as exc:  # noqa: BLE001 - already-exists is benign
                log.debug("create_topic_skipped", topic=topic.name, error=repr(exc))
    finally:
        await admin.stop()


class KafkaFrontier:
    def __init__(
        self,
        settings: Settings | None = None,
        producer: AIOKafkaProducer | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._producer = producer
        self._owns_producer = producer is None
        names = topic_names(self.settings)
        self.topic_frontier = names["frontier"]
        self.topic_parsed = names["parsed"]
        self.topic_retry = names["retry"]
        self.topic_dlq = names["dlq"]
        self._ring = ConsistentHashRing(
            [str(i) for i in range(self.settings.kafka.partitions)],
            virtual_nodes=self.settings.sharding.virtual_nodes,
        )

    async def start(self) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                acks="all",
                enable_idempotence=True,
            )
            await self._producer.start()

    async def stop(self) -> None:
        if self._owns_producer and self._producer is not None:
            await self._producer.stop()
            self._producer = None

    @property
    def producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("KafkaFrontier.start() must be called first")
        return self._producer

    def partition_for(self, domain: str) -> int:
        node = self._ring.get(domain)
        return int(node) if node is not None else 0

    async def add(self, item: FrontierItem) -> None:
        await self.add_message(FrontierMessage.from_item(item))

    async def add_message(self, message: FrontierMessage) -> None:
        domain = registered_domain(message.url)
        await self.producer.send_and_wait(
            self.topic_frontier,
            value=message.to_bytes(),
            key=domain.encode("utf-8"),
            partition=self.partition_for(domain),
        )
        URLS_ENQUEUED.inc()

    async def send_retry(self, message: FrontierMessage) -> None:
        domain = registered_domain(message.url)
        await self.producer.send_and_wait(
            self.topic_retry, value=message.to_bytes(), key=domain.encode("utf-8")
        )

    async def send_dlq(self, message: FrontierMessage) -> None:
        domain = registered_domain(message.url)
        await self.producer.send_and_wait(
            self.topic_dlq, value=message.to_bytes(), key=domain.encode("utf-8")
        )
