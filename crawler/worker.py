"""Distributed crawler worker.

Consumes the Kafka ``frontier`` (and ``retry``) topic in a consumer group,
processes each URL through the shared pipeline, enqueues newly-seen child links
(deduped via a Redis Bloom filter), and routes transient failures to ``retry``
and poison messages to ``dlq``.

Delivery is at-least-once: offsets are committed only after a batch is handled,
and all writes are idempotent (document upserts keyed by URL), so reprocessing a
message is harmless. Run multiple instances to scale; Kafka spreads partitions
(and therefore domains) across the group.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import time

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

from .config import Settings, get_settings
from .dedup.seen import UrlSeen
from .factory import (
    build_backpressure,
    build_circuit_breaker,
    build_content_dedup,
    build_domain_budget,
    build_object_store_for,
    build_politeness,
    build_url_seen,
)
from .fetcher import Fetcher
from .frontier.kafka_frontier import KafkaFrontier, ensure_topics
from .frontier.messages import FrontierMessage
from .logging_setup import configure_logging, get_logger
from .pipeline import Pipeline, process_message
from .redis_client import close_redis, get_redis
from .storage.sinks import SqlDocumentSink

log = get_logger(__name__)


class CrawlWorker:
    def __init__(self, settings: Settings | None = None, *, metrics_port: int = 8001) -> None:
        self.settings = settings or get_settings()
        self.metrics_port = metrics_port
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        s = self.settings
        configure_logging(s.log_level, s.log_json)
        start_http_server(self.metrics_port)
        await ensure_topics(s)

        redis = get_redis(s)
        frontier = KafkaFrontier(s)
        await frontier.start()
        object_store = build_object_store_for(s)
        sink = SqlDocumentSink(s, object_store)
        consumer = AIOKafkaConsumer(
            frontier.topic_frontier,
            frontier.topic_retry,
            bootstrap_servers=s.kafka.bootstrap_servers,
            group_id=s.kafka.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._install_signal_handlers()

        try:
            async with Fetcher(s) as fetcher:
                pipeline = Pipeline(
                    fetcher,
                    sink,
                    s,
                    politeness=build_politeness(fetcher, s, redis=redis),
                    content_dedup=build_content_dedup(s, redis=redis),
                    object_store=object_store,
                    conditional_get=True,
                    circuit_breaker=build_circuit_breaker(s),
                    domain_budget=build_domain_budget(s, redis=redis),
                    backpressure=build_backpressure(s),
                )
                seen = build_url_seen(s, redis=redis)
                log.info(
                    "worker_started",
                    group=s.kafka.consumer_group,
                    metrics_port=self.metrics_port,
                )
                while not self._stop.is_set():
                    batch = await consumer.getmany(timeout_ms=1000, max_records=100)
                    # Process the batch concurrently; the pipeline's back-pressure
                    # gate and per-host politeness bound real parallelism. Offsets
                    # are committed only after the whole batch is handled
                    # (at-least-once + idempotent writes make reprocessing safe).
                    tasks = [
                        self._handle(msg, pipeline, frontier, seen)
                        for messages in batch.values()
                        for msg in messages
                    ]
                    if tasks:
                        await asyncio.gather(*tasks)
                        await consumer.commit()
        finally:
            log.info("worker_stopping")
            await consumer.stop()
            await frontier.stop()
            await sink.close()
            await close_redis()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, AttributeError):
                loop.add_signal_handler(sig, self._stop.set)

    async def _handle(
        self, msg, pipeline: Pipeline, frontier: KafkaFrontier, seen: UrlSeen
    ) -> None:
        try:
            message = FrontierMessage.from_bytes(msg.value)
        except Exception:  # noqa: BLE001
            log.warning("undecodable_message", topic=msg.topic)
            return

        if msg.topic == frontier.topic_retry:
            delay = message.not_before - time.time()
            if delay > 0:
                await asyncio.sleep(min(delay, 5.0))
            message.not_before = 0.0
            await frontier.add_message(message)
            return

        if message.depth > self.settings.crawl.max_depth:
            return

        try:
            result = await process_message(
                pipeline, frontier.add, seen, message.to_item(), self.settings.crawl.max_depth
            )
        except Exception:  # noqa: BLE001
            log.exception("processing_failed", url=message.url)
            await self._schedule_retry(message, frontier)
            return

        if result.skipped == "fetch_error":
            await self._schedule_retry(message, frontier)

    async def _schedule_retry(self, message: FrontierMessage, frontier: KafkaFrontier) -> None:
        if message.attempts >= self.settings.kafka.max_retries:
            await frontier.send_dlq(message)
            log.warning("sent_to_dlq", url=message.url, attempts=message.attempts)
            return
        message.attempts += 1
        message.not_before = time.time() + min(2**message.attempts, 30)
        await frontier.send_retry(message)


async def run_worker(metrics_port: int = 8001) -> None:
    await CrawlWorker(metrics_port=metrics_port).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a distributed crawler worker")
    parser.add_argument("--metrics-port", type=int, default=8001)
    args = parser.parse_args()
    asyncio.run(run_worker(args.metrics_port))


if __name__ == "__main__":
    main()
