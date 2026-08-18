"""Strongly-typed, environment-overridable configuration.

All settings live under the ``CRAWLER_`` prefix and nested values use a double
underscore delimiter, e.g. ``CRAWLER_FETCH__CONCURRENCY=200`` or
``CRAWLER_POSTGRES__DSN=postgresql+asyncpg://...``.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    # Default is an embedded SQLite file so the crawler runs with zero external
    # services (no Docker). Point this at Postgres
    # (``postgresql+asyncpg://user:pass@host:5432/db``) to scale out / run the
    # distributed worker fleet. The setting name is kept as ``postgres`` for
    # backwards-compatible ``CRAWLER_POSTGRES__DSN`` env overrides.
    dsn: str = "sqlite+aiosqlite:///./crawler.db"
    echo: bool = False
    pool_size: int = 10


class RedisSettings(BaseModel):
    url: str = "redis://localhost:6379/0"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "crawler"
    partitions: int = 6
    replication_factor: int = 1
    consumer_group: str = "crawler-workers"
    max_retries: int = 3


class FetchSettings(BaseModel):
    concurrency: int = 100
    per_host_concurrency: int = 2
    timeout_seconds: float = 15.0
    connect_timeout_seconds: float = 5.0
    retries: int = 2
    max_page_bytes: int = 5_000_000
    user_agent: str = "MoonshotCrawler/0.1 (+https://github.com/moonshot/web-crawler)"


class PolitenessSettings(BaseModel):
    respect_robots: bool = True
    default_crawl_delay_ms: int = 1000
    robots_cache_size: int = 10_000
    robots_cache_ttl_seconds: int = 3600
    dns_cache_size: int = 10_000
    dns_cache_ttl_seconds: int = 300


class CrawlSettings(BaseModel):
    max_depth: int = 3
    max_pages: int = 1000
    max_pages_per_domain: int = 500
    max_url_length: int = 2048
    allowed_content_types: list[str] = Field(
        default_factory=lambda: ["text/html", "application/xhtml+xml"]
    )


class DedupSettings(BaseModel):
    enable_url_dedup: bool = True
    enable_content_dedup: bool = True
    bloom_expected_insertions: int = 1_000_000
    bloom_false_positive_rate: float = 0.01
    bloom_namespace: str = "bloom:seen"
    simhash_hamming_threshold: int = 3
    simhash_lsh_bands: int = 4  # 64-bit simhash split into 4 x 16-bit bands


class FrontierSettings(BaseModel):
    backend: Literal["memory", "kafka"] = "memory"
    max_in_flight: int = 1000


class ShardingSettings(BaseModel):
    workers: int = 3
    virtual_nodes: int = 150


class ResilienceSettings(BaseModel):
    # Per-domain crawl budget (0 disables; falls back to crawl.max_pages_per_domain).
    enable_domain_budget: bool = True
    domain_budget_namespace: str = "budget:domain"
    # Per-host circuit breaker: trip after N consecutive failures, cool down, then
    # half-open to probe recovery.
    enable_circuit_breaker: bool = True
    circuit_failure_threshold: int = 5
    circuit_reset_seconds: float = 30.0
    # Global back-pressure: cap simultaneously in-flight pipeline tasks (0 = use
    # fetch.concurrency).
    max_global_in_flight: int = 0


class RecrawlSettings(BaseModel):
    # Adaptive recrawl interval (seconds). Unchanged pages back off geometrically up
    # to ``max_interval``; changed pages reset to ``min_interval``.
    min_interval_seconds: float = 3600.0
    max_interval_seconds: float = 7 * 24 * 3600.0
    backoff_factor: float = 2.0
    batch_size: int = 100


class StorageSettings(BaseModel):
    # Where raw page bodies are stored. "postgres" keeps everything in one place
    # for a zero-dependency demo; "minio" offloads bodies to S3-compatible storage.
    body_backend: Literal["postgres", "minio"] = "postgres"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "crawl-content"
    minio_secure: bool = False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRAWLER_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = "http://localhost:8010"
    log_level: str = "INFO"
    log_json: bool = False

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    fetch: FetchSettings = Field(default_factory=FetchSettings)
    politeness: PolitenessSettings = Field(default_factory=PolitenessSettings)
    crawl: CrawlSettings = Field(default_factory=CrawlSettings)
    dedup: DedupSettings = Field(default_factory=DedupSettings)
    frontier: FrontierSettings = Field(default_factory=FrontierSettings)
    sharding: ShardingSettings = Field(default_factory=ShardingSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    recrawl: RecrawlSettings = Field(default_factory=RecrawlSettings)


@lru_cache
def get_settings() -> Settings:
    return Settings()
