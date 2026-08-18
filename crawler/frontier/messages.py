"""Wire format for frontier/retry/dlq messages (JSON)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from ..models import FrontierItem


@dataclass(slots=True)
class FrontierMessage:
    url: str
    depth: int = 0
    crawl_id: int | None = None
    attempts: int = 0
    not_before: float = 0.0  # epoch seconds; used by the retry topic

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> FrontierMessage:
        obj = json.loads(data)
        return cls(
            url=obj["url"],
            depth=int(obj.get("depth", 0)),
            crawl_id=obj.get("crawl_id"),
            attempts=int(obj.get("attempts", 0)),
            not_before=float(obj.get("not_before", 0.0)),
        )

    @classmethod
    def from_item(cls, item: FrontierItem) -> FrontierMessage:
        return cls(url=item.url, depth=item.depth, crawl_id=item.crawl_id)

    def to_item(self) -> FrontierItem:
        return FrontierItem(url=self.url, depth=self.depth, crawl_id=self.crawl_id)
