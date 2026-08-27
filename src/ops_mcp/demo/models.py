from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OrderInput:
    order_id: str
    catalog_should_fail: bool = False
    retry_interval_seconds: int = 20
