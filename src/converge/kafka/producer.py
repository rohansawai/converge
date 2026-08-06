"""Kafka producer helpers."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer


class EventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send(self, event: dict[str, Any]) -> None:
        future = self._producer.send(self.topic, value=event)
        future.get(timeout=10)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
