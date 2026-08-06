"""Kafka consumer helpers."""

from __future__ import annotations

import json
from typing import Any, Iterator

from kafka import KafkaConsumer, TopicPartition


class MultiTopicConsumer:
    """Consume from multiple topics in one process."""

    def __init__(
        self,
        bootstrap_servers: str,
        topics: list[str],
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def poll(self, timeout_ms: int = 1000) -> list[tuple[str, dict[str, Any]]]:
        records: list[tuple[str, dict[str, Any]]] = []
        batch = self._consumer.poll(timeout_ms=timeout_ms, max_records=500)
        for tp, messages in batch.items():
            topic = tp.topic
            for msg in messages:
                records.append((topic, msg.value))
        return records

    def close(self) -> None:
        self._consumer.close()


class SingleTopicConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.topic = topic
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for msg in self._consumer:
            yield msg.value

    def close(self) -> None:
        self._consumer.close()
