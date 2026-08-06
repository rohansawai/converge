"""Shared generator utilities."""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable

from converge.kafka.producer import EventProducer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second")
    parser.add_argument(
        "--malformed-rate",
        type=float,
        default=0.0,
        help="Fraction of malformed records (0.0-1.0)",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Write JSON lines to stdout instead of Kafka",
    )
    parser.add_argument("--kafka-bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=None, help="Kafka topic name")
    parser.add_argument("--max-events", type=int, default=0, help="Stop after N events (0=forever)")
    return parser


class BaseGenerator(ABC):
    def __init__(
        self,
        *,
        rate: float,
        malformed_rate: float,
        stdout_only: bool,
        kafka_bootstrap: str | None,
        topic: str | None,
        max_events: int,
    ) -> None:
        self.rate = max(rate, 0.1)
        self.malformed_rate = min(max(malformed_rate, 0.0), 1.0)
        self.stdout_only = stdout_only
        self.max_events = max_events
        self.producer: EventProducer | None = None
        if not stdout_only:
            if not kafka_bootstrap or not topic:
                raise ValueError("kafka_bootstrap and topic required when not in stdout-only mode")
            self.producer = EventProducer(kafka_bootstrap, topic)

    @abstractmethod
    def generate_valid(self) -> dict[str, Any]:
        ...

    @abstractmethod
    def generate_malformed(self) -> dict[str, Any]:
        ...

    def next_event(self) -> dict[str, Any]:
        if random.random() < self.malformed_rate:
            return self.generate_malformed()
        return self.generate_valid()

    def emit(self, event: dict[str, Any]) -> None:
        payload = json.dumps(event)
        if self.stdout_only:
            print(payload, flush=True)
        elif self.producer:
            self.producer.send(event)
        else:
            print(payload, flush=True)

    def run(self) -> None:
        interval = 1.0 / self.rate
        count = 0
        try:
            while self.max_events == 0 or count < self.max_events:
                self.emit(self.next_event())
                count += 1
                time.sleep(interval)
        finally:
            if self.producer:
                self.producer.close()


def run_generator(description: str, factory: Callable[..., BaseGenerator], argv: list[str] | None = None) -> None:
    parser = build_arg_parser(description)
    args = parser.parse_args(argv)
    generator = factory(
        rate=args.rate,
        malformed_rate=args.malformed_rate,
        stdout_only=args.stdout_only,
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        max_events=args.max_events,
    )
    generator.run()
