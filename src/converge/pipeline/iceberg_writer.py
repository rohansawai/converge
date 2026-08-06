"""Consume normalized-events and append to Iceberg raw table."""

from __future__ import annotations

import logging
import signal
import sys

from converge.config import settings
from converge.kafka.consumer import SingleTopicConsumer
from converge.storage.iceberg import ensure_tables, security_event_to_arrow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class IcebergWriter:
    def __init__(self) -> None:
        self.running = True
        self.raw_table, _ = ensure_tables()
        self.consumer = SingleTopicConsumer(
            settings.kafka_bootstrap,
            settings.topic_normalized,
            group_id="converge-iceberg-writer",
        )
        self.buffer: list[dict] = []
        self.batch_size = 50

    def stop(self, *_: object) -> None:
        self.running = False

    def flush(self) -> None:
        if not self.buffer:
            return
        arrow_table = security_event_to_arrow(self.buffer)
        self.raw_table.append(arrow_table)
        logger.info("Appended %d events to raw_security_events", len(self.buffer))
        self.buffer.clear()

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        logger.info("Iceberg writer consuming %s", settings.topic_normalized)
        for event in self.consumer:
            if not self.running:
                break
            self.buffer.append(event)
            if len(self.buffer) >= self.batch_size:
                self.flush()
        self.flush()
        self.consumer.close()


def main() -> None:
    writer = IcebergWriter()
    try:
        writer.run()
    except KeyboardInterrupt:
        writer.stop()
        writer.flush()
    sys.exit(0)


if __name__ == "__main__":
    main()
