"""Normalizer service: raw topics -> normalized-events + DLQ."""

from __future__ import annotations

import logging
import signal
import sys
import time

from converge.config import settings
from converge.kafka.consumer import MultiTopicConsumer
from converge.kafka.producer import EventProducer
from converge.normalizers import normalize_record, validation_error_message
from converge.schemas.canonical import DeadLetterRecord

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_TOPICS = [
    settings.topic_account1,
    settings.topic_account2,
    settings.topic_account3,
]


class NormalizerService:
    def __init__(self) -> None:
        self.running = True
        self.consumer = MultiTopicConsumer(
            settings.kafka_bootstrap,
            SOURCE_TOPICS,
            group_id="converge-normalizer",
        )
        self.output = EventProducer(settings.kafka_bootstrap, settings.topic_normalized)
        self.dlq = EventProducer(settings.kafka_bootstrap, settings.topic_dlq)

    def stop(self, *_: object) -> None:
        self.running = False

    def handle_record(self, topic: str, raw: dict) -> None:
        try:
            event = normalize_record(topic, raw)
            self.output.send(event.model_dump(mode="json"))
            logger.debug("Normalized event from %s: %s", topic, event.event_id)
        except Exception as exc:
            dlq_record = DeadLetterRecord(
                source_topic=topic,
                error=validation_error_message(exc),
                raw_payload=raw,
            )
            self.dlq.send(dlq_record.model_dump(mode="json"))
            logger.warning("DLQ record from %s: %s", topic, exc)

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        logger.info("Normalizer listening on topics: %s", SOURCE_TOPICS)
        while self.running:
            batch = self.consumer.poll(timeout_ms=1000)
            for topic, raw in batch:
                self.handle_record(topic, raw)
        self.consumer.close()
        self.output.close()
        self.dlq.close()


def main() -> None:
    service = NormalizerService()
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
