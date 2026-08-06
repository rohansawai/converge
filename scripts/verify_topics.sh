#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKERS:-localhost:19092}"

echo "Consuming 3 messages from each source topic..."
for topic in account1-events account2-events account3-events; do
  echo "--- $topic ---"
  docker compose exec -T redpanda rpk topic consume "$topic" --num 3 --brokers redpanda:9092 || true
done

echo "Done."
