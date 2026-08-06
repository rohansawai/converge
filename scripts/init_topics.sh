#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKERS:-redpanda:9092}"

topics=(
  account1-events
  account2-events
  account3-events
  normalized-events
  dead-letter-events
)

for topic in "${topics[@]}"; do
  echo "Creating topic: $topic"
  rpk topic create "$topic" -p 1 -r 1 --brokers "$BROKER" || true
done

echo "Topics ready."
