#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for normalized-events..."
for _ in $(seq 1 30); do
  count=$(docker compose exec -T redpanda rpk topic consume normalized-events --num 1 --offset end --brokers redpanda:9092 2>/dev/null | wc -l || echo 0)
  if [ "$count" -gt 0 ]; then
    echo "Pipeline producing normalized events."
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for normalized events."
exit 1
