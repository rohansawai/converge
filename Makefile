#!/usr/bin/env bash
set -euo pipefail

# Sprint 1: stdout-only generators
run-generators-stdout:
	docker compose --profile generators-stdout up generator-account1-stdout generator-account2-stdout generator-account3-stdout

# Sprint 2+: Kafka producers only
run-generators:
	docker compose --profile generators up generator-account1 generator-account2 generator-account3

# Full pipeline
up:
	docker compose --profile core up --build

up-detach:
	docker compose --profile core up --build -d

down:
	docker compose down -v

test:
	pytest -q

verify-topics:
	bash scripts/verify_topics.sh

verify-pipeline:
	bash scripts/verify_pipeline.sh

install:
	pip install -e ".[dev]"
