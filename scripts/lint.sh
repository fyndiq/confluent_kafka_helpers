#!/usr/bin/env bash
set -e
uv run --locked flake8 confluent_kafka_helpers/ tests/
uv run --locked mypy confluent_kafka_helpers/
