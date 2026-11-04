#!/usr/bin/env bash
set -e
uv run --locked pytest tests/ --cov=confluent_kafka_helpers/ --junitxml=/tmp/test-results/report.xml --no-cov-on-fail --cov-report term-missing
