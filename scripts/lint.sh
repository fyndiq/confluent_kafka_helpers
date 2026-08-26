#!/usr/bin/env bash
set -e
flake8 .
mypy confluent_kafka_helpers/
