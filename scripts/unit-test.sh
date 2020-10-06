#!/bin/bash
set -e
pytest tests/ --cov=confluent_kafka_helpers/ --junitxml=/tmp/test-results/report.xml --no-cov-on-fail --cov-report term-missing

if [ "$1" == "ci" ]; then
    codecov
fi
