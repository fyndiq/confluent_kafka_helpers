#!/bin/bash
set -e

[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
pytest tests/ --cov=confluent_kafka_helpers/ --junitxml=/tmp/test-results/report.xml --no-cov-on-fail --cov-report term-missing

if [ "$1" == "ci" ]; then
    codecov
fi
