#!/bin/bash
set -e

[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
mypy confluent_kafka_helpers/
flake8 .
