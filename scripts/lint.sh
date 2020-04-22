#!/bin/bash
set -e
flake8 .
mypy confluent_kafka_helpers/
