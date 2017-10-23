#!/usr/bin/env sh
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
pytest -s --cov=confluent_kafka_helpers/

if [ "$1" == "ci" ]; then
    codecov
fi
