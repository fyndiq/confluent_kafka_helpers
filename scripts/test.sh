#!/bin/bash
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
py.test -vv -x tests/ --cov confluent_kafka_helpers/ --no-cov-on-fail --cov-report term-missing

if [ "$1" == "ci" ]; then
    codecov
fi
