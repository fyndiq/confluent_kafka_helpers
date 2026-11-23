default:
    @just --list

setup:
    @./scripts/setup.sh

unit-test:
    @./scripts/unit-test.sh

lint:
    @./scripts/lint.sh

build:
    @./scripts/build.sh

lock:
    @uv lock

test: unit-test lint
