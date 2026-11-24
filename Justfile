default:
    @just --list

setup:
    @./scripts/setup.sh

lint:
    @./scripts/lint.sh

unit-test:
    @./scripts/unit-test.sh

test: unit-test lint

build:
    @./scripts/build.sh

lock:
    @uv lock
