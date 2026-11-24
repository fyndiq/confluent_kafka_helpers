default:
    @just --list

setup:
    @./scripts/setup.sh

lint:
    @./scripts/lint.sh

unit-test:
    @./scripts/unit-test.sh

test: lint unit-test

build:
    @./scripts/build.sh

lock:
    @uv lock
