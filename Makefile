unit-test:
	./scripts/unit-test.sh

setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

build:
	./scripts/build.sh

lock:
	uv lock

test: unit-test lint

.PHONY: unit-test setup lint build lock test
