test:
	./scripts/test.sh

setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

test-ci:
	./scripts/test.sh ci

.PHONY: test
