test:
	./scripts/test.sh

setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

test-ci:
	./scripts/test.sh ci

publish:
	./scripts/publish.sh

pip-update:
	./scripts/pip-update.sh

.PHONY: test
