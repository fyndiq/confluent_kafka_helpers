# Lists all available commands.
default:
    @just --list

# Runs the unit tests.
unit-test:
    ./scripts/unit-test.sh

# Sets up the local development environment.
setup:
    ./scripts/setup.sh

# Lints the codebase.
lint:
    ./scripts/lint.sh

# Builds the package.
build:
    ./scripts/build.sh

# Publishes the package.
publish:
    ./scripts/publish.sh

# Updates pinned pip dependencies.
pip-update:
    ./scripts/pip-update.sh

# Runs the full test suite.
test: unit-test lint
