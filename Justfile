# Lists all available commands.
default:
    @just --list

# Runs the unit tests.
unit-test:
    ./scripts/unit-test.sh

# Installs dependencies and git hooks.
setup:
    ./scripts/setup.sh

# Lints the codebase.
lint:
    ./scripts/lint.sh

# Builds a wheel and source distribution.
build:
    ./scripts/build.sh

# Updates the lockfile.
lock:
    uv lock

# Runs the full test suite.
test: unit-test lint
