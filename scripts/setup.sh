#!/usr/bin/env bash
set -e
uv sync --locked

uv run pre-commit install --hook-type pre-push
uv run pre-commit install --hook-type commit-msg
