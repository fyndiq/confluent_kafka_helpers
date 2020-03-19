#!/usr/bin/env bash
pip-compile -U requirements.in --output-file requirements.txt --no-annotate --no-header
pip-compile -U requirements.test.in --output-file requirements.test.txt --no-annotate --no-header
