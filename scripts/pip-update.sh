#!/usr/bin/env bash
pip-compile --allow-unsafe --resolver=backtracking -U requirements.in --output-file requirements.txt
