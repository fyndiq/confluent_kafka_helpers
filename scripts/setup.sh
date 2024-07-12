#!/usr/bin/env bash
set -e
python3.10 -m venv .venv
./.venv/bin/pip3 install -r requirements.txt
