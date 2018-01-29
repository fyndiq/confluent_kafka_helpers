#!/bin/bash
command -v virtualenv >/dev/null 2>&1 || echo "virtualenv is required"

deactivate ; virtualenv -p python3 .venv
source .venv/bin/activate
pip install -r requirements.txt
