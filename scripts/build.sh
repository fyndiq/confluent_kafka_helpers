#!/bin/bash
set -e
rm -rf dist/* build/*
python -m build -w
