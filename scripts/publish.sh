#!/bin/bash
set -e
rm -rf dist/* build/*
python setup.py sdist bdist_wheel
twine upload dist/*
