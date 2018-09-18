#!/bin/bash
set -e
rm -rf dist/*
python setup.py sdist bdist_wheel
twine upload dist/*
