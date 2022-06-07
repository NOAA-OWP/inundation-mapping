#!/bin/sh

umask 002
cd /cahaba

echo "Starting Code Standardizer"

echo "Running Python Black..."
black .

echo "Running iSort..."
isort --profile black .

echo "Running Flake8..."
flake8 .

echo " ALL DONE!"
