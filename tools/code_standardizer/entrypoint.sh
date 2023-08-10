#!/bin/sh

umask 002
cd /foss_fim

echo "Starting Code Standardizer"

echo "Running Python Black..."
black .

echo "Running iSort..."
isort --profile black .

echo "Running Flake8..."
flake8 .

echo " ALL DONE!"
