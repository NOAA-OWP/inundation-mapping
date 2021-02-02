#!/bin/bash -e

codeDir=$1

docker run --rm -v "$1":/foss_fim fim3_latex:20210131 /foss_fim/manuscripts/owp_fim3_2020/Makefile.sh
