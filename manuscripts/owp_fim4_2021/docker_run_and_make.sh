#!/bin/bash -e

codeDir=$1

docker run --rm -v "$1":/foss_fim fim4_latex:20210131 /foss_fim/manuscripts/owp_fim4_2021/Makefile.sh
