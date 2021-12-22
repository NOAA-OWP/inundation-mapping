#!/bin/bash -e

codeDir=$1

docker run --rm -v "$1":/foss_fim fernandoaristizabal/latex:20211220 /foss_fim/manuscripts/owp_fim4_2021/Makefile.sh
