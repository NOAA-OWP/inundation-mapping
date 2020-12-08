#!/bin/bash -e

codeDir=$1

docker run --rm -v "$1":/foss_fim fernandoaristizabal/latex-full:dev_20201207 /foss_fim/manuscripts/owp_fim3_2020/Makefile.sh
