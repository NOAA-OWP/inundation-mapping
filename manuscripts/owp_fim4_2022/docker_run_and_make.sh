#!/bin/bash -e

codeDir=$1

docker run --rm -v "$1":/foss_fim fernandoaristizabal/latex:20220821 /foss_fim/manuscripts/owp_fim4_2022/Makefile.sh
