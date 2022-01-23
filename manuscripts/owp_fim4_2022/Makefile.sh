#!/bin/bash -e

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path

fullFile=owp_fim4.tex
baseName="${fullFile%%.*}"
extension="${fullFile#*.}"

pdflatex "$fullFile"
bibtex "$baseName".aux
pdflatex "$fullFile"
pdflatex "$fullFile"




