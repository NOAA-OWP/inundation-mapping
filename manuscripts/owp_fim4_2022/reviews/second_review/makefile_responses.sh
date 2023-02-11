#!/bin/bash -e

#parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
#cd $parent_path

fullFile=responses_aristizabal_et_al_202302.tex
baseName="${fullFile%%.*}"
extension="${fullFile#*.}"

pdflatex "$fullFile"
bibtex "$baseName".aux
pdflatex "$fullFile"
pdflatex "$fullFile"
