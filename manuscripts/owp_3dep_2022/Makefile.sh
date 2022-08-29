#!/bin/bash -e

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path

fullFile=owp_3dep.tex
baseName="${fullFile%%.*}"
extension="${fullFile#*.}"

pdflatex "$fullFile"
bibtex "$baseName".aux
pdflatex "$fullFile"
pdflatex "$fullFile"

# expands all tex files into one file
#echo 'Lat-Expand to Create One Tex File ...'
#/usr/bin/latexpand/latexpand --keep-comments --biber bibliography/owp_3dep_2022.bib owp_3dep.tex -o owp_3dep_expanded.tex



