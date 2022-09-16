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

# expands all tex files into one file
echo 'Lat-Expand to Create One Tex File ...'
latexpand --keep-comments --biber bibliography/owp_fim4_2021.bib owp_fim4.tex -o owp_fim4_expanded.tex



