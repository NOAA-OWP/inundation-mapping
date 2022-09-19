#!/bin/bash -e

# declare files
mainFile=owp_fim4.tex
bibFile=bibliography/owp_fim4_2022.bib
expandedFile=owp_fim4_expanded.tex
differenceFile=owp_fim4_expanded_diff.tex
originalExandedFile=owp_fim4_expanded_original.tex

# record parent path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path

# compile document
pdflatex "$mainFile"
bibtex "${mainFile%%.*}".aux
pdflatex "$mainFile"
pdflatex "$mainFile"

# expands all tex files into one file
echo 'Latexpand to Create One Tex File ...'
latexpand --keep-comments --biber "$bibFile" "$mainFile" -o "$expandedFile"

# makes difference file
if test -f "$originalExandedFile"; then
    echo 'Latexdiff to make a difference file'
    latexdiff "$originalExandedFile" "$expandedFile" > "$differenceFile"
    pdflatex "$differenceFile"
    bibtex "${differenceFile%%.*}".aux
    pdflatex "$differenceFile"
    pdflatex "$differenceFile"
fi

