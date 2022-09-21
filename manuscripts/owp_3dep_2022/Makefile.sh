#!/bin/bash -e

# declare files
mainFile=owp_3dep.tex
bibFile=bibliography/owp_3dep_2022.bib
#expandedFile=owp_3dep_expanded.tex
#differenceFile=owp_3dep_expanded_diff.tex
#originalExandedFile=owp_3dep_expanded_original.tex

# record build directory
buildDir=build

# record parent path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path

# get main file basename
mainfile_base="${mainFile%%.*}"

# compile document
pdflatex -output-directory "$buildDir" "$mainFile"
bibtex $buildDir/"$mainfile_base".aux
pdflatex -output-directory "$buildDir" "$mainFile"
pdflatex -output-directory "$buildDir" "$mainFile"

# move pdf output
echo Moving $buildDir/$mainfile_base.pdf to main directory.
mv $buildDir/"$mainfile_base".pdf "$mainfile_base".pdf

# expands all tex files into one file
#echo 'Latexpand to create one tex file ...'
#latexpand --keep-comments --biber "$bibFile" "$mainFile" -o "$expandedFile"

# makes difference file
#if test -f "$originalExandedFile"; then
#    echo 'Latexdiff to make a difference file'
#    latexdiff "$originalExandedFile" "$expandedFile" > "$differenceFile"
#    pdflatex -output-directory "$buildDir" "$differenceFile"
#    bibtex -output-directory "$buildDir" "$buildDir"/"${differenceFile%%.*}".aux
#    pdflatex -output-directory "$buildDir" "$differenceFile"
#    pdflatex -output-directory "$buildDir" "$differenceFile"
#fi

