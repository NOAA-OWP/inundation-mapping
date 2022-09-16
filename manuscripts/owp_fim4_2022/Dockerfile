## Dockerfile for manuscript building from Latex
FROM ubuntu:20.04 as builder
ARG dataDir=/data
ARG projectDir=/foss_fim
ARG binDir=/usr/local/bin
ARG manDir=/usr/local/man
ARG tmpDir=/tmp

## INSTALLS
RUN apt update --fix-missing && \
    DEBIAN_FRONTEND=noninteractive apt install -y wget unzip make && \
    apt -y auto-remove && \
    rm -rf /var/lib/apt/lists/*

## LATEX PACKAGES
WORKDIR $tmpDir

# latex expand: used to flatten tex files with "input" or "include" commands.
RUN wget https://mirrors.ctan.org/support/latexpand.zip
RUN unzip latexpand.zip 
RUN chmod 774 latexpand/latexpand
RUN mv latexpand/latexpand $binDir

# latex diff: used to create difference document with additions and removals
RUN wget https://mirrors.ctan.org/support/latexdiff.zip 
RUN unzip latexdiff.zip
WORKDIR $tmpDir/latexdiff
RUN make install
WORKDIR $tmpDir

# texcount: used to count characters, words, etc
RUN wget https://mirrors.ctan.org/support/texcount.zip
RUN unzip texcount.zip
RUN chmod 774 texcount/texcount.pl
RUN mv texcount/texcount.pl $binDir

################################################################################
FROM ubuntu:20.04 as runtime

## INSTALLS
RUN apt update --fix-missing && \
    DEBIAN_FRONTEND=noninteractive apt install -y texlive-full perl-doc && \
    apt -y auto-remove && \
    rm -rf /var/lib/apt/lists/*

## Copy over packages
COPY --from=builder $binDir $binDir
#RUN chmod -R 774 $binDir/*

## ADDING FIM USER AND GROUP ##
#ARG GroupID=1370800235
ARG id=1001
ARG name=fim
#RUN addgroup --gid $GroupID $GroupName
RUN useradd -Ums /bin/bash -u $id $name
USER $name
WORKDIR /home/$name

## ADD TO PATHS ##
ENV PATH="$projectDir:${PATH}"

## ALIASES ##
#RUN alias manuscript_make='(cd /foss_fim/dev-manuscript/manuscripts/owp_fim4_2022 && ./Makefile.sh)'
#RUN alias rebuttal_make='(cd /foss_fim/dev-manuscript/manuscripts/owp_fim4_2022/reviews/first_review && ./makefile_rebuttal.sh)

## RUN UMASK TO CHANGE DEFAULT PERMISSIONS ##
ADD ./src/entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
