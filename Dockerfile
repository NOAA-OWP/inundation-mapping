## Temporary image to build the libraries and only save the needed artifacts
FROM osgeo/gdal:ubuntu-full-3.1.2 AS builder
WORKDIR /opt/builder
ARG dataDir=/data
ARG projectDir=/foss_fim
ARG depDir=/dependencies
ARG taudemVersion=98137bb6541a0d0077a9c95becfed4e56d0aa0ac
ARG taudemVersion2=81f7a07cdd3721617a30ee4e087804fddbcffa88
ENV taudemDir=$depDir/taudem/bin
ENV taudemDir2=$depDir/taudem_accelerated_flowDirections/taudem/build/bin

RUN apt-get update && apt-get install -y git  && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/dtarb/taudem.git
RUN git clone https://github.com/fernandoa123/cybergis-toolkit.git taudem_accelerated_flowDirections

RUN apt-get update --fix-missing && apt-get install -y cmake mpich \
    libgtest-dev libboost-test-dev libnetcdf-dev && rm -rf /var/lib/apt/lists/*

## Compile Main taudem repo ##
RUN mkdir -p taudem/bin
RUN cd taudem \
    && git checkout $taudemVersion \
    && cd src \
    && make

## Compile taudem repo with accelerated flow directions ##
RUN cd taudem_accelerated_flowDirections/taudem \
    && git checkout $taudemVersion2 \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make

RUN mkdir -p $taudemDir
RUN mkdir -p $taudemDir2

## Move needed binaries to the next stage of the image
RUN cd taudem/bin && mv -t $taudemDir flowdircond aread8 threshold streamnet gagewatershed catchhydrogeo dinfdistdown
RUN cd taudem_accelerated_flowDirections/taudem/build/bin && mv -t $taudemDir2 d8flowdir dinfflowdir




###############################################################################################



# Base Image that has GDAL, PROJ, etc
FROM osgeo/gdal:ubuntu-full-3.1.2
ARG dataDir=/data
ENV projectDir=/foss_fim
ARG depDir=/dependencies
ENV inputsDir=$dataDir/inputs
ENV outputsDir=/outputs
ENV srcDir=$projectDir/src
ENV workDir=/fim_temp
ENV taudemDir=$depDir/taudem/bin
ENV taudemDir2=$depDir/taudem_accelerated_flowDirections/taudem/build/bin

## ADDING FIM GROUP ##
ARG GroupID=1370800235
ARG GroupName=fim
RUN addgroup --gid $GroupID $GroupName
ENV GID=$GroupID
ENV GN=$GroupName

RUN mkdir -p $workDir

RUN mkdir -p $depDir
COPY --from=builder $depDir $depDir

RUN apt update --fix-missing 
RUN apt install -y p7zip-full python3-pip time mpich=3.3.2-2build1 parallel=20161222-1.1 libgeos-dev=3.8.0-1build1 expect=5.45.4-2build1 tmux rsync

RUN apt auto-remove

## adding AWS CLI (for bash) ##
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

## adding environment variables for numba and python ##
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=TRUE

## ADD TO PATHS ##
ENV PATH="$projectDir:${PATH}"
ENV PYTHONPATH=${PYTHONPATH}:$srcDir:$projectDir/tests:$projectDir/tools

## install python 3 modules ##
COPY Pipfile .
COPY Pipfile.lock .
RUN pip3 install pipenv==2022.4.8 && PIP_NO_CACHE_DIR=off pipenv install --system --deploy --ignore-pipfile

# ----------------------------------
# Mar 2023
# There are some nuances in the whitebox python downloads in that the first time it loads
# it goes to the internet and downloads the latest/greatest WBT (whiteboxtools) engine which is
# required for the whitebox python library to work. We don't want to have FIM attempting a download
# each time a container is opened and the whitebox engine is called.
# Instead we will setup the WBT engine at time of docker build (same as Taudem and AWS).
# Whitebox code detects that the engine it there and makes no attempt to update it.
# We download and unzip it to the same file folder that pip deployed the whitebox library.
# Whitebox also attempts to always download a folder called testdata regardless of use. 
# We added an empty folder to fake out whitebox_tools.py so it doesn't try to download the folder
RUN wbox_path=/usr/local/lib/python3.8/dist-packages/whitebox/ && \
wget -P $wbox_path https://www.whiteboxgeo.com/WBT_Linux/WhiteboxTools_linux_musl.zip && \
unzip -o $wbox_path/WhiteboxTools_linux_musl.zip -d $wbox_path && \
cp $wbox_path/WBT/whitebox_tools $wbox_path && \
mkdir $wbox_path/testdata
# ----------------------------------

## RUN UMASK TO CHANGE DEFAULT PERMISSIONS ##
ADD ./src/entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
