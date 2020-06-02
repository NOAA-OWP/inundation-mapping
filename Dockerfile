## initial image ##
FROM ubuntu:18.04

## install dependencies from apt repository ##
RUN apt update --fix-missing
RUN apt install -y git gdal-bin python python-gdal python-pip python-numpy \
                   python3-gdal libgdal-dev python3-pip python3-numpy \
                   libspatialindex-dev mpich python3-rtree cmake p7zip-full \
                   unzip parallel time
RUN apt auto-remove

## install python 2&3 modules ##
RUN pip3 install numba pandas geopandas tqdm gdal richdem Rtree \
                 rasterio
RUN pip3 install -U numpy
RUN pip2 install numpy gdal rasterstats

#### directories ####
ARG dataDir=/data
ARG projectDir=/foss_fim
ARG depDir=/dependencies
ENV inputDataDir=$dataDir/inputs
ENV outputDataDir=$dataDir/outputs 
ENV libDir=$projectDir/lib
ENV taudemDir=$depDir/taudem/bin
ENV taudemDir2=$depDir/taudem_accelerated_flowDirections/taudem/build/bin

## Clone and compile Main taudem repo ##
RUN git clone https://github.com/dtarb/taudem.git $depDir/taudem
RUN mkdir -p $taudemDir
RUN cd $depDir/taudem/src \
    && make

## Clone and compile taudem repo with accelerated flow directions ##
RUN git clone https://github.com/fernandoa123/cybergis-toolkit.git $depDir/taudem_accelerated_flowDirections
RUN cd $depDir/taudem_accelerated_flowDirections/taudem \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make

## adding environment variables for numba and python ##
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=TRUE

## Disables citation warning from GNU Parallel ##
RUN yes "will cite" | parallel --citation

## Copy over project source code for production container only (untested still) ##
# COPY . $projectDir

## ADDING FIM GROUP ##
ARG GroupID=1370800235
ARG GroupName=fim
RUN addgroup --gid $GroupID $GroupName
ENV GID=$GroupID
ENV GN=$GroupName

## ADD TO PATHS ##
ENV PATH="$projectDir:${PATH}"

## RUN UMASK TO CHANGE DEFAULT PERMISSIONS ##
ADD ./lib/entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]

