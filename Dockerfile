## initial image ##
FROM ubuntu:18.04

## install dependencies from apt repository ##
RUN apt update
RUN apt install -y git gdal-bin python python-gdal python-pip python-numpy \
                   python3-gdal libgdal-dev python3-pip python3-numpy \
                   libspatialindex-dev mpich python3-rtree cmake p7zip-full \
                   unzip parallel
RUN apt auto-remove

## install python 2&3 modules ##
RUN pip3 install numba pandas geopandas tqdm gdal richdem Rtree \
                 rasterio
RUN pip3 install -U numpy
RUN pip2 install numpy gdal rasterstats

## make projects and data directories ##
RUN mkdir /home/projects /home/data

## Clone and compile Main TauDEM repo ##
RUN git clone https://github.com/dtarb/TauDEM.git /home/projects/TauDEM

RUN mkdir /home/projects/TauDEM/bin

RUN cd /home/projects/TauDEM/src \
    && make

## Clone and compile taudem repo with accelerated flow directions ##
RUN git clone https://github.com/fernandoa123/cybergis-toolkit.git /home/projects/TauDEM_accelerated_flowDirections
RUN cd /home/projects/TauDEM_accelerated_flowDirections/taudem \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make

## adding environment variables for numba ##
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

## Disables citation warning from GNU Parallel ##
RUN yes "will cite" | parallel --citation

## Copy over project source code for production container only (untested still) ##
# COPY . /home/projects/foss_fim

## ADDING FIM GROUP ##
ARG FIM_GROUP_ID=1370800235
ARG FIM_GROUP_NAME=fim
RUN addgroup --gid $FIM_GROUP_ID $FIM_GROUP_NAME
#
### ADDING USER INFO ##
#ARG USER_ID=1000
#ARG USER_NAME=fernandoa
#RUN addgroup --gid $USER_ID $USER_NAME
#RUN adduser --no-create-home --disabled-password --gecos '' --uid $USER_ID --gid $USER_ID $USER_NAME
#
### ADDING USER INFO ##
#ARG USER_ID=1370800178
#ARG USER_NAME=fernando_aristizabal
#RUN addgroup --gid $USER_ID $USER_NAME
#RUN adduser --no-create-home --disabled-password --gecos '' --uid $USER_ID --gid $USER_ID $USER_NAME
#
### ADDING USER INFO ##
#ARG USER_ID=1370800122
#ARG USER_NAME=bradford_bates
#RUN addgroup --gid $USER_ID $USER_NAME
#RUN adduser --no-create-home --disabled-password --gecos '' --uid $USER_ID --gid $USER_ID $USER_NAME
#
### ADDING USER INFO ##
#ARG USER_ID=1370800215
#ARG USER_NAME=brian_avant
#RUN addgroup --gid $USER_ID $USER_NAME
#RUN adduser --no-create-home --disabled-password --gecos '' --uid $USER_ID --gid $USER_ID $USER_NAME
