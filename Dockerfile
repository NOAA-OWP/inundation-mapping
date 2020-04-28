# initial image
FROM ubuntu:18.04

# install dependencies from apt repository
RUN apt update
RUN apt install -y git gdal-bin python3-gdal libgdal-dev python3-pip python3-numpy libspatialindex-dev mpich python3-rtree cmake
RUN apt auto-remove

# install python 3
RUN pip3 install numpy pandas geopandas tqdm numba gdal richdem Rtree

# make projects and data directories
RUN mkdir /projects /data

# Clone and compile Main TauDEM repo
RUN git clone https://github.com/dtarb/TauDEM.git /projects/TauDEM
RUN mkdir /projects/TauDEM/bin
RUN cd /projects/TauDEM/src && make

# Clone and compile taudem repo with accelerated flow directions
RUN git clone https://github.com/fernandoa123/cybergis-toolkit.git /projects/TauDEM_accelerated_flowDirections
RUN cd /projects/TauDEM_accelerated_flowDirections/taudem && mkdir build && cd build && cmake .. && make
#RUN mkdir build
#RUN cd build
#RUN cmake ..
#RUN make
