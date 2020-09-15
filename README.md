### Cahaba: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the 
National Oceanic and Atmospheric Administration (NOAA) National Weather Service (NWS). Software enables inundation mapping 
capability by generating Relative Elevation Models (REMs) and Synthetic Rating Curves (SRCs). Included in package are tests 
to evaluate skill and computational efficiency as well as functions to generate inundation maps.

## Dependencies

Docker (https://docs.docker.com/get-docker/)

## Installation

 - Install Docker : https://docs.docker.com/get-docker/
 - Build Docker Image : `docker build -f Dockerfile.dev -t <image_name>:<tag> <path/to/repository>`

## Usage

 - Run Docker Container : `docker run --rm -it -v <path/to/data>:/data -v <path/to/repository>:/foss_fim <image_name>:<tag>`
 - Produce Hydrofabric : `fim_run.sh -u <huc4,6,or8s> -c /foss_fim/config/<your_params_file.env> -n <name_your_run>`

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)

