### Cahaba: Flood Inundation Mapping for U.S. National Water Model

For use with the U.S. National Water Model (NWM) produced by the National Oceanic and Atmospheric Administration's 
(NOAA) National Weather Service (NWS) Office of Water Prediction (OWP). Takes forecast discharges and converts 
to river stages via synthetic rating curves. Produces maps with a modification of the Height Above Nearest 
Drainage (HAND) method.

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

