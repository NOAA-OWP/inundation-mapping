### Building LaTex Manuscript to PDF

This contains instructions on how to build the manuscript written with LaTex into PDF along the associated bibliography.

## Dependencies

[Docker](https://docs.docker.com/get-docker/)

## Installation

1. Install Docker : [Docker](https://docs.docker.com/get-docker/)
2. Clone repo to a path on local machine: `git clone https://github.com/fernando-aristizabal/cahaba.git <path/to/repository/desired> `
3. Build Docker Image : `docker build -f <path_to_repo/manuscripts/owp_fim3_2020/Dockerfile.latex> -t <image_name>:<tag> <path/to/repository>`
4. Create FIM group on host machine: 
    - Linux: `groupadd -g 1370800178 fim`
5. Change group ownership of repo (needs to be redone when a new file occurs in the repo):
    - Linux: `chgrp -R fim <path/to/repository>`

## Usage for Machines with Bash

1. Run Docker container & compile document (may need to run as root/admin user): `/path_to_repo/manuscripts/owp_fim3_2020/docker_run_and_make.sh <path_to_repo>`
2. View pdf output on your host machine located at `/path_to_repo/manuscripts/owp_fim3_2020/owp_fim3.pdf`.

## Usage for Machines without Bash

1. Run Docker container & compile document (may need to run as root/admin user): `docker run --rm -v <path_to_repo>:/foss_fim <image_name>:<tag> /foss_fim/manuscripts/owp_fim3_2020/Makefile.sh`
2. View pdf output on your host machine located at `/path_to_repo/manuscripts/owp_fim3_2020/owp_fim3.pdf`.
   
## Known Issues & Getting Help

Please see the issue tracker on Github for known issues and for getting help.

## Getting Involved

NOAA's National Water Center welcomes anyone to contribute to the Cahaba repository to improve flood inundation mapping capabilities. Please contact Fernando Aristizabal (fernando.aristizabal@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.


