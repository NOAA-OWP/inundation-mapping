### Cahaba: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Weather Service (NWS). Software enables inundation mapping capability by generating Relative Elevation Models (REMs) and Synthetic Rating Curves (SRCs). Included are tests to evaluate skill and computational efficiency as well as functions to generate inundation maps.

## Dependencies

[Docker](https://docs.docker.com/get-docker/)

## Installation

1. Install Docker : [Docker](https://docs.docker.com/get-docker/)
2. Build Docker Image : `docker build -f Dockerfile.dev -t <image_name>:<tag> <path/to/repository>`
3. Create FIM group on host machine:
    - Linux: `groupadd -g 1370800178 fim`
4. Change group ownership of repo (needs to be redone when a new file occurs in the repo):
    - Linux: `chgrp -R fim <path/to/repository>`

## Configuration

Software is configurable via parameters found in config directory. Copy files before editing and remove "template" pattern from the filename.
Make sure to set the config folder group to 'fim' recursively using the chown command
- params_template.env
- mannings_default.json
    - must change filepath in params_template.env under "manning_n" variable name
- params_calibrated.env
    - runs calibrated mannings parameters from mannings_calibrated.json

## Usage

1. Run Docker Container : `docker run --rm -it -v <path/to/data>:/data -v <path/to/repository>:/foss_fim <image_name>:<tag>`
2. Acquire and Prepare Data : `/foss_fim/lib/acquire_and_preprocess_inputs.py -u <huc4s_to_process>`
    - `-u` can be a single HUC4, series of HUC4s (e.g. 1209 1210), path to line-delimited file with HUC4s.
    - Please run `/foss_fim/lib/acquire_and_preprocess_inputs.py --help` for more information.
    - See United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR) [site](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution) for more information
3. Produce Hydrofabric : `fim_run.sh -u <huc4,6,or8s> -c /foss_fim/config/<your_params_file.env> -n <name_your_run>`
    - `-u` can be a single huc, a series passed in quotes, or a line-deliminted file
        i. To run entire domain of available data use one of the `/data/inputs/included_huc[4,6,8].lst` files
    - Outputs can be found under `/data/outputs/<name_your_run>`

## How to test the software

Binary contingency statistics are currently being computed for Cahaba FIM comparing to Federal Emergency Management Agency (FEMA) Base Level Engineering (BLE) sites. More test cases are being developed from a variety of sources.

1. Acquire and process test case data: `TBD`
2. Run hydrologic evaluation (from inside Docker container): `/foss_fim/tests/run_test_case.py -r <fim_run_name/hucID> -b <name_of_test_instance_to_use> -t <test_case_id>`
    - More information can be found by running `/foss_fim/tests/run_test_case.py --help`

## Known Issues & Getting Help

Please see the issue tracker on Github for known issues and for getting help.

## Getting Involved

NOAA's National Water Center welcomes anyone to contribute to the Cahaba repository to improve flood inundation mapping capabilities. Please contact Fernando Aristizabal (fernando.aristizabal@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

----

## Open Source Licensing Info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)

----

## Credits and References
1. Office of Water Prediction [(OWP)](https://water.noaa.gov/)
2. National Flood Interoperability Experiment [(NFIE)](https://web.corral.tacc.utexas.edu/nfiedata/)
3. Garousi‐Nejad, I., Tarboton, D. G.,Aboutalebi, M., & Torres‐Rua, A.(2019). Terrain analysis enhancements to the Height Above Nearest Drainage flood inundation mapping method. Water Resources Research, 55 , 7983–8009. https://doi.org/10.1029/2019WR0248375.
4. Zheng, X., D.G. Tarboton, D.R. Maidment, Y.Y. Liu, and P. Passalacqua. 2018. “River Channel Geometry and Rating Curve Estimation Using Height above the Nearest Drainage.” Journal of the American Water Resources Association 54 (4): 785–806. https://doi.org/10.1111/1752-1688.12661.
5. Barnes, Richard. 2016. RichDEM: Terrain Analysis Software. http://github.com/r-barnes/richdem
6. [TauDEM](https://github.com/dtarb/TauDEM)
7. Federal Emergency Management Agency (FEMA) Base Level Engineering [(BLE)](https://webapps.usgs.gov/infrm/estBFE/)
8. Verdin, James; Verdin, Kristine; Mathis, Melissa; Magadzire, Tamuka; Kabuchanga, Eric; Woodbury, Mark; and Gadain, Hussein, 2016, A software tool for rapid flood inundation mapping: U.S. Geological Survey Open-File Report 2016–1038, 26 p., http://dx.doi.org/10.3133/ofr20161038.
9. United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR). https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution
