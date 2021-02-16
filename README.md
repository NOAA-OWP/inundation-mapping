### Cahaba: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Weather Service (NWS). This software uses the Height Above Nearest Drainage (HAND) method to generate Relative Elevation Models (REMs), Synthetic Rating Curves (SRCs), and catchment grids, which together are used to produce flood inundation maps (FIM). This repository also includes functionality to generate FIMs as well as tests to evaluate FIM prediction skill.

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
Make sure to set the config folder group to 'fim' recursively using the chown command. Each development version will include a calibrated parameter set of manning’s n values.
- params_template.env
- mannings_default.json
    - must change filepath in params_template.env under "manning_n" variable name
- params_calibrated.env
    - runs calibrated mannings parameters from mannings_calibrated.json

## Input Data

The following input data sources should be downloaded and preprocessed prior to executing the preprocessing & hydrofabric generation code:
USACE National Levee Database:
-Access here: https://levees.sec.usace.army.mil/
-Recommend downloading the “Full GeoJSON” file for the area of interest
-Unzip data and then use the preprocessing scripts to filter data and fix geometries where needed
AHPs site locations for MS extent (currently not available to public)
NHDPlus HR datasets
-Acquire_and_preprocess_inputs.py
-aggregate_nhd_hr_streams.py
NWM Hydrofabric
-nwm_flows.gpkg (currently not available to public)
-nwm_catchments.gpkg (currently not available to public)
-nwm_lakes.gpkg (currently not available to public)
-nwm_headwaters.gpkg - derived

NOTE: Some of the input data is not easy to acquire and will need to be shared with outside users. We are currently working on providing this functionality and should be available soon.

## Usage

1. Run Docker Container : `docker run --rm -it -v <path/to/data>:/data -v <path/to/repository>:/foss_fim <image_name>:<tag>`
2. Acquire and Prepare Data : `/foss_fim/lib/acquire_and_preprocess_inputs.py -u <huc4s_to_process>`
    - `-u` can be a single HUC4, series of HUC4s (e.g. 1209 1210), path to line-delimited file with HUC4s.
    - Please run `/foss_fim/lib/acquire_and_preprocess_inputs.py --help` for more information.
    - See United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR) [site](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution) for more information
3. Aggregate NHD HR streams and create NWM headwater points : /foss_fim/lib/aggregate_vector_inputs.py
4. Produce Hydrofabric : `fim_run.sh -u <huc4,6,or8s> -c /foss_fim/config/<your_params_file.env> -n <name_your_run>`
    - `-u` can be a single huc, a series passed in quotes, or a line-delimited file
        i. To run entire domain of available data use one of the `/data/inputs/included_huc[4,6,8].lst` files
    - Outputs can be found under `/data/outputs/<name_your_run>`

## Evaluate FIM output to a Benchmark Dataset
Once the hydrofabric has been generated from fim_run.sh for, evaluation against a benchmark dataset can be performed using binary contingency statistics. One benchmark dataset that can be used for evaluations are Base Level Engineering studies available on the FEMA Base Flood Elevation Viewer. To acquire FEMA datasets go to the FEMA Base Flood Elevation Viewer (https://webapps.usgs.gov/infrm/estbfe/) and download the file geodatabase and depth grids for a HUC. To perform an evaluation a flow forecast file is required and benchmark grids are preprocessed prior to running run_test_case.py.

1. Flow Forecast File Creation
`/foss_fim/tests/preprocess/create_flow_forecast_file.py -b <path to BLE geodatabase> -n <path to NWM geodatabase> -o <output directory> -xs <Cross Section layer name in BLE geodatabase> -hu <HUC layer name in BLE geodatabase> -huid <HUC ID field in HUC layer> -l <Stream layer name in NWM geodatabase> -f <feature id field in stream layer of NWM geodatabase>`
For example, if HUC 12090301 were downloaded from the FEMA BFE viewer the geodatabase, “BLE_LowColoradoCummins.gdb”, contains a HUC Layer “S_HUC_Ar” (-hu) and a cross section layer “XS” (-xs). The HUC ID corresponds to the “HUC_CODE” field (-huid) within the “S_HUC_AR” layer.  Additionally, the National Water Model geodatabase (-n) will be required with the stream layer (-l) along with the ID field (-f) in the attribute table. Instructions on how to obtain the National Water Model GIS layers will be forthcoming.

2. Process benchmark grid data
`/foss_fim/tests/preprocess/preprocess_benchmark.py -b <path to ble grid (in geotiff format)> -r <path to a reference dataset> -o <path to output raster>`
For HUC 12090301, the benchmark datasets (-b) are the 100 year (“BLE_DEP01PCT”) and 500 year (“BLE_DEP0_2PCT”) depth grids converted to Geotiff format. An example of a reference dataset (-r) is the “rem_zeroed_masked.tif” produced as part of the hydrofabric from fim_run.sh. The output raster name (if doing ble data) should be `ble_huc_<huc 08 code>_depth_<event>.tif` where event is '100yr' or '500yr'. Once the flow file and benchmark grids are processed, the output files are then placed in this folder (from inside a Docker container):
`/foss_fim/tests_cases/validation_data_ble/<huc 08 code>/<event>/` where event is ‘100yr’ or ‘500yr’

3. Run hydrologic evaluation (from inside Docker container): `/foss_fim/tests/run_test_case.py -r <fim_run_name/hucID> -b <name_of_test_instance_to_use> -t <test_case_id>`
    - More information can be found by running `/foss_fim/tests/run_test_case.py --help`

## Dependencies

Dependencies are managed via [Pipenv](https://pipenv.pypa.io/en/latest/). To add new dependencies, from the projects's top-level directory:

```bash
pipenv install ipython --dev
```

The `--dev` flag adds development dependencies, omit it if you want to add a production dependency. If the environment looks goods after adding dependencies, lock it with:

```bash
pipenv lock
```

and include both `Pipfile` and `Pipfile.lock` in your commits. The docker image installs the environment from the lock file.

If you are on a machine that has a particularly slow internet connection, you may need to increase the timeout of pipenv. To do this simply add `PIPENV_INSTALL_TIMEOUT=10000000` in front of any of your pipenv commands.


## Known Issues & Getting Help

Please see the issue tracker on GitHub for known issues and for getting help.

## Getting Involved

NOAA's National Water Center welcomes anyone to contribute to the Cahaba repository to improve flood inundation mapping capabilities. Please contact Brad Bates (bradford.bates@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

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
5. Liu, Y. Y., D. R. Maidment, D. G. Tarboton, X. Zheng and S. Wang, (2018), "A CyberGIS Integration and Computation Framework for High-Resolution Continental-Scale Flood Inundation Mapping," JAWRA Journal of the American Water Resources Association, 54(4): 770-784, https://doi.org/10.1111/1752-1688.12660.
6. Barnes, Richard. 2016. RichDEM: Terrain Analysis Software. http://github.com/r-barnes/richdem
7. [TauDEM](https://github.com/dtarb/TauDEM)
8. Federal Emergency Management Agency (FEMA) Base Level Engineering [(BLE)](https://webapps.usgs.gov/infrm/estBFE/)
9. Verdin, James; Verdin, Kristine; Mathis, Melissa; Magadzire, Tamuka; Kabuchanga, Eric; Woodbury, Mark; and Gadain, Hussein, 2016, A software tool for rapid flood inundation mapping: U.S. Geological Survey Open-File Report 2016–1038, 26 p., http://dx.doi.org/10.3133/ofr20161038.
10. United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR). https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution
11. Esri Arc Hydro. https://www.esri.com/library/fliers/pdfs/archydro.pdf
