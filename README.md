## Inundation Mapping: Flood Inundation Mapping for U.S. National Water Model

This repository includes flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

This software uses the Height Above Nearest Drainage (HAND) method to generate Relative Elevation Models (REMs), Synthetic Rating Curves (SRCs), and catchment grids. This repository also includes functionality to generate flood inundation maps (FIMs) and evaluate FIM accuracy.

#### For more information, see the [Inundation Mapping Wiki](https://github.com/NOAA-OWP/cahaba/wiki).

---

# FIM Version 4 

## Accessing Data through ESIP S3 Bucket
The latest national generated HAND data and a subset of the inputs can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS CLI tools.  You will need permission from ESIP to access this data. Please contact Carson Pruitt (carson.pruitt@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) for assistance.

AWS Region: `US East (N. Virginia) us-east-1`

AWS Resource Name: `arn:aws:s3:::noaa-nws-owp-fim`

### Configuring the AWS CLI

1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### Accessing Data using the AWS CLI

This S3 Bucket (`s3://noaa-nws-owp-fim`) is set up as a "Requester Pays" bucket. Read more about what that means [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html). If you are using compute resources in the same region as the S3 Bucket, then there is no cost.

#### Examples

List bucket folder structure:
```
aws s3 ls s3://noaa-nws-owp-fim/ --request-payer requester
```

Download a directory of outputs for a HUC8:
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/hand_fim/outputs/fim_4_0_18_02/12090301 \
    /your_local_folder_name/12090301 --request-payer requester
```
By adjusting pathing, you can also download entire directories such as the fim_4_0_18_0 folder.
**Note**: There may be newer editions than fim_4_0_18_0, and it is recommended to adjust the command above for the latest version.


## Running the Code
### Input Data
Input data can be found on the ESIP S3 Bucket (see "Accessing Data through ESIP S3 Bucket" section above). All necessary non-publicly available files are in this S3 bucket, as well as sample input data for HUCs 1204 and 1209.

### Dependencies
[Docker](https://docs.docker.com/get-docker/)

### Installation
1. Install Docker : [Docker](https://docs.docker.com/get-docker/)
2. Build Docker Image : `docker build -f Dockerfile -t <image_name>:<tag> <path/to/repository>`
3. Create FIM group on host machine:
    - Linux: `groupadd -g 1370800178 fim`
4. Change group ownership of repo (needs to be redone when a new file occurs in the repo):
    - Linux: `chgrp -R fim <path/to/repository>`

### Configuration
This software is configurable via parameters found in the `config` directory. Copy files before editing and remove "template" pattern from the filename.
Make sure to set the config folder group to 'fim' recursively using the chown command. Each development version will include a calibrated parameter set of manning’s n values.
- `params_template.env`

This system has an optional tool called the `calibration database tool`. In order to use this system, you have three options:  
1.  Install the calibration database service.
2.  Disable it by providing the `-skipcal` command line option to `fim_pipeline.sh` or `fim_preprocessing.sh`.
3.  Disable it in the `params_template.env` file. See [calibration tool README](https://github.com/NOAA-OWP/inundation-mapping/blob/dev/tools/calibration-db/README.md) for more details.

### Start/run the Docker Container

Since all of the dependencies are managed in utilizing a Docker container, we must issue the [`docker run`](https://docs.docker.com/engine/reference/run/#clean-up---rm) command to start a container as the run-time environment. The container is launched from a Docker Image which was built in [Installation](#installation) step 2. The correct input file pathing is necessary for the `/data` volume mount (`-v`) for the `<input_path>`. The `<input_path>` should contain a subdirectory named `/inputs` (similar to `s3://noaa-nws-owp-fim/hand_fim`). If the pathing is set correctly, we do not need to adjust the `params_template.env` file, and can use the default file paths provided.

```bash 
docker run --rm -it --name <your_container_name> \
    -v <path/to/repository>/:/foss_fim \
    -v <desired_output_path>/:/outputs \
    -v <input_path>:/data \
    <image_name>:<tag>
```
For example:  
```bash
docker run --rm -it --name robs_container \
    -v /home/projects/inundation-mapping/:/foss_fim \
    -v /home/projects/fim/outputs/:/outputs \
    -v /home/projects/fim/inputs/:/data \
    fim_4:dev_20230224_ad87a74
```

### Produce HAND Hydrofabric
```
fim_pipeline.sh -u <huc8> -n <name_your_run>
```
- There are a wide number of options and defaulted values, for details run ```fim_pipeline.sh -h```.
- Manditory arguments:
    - `-u` can be a single huc, a series passed in quotes space delimited, or a line-delimited (.lst) file. To run the entire domain of available data use one of the ```/data/inputs/included_huc8.lst``` files or a HUC list file of your choice.  Depending on the performance of your server, especially the number of CPU cores, running the full domain can take multiple days.
    - `-n` is a name of your run (only alphanumeric)
- Outputs can be found under ```/outputs/<name_your_run>```.

Processing of HUC's in FIM4 comes in three pieces. You can run `fim_pipeline.sh` which automatically runs all of three major section, but you can run each of the sections independently if you like. The three sections are:
- `fim_pre_processing.sh` : This section must be run first as it creates the basic output folder for the run. It also creates a number of key files and folders for the next two sections. 
- `fim_process_unit_wb.sh` : This script processes one and exactly one HUC8 plus all of it's related branches. While it can only process one, you can run this script multiple times, each with different HUC (or overwriting a HUC). When you run `fim_pipeline.sh`, it automatically iterates when more than one HUC number has been supplied either by command line arguments or via a HUC list. For each HUC provided, `fim_pipeline.sh` will `fim_process_unit_wb.sh`. Using the `fim_process_unit_wb.sh`  script allows for a run / rerun of a HUC, or running other HUCs at different times / days or even different docker containers.
- `fim_post_processing.sh` : This section takes all of the HUCs that have been processed, aggregates key information from each HUC directory and looks for errors across all HUC folders. It also processes the group in sub-steps such as usgs guages processesing, rating curve adjustments and more. Naturally, running or re-running this script can only be done after running `fim_pre_processing.sh` and at least one run of `fim_process_unit_wb.sh`.

Running the `fim_pipeline.sh` is a quicker process than running all three steps independently.

### Testing in Other HUCs
To test in HUCs other than the provided HUCs, the following processes can be followed to acquire and preprocess additional NHDPlus rasters and vectors. After these steps are run, the "Produce HAND Hydrofabric" step can be run for the new HUCs.

```
/foss_fim/src/acquire_and_preprocess_inputs.py -u <huc4s_to_process>
```
    Note: This tool is deprecated, updates will be coming soon.

- `-u` can be a single HUC4, series of HUC4s (e.g. 1209 1210), path to line-delimited file with HUC4s.
- Please run `/foss_fim/src/acquire_and_preprocess_inputs.py --help` for more information.
- See United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR) [site](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution) for more information

#### Reproject NHDPlus High-Res Rasters and Convert to Meters.
```
/foss_fim/src/preprocess_rasters.py
```
    Note: This tool is deprecated, updates will be coming soon.

----
### Evaluating Inundation Map Performance
After `fim_pipeline.sh` completes, or combinations of the three major steps described above, you can evaluate the model's skill. The evaluation benchmark datasets are available through ESIP in the `test_cases` directory.

To evaluate model skill, run the following:
```
python /foss_fim/tools/synthesize_test_cases.py \
    -c DEV \
    -v <fim_run_name> \
    -m <path/to/output/metrics.csv> \
    -jh [num of jobs (cores and/or procs) per huc] \
    -jb [num of jobs (cores and/or procs) per branch]
```

More information can be found by running:
```
python /foss_fim/tools/synthesize_test_cases.py --help
```

----
### Managing Dependencies

Dependencies are managed via [Pipenv](https://pipenv.pypa.io/en/latest/). 

When you execute `docker build` from the `Installation` section above, all of the dependencies you need are included. This includes dependencies for you to work in JupyterLab for testing purposes. 

While very rare, you may want to add more dependencies. You can follow the following steps:

- From inside your docker container, run the following command:
    ```bash
    pipenv install <your package name> --dev
    ```
    The `--dev` flag adds development dependencies, omit it if you want to add a production dependency.
    
    This will automatically update the Pipfile in the root of your docker container directory. If the environment looks goods after adding dependencies, lock it with:

    ```bash
    pipenv lock
    ```

    This will update the `Pipfile.lock`. Copy the new updated `Pipfile` and `Pipfile.lock` in the source directory and include both in your git commits. The docker image installs the environment from the lock file. 
    
**Make sure you test it heavily including create new docker images and that it continues to work with the code.**

If you are on a machine that has a particularly slow internet connection, you may need to increase the timeout of pipenv. To do this simply add `PIPENV_INSTALL_TIMEOUT=10000000` in front of any of your pipenv commands.


----
## Citing This Work

Please cite this work in your research and projects according to the CITATION.cff file found in the root of this repository.

----
### Known Issues & Getting Help

Please see the issue tracker on GitHub and the [Inundation Mapping Wiki](https://github.com/NOAA-OWP/inundation-mapping/wiki/Known-Shortcomings-and-Opportunities-for-Improvement) for known issues and getting help.

### Getting Involved

NOAA's National Water Center welcomes anyone to contribute to the Inundation Mapping repository to improve flood inundation mapping capabilities. Please contact Carson Pruitt (carson.pruitt@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

### Open Source Licensing Info
1. [TERMS](docs/TERMS.md)
2. [LICENSE](LICENSE)

### Credits and References
1. [Office of Water Prediction (OWP)](https://water.noaa.gov/)
2. [National Flood Interoperability Experiment(NFIE)](https://web.corral.tacc.utexas.edu/nfiedata/)
3. Garousi‐Nejad, I., Tarboton, D. G.,Aboutalebi, M., & Torres‐Rua, A.(2019). Terrain analysis enhancements to the Height Above Nearest Drainage flood inundation mapping method. Water Resources Research, 55 , 7983–8009.
4. [Zheng, X., D.G. Tarboton, D.R. Maidment, Y.Y. Liu, and P. Passalacqua. 2018. “River Channel Geometry and Rating Curve Estimation Using Height above the Nearest Drainage.” Journal of the American Water Resources Association 54 (4): 785–806.](https://doi.org/10.1111/1752-1688.12661)
5. [Liu, Y. Y., D. R. Maidment, D. G. Tarboton, X. Zheng and S. Wang, (2018), "A CyberGIS Integration and Computation Framework for High-Resolution Continental-Scale Flood Inundation Mapping," JAWRA Journal of the American Water Resources Association, 54(4): 770-784.](https://doi.org/10.1111/1752-1688.12660)
6. [Barnes, Richard. 2016. RichDEM: Terrain Analysis Software](http://github.com/r-barnes/richdem)
7. [TauDEM](https://github.com/dtarb/TauDEM)
8. [Federal Emergency Management Agency (FEMA) Base Level Engineering (BLE)](https://webapps.usgs.gov/infrm/estBFE/)
9. [Verdin, James; Verdin, Kristine; Mathis, Melissa; Magadzire, Tamuka; Kabuchanga, Eric; Woodbury, Mark; and Gadain, Hussein, 2016, A software tool for rapid flood inundation mapping: U.S. Geological Survey Open-File Report 2016–1038, 26](http://dx.doi.org/10.3133/ofr20161038)
10. [United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR)](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution)
11. [Esri Arc Hydro](https://www.esri.com/library/fliers/pdfs/archydro.pdf)
