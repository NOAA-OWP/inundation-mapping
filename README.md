## Cahaba: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

This software uses the Height Above Nearest Drainage (HAND) method to generate Relative Elevation Models (REMs), Synthetic Rating Curves (SRCs), and catchment grids. This repository also includes functionality to generate flood inundation maps (FIMs) and evaluate FIM accuracy.

#### For more information, see the [Cahaba Wiki](https://github.com/NOAA-OWP/cahaba/wiki).

## Accessing Data through ESIP S3 Bucket
The latest national generated HAND data and a subset of the inputs can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS CLI tools.

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
aws s3 cp --recursive s3://noaa-nws-owp-fim/hand_fim/fim_3_0_21_0/outputs/fr/12090301 12090301 --request-payer requester
```

## Running the Code
### Input Data
Input data can be found on the ESIP S3 Bucket (see "Accessing Data through ESIP S3 Bucket" section above). All necessary non-publicly available files are in this S3 bucket, as well as sample input data for HUCs 1204 and 1209.

### Dependencies
[Docker](https://docs.docker.com/get-docker/)

### Installation
1. Install Docker : [Docker](https://docs.docker.com/get-docker/)
2. Build Docker Image : `docker build -f Dockerfile.dev -t <image_name>:<tag> <path/to/repository>`
3. Create FIM group on host machine:
    - Linux: `groupadd -g 1370800178 fim`
4. Change group ownership of repo (needs to be redone when a new file occurs in the repo):
    - Linux: `chgrp -R fim <path/to/repository>`

### Configuration
This software is configurable via parameters found in the `config` directory. Copy files before editing and remove "template" pattern from the filename.
Make sure to set the config folder group to 'fim' recursively using the chown command. Each development version will include a calibrated parameter set of manning’s n values.
- `params_template.env`
- `mannings_default.json`
    - must change filepath in `params_template.env` in `manning_n` variable name
- `params_calibrated.env`
    - runs calibrated mannings parameters from `mannings_calibrated.json`

### Produce HAND Hydrofabric
```
fim_run.sh -u <huc8> -c /foss_fim/config/<your_params_file.env> -n <name_your_run>
```
- `-u` can be a single huc, a series passed in quotes, or a line-delimited file
    i. To run entire domain of available data use one of the ```/data/inputs/included_huc[4,6,8].lst``` files
- Outputs can be found under ```/data/outputs/<name_your_run>```

### Testing in Other HUCs
To test in HUCs other than the provided HUCs, the following processes can be followed to acquire and preprocess additional NHDPlus rasters and vectors. After these steps are run, the "Produce HAND Hydrofabric" step can be run for the new HUCs.

```
/foss_fim/src/acquire_and_preprocess_inputs.py -u <huc4s_to_process>
```
- `-u` can be a single HUC4, series of HUC4s (e.g. 1209 1210), path to line-delimited file with HUC4s.
- Please run `/foss_fim/src/acquire_and_preprocess_inputs.py --help` for more information.
- See United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR) [site](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution) for more information

#### Reproject NHDPlus High-Res Rasters and Convert to Meters.
```
/foss_fim/src/preprocess_rasters.py
```

----
### Evaluating Inundation Map Performance
After `fim_run.sh` completes, you can evaluate the model's skill. The evaluation benchmark datasets are available through ESIP in the `test_cases` directory.

To evaluate model skill, run the following:
```
python /foss_fim/tools/synthesize_test_cases.py -c DEV -v <fim_run_name> -m <path/to/output/metrics.csv> -j [num_of_jobs]
```

More information can be found by running:
```
python /foss_fim/tools/synthesize_test_cases.py --help
```

----
### Managing Dependencies

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

----
### Known Issues & Getting Help

Please see the issue tracker on GitHub and the [Cahaba Wiki](https://github.com/NOAA-OWP/cahaba/wiki/Cahaba-Wiki-Home) for known issues and getting help.

### Getting Involved

NOAA's National Water Center welcomes anyone to contribute to the Cahaba repository to improve flood inundation mapping capabilities. Please contact Brad Bates (bradford.bates@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

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
