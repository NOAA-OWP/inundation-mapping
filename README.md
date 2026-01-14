## Inundation Mapping: Flood Inundation Mapping for U.S. National Water Model

This repository includes flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

This software uses the Height Above Nearest Drainage (HAND) method to generate Relative Elevation Models (REMs), Synthetic Rating Curves (SRCs), and catchment grids. This repository also includes functionality to generate flood inundation maps (FIMs) and evaluate FIM accuracy.

#### For more information, see the [Inundation Mapping Wiki](https://github.com/NOAA-OWP/inundation-mapping/wiki).

---

# FIM Version 4

#### Note: While we use the phrase "FIM" regularily, the phrase "HAND" is also used and is generally interchangeable. Most output folders now follow the convenction of "hand_4_x_x_x".

## Accessing Data through ESIP S3 Bucket
The latest national generated HAND data and a subset of the inputs can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS CLI tools. Please contact Carson Pruitt (carson.pruitt@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) if you experience issues with permissions.

AWS Region: `US East (N. Virginia) us-east-1`

AWS Resource Name: `arn:aws:s3:::noaa-nws-owp-fim`

### Configuring the AWS CLI

1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### Accessing Data using the AWS CLI

Before attempting to download, you will need ESIP AWS cli credentials (Access key ID and Secret Access Key). You do not have to have your own AWS account. Please contact Carson Pruitt (carson.pruitt@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov). 

Once you get AWS credentials, open your terminal window and type:
```
aws configure --profile esip
```
It will ask you for the Access key ID, Secret Access Key, Region and default language (just hit tab for that entry).

With the keys in place, you can test your credentials get a list folders prior to download as well as execute other S3 cli commands:
```
aws s3 ls s3://noaa-nws-owp-fim --profile esip
```

### Examples

**Note:** All examples are based on linux pathing. Also, for each sample below, remove the line breaks [backslash(s) "\"] before running the command.

The available inputs, test cases, and versioned FIM outputs can be found by running:
```
aws s3 ls s3://noaa-nws-owp-fim/hand_fim/  --profile esip
```

By adjusting pathing, you can also download entire directories such as the `hand_4_5_2_11` folder. An entire output HAND set is approximately 1.7 TB.

**Note**: There may be newer editions than `hand_4_5_11_1`, and it is recommended to adjust the command above for the latest version.

## Setting up your Environment

### Folder Structure
You are welcome to set up your folder structure in any pattern you like. For example purposes, we will use a folder structure shown below.
Starting with a base folder, e.g `/home/projects/` add the following folders:
- `fim`
   - `code`
   - `data`
      - `inputs`
      - `outputs`
      - `outputs_temp`
### Getting FIM source code
(based on sample pathing described above)

``` path to your "code" folder. e.g.
cd /home/projects/fim/code
```

``` download the current FIM inundation mapping code
git clone https://github.com/NOAA-OWP/inundation-mapping.git
```

Git will auto create a subfolder named `inundation-mapping` where the code will be. Your Docker mounts should include this `inundation-mapping` folder.

### Dependencies
[Docker](https://docs.docker.com/get-docker/)

### Installation
1. Install Docker : [Docker](https://docs.docker.com/get-docker/)
2. Build Docker Image : `docker build -f Dockerfile.dev -t <image_name>:<tag> <path/to/repository>`
3. Create FIM group on host machine:
    - Linux: `groupadd -g 1370800178 fim`
4. Change group ownership of repo (needs to be redone when a new file occurs in the repo):
    - Linux: `chgrp -R fim <path/to/repository>`

### Input Data
Input data can be found on the ESIP S3 Bucket (see "Accessing Data through ESIP S3 Bucket" section above). The FIM inputs directory can be found at `s3://noaa-nws-owp-fim/hand_fim/inputs`. It is appx 400GB and it needs to be in your `data` folder.

```
aws s3 sync s3://noaa-nws-owp-fim/hand_fim/inputs /home/projects/fim/data/inputs --profile esip --dryrun
```
**Note**: When you include the `--dryrun` argument in the command, a large list will be returned showing you exactly which files are to be downloaded and where they will be saved. We recommend including this argument the first time you run the command, then quickly aborting it (CTRL-C) so you don't get the full list. However, you can see that your chosen target path on your machine is correct.  When you are happy with the pathing, run the `aws s3` command again and leave off the `--dryrun` argument.

The S3 inputs directory has all of the folders and files you need to run FIM. It includes some publicly available and some non-publicly availible data.

## Running the Code

### Configuration

There are two ways, which can be used together, to configure the system and/or data processing. Some configuration is based on input arguments when running `fim_pipeline.sh` described below in the "Produce HAND Hydrofabric" section. Another configuration option is based on using a file named `params_template.env`, found in the `config` directory. To use this latter technique, copy the `params_template.env` file before editing and remove the word "template" from the filename. The `params_template.env` file includes, among other options, a calibrated parameters set of Manning’s n values. The new `params.env` becomes one of the arguments submitted when running `fim_pipeline.sh`.

Make sure to set the config folder group to `fim` recursively using the chown command.

This application has an default optional tool called the `calibration points tool`. In order to disable its' use, you can:
1.  Disable it by providing the `-skipcal` command line option to `fim_pipeline.sh` or `fim_pre_processing.sh`.
2.  Disable it in the [`params_template.env`](/config/params_template.env) file by setting `src_adjust_spatial="FALSE"`.

### Start/run the Docker Container

Since all of the dependencies are managed by utilizing a Docker container, we must issue the [`docker run`](https://docs.docker.com/engine/reference/commandline/run/) command to start a container as the run-time environment. The container is launched from a Docker image which was built in [Installation](#installation). The `-v <input_path>:/data` must contain a subdirectory named `inputs` (similar to `s3://noaa-nws-owp-fim/hand_fim`). If the pathing is set correctly, we do not need to adjust the `params_template.env` file, and can use the default file paths provided.


```bash
docker run --rm -it --name <your_container_name> \
    -v <path/to/repository>/:/foss_fim \
    -v <desired_output_path>/:/outputs \
    -v <desired_outputs_temp_path>/:/fim_temp \
    -v <input_path>:/data \
    <image_name>:<tag>
```
For example:
```bash
docker run --rm -it --name Robs_container \
    -v /home/projects/fim/code/inundation-mapping/:/foss_fim \
    -v /home/projects/fim/data/outputs/:/outputs \
    -v /home/projects/fim/data/outputs_temp/:/fim_temp \
    -v /home/projects/fim/data/:/data \
    fim_4:dev_20230620
```

### Subsetting input data
A subset of the data required to run and evaluate FIM can be obtained with the use of ESIP AWS keys. In order to generate these data:
1. Start a Docker container as in the previous step
2. Run `/foss_fim/data/get_sample_data.py` replacing `<aws_access_key_id>` and `<aws_secret_access_key>` with your AWS access keys. To generate data for HUC 03100204, for example:
```
python /foss_fim/data/get_sample_data.py -u 03100204 -i s3://noaa-nws-owp-fim/hand_fim/ -o /outputs/sample-data -r hand_fim -s3 -ak <aws_access_key_id> -sk <aws_secret_access_key>
```
3. Exit the Docker container by typing `exit`. Alternatively, you can leave this container running and run the next command in a new terminal tab or window.
4. Start a new Docker container with the `/data` volume mount pointing at the local output location (`-o`) used in `get_sample_data.py` (step 2). For example:
```bash
docker run --rm -it --name Robs_data_container \
    -v /home/projects/fim/code/inundation-mapping/:/foss_fim \
    -v /home/projects/fim/data/outputs/:/outputs \
    -v /home/projects/fim/data/outputs_temp/:/fim_temp \
    -v /home/projects/fim/data/outputs/sample-data:/data \
    fim_4:dev_20230620
```
5. Now you can run the following commands with the sample data.

### Produce HAND Hydrofabric
```
fim_pipeline.sh -u <huc8> -n <name_your_run> -o
```
- There are a wide number of options and defaulted values, for details run ```fim_pipeline.sh -h```.
- Mandatory arguments:
    - `-u` can be a single huc, a series passed in quotes space delimited, or a line-delimited (.lst) file. To run the entire domain of available data use one of the ```/data/inputs/included_huc8.lst``` files or a HUC list file of your choice.  Depending on the performance of your server, especially the number of CPU cores, running the full domain can take multiple days.
    - `-n` is a name of your run (only alphanumeric). This becomes the name of the folder in your `outputs` folder.
    - `-o` is an optional param but means "overwrite". Add this argument if you want to allow the command to overwrite the folder created as part of the `-n` (name) argument.
    - While not mandatory, if you override the `params_template.env` file, you may want to use the `-c` argument to point to your adjusted file.
- Outputs can be found under ```/outputs/<name_your_run>```.

Processing of HUCs in FIM4 occurs in three sections.
You can run `fim_pipeline.sh` which automatically runs all of three major section,
OR you can run each of the sections independently if you like.

The three sections are:
1. `fim_pre_processing.sh` : This section must be run first as it creates the basic output folder for the run. It also creates a number of key files and folders for the next two sections.
2. `fim_process_unit_wb.sh` : This script processes one and exactly one HUC8 plus all of it's related branches. While it can only process one, you can run this script multiple times, each with different HUC (or overwriting a HUC). When you run `fim_pipeline.sh`, it automatically iterates when more than one HUC number has been supplied either by command line arguments or via a HUC list. For each HUC provided, `fim_pipeline.sh` will run  `fim_process_unit_wb.sh`. Using the `fim_process_unit_wb.sh`  script allows for a run / rerun of a HUC, or running other HUCs at different times / days or even different docker containers.
3. `fim_post_processing.sh` : This section takes all of the HUCs that have been processed, aggregates key information from each HUC directory and looks for errors across all HUC folders. It also processes the HUC group in sub-steps such as usgs guages processesing, rating curve adjustments and more. Naturally, running or re-running this script can only be done after running `fim_pre_processing.sh` and at least one run of `fim_process_unit_wb.sh`.

Running the `fim_pipeline.sh` is a quicker process than running all three steps independently, but you can run some sections more than once if you like.

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

- From inside your docker container, run the following command from your root directory in your docker container :
    ```bash
    pipenv install <your package name> --dev
    ```
    The `--dev` flag adds development dependencies, omit it if you want to add a production dependency.

    This will automatically update the Pipfile in the root of your docker container directory. If the environment looks goods after adding dependencies, lock it with:

    ```bash
    pipenv lock
    ```

    This will update the `Pipfile.lock`. Copy the new updated `Pipfile` and `Pipfile.lock` in the FIM source directory and include both in your git commits. The docker image installs the environment from the lock file.


**Make sure you test it heavily including create new docker images and that it continues to work with the code.**

If you are on a machine that has a particularly slow internet connection, you may need to increase the timeout of pipenv. To do this simply add `PIPENV_INSTALL_TIMEOUT=10000000` in front of any of your pipenv commands.


----
## Citing This Work

Please cite this work in your research and projects according to the [`CITATION.cff`](/CITATION.cff) file.

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
2. [Fernando Aristizabal, Fernando Salas, Gregory Petrochenkov, Trevor Grout, Brian Avant, Bradford Bates, Ryan Spies, Nick Chadwick, Zachary Wills, Jasmeet Judge. 2023. "Extending Height Above Nearest Drainage to Model Multiple Fluvial Sources in Flood Inundation Mapping Applications for the U.S. National Water Model.' Water Resources Research](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2022WR032039).
3. [National Flood Interoperability Experiment (NFIE)](https://web.corral.tacc.utexas.edu/nfiedata/)
4. Garousi‐Nejad, I., Tarboton, D. G.,Aboutalebi, M., & Torres‐Rua, A.(2019). Terrain analysis enhancements to the Height Above Nearest Drainage flood inundation mapping method. Water Resources Research, 55 , 7983–8009.
5. [Zheng, X., D.G. Tarboton, D.R. Maidment, Y.Y. Liu, and P. Passalacqua. 2018. “River Channel Geometry and Rating Curve Estimation Using Height above the Nearest Drainage.” Journal of the American Water Resources Association 54 (4): 785–806.](https://doi.org/10.1111/1752-1688.12661)
6. [Liu, Y. Y., D. R. Maidment, D. G. Tarboton, X. Zheng and S. Wang, (2018), "A CyberGIS Integration and Computation Framework for High-Resolution Continental-Scale Flood Inundation Mapping," JAWRA Journal of the American Water Resources Association, 54(4): 770-784.](https://doi.org/10.1111/1752-1688.12660)
7. [Barnes, Richard. 2016. RichDEM: Terrain Analysis Software](http://github.com/r-barnes/richdem)
8. [TauDEM](https://github.com/dtarb/TauDEM)
9. [Federal Emergency Management Agency (FEMA) Base Level Engineering (BLE)](https://webapps.usgs.gov/infrm/estBFE/)
10. [Verdin, James; Verdin, Kristine; Mathis, Melissa; Magadzire, Tamuka; Kabuchanga, Eric; Woodbury, Mark; and Gadain, Hussein, 2016, A software tool for rapid flood inundation mapping: U.S. Geological Survey Open-File Report 2016–1038, 26](http://dx.doi.org/10.3133/ofr20161038)
11. [United States Geological Survey (USGS) National Hydrography Dataset Plus High Resolution (NHDPlusHR)](https://www.usgs.gov/core-science-systems/ngp/national-hydrography/nhdplus-high-resolution)
12. [Esri Arc Hydro](https://www.esri.com/library/fliers/pdfs/archydro.pdf)
