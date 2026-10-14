##  About CatFIM

CatFIM is short for Categorical Flood Inundation Mapping. CatFIM is a tool that is run on HAND FIM outputs to inundate a bunch of AHPS sites to specific stages or flow thresholds. The static inundation maps produced by CatFIM can be used for long-range flood planning, comparing the impacts different inundation scenarios for a site, and monitoring HAND FIM quality in different locations. CatFIM produces static flood inundation maps for up to 5 different magnitudes per site: action, minor, moderate, major, and record. The flow and stage thresholds for each site are provided by the local River Forecast Centers (RFCs), which we access via our WRDS API.

### Stage-based vs Flow-based

There are two modes for running CatFIM: stage-based and flow-based. Flow-based CatFIM inundates the REM based on different magnitudes of flow, which is measured as the volume over time of water flowing past a certain point of the river. Stage-based CatFIM inundates the HAND FIM relative elevation model (REM) based on different magnitudes of stage, which is the height of the water surface. Stage-based CatFIM also maps inundation at 1-foot intervals between the stage thresholds, which provides more detailed inundation data wherever possible.  

### How are CatFIM sites selected?

Our goal is to produce accurate CatFIM for as many APHS sites as possible. In order to meet this goal, a site must meet a variety of acceptance critera and data availability checks for us to produce CatFIM at the site. 

In order to have CatFIM, sites must meet the following criteria:
- Has at least one stage threshold available
- Has an RFC or USGS rating curve available (for flow-based CatFIM)
- Is an NWM forecast point (for sites in CONUS*)
- Has an accurate vertical datum (for stage-based CatFIM)
- Is not on our [restricted sites list](https://github.com/NOAA-OWP/inundation-mapping/blob/26fcd5f761e8de3de1a946f489a7d4bd4b2e2d40/tools/catfim/ahps_restricted_sites.csv)
- Meets the USGS Gages Acceptance Criteria (detailed below)

USGS Gages Acceptance Criteria:
- [Lat/Long Coordinate Method](https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html) must be one of the following: "C", "D", "W", "X", "Y", "Z", "N", "M", "L", "G", "R", "F", "S"
- [Acceptable Altitute Accuracy Threshold](https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI) must be 1 or lower
- [Altitute Method Type](https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html) must be one of the following: "A", "D", "F", "G", "I", "J", "L", "N", "R", "U", "W", "X", "Y", "Z"**
- [Site Type](https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html) must be: "ST" (stream)

*Note: Previous versions of CatFIM also restricted sites based on [Lat/Long Coordinate Accuracy](https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html), but that criteria was removed in Fall 2024 to increase availability of CatFIM sites.*

*For sites outside of CONUS: As of 12/4/2024, these criteria are currently being workshopped to account for the unique challenges of producing CatFIM in non-CONUS locations. Check back for updates or reach out to GID via Slack if you have specific questions! 

**Altitude method types "G" and "U" added in January 2026, so subsequent CatFIM versions will not remove sites based on those values. 

### What does the site status mean?

If you have a question about why a specific point is being excluded, you can check the "Status" attribute of the CatFIM point to see what issue might be occurring.

If there is a question or concern about a specific point, feel free to reach out to GID via Slack or VLAB.

## Running CatFIM
### Who can run CatFIM?
CatFIM can only be run by systems that can access the WRDS API, which is restricted to computers on the NOAA network. If you are outside the NOAA network and would like to run code from inundation-mapping, see the README in NOAA-OWP/inundation-mapping.

### Commands

Flow-based example:

`fim_version="4_9_20_1" ; python /foss_fim/tools/catfim/generate_categorical_fim.py -f /data/previous_fim/hand_${fim_version}/ -j 1 -t /temp/test5_hand -o -lh "12100202" -e data/config/catfim.env -ct fb -mod handhrp`

Stage-based example (with specified meta file and threshold file):

`fim_version="4_9_20_1" ; python /foss_fim/tools/catfim/generate_categorical_fim.py -f /data/previous_fim/hand_${fim_version}/ -j 1 -t /projects/catfim_hecras_fb/test_outputs/test5 -o -lh "12100202" -e data/config/catfim.env -ct sb -tf '/data/inputs/wrds/WRDS_Thresholds_20260723.pkl' -mf '/data/inputs/wrds/WRDS_Metadata_20260723.pkl'`

### Arguments
- `-f`, `--fim_run_dir`: Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_5_2_11
- `-ct`, `--catfim-type`: Whether to run flow-based (`fb`) or stage-based (`sb`) CatFIM.
- `-t`, `--output_folder`: Target location, where the output folder will be. Defaults to /data/catfim/
- `-e`, `--env-file`: OPTIONAL: Docker mount path to the environment file. Defaults to /data/config/fim_enviro_values.env
- `-jh`, `--number-jobs`: Number of HUCs to process simultaneously.
- `-s`, `--search`: OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.
- `-lh`, `--lst-hucs`: OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs.
- `-mc`, `--past-major-interval-cap`: OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs? of the machine. Defaults to 5.
- `-mf`, `--nwm-meta-file`: OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.
- `-gmf`, `--get-new-meta-data`: OPTIONAL: Whether or not to download new metadata from WRDS. Only works on OWP servers, not AWS.
- `-tf`, `--threshold-file`: OPTIONAL: If you have a pre-existing threshold file, you can path to it here. Providing this manual input will prevent the WRDS API from being queried for thresholds.
- `-gtf`, `--get-new-threshold-data`: OPTIONAL: Whether or not to download new metadata from WRDS. Only works on OWP servers, not AWS.
- `-mod`, `--model`: OPTIONAL: Define which model(s) to run between HAND and HEC-RAS. Currently only for flow-based CatFIM.
         hand : run HAND only (default)
         hrp : run HEC-RAS only - with model preference (use one HEC-RAS model per site)
         hr : run HEC-RAS only - without model preference (use ALL available HEC-RAS models, not recommended for large runs)
         handhrp : run HAND and HEC-RAS - with model preference (use one HEC-RAS model per site, prioritize HEC-RAS over HAND
         handhr : run HAND and HEC RAS - without model preference (use ALL available HEC-RAS models, keep HEC-RAS and HAND sites, not recommended for large runs)

- `-hf`, `--preprocessed-hecras-folder`: OPTIONAL: Path to the HEC-RAS files directory (generated via catfim_hecras_preprocessing.py). e.g.: /data/catfim/hecras/catfim_hecras_preprocessing_20260910 . Default value comes from bash_variables. Must contain the following files: sites_with_hecras_models.csv and combined_controls_output.csv
- `-fp`, `--flows2fim-software-path`: OPTIONAL: Path to the flows2fim software.
- `-sp`, `--skip-processing`: OPTIONAL: If this flag is set, it will set up all of the initial 'pre-processing' steps, but will  not continue with the processing of the hucs or post processing. This allows this tool to be used as either a full fun, or just do post processing and let other tools like AWS do process hucs and post processing.
- `-o`, `--overwrite`: OPTIONAL: Overwrite files.

## Running CatFIM Site Comparison
### What is CatFIM Site Comparison?
CatFIM site comparison is an internal tool that allows us to compare multiple different versions of CatFIM outputs. 

### Commands

Generating geopackages:
`python /foss_fim/tools/catfim/catfim_sites_compare.py -p '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/' -o '/test_outputs' -g`

Multiple comparisons:
`python /foss_fim/tools/catfim/catfim_sites_compare.py -p '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/' -o '/test_outputs'`

### Arguments

- `-p`, `--path-list`: Space-delimited list of CatFIM output paths from which to compile sites. Paths MUST have the version number in it (i.e. `4_5_11_1`) because the version number is used to put the versions in sequential order.
- `-o`, `--output-save-filepath`: Path to where the results files will be saved.
- `-k`, `--keep-differences-only`: OPTIONAL: Option to keep only changed sites in the comparison files.
- `-g`, `--generate-geopackages`: OPTIONAL: Option to generate spatial difference maps and a points geopackage for the version comparisons.
- `-d`, `--debug-mode`: OPTIONAL: Debug mode. Will only iterate through 100 records while generating geopackages.

## Running CatFIM HEC-RAS Pre-Processing

### What is CatFIM HEC-RAS preprocessing?

It is the processing that occurs to pre-download and subset the controls CSVs required to run HEC-RAS in flow-based CatFIM. It only takes about 8 minutes.

### Example Commands

`python /foss_fim/tools/catfim/catfim_hecras_preprocessing.py -o '/projects/catfim_hecras_fb/hecras_preprocess_test6'`
 
### Arguments

- `-o`, `--output-folder-location`: REQUIRED: Target location to create the catfim_hecras_preprocessing folder to store final outputs. A temp folder will also be created in the catfim_hecras_preprocessing dir. e.g.: /projects/catfim_hecras_fb/test_outputs/final_outputs.
- `-tf`, `--threshold-file`: OPTIONAL: Filepath to the threshold pkl file. If not provided, will use the default from the env file. e.g.: /data/inputs/wrds/thresholds_20260413.pkl
- `-mf`, `--nwm-meta-file`: OPTIONAL: Filepath to nwm metadata pickle file. If not provided, will use the default from the env file. e.g.: /data/inputs/wrds/nwm_metadata_20260413.pkl
- `-r`, `--ripple-filename`: OPTIONAL: Folder from which to get Ripple model inputs.
- `-f`, `--flows2fim-software-path`: OPTIONAL: Path to the flows2fim software.
- `-w`, `--ripple-model-status-path`: OPTIONAL: Path to the ripple model status file.
- `-l`, `--lst-models`: OPTIONAL: Space-delimited list of models to preprocess HEC-RAS for. Defaults to all models in the given ripple folder.

## Visualization Tips & Tricks

### Changing Symbol Drawing Order

- **ArcGIS Pro:** Go to the Symbology pane and navgate to the Symbol layer drawing tab. Make sure the "Enable symbol layer drawing" option is switched "ON", and then adjust the Drawing Order of the magnitudes so they are in this order: action, minor, moderate, major, record.

![screenshot of Symbology pane in ArcGIS Pro](https://github.com/NOAA-OWP/inundation-mapping/blob/b527e762478fef2c1ffc5f0ff4d494f1746663bb/tools/catfim/images/screenshot_vis_settings.JPG)

- **QGIS**: To change the symbol drawing order in QGIS...

## Glossary
- AHPS: Advanced Hydrologic Prediction Services (NOAA)
- WRDS: Water Resources Data System (NOAA)
- REM: relative elevation model
- HAND: height above nearest drainage
