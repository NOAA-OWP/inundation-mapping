##  About CatFIM
CatFIM is short for Categorical Flood Inundation Mapping. CatFIM is a tool that is run on HAND FIM outputs to inundate a bunch of AHPS sites to specific stages or flow levels. This is useful for quality controlling HAND FIM as well as for preparing for hypothetical flood events. CatFIM models floods at up to 5 different magnitudes: action, minor, moderate, major, and record. The flows or stages for these magnitudes is pulled from the WRDS API.

### Stage-based vs Flow-based
There are two modes for running CatFIM: stage-based and flow-based. Stage-based CatFIM inundates the HAND FIM relative elevation model (REM) based on different magnitudes of stage, which is the height of the water surface. Flow-based CatFIM inundates the REM based on different magnitudes of flow, which is measured as the volume over time of water flowing past a certain point of the river. A rating curve is used to get the water height from the flow volume at a given location.

### How are CatFIM sites selected?

Our goal is to produce good CatFIM for as many APHS sites as possible. In order to meet this goal, a site must meet a variety of acceptance critera and data availability checks for us to produce CatFIM at the site. 

If you have a question about why a specific point is being excluded, you can check the "Status" attribute of the CatFIM point to see what issue might be occurring. If there is a question or concern about a specific point, feel free to reach out to GID via Slack or VLAB.


CatFIM sites must: 
- have site-specific stage- or flow- thresholds available
- be an NWM forecast point (for sites in CONUS*)
- not be on the stage-based AHPS restricted sites list (for stage-based CatFIM)
- have an accurate vertical datum (for stage-based CatFIM)
- meet the USGS Gages Acceptance Criteria (detailed below)

USGS Gages Acceptance Criteria:
- [Lat/Long Coordinate Method](https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html) must be one of the following: "C", "D", "W", "X", "Y", "Z", "N", "M", "L", "G", "R", "F", "S"
- [Acceptable Altitute Accuracy Threshold](https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI) must be 1 or lower
- [Altitute Method Type](https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html) must be one of the following: "A", "D", "F", "I", "J", "L", "N", "R", "W", "X", "Y", "Z"
- [Site Type](https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html) must be: "ST" (stream)

*Note: Previous versions of CatFIM also restricted sites based on [Lat/Long Coordinate Accuracy](https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html), but that criteria was removed in Fall 2024 to increase availability of CatFIM sites.*

*For sites outside of CONUS: As of 12/4/2024, these criteria are currently being workshopped to account for the unique challenges of producing CatFIM in non-CONUS locations. Check back for updates or reach out to GID via Slack if you have specific questions! 


## Running CatFIM
### Who can run CatFIM?
CatFIM can only be run by systems that can access the WRDS API, which is restricted to computers on the NOAA network. If you are outside the NOAA network and would like to run code from inundation-mapping, see the README in NOAA-OWP/inundation-mapping.


### Commands
Stage-based example with step system and pre-downloaded metadata: 

`python /foss_fim/tools/generate_categorical_fim.py -f /outputs/Rob_catfim_test_1 -jh 1 -jn 10 -ji 8 -e /data/config/catfim.env -t /data/docker_test_1 -me '/data/nwm_metafile.pkl' -sb -step 2`

Flow-based example with HUC list:

`python /foss_fim/tools/catfim/generate_categorical_fim.py -f /data/previous_fim/fim_4_5_2_11/ -jh 4 -jn 2 -e /data/config/catfim.env -t /data/hand_4_5_11_1_catfim_datavis -o -lh '06010105 17110004 10300101 19020401 19020302'`


### Arguments
- `-f`, `--fim_run_dir`: Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_5_2_11
- `-e`, `--env_file`: Docker mount path to the catfim environment file. ie) data/config/catfim.env
- `-jh`, `--job_number_huc`: OPTIONAL: Number of processes to use for HUC scale operations. HUC and inundation job numbers should multiply to no more than one less than the CPU count of the machine. CatFIM sites generally only have 2-3 branches overlapping a site, so this number can be kept low (2-4). Defaults to 1.
- `-jn`, `--job_number_inundate`: OPTIONAL: Number of processes to use for inundating HUC and inundation job numbers should multiply to no more than one less than the CPU count of the machine. Defaults to 1.
- `-ji`, `--job_number_intervals`: OPTIONAL: Number of processes to use for inundating multiple intervals in stage-based inundation and interval job numbers should multiply to no more than one less than the CPU count of the machine. Defaults to 1.
- `-sb`, `--is_stage_based`: Run stage-based CatFIM instead of flow-based? Add this -sb param to make it stage based, leave it off for flow based.
- `-t`, `--output_folder`: OPTIONAL: Target location, Where the output folder will be. Defaults to /data/catfim/
- `-s`, `--search`: OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.
- `-lh`, `--lst_hucs`: OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs.
- `-mc`, `--past_major_interval_cap`: OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs? of the machine. Defaults to 5.
- `-step`: 'OPTIONAL: By adding a number here, you may be able to skip levels of processing. The number you submit means it will start at that step. e.g. step of 2 means start at step 2 which for flow based is the creating of tifs and gpkgs. Note: This assumes those previous steps have already been processed and the files are present. Defaults to 0 which means all steps processed.
- `-me`, `--nwm_metafile`: OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.  NOTE: This parameter is for quick debugging only and should not be used in a production mode.
- `-o`, `--overwrite`: OPTIONAL: Overwrite files.

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
