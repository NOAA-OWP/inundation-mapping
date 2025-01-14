All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

## vx.x.x.x - 2025-01-08 - [PR#1340](https://github.com/NOAA-OWP/inundation-mapping/pull/1340)

This PR focuses on adjusting rating curves by using bathymetric data and optimized channel roughness values. The bathymetry data includes eHydro surveys and AI-based datasets created for all NWM streams. New manning roughness values were developed for each feature-id using a differential evolution objective function (OF). The OF minimizes the number of the false_positives and false_negatives cells in our flood inundation maps where we have test cases across the CONUS. 

Even though the Python scripts of roughness manning number optimization were not included in this branch, optimized roughness values can be found here: `/fim-data/inputs/rating_curve/variable_roughness/mannings_optz_fe_clusters_so3.csv`. Detailed python scripts also can be found here: `/fim-data/outputs/heidi-mannN-optimization/projects/bathy_mannN_projects/dev-bathymetric-adjustment-mannN-optz/`.

### Changes
- `src/bathymetric-adjustment.py`: `correct_rating_for_ai_based_bathymetry` function was added to the script. This function processes AI-based bathymetry data and adjusts rating curves using this data. Also `apply_src_adjustment_for_bathymetry` function was added to prioritize USACE eHydro over AI-based bathymetry dataset. The multi-processing function `multi_process_hucs` was updated based on the latest code. Also, an ai_toggle parameter was added to `apply_src_adjustment_for_bathymetry` and `process_bathy_adjustment` functions. When ai_toggle = 1, The SRCs will be adjusted with the ai_based bathymetry data. the default value for ai_toggle = 0, means no ai_based bathy data is included. 

- `src/bash_variables.env`: New variables and their paths were added. Also, a new input file with the nwm feature_ids and optimized channel roughness and overbank roughness attributes was created and stored here:
`/fim-data/inputs/rating_curve/variable_roughness/mannings_optz_fe_clusters_so3.csv`
The locations of these files were also added to the `bash_variables.env`.
Please note that when ai_toggle = 1, the manning roughness values should be switched to `vmann_input_file=${inputsDir}/rating_curve/variable_roughness/mannings_optz_fe_clusters_so3.csv` in the current version. 

Here is a list of new/updated input files:

1. `/fim-data/inputs/rating_curve/variable_roughness/mannings_optz_fe_clusters_so3.csv`
This CSV file contains the new optimized roughness values. It will replace this file:
`vmann_input_file=${inputsDir}/rating_curve/variable_roughness/mannings_global_nwm3.csv`

2. `bathy_file_aibased=${inputsDir}/bathymetry/ml_outputs_v1.01.parquet`
This file contains the ml-bathymetry and manning roughness values data.

3. `bathy_file_ehydro=${inputsDir}/bathymetry/final_bathymetry_ehydro.gpkg`
We already had this file, the name of the variable has changed from `bathymetry_file` to `bathy_file_ehydro`, and it was updated.

- `fim_post_processing.sh`: New arguments were added. Please note that the default value for ai_toggle = 0 is included here. 

<br/><br/>


## v4.5.13.7 - 2025-01-10 - [PR#1379](https://github.com/NOAA-OWP/inundation-mapping/pull/1379)

There are many sites in non-CONUS regions (AK, PR, HI) where we would like to run CatFIM but they are being excluded because they are not NWM forecast points. This update brings back the double API pull and adds in some code to filter out duplicate (and NULL) lids from the metadata lists. 

### Additions
- `inundation-mapping/tools/catfim/vis_categorical_fim.py`: Functions for reading in, processing, and visualizing CatFIM results. 
-  `inundation-mapping/tools/catfim/notebooks/vis_catfim_cross_section.ipynb`: A new Jupyter notebook for viewing and analyzing CatFIM results.
- `inundation-mapping/tools/catfim/notebooks/eval_catfim_metadata.ipynb`: A new Jupyter notebook for evaluating metadata and results from CatFIM runs. 
- `inundation-mapping\config/symbology/qgis/catfim_library.qml`: Symbology preset for viewing CatFIM library in QGIS.


### Changes

- `inundation-mapping/tools/catfim/generate_categorical_fim_flows.py`: Re-implements the dual API call and filters out duplicate sites.


<br/><br/>

## v4.5.13.6 - 2025-01-10 - [PR#1387](https://github.com/NOAA-OWP/inundation-mapping/pull/1387)

Fixes two issues in test_cases:
1. An error in `synthesize_test_cases` and `run_test_case` if any directories of the 5 benchmark sources (BLE, NWS, IFC, USGS, or ras2fim) do not exist. This issue was originally discovered and fixed in #1178, but is being elevated to its own PR here. Fixes #1386.
2. Updated `run_test_cases` to accommodate levee and waterbody masking in Alaska. As part of these changes, hardcoded paths were replaced by environment variables.

### Changes

- `tools/`
    - `run_test_case.py`: Fixed error if missing validation data. Updated masking data to include Alaska.
    - `synthesize_test_cases.py`: Fixed error if missing validation data.
    
<br/><br/>


## v4.5.13.5 - 2025-01-09 - [PR#1389](https://github.com/NOAA-OWP/inundation-mapping/pull/1389)

Updates Python packages to resolve dependency conflicts that were preventing `Dockerfile.dev` to build on Mac. This also resolves two security warnings: https://github.com/NOAA-OWP/inundation-mapping/security/dependabot/51 and https://github.com/NOAA-OWP/inundation-mapping/security/dependabot/52.

### Changes

- `Pipfile` and `Pipfile.lock`: Upgrades Python packages

<br/><br/>


## v4.5.13.4 - 2024-01-03 - [PR#1382](https://github.com/NOAA-OWP/inundation-mapping/pull/1382)

Cleans up Python files within `delineate_hydros_and_produce_HAND.sh` to improve performance, especially memory management, including removing unused imports, deleting object references when objects are no longer needed, and removing GDAL from the `fim_process_unit_wb.sh` step of FIM pipeline. Contributes to #1351 and #1376.

### Changes
- `data/create_vrt_file.py` and `tools/pixel_counter.py`: Removes unused import
- `src/`
    - `accumulate_headwaters.py`, `add_crosswalk.py`, `adjust_thalweg_lateral.py`, `filter_catchments_and_add_attributes.py`, `heal_bridges_osm.py`, `make_rem.py`, `make_stages_and_catchlist.py`, `mitigate_branch_outlet_backpool.py`, `reachID_grid_to_vector_points.py`, `split_flows.py`, `unique_pixel_and_allocation.py`: Deletes objects no longer in use
    - `delineate_hydros_and_produce_HAND.sh`, `run_by_branch.sh`, `run_unit_wb.sh` : Updates arguments
    - `getRasterInfoNative.py`: Refactors in `rasterio` (removed `gdal`)
- `tools/evaluate_crosswalk.py`: Deletes objects no longer in use

<br/><br/>


## v4.5.13.3 - 2025-01-03 - [PR#1048](https://github.com/NOAA-OWP/inundation-mapping/pull/1048)

This script produces inundation depths and attempts to overcome the catchment boundary issue by interpolating water surface elevations between catchments. Water surface calculations require the hydroconditioned DEM (`dem_thalwegCond_{}.tif`) for computation, however, this file is not in the standard outputs from fim_pipeline.sh. Therefore, users may have to re-run fim_pipeline.sh with dem_thalwegCond_{}.tif removed from all deny lists.

### Additions

- `tools/interpolate_water_surface.py`: New post-inundation processing tool for extending depths beyond catchment limits. The `interpolate_wse()` contains the logic for computing the updated depth raster, but users can also call this module directly to perform inundation, similar to how `inundate_mosaic_wrapper.py` works, but with the new post-processing enhancement.

<br/><br/>


## v4.5.13.2 - 2025-01-03 - [PR#1360](https://github.com/NOAA-OWP/inundation-mapping/pull/1360)

Fixed missing osmid in osm_bridge_centroid.gpkg. Also, HUC column is added to outputs.

### Changes
- `data/bridges/pull_osm_bridges.py`
- `src/aggregate_by_huc.py`

<br/><br/>


## v4.5.13.1 - 2024-12-13 - [PR#1361](https://github.com/NOAA-OWP/inundation-mapping/pull/1361)

This PR was triggered by two dep-bot PR's. One for Tornado, one for aiohttp. Upon further research, these two exist only as dependencies for Jupyter and Jupyterlab which were very out of date. Upgrading Jupyter/JupyterLab took care of the other two.

Also fixed a minor warning during docker builds.

Covers PR [1237](https://github.com/NOAA-OWP/inundation-mapping/pull/1347): Bump aiohttp from 3.10.5 to 3.10.11  and  PR [1348](https://github.com/NOAA-OWP/inundation-mapping/pull/1348): Bump tornado from 6.4.1 to 6.4.2


### Changes
- `Dockerfile.dev` and `Dockerfile.prod`:  As described above.
- `Pipfile` and `Pipefile.lock`:   As described above.

<br/><br/>


## v4.5.13.0 - 2024-12-10 - [PR#1285](https://github.com/NOAA-OWP/inundation-mapping/pull/1285)

Major upgrades and bug fixes to the CatFIM product, informally called CatFIM 2.1. See the PR for all details

<br/><br/>


## v4.5.12.2 - 2024-12-10 - [PR#1346](https://github.com/NOAA-OWP/inundation-mapping/pull/1346)

This PR updates deny lists to avoid saving unnecessary files.
I also added PR #1260 (changes to data/bathymetry/preprocess_bathymetry.py ) to this PR.

### Changes

- `config/deny_branch_zero.lst`
- `config/deny_branches.lst`
- `config/deny_unit.lst`
- `data/bathymetry/preprocess_bathymetry.py`

<br/><br/>


## v4.5.12.1 - 2024-11-22 - [PR#1328](https://github.com/NOAA-OWP/inundation-mapping/pull/1328)

Fixes bug and adds error checking in FIM Performance. Fixes #1326.

### Changes
- `src/utils/fim_logger.py`: Fix a spacing issue
- `tools/`
    - `pixel_counter.py`: Adds check if file exists
    - `run_test_case.py`: if there is a .aux.xml file in the test_case dir, this can fail. now fixed.
    - `test_case_by_hydro_id.py`: Fixes bug and adds error checking/logging

<br/><br/>


## v4.5.12.0 - 2024-11-01 - [PR#1327](https://github.com/NOAA-OWP/inundation-mapping/pull/1327)

The purpose of this PR is to cut down the runtime for four Alaska HUCs (19020104, 19020503, 19020402 , and 19020602). It significantly optimizes runtime by replacing a nested for loop, used for updating rating curve for small segments, with a vectorized process. This changes were applied only to the Alaska HUCs.
As part of this PR, small modification was applied to bridge_inundation.py.

### Changes

- `src/add_crosswalk.py`
- `src/delineate_hydros_and_produce_HAND.sh`
- `tools/bridge_inundation.py`

<br/><br/>


## v4.5.11.3 - 2024-10-25 - [PR#1320](https://github.com/NOAA-OWP/inundation-mapping/pull/1320)

The fix: During the post processing scan for the word "error" or "warning", it was only finding records which had either of those two words as stand alone words and not part of bigger phrases.  ie); "error" was found, but not "fielderror". Added wildcards and it is now fixed.

Note: it is finding a good handful more errors and warnings that were being missed in earlier code versions.

### Changes
`fim_post_processing.sh`: fix as described.

<br/><br/>


## v4.5.11.2 - 2024-10-25 - [PR#1322](https://github.com/NOAA-OWP/inundation-mapping/pull/1322)

For security reasons, we needed to create a docker image that does not use the root user in anyway. The new `Dockerfile.prod` file is to be used when we want to use a non-root user. The  original `Dockerfile` has been renamed to `Dockerfile.dev` and will continue to use it's root users which has no problems with interacting with external mounts.

Note: Re: using pip or pipenv installs.
In the Dockerfile.prod, you can not do installs or update using either pipenv or pip.  Those types of tests and adjustments need to be done in the `Dockerfile.dev`. `Dockerfile.dev` will also allow change to the `Pipfile` and `Pipfile.lock` . Both docker files share the Pipfiles so it should be just fine.

### File Renames
- Was: `Dockerfile`,  now `Dockerfile.dev`

### Additions

- Dockerfile.prod: as described

### Changes
- `README.md`: change notes from phrase `Dockerfile` to `Dockerfile.dev`. Also added some notes about the new convention of outputs no longer starting with `fim_` but now `hand_`
- `fim_pipeline.sh`: Change for the new `Dockerfile.prod` for permissions.
- `fim_post_processing.sh`: Change for the new `Dockerfile.prod` for permissions.
- `fim_pre_processing.sh`: Change for the new `Dockerfile.prod` for permissions.
- `fim_process_unit_wb.sh`: Change for the new `Dockerfile.prod` for permissions.

<br/><br/>


## v4.5.11.1 - 2024-10-16 - [PR#1318](https://github.com/NOAA-OWP/inundation-mapping/pull/1318)

Bug fixes to address issues during `fim_pipeline.sh`.

### Changes

- `src/`
    - `aggregate_by_huc.py`: Fix `pyogrio` field error.
    - `stream_branches.py`: Remove `bids_temp` and fix index.

<br/><br/>

## v4.5.11.0 - 2024-10-11 - [PR#1298](https://github.com/NOAA-OWP/inundation-mapping/pull/1298)

This PR addresses four issues regarding OSM bridges. It dissolves touching bridge lines so each bridge has a single linestring. It also removes abandoned bridge from the dataset and it adds bridge type field to bridge centroids. As part of this PR, `max_hand_ft` and `max_discharge_cfs` columns are added to `osm_bridge_centroids.gkpg`.

### Changes

- `data/bridges/pull_osm_bridges.py`
- `src/heal_bridges_osm.py`

<br/><br/>


## v4.5.10.3 - 2024-10-11 - [PR#1306](https://github.com/NOAA-OWP/inundation-mapping/pull/1306)

Extends outlet levelpath(s) outside HUC.

Previously, levelpaths at the outlet of a HUC may not extend to the buffered WBD that is used to clip the DEM, and during pit-filling this results in reverse flow which can cause DEM-derived reaches to deviate from the channel in the DEM and may result in dropped catchments where the midpoint of the reaches exceeds the snap distance from the NWM stream lines.

This PR extends outlet levelpaths in two ways:
- Segments of levelpaths that terminate in waterbodies are removed from the levelpath. If there is a waterbody downstream of the HUC then the outlet reaches may be trimmed such that the outlet no longer reaches the edge of the DEM, which causes a number of cascading issues originating in the pit-filling such that reverse flow in the DEM-derived reaches can result in erroneous flowlines and inundation. This PR stops trimming levelpaths outside of the HUC.
- Dissolved outlet levelpaths may terminate downstream outside of the HUC (e.g., at a confluence with a larger river) at a point that is within the buffered WBD. These levelpaths are extended by adding on the downstream segment(s) of the HUC's `nwm_subset_streams` layer. The extended levelpath(s) are saved in a new file that is used to create the boolean raster stream network.

### Changes

- `config/`
    - `deny_unit.lst`, `deny_branch_zero.lst`, and `deny_branches.lst`: Adds new file to deny lists
- `src/`
    - `derive_level_paths.py`:  Adds WBD as an input to `stream_network.trim_branches_in_waterbodies()` and adds new argument for new filename.
    - `run_unit_wb.sh`: Adds new argument for new filename.
    - `stream_branches.py`: Selects only segments intersecting the WBD as candidates for removal if they end in waterbodies and adds downstream segment(s) to outlet levelpath(s).
    
<br/><br/>


## v4.5.10.2 - 2024-10-11 - [PR#1244](https://github.com/NOAA-OWP/inundation-mapping/pull/1244)

New tool that can assess the impact of a flood on road and/or building vector files. Closes #1226.

### Additions
- `tools/analyze_flood_impact.py` : added a tool that assesses the impact of a flood on roads and buildings by calculating how many roads and structures the test flood extent intersects, comparing the test impacted roads and structures to a benchmark, and calculating CSI.

 <br/><br/>


## v4.5.10.1 - 2024-10-11 - [PR#1314](https://github.com/NOAA-OWP/inundation-mapping/pull/1314)

This PR fixes bugs from hand_4_5_10_0, which failed to run for Alaska HUCs and HUC 02030201. It modifies scripts to use two different DEM paths: one for Alaska and one for the CONUS.

### Changes

- `src/derive_level_paths.py`
- `src/stream_branches.py`
- `src/run_unit_wb.sh`

<br/><br/>


## v4.5.10.0 - 2024-09-25 - [PR#1301](https://github.com/NOAA-OWP/inundation-mapping/pull/1301)

A reload of all 3Dep DEMs from USGS was performed to refresh our data.

`acquire_and_preprocess_3dep_dems.py` had to be run twice, one for Alaska and once for the rest to two different folder. This is due to different CRS's. Eventually, we could merge these into one run. This also meant two separate vrt runs / files. 

This also triggered a new set of pre-clips for both AK and CONUS+ but the outputs can/were put into the same folder, so fim_pipeline looks in one common pre-clip folder.

Other minor adjustment include:
- A change to chmod (permissions) files / folder for the logging folders. After careful re-analysis, it was discovered there was some duplication. 
- Added a simple duration system to the sierra test system, `rating_curve_comparions.py`. This was added as it is expected to be used soon for a  full BED/Production.  The fix focuses purely on duration, but a test did detect a possible pre-existing logic problem. A separate card will be created for that.

Note:
The root folder for DEM is being changed from:
    /inputs/3dep_dems/....   to  
    /inputs/dems/3dep_dems/....
    This recognizes other DEMs that may be coming in the near future.
    The same sub-folder patterns have not be changed.
    No attempts will be made at this time to move older files, only new incoming from this PR.

### Changes
- `CITATION.cff`: has not be updated for a very long time.
- `fim_post_processing.sh`: Update to file/folder permissions.
- `data`
    - `usgs\acquire_and_preprocesss_3dep_dem.pys
        - Minor text updates and updated datatime.now patterns as the old ones are not deprecated
        - An adjustment to how number of jobs are handled. The system dis-likes too many multi-procs due to open network connections to the source.
        - Change the target output folder from optional to required.
    - `wbd`
        - `generate_pre_clip_from_huc8.py`: 
            - Minor text updates
        - `preprocess_wbd.py`
            - Minor text updates
- `src\base_variables.env`: Changes to variables to reflect new dems and pre-clip paths.
- `tools\rating_curve_comparisons.py`
    - Added duration system as mentioned above.

<br/><br/>


## v4.5.9.0 - 2024-09-25 - [PR#1291](https://github.com/NOAA-OWP/inundation-mapping/pull/1291)

Changes Docker base image to `gdal:ubuntu-small` in order to avoid JDK from being carried over in the base image and triggering security vulnerabilities.

This PR incorporates a number of changes to the Docker environment:
- Changes Docker base image to `gdal:ubuntu-small` in order to avoid JDK from being carried over in the base image and triggering security vulnerabilities. Resolves #1278.
- Upgrades `fiona` and `jupterlab`. Closes #1270 and closes #1290.
- Eliminates `whitebox` downloading during `fim_pipeline`. Resolves #1209 and closes #1293.

During testing, it was discovered that many files which are not in the `src` directory, can no longer see the `src\utils` files. Adjusting the dockerfile to add extra values to the PYTHONPATH variable fixed it.

Note: This triggers new docker images to be made.

### Changes

- `Dockerfile`: Changes base image to `gdal:ubuntu-small-3.8.4` and removes code related to JDK
- `Pipfile` and `Pipfile.lock`: Upgrades `fiona`, `jupyterlab`, and `whitebox`
- `fim_pre_processing`: Removes `WBT_PATH` assignment
- `src/`
    - `agreedem.py` and `unique_pixel_and_allocation.py`: sets `whitebox_dir` to `WBT_PATH`

<br/><br/>

## v4.5.8.0 - 2024-09-13 - [PR#1165](https://github.com/NOAA-OWP/inundation-mapping/pull/1165)

This PR was originally intended to get Alaska HUCs incorporated into CatFIM, but there were a very, very large array of problems and the tool was unable to run. We have made some major modifications and many more will come in the near future. There are partial hooks and commented code for Alaska integration, but temporarily disabled are included and will be handled by a separate branch / PR.

One of the biggest improvement was to add a logging system to track what is breaking and where.  Earlier, there were a very large number of places were errors occurred but they were suppressed and never recorded anywhere. A few put the errors on screen but this is a very long running process tool, which can take 2 days, and any messages to the screen are lost. Now all  errors and warning are caught and at least logged in the master log but also the "warning" or "error" log to help them stand out better. Many of the warnings are truly and fairly rejected but at least we know when and why. When we started working with CatFIM again a few months back, there were show stopping errors and we did not know where or why but now we can find and diagnose them.

All three of the core "generate_catfim...py" files include major amounts of changes to improve variable and function names, improve flow and readability, move functions for better understanding of the product, lots of new inline commenting. However, there is a lot to do but it is on a better footing, is pretty stable and hopefully easier to continue upgrades in the near future.

CatFIM is still considered a WIP but it is fully functional again and more adjustments hopefully will go much quicker and smoother.

Also added a system where a config file can be passed into the CatFIM tools instead of assuming a file name and path of simply ".env" in the tools directory. 

This update also relaxes the coordinate accuracy requirements for stage-based CatFIM, which will result in stage-based CatFIM being generated for more sites. 

#### Informally, this is now known as CatFIM 2.0


### Additions
- `config/catfim_template.env`:  Template version of the required catfim env file. The template keeps all values that are non sensitive but removes one that is. The true catfim.env for OWP can be found in our .. data/config/catfim.env. Example pathing here based on docker mounts.

- `src/utils/fim_logger.py`:  A new multi proc custom logging system, modelled directly off of the proven ras2fim logging system. The reason for this custom system is that the native Python logging is not stable in multi-proc environments and tends to loose data. This new logger can relatively easily be bolted into almost any of our py scripts if required.

### Changes
- `.pre-commit-config.yaml`: A linting config adjustment.
- `pyproject.toml`: linting config adjustments
- `src/utils/shared_variables.py`:  Added a comment
- `tools`
    - `generate_categorical_fim.py`: As mentioned above
    - `generate_categorical_fim_flows.py`: As mentioned above
    - `generate_categorical_fim_mapping.py`: As mentioned above
    - `generate_nws_lid.py`:  No real changes but Git thinks something did. It is the same as in current Dev.
    - `mosaic_inundation.py`: Added a comment
    - `tools_shared_functions.py`
         - added some better error handing in a few places, plus some commenting and cleanup.
         - Added a feature to the `aggregate_wbd_hucs` function to optionally submit a list of HUCs for filtering results.

<br/><br/>

## v4.5.7.2 - 2024-09-13 - [PR#1149](https://github.com/NOAA-OWP/inundation-mapping/pull/1149)

This PR adds scripts that can identify areas within produced inundation rasters where glasswalling of inundation occurs due to catchment boundaries, know as catchment boundary issues.

### Additions
- `tools/identify_catchment_boundary.py`: Identifies where catchment boundaries are glasswalling inundation extent.

- `tools/inundate_catchment_boundary.py`: Produces inundation for given HUC and identifies catchment boundary issues in produced FIM. 

 <br/><br/>

## v4.5.7.1 - 2024-09-13 - [PR#1246](https://github.com/NOAA-OWP/inundation-mapping/pull/1246)

Indents the mosaicking block so that `inundate_mosaic_wrapper.py` mosaics both inundation extents and depths.

### Changes

- `tools/inundate_mosaic_wrapper.py`: Moves mosaicking inside `for` loop.

 <br/><br/>

 
## v4.5.7.0 - 2024-09-13 - [PR#1267](https://github.com/NOAA-OWP/inundation-mapping/pull/1267)

`pyogrio` seems to have a difficulty writing files when all values in a column are null (None or nan). The workaround here is to use `fiona` for writing files where `pyogrio` is explicitly set in geopandas (gpd) by `gpd.options.io_engine = "pyogrio"`.

### Changes
Adds `engine='fiona'` to `.to_file()` in all of the following files
- `data/`: `esri.py`, `nld/levee_download.py`, `usgs/acquire_and_preprocess_3dep_dems.py`, `usgs/rating_curve_get_usgs_curves.py`, `wbd/preprocess_wbd.py`
- `src/`: `derive_headwaters.py`, `derive_level_paths.py`, `edit_points.py`, `filter_catchments_and_add_attributes.py`, `reachID_grid_to_vector_points.py`, `reachID_grid_to_vector_points.py`, `split_flows.py`, `src_adjust_spatial_obs.py`, `src_roughness_optimization.py`, `stream_branches.py`
- `tools/`: `eval_plots.py`, `evaluate_continuity.py`, `generate_nws_lid.py`, `make_boxes_from_bounds.py`, `mosaic_inundation.py`, `rating_curve_comparison.py`, `test_case_by_hydro_id.py`

<br/><br/>


## v4.5.6.1 - 2024-09-13 - [PR#1271](https://github.com/NOAA-OWP/inundation-mapping/pull/1271)

Upgrade for `test_case_by_hydro_id.py` that enables the ability to run on HUCs with differing projections (e.g. Alaska) and adds a logging system.

### Changes

- `tools/test_case_by_hydro_id.py`: Moved the reprojection step to accommodate  multiple input projections and fixed a lot of unnecessary logic. Also added an optional logging system that is activated by the new `-l` flag.

<br/><br/>


## v4.5.6.0 - 2024-08-23 - [PR#1253](https://github.com/NOAA-OWP/inundation-mapping/pull/1253)

Upgrades Python packages and dependencies and fixes backwards incompatibilities with new version of `geopandas`. Major changes include:
- Upgrading `boto3`, `fiona`, `geopandas`, `gval`, `pyarrow`, `pyogrio`, `pyproj`, and `rasterio`
- Removing `aiobotocore` and `aiohttp`

### Changes

- `Dockerfile`: Upgrade GDAL (v3.8.3) and pipenv
- `Pipfile` and `Pipfile.lock`: Upgrade Python packages, add dask-expr, and remove aiohttp
- `src/`
    - `build_stream_traversal.py`, `derive_level_paths.py`, `heal_bridges_osm.py`, `mitigate_branch_outlet_backpool.py`, `split_flows.py`, `stream_branches.py`, `usgs_gage_unit_setup.py`: Fix backwards incompatibilities with new version of `geopandas`.

<br/><br/>

## v4.5.5.1 - 2024-08-16 - [PR#1225](https://github.com/NOAA-OWP/inundation-mapping/pull/1225)

Removes warning when running `heal_bridges_osm.py` by not saving the empty DataFrame.

### Changes

- `src/heal_bridges_osm.py`

<br/><br/>


## v4.5.5.0 - 2024-08-16 - [PR#1247](https://github.com/NOAA-OWP/inundation-mapping/pull/1247)

Updated the gauge crosswalk and SRC adjustment routine to use the ras2fim v2 files. The v2 ras2fim file structure was changed to organize the data by huc8 - one gpkg and csv per huc8. Addresses #1091 

### Changes
- `fim_post_processing.sh`: added new input variables for running the `src_adjust_ras2fim_rating.py`
- `src/bash_variables.env`: renamed and reassigned the ras2fim input variables: `ras2fim_input_dir`, `ras_rating_curve_csv_filename`, `ras_rating_curve_gpkg_filename`
- `src/run_unit_wb.sh`: Added logic to check if huc in process has ras2fim input data to process. If yes - copy the ras2fim cross section point gpkg to the huc run directory.
- `src/src_adjust_ras2fim_rating.py`: Updated code logic to use the huc-specific input files containing the ras2fim rating curve data (previous ras2fim input file contained all hucs in one csv)
- `src/utils/shared_functions.py`: Added function to find huc subdirectories with the same name btw two parent folders

<br/><br/>



## v4.5.4.4 - 2024-08-02 - [PR#1238](https://github.com/NOAA-OWP/inundation-mapping/pull/1238)

Prior to this fix, fim_post_processing.sh took just under 4 hours to reset permissions on all files and folder under the entire run. On closer inspection, it was updating permissions for all HUC folders where were already correct. A few other folders needed to have permission updates added. This will speed that up significantly.

Also, use this opportunity to added a new note to hash_compare.py and fix an annoying duration time showing milliseconds.

### Changes
- `fim_pipeline.sh`: fix duration msgs.
- `fim_post_processing.sh`:  permissions reset fix, a bit of output cleanup and fix duration msgs.
- `src`
    - `bash_functions.env`: update the Calc duration to allow for a msg prefix to be added to the duration calcs. Also adjusted the duration message to show hours as well, previously only min and seconds.
    - `run_by_branch.sh`: fix duration msgs.
    - `run_unit_wb.sh`: fix duration msgs.
    - `src\src_adjust_ras2fim_rating.py`: minor duration display msg change.
- `tools\hash_compare.py`: Added note

 <br/><br/>


## v4.5.4.3 - 2024-08-02 - [PR#1136](https://github.com/NOAA-OWP/inundation-mapping/pull/1136)

Levee-protected areas are associated with levelpaths based on a 1000 m buffer on each side of the levee line. However, not all levees are designed to protect against all associated levelpaths, especially where the levelpath flows through the levee-protected area. Levee-protected areas are unmasked by removing levelpaths from association that don't intersect levees but instead flow around them which allows inundation by these branches.

### Changes

- `src/associate_levelpaths_with_levees.py`: Finds levelpaths that don't intersect levees and removes them from their association with their levee-protected area.

<br/><br/>


## v4.5.4.2 - 2024-08-02 - [PR#1125](https://github.com/NOAA-OWP/inundation-mapping/pull/1125)

This PR focuses on updating the preprocess_bathymetry.py for 3 issues: 1) the capability of preprocessing SurveyJobs that have negative depth values, 2) changing the SurveyDateStamp format, and 3) the capability of including multiple SurveyJobs for one NWM feature-id if needed.

### Changes
`data/bathymetry/preprocess_bathymetry.py`: Addressing 3 issues including, the capability of preprocessing SurveyJobs that have negative depth values, changing the SurveyDateStamp format, and the capability of including multiple SurveyJobs for one NWM feature-id.


<br/><br/>

## v4.5.4.1 - 2024-08-02 - [PR#1185](https://github.com/NOAA-OWP/inundation-mapping/pull/1185)

This PR brings back the `preprocess_ahps_nws.py` code to FIM4 and generates new AHPS benchmark datasets for sites SXRA2 and SKLA2 in Alaska.  The new AHPS benchmark datasets are available on dev1 here: "/dev_fim_share/foss_fim/outputs/ali_ahps_alaska/AHPS_Results_Alaska/19020302/"


To process a new station, follow these steps:

1. Add the name of the new site (s) to the`/data/inputs/ahps_sites/evaluated_ahps_sites.csv` file. 
2. Collect/Download the grid depth dataset, typically available as ESRI gdb.
3. Use arcpy (or ArcGIS pro ) to convert the grid depths (in ESRI gdb) into TIFF files
    - Make sure the TIFF files have crs
    - Store all the TIFF files in a directory called "depth_grid," which should be a sub-folder inside a folder named after the gage code (must be a 5-character code)
4. Run the script as described below. **Note that sites in CONUS and Alaska cannot be mixed in a single run. Separate runs should be done for Alaska sites and CONUS sites.**

Note that for the "SKLA2" site, the downloaded ESRI-GDB grid files had a datum issue. This issue was manually corrected during the conversion of GDB files into TIFF files.

### Additions
- `data/nws/preprocess_ahps_nws.py`  ... retrieved from previous versions of FIM and updated for shapely v2

### Changes
- `tools/tools_shared_functions.py`  ... updated for shapely v2

<br/><br/>

## v4.5.4.0 - 2024-08-02 - [PR#1198](https://github.com/NOAA-OWP/inundation-mapping/pull/1198)

### Changes
- `src/bash_variables.env`: high water threshold and recurrence flows CSV files were updated into new NWM v3 flow files. Also, a new Manning numbers file created from the new NWM v3 dataset was used.
-  `src/src_adjust_ras2fim_rating.py`: 100 year recurrence was removed since it is not included in the new AEP.
-  `src/src_adjust_usgs_rating_trace.py`: 100 year recurrence was removed since it is not included in the new AEP.
-  `tools/rating_curve_comparison.py`: 100 year recurrence was removed since it is not included in the new AEP. Also, the name of recurrence flow CSV file was updated.
-  `tools/composite_inundation.py`
-  `tools/inundate_nation.py`

<br/><br/>

## v4.5.3.1 - 2024-07-24 - [PR#1233](https://github.com/NOAA-OWP/inundation-mapping/pull/1233)

In a PR [1217](https://github.com/NOAA-OWP/inundation-mapping/pull/1217), which is about to be merged, it updates a bunch of python packages. One is numpy. This has triggered a very large amount of on-screen output from a new numpy warning while running `synthesize_test_cases.py`.

### Changes
- `tools\overlapping_inundation.py`: As described

 <br/><br/>
 

## v4.5.3.0 - 2024-07-24 - [PR#1217](https://github.com/NOAA-OWP/inundation-mapping/pull/1217)

This PR rolls up a bunch of other PR's and python packages requests including:
- Issue [1208](https://github.com/NOAA-OWP/inundation-mapping/issues/1208)  Bump OpenJDK from 17.0.8 to 17.0.10 (via updating to JDK 21.0.3)
- PR [1207](https://github.com/NOAA-OWP/inundation-mapping/pull/1207) - Dependabot bump certifi from 2023.7.22 to 2024.7.4
- PR [1192](https://github.com/NOAA-OWP/inundation-mapping/pull/1192) - Dependabot Bump urllib3 from 1.26.18 to 1.26.19
- Updates required from ongoing PR [1206](https://github.com/NOAA-OWP/inundation-mapping/pull/1206) - Probabilistic Flood Inundation Mapping. These updates make it easier for that branch/task to continue forward and staying in sync with dev. This triggered a few other packages that needed to be updated.

Other tasks included are:
- Removing the now heavily obsolete `unit_test` system, including the package `pytest`. This included some changes to the `CONTRIBUTING.md` document.
- Clean of a couple of packages no longer in use: `pluggy` and `iniconfig`
- Removal of a deprecated file named `config/aws_s3_put_fim3_hydrovis_whitelist.lst`
- Removed duration stamps around a few parts in `fim_post_processing.sh`
- Fixes and updates to linting files. e.g. `pyproject.toml`. (line length was not working correctly)

### Changes
- `Dockerfile`, `Pipfile`, `Pipfile.lock`: as describe above for python package changes
- `.gitignore`, `CONTRIBUTING.md`: File changes related to removing the `unit_test` system.
- `fim_post_processing.sh`: noted above.
- `pyproject.toml`: fixes and updates for linting

### Removals
- `unit_tests` folder and all related files under it. Appx 25 to 30 files removed.

<br/><br/>


## v4.5.2.11 - 2024-07-19 - [PR#1222](https://github.com/NOAA-OWP/inundation-mapping/pull/1222)

We are having problems with post processing overall duration taking a long time. This new system captures duration times for each module/section inside fim_post_processing.sh and records it to a file on the output directory. It records it as it progress and will also help us learn if fim_post_processing.sh stopped along the way.

Note: When used in code, we call `Set_log_file_path` shell variable with a file name and path (no validation done at this time).  The each time a person wants to print to screen and console, use the `l_echo` command instead of the native `echo` command. If the log file has not been set, the output will continue to go to screen, just not the log file.

### Changes
- `fim_pipeline.sh`: A couple of minor text output changes.
- `fim_post_processing.sh`:  As described above.
- `src\bash_functions.env`:  New functions and adjustments to support the new log system.

<br/><br/>


## v4.5.2.10 - 2024-07-19 - [PR#1224](https://github.com/NOAA-OWP/inundation-mapping/pull/1224)

Addresses warnings to reduce output messages.

### Changes

- `src/'
    - `adjust_thalweg_lateral.py`: fixes number type
    - `src/delineate_hydros_and_produce_HAND.sh`: removes division by zero warning
    - `getRasterInfoNative.py`: adds `gdal.UseExceptions()`

<br/><br/>


## v4.5.2.9 - 2024-07-19 - [PR#1216](https://github.com/NOAA-OWP/inundation-mapping/pull/1216)

Adds `NO_VALID_CROSSWALKS` to `FIM_exit_codes` which is used when the crosswalk table or output_catchments DataFrame is empty. Removes branches that fail with `NO_VALID_CROSSWALKS`.

### Changes
    - `add_crosswalk.py`: Added `NO_VALID_CROSSWALKS` as exit status when crosswalk or output_catchments is empty
    - `process_branch.sh`: Removed branches that fail with `NO_VALID_CROSSWALKS`
    - `utils/fim_enums.py`: Added `NO_VALID_CROSSWALKS` to `FIM_exit_codes`

<br/><br/>


## v4.5.2.8 - 2024-07-19 - [PR#1219](https://github.com/NOAA-OWP/inundation-mapping/pull/1219)

Changes non-fatal `ERROR` messages to `WARNINGS` to avoid triggering being logged as errors.

### Changes

- `src/`
    - `bathymetric_adjustment.py`: Changes `WARNING` to `ERROR` in Exception
    - `src_roughness_optimization.py`: Changes `ERROR` messages to `WARNING`

<br/><br/>

## v4.5.2.7 - 2024-07-19 - [PR#1220](https://github.com/NOAA-OWP/inundation-mapping/pull/1220)

With this PR we can run post_processing.sh multiple times on a processed batch without any concerns that it may change the hydroTable or src_full_crosswalked files.

### Additions

- `src/update_htable_src.py`

### Changes

-  `config/deny_branch_zero.lst`
-  `config/deny_branches.lst`
-  `fim_post_processing.sh`

<br/><br/>

## v4.5.2.6 - 2024-07-12 - [PR#1184](https://github.com/NOAA-OWP/inundation-mapping/pull/1184)

This PR adds a new script to determine which bridges are inundated by a specific flow. It will assign a risk status to each bridge point based on a specific threshold.

### Additions

- `tools/bridge_inundation.py`

<br/><br/>

## v4.5.2.5 - 2024-07-08 - [PR#1205](https://github.com/NOAA-OWP/inundation-mapping/pull/1205)

Snaps crosswalk from the midpoint of DEM-derived reaches to the nearest point on NWM streams within a threshold of 100 meters. DEM-derived streams that do not locate any NWM streams within 100 meters of their midpoints are removed from the FIM hydrofabric and their catchments are not inundated.

### Changes

- `src/add_crosswalk.py`: Locates nearest NWM stream to midpoint of DEM-derived reaches if within 100 meters. Also fixes a couple of minor bugs. 

<br/><br/>

## v4.5.2.4 - 2024-07-08 - [PR#1204](https://github.com/NOAA-OWP/inundation-mapping/pull/1204)

Bug fix for extending outlets in order to ensure proper flow direction in depression filling algorithm. This PR adds a distance criteria that in order for the end of an outlet stream to be snapped to the wbd_buffered boundary, the end point must be less than 100 meters from the WBD boundary.

Also adds missing argparse arguments so that the script can be run from the command line.

### Changes

- `data`
     - `wbd`
          - `clip_vectors_to_wbd.py`: Adds a 100 meter distance threshold to WBD to snap outlets to buffered WBD.
          - `generate_pre_clip_fim_huc8.py`: Upgrading logging system.
- `src`
     - `bash_variables.env`: Updated pre-clip input path to new pre-clip files.

<br/><br/>

## v4.5.2.3 - 2024-06-14 - [PR#1169](https://github.com/NOAA-OWP/inundation-mapping/pull/1169)

This tool scans all log directory looking for the word "error" (not case-sensitive). This is primary added to help find errors in the post processing logs such as src_optimization folder (and others).

### Changes

- `fim_post_processing.sh`: as described
- `src\mitigate_branch_outlet_backpool.py`: Has the word error in text which fills up the error scan logs.

<br/><br/>

## v4.5.2.2 - 2024-06-14 - [PR#1183](https://github.com/NOAA-OWP/inundation-mapping/pull/1183)

Upgrades whitebox from v2.3.1 to 2.3.4. Whitebox was upgraded by `pip install -U whitebox` and then `pipenv lock` to update the `Pipfile`.

### Changes

- `Dockerfile`: Removed whitebox hack
- `Pipfile` and `Pipfile.lock`: Upgraded whitebox to v2.3.4.

<br/><br/>

## v4.5.2.1 - 2024-05-21 - [PR#1172](https://github.com/NOAA-OWP/inundation-mapping/pull/1172)

Removes loading of `apache-arrow` repository from the Dockerfile where it was causing a GPG key error during `docker build`.

A number of python packages were updated in this PR. You will need to build a new Docker image for this release.

### Changes

- Dockerfile: Adds a line remove the loading of apache-arrow during `apt-get update`.

<br/><br/>


## v4.5.2.0 - 2024-05-20 - [PR#1166](https://github.com/NOAA-OWP/inundation-mapping/pull/1166)

The main goal of this PR is to create bridge point data that be used as a service in HydroVIS. Since every branch processes bridges separately, it's possible to inundate a bridge from more than just the feature_id it crosses. To reflect this, the `osm_bridge_centroids.gpkg` now found in HUC directories will have coincident points - one that is inundated from the reach it crosses and the other a backwater-influenced point indicated by the `is_backwater` field.

### Changes

- ` src/`
    - `aggregate_by_huc.py`: Added the aggregation steps for bridge centroids; aggregation includes using SRCs to lookup flow values for each bridge, filtering out coincident points that have the same assigned feature_ids and higher overtopping flow, and assigning select points as backwater-influenced.
    - ` delineate_hydros_and_produce_HAND.sh`: Moved the bridge healing to after the crosswalk so that the centroids can use the crosswalked catchments for feature_id and flow lookups.
    -  `heal_bridges_osm.py`: Optimized the bridge healing so that it doesn't have to write out an intermediate raster; exports bridge centroids and spatial joins them to catchments; added functions for SRC flow lookups used in `aggregate_by_huc.py`.
- ` fim_post_processing.sh`: Added a bridge flag input for `aggregate_by_huc.py`.
- `data/bridges/pull_osm_bridges.py`: Removed the saving of a midpoint geopackage.
- `config/deny_branch_zero.lst` & `deny_branches.lst`: Added `#osm_bridge_centroids_{}.gpkg` to the deny lists.

<br/><br/>


## v4.5.1.3 - 2024-05-17 - [PR#1170](https://github.com/NOAA-OWP/inundation-mapping/pull/1170)

This hotfix addresses the issue #1162 by explicitly using 'fiona' engine for reading gpkg files with Boolean dtype. This is applicable only for `usgs_gages.gpkg` and `usgs_subset_gages.gpkg` files. 

### Changes
- `src/usgs_gage_unit_setup.py`  ... changed only two lines for fiona engine
- `src/usgs_gage_crosswalk.py` ...  changed only one line for fiona engine + two small changes to use `self.branch_id` for the correct log report
- `tools/rating_curve_comparison.py`...  changed only one line for fiona engine

<br/><br/>

## v4.5.1.2 - 2024-05-17 - [PR#1135](https://github.com/NOAA-OWP/inundation-mapping/pull/1135)

Updates USGS gage processing to use the correct projection (determined by whether the HUC is in Alaska or not).

### Changes
- `src/run_by_branch.sh`: Added `huc_CRS` as an input argument for `usgs_gage_crosswalk.py`
- `src/run_unit_wb.sh`: Added `huc_CRS` as an input argument for `usgs_gage_unit_setup.py` and `usgs_gage_crosswalk.py`
- `src/usgs_gage_crosswalk.py`: Added `huc_CRS` as an input argument for the `run_crosswalk()` function and added re-projection steps wherever new data is being read in so that the files are able to be properly merged.
- `src/usgs_gage_unit_setup.py`: Added `huc_CRS` as an input argument for the `Gage2Branch()` crosswalking class.

<br/><br/>

## v4.5.1.1 - 2024-05-17 - [PR#1094](https://github.com/NOAA-OWP/inundation-mapping/pull/1094)

Extends flows (i.e., discharge) to stream segments missing from NWS and USGS validation flow files. The levelpath associated with existing flows in the AHPS domain is identified, and any stream segments of the levelpath in the domain missing from the flow file are added to the flow file by assigning the existing flow (this is a constant value regardless of other tributaries including other levelpaths in the domain). Stream segments not on the levelpath are dropped from the flow file, including tributary flows. The original flow file is saved along with the output with an appended `.bak`.

### Additions

- `data/extend_benchmark_flows.py`: Adds missing flows to NWS or USGS benchmark flow files and removes flows from tributaries. The original flow file is saved with an appended `.bak`.

### Changes

- `tools/tools_shared_variables.py`: Removed corrected flow files from `BAD_SITES` list.

<br/><br/>

## v4.5.1.0 - 2024-05-17 - [PR#1156](https://github.com/NOAA-OWP/inundation-mapping/pull/1156)

This focuses on removing hydro-conditioning artifacts by subtracting the thalweg DEM from HAND REM and adding back the original DEM. Also, a new tool was created to test this feature over multiple HUCs

### Additions
- `tools/analyze_for_missing_FIM_cells.py`: A new script `analyze_for_missing_FIM_cells.py` was added to test and analyze healed HAND for hydro-conditioning artifacts FIM. 

### Changes
- `src/delineate_hydros_and_produce_HAND.sh`: Removing hydro-conditioning artifacts from HAND REM.
- `config/params_template.env`: Creating an option to include/exclude healed HAND from FIM pipeline.

<br/><br/>

## v4.5.0.2 - 2024-05-17 - [PR#1159](https://github.com/NOAA-OWP/inundation-mapping/pull/1159)

This PR addresses issue #1132 and include the following changes on `tools/generate_nws_lid.py` for updating `nws_lid.gpkg` dataset.

In this revised version, stations only from these two groups are retrieved:
- lid stations with `rfc_forecast_point= True` 
- lid stations in `/data/inputs/ahp_sites/evaluated_ahps_sites.csv`

The lid stations in AK (Alaska), HI, and PR, with above two criteria have also been selected, as shown in the map below. In the previous version of the code, **all of lid stations** in PR and HI (regardless of meeting above two criteria), were also being retrieved. I have updated this version to exclude such stations. 

Also, In this revised version, I've eliminated the code sections that previously generated the "is_headwater" and "is_colocated" columns, which are not needed in FIM4. Therefore, in this updated version, these columns are no longer present. 

Similar to 'usgs_gages.gpkg' dataset, all lid stations, including those in Alaska, are stored in a single gpkg file (`nws_lid.gpkg`) with EPSG=5070. The Alaska stations can be identified using their HUC8 numbers (beginning with '19'). 

### Changes
- tools/generate_nws_lid.py

<br/><br/>


## v4.5.0.1 - 2024-05-09 - [PR#1150](https://github.com/NOAA-OWP/inundation-mapping/pull/1150)

Fixes two bugs discovered in v4.5.0.0:
1. `echo` missing in bash command
2. raster resolution of `dem_meters.tif` has now been explicitly set in `gdalwarp`.

### Changes

- `src/`
    - `add_crosswalk.py`: fixed stream order if max > `max_order`
    - `bash_variables.env`: added `res` environment variable for default raster cell size
    - `delineate_hydros_and_produce_HAND.sh`: added missing `echo`
    - `heal_bridges_osm.py`: fixed raster resolution and number of rows/columns
    - `run_unit_wb.sh`: added `-tr` to gdalwarp when generating `dem_meters.tif`; removed extraneous `Tcount`

<br/><br/>

## v4.5.0.0 - 2024-05-06 - [PR#1122](https://github.com/NOAA-OWP/inundation-mapping/pull/1122)

This PR includes 2 scripts to add Open Street Map bridge data into the HAND process: a script that pulls data from OSM and a script that heals those bridges in the HAND grids. Both scripts should be run as part of a pre-processing step for FIM runs. They only need to be run if we think OSM data has changed a lot or for any new FIM versions.

A new docker image is also required for `pull_osm_bridges.py` (acquire and preprocess) script.

### Additions
- `data/bridges/pull_osm_bridges.py`: First pre-processing script that pulls OSM data and saves bridge lines out as separate shapefiles by HUC8 to a specified location
- `src/heal_bridges_osm.py`: Second pre-processing script that uses the pre-saved OSM bridge lines and heals max HAND values across those bridge lines. Healed HAND grids are saved to a specified location.

### Changes
- `Pipfile`, `Pipfile.lock`: Adjusted files to add new python package to docker image.
- `data`
    - `clip_vectors_to_wdbd.py`: Updated to pre-clip new bridge data. Logging upgraded.
    - `generate_pre_clip_fim_huc8.py`: Updated to pre-clip new bridge data. Logging added and a system for multi-process logging.
- `src`
    - `delineate_hydros_and_produce_HAND.sh`: add python call to run `heal_bridges_osm.py` after hydraulic properties are calculated.
    - `bash_variables.env`: Added new variable for OSM bridges and adjusted pre-clip output date
    - `utils`
        - `shared_functions.py`: removed function no longer in use.
        - `shared_variables.py`: removed variables no longer in use.
  
<br/><br/>

## v4.4.16.0 - 2024-05-06 - [PR#1121](https://github.com/NOAA-OWP/inundation-mapping/pull/1121)

Some NWM streams, particularly in coastal areas, fail to reach the edge of the DEM resulting in reverse flow. This issue was resolved by clipping the ocean mask from the buffered WBD and DEM, and any remaining streams that didn't have outlets reaching the edge of the buffered WBD boundary were extended by snapping the end to the nearest point on the buffered WBD.

### Changes

- `data/wbd/clip_vectors_to_wbd.py`: Clips `landsea` ocean mask from the buffered WBD and adds a function to extend outlet streams to the buffered WBD

<br/><br/>


- `data/wbd/clip_vectors_to_wbd.py`: Updated multi-processing and added more logging.

<br/><br/>

## v4.4.15.4 - 2024-05-06 - [PR#1115](https://github.com/NOAA-OWP/inundation-mapping/pull/1115)

This PR addresses issue #1040 and includes the following updates:
- Upgraded to WRDS API version 3 and ensured schema compatibility of new USGS gages data.
- Expanded data retrieval to include Alaska gages alongside CONUS gages. 
- Enables retrieving SRC data for individual USGS gages, removing the necessity of using 'all' for the '-l' flag in rating_curve_get_usgs_curves.py." 


### Changes
 - `tools/tools_shared_functions.py`   
    -  Improved the stability of API calls.
    - Removed the exclusion of Alaska gages from USGS gages metadata (`usgs_gages.gpkg` output), preserving Alaska gages in the metadata.  
- `rating_curve_get_usgs_curves.py` 
    - Removed the exclusion of Alaska gages when retrieving SRC values.
    - Enabled retrieving SRC data for individual USGS gages.
- Moved the script `rating_curve_get_usgs_curves.py` from `tools` folder into `data/usgs`.

<br/><br/>

## v4.4.15.3 - 2024-05-06 - [PR#1128](https://github.com/NOAA-OWP/inundation-mapping/pull/1128)

Fixes a KeyError in `src/mitigate_branch_outlet_backpool.py`.

### Changes

`src/mitigate_branch_outlet_backpool.py`: Addresses case where `catchments_df['outlier']` are all False.

<br/><br/>

## v4.4.15.2 - 2024-05-06 - [PR#1133](https://github.com/NOAA-OWP/inundation-mapping/pull/1133)

Bug fix for error when reading the subfolders of a directory using `listdir()` where files exist that start with an 8-digit number that are later interpreted as directories.

### Changes

The following files were modified to use `listdir()` to read only directories instead of both directories and files:
- `src/`
    - `bathy_src_adjust_topwidth.py`, `identify_src_bankfull.py`, `subdiv_chan_obank_src.py`, `utils/shared_functions.py`
- `tools/vary_mannings_n_composite.py`


<br/><br/>


## v4.4.15.1 - 2024-05-06 - [PR#1081](https://github.com/NOAA-OWP/inundation-mapping/pull/1038)

This hotfix address a bug within the SRC adjustment routine to filter out USGS gauge locations that were conflated to lakeid reaches. These fatal errors were preventing `fim_post_processing.sh` from completing. There are also new try except blocks to handle potential errors when opening/writing SRC adjustment attributes to the catchment gpkg (unknown issues with collisions or corrupt gpkg files). Closes #1137 

### Changes

- `src/src_adjust_usgs_rating_trace.py`: Added filter for processing valid hydroids that meet criteria (i.e non-lakes) and more robust logging.
- `src/src_roughness_optimization.py`: Added data checks and logging to ensure input calibration data files contains necessary attributes. Also included a new try/except block to trap and log issues with file collisions or corrupt catchment gpkg read/write.

<br/><br/>

## v4.4.15.0 - 2024-04-17 - [PR#1081](https://github.com/NOAA-OWP/inundation-mapping/pull/1081)

This enhancement includes changes to the SRC calibration routine that uses the USGS published rating curve database. The modifications attempt to mimic the technique used in the stage-based CatFIM where the USGS WSE/flow is propagated upstream and downstream of the gauge location. This closes #892 

### Additions
`src/src_adjust_usgs_rating_trace.py`: updated SRC calibration routine to include the a new upstream/downstream tracing routine. The WSE(HAND stage) and flow targets obtained from the USGS rating curve are now applied to all hydroids within 8km (~5 miles) of the gauge location.  

### Changes
`fim_post_processing.sh`: using the new `src_adjust_usgs_rating_trace.py` in place of the `src_adjust_usgs_rating.py`
`src/src_roughness_optimization.py`: minor changes to facilitate new calibration input (reset index)
`src/utils/shared_variables.py`: added `USGS_CALB_TRACE_DIST` as the trace distance variable

### Removals
`src/src_adjust_usgs_rating.py`: deprecated (replaced with the new `src_adjust_usgs_rating_trace.py`)

<br/><br/>


## v4.4.14.1 - 2024-04-17 - [PR#1103](https://github.com/NOAA-OWP/inundation-mapping/pull/1103)

Adds checks for intermediate files produced by Whitebox in the AGREE process (`src/agreedem.py`). Without these checks, if Whitebox fails to produce an output, no error is generated until much later in the `src/delineate_hydros_and_produce_HAND.sh` processing chain which makes troubleshooting difficult.

### Changes

- `src/agreedem.py`: Added checks to verify existence of intermediate files before continuing

<br/><br/>

## v4.4.14.0 - 2024-04-17 - [PR#1106](https://github.com/NOAA-OWP/inundation-mapping/pull/10106)

Updates the FIM pipeline so it can process HUCs in southern Alaska. Running FIM in southern Alaska requires that a different CRS and a few different files be used. Additionally, some of the Alaska HUCs displayed an issue where the input stream density was too high, so this update introduces some logic to adjust the threshold of stream orders to exclude based on whether an Alaska HUC is listed as high or medium-high stream density. This update intriduces new Alaska-specific inputs, which are listed in the PR. 

### Changes
- `data/wbd/generate_pre_clip_fim_huc8.py`: Adjusted comment.
- `src/bash_variables.env`: Changed pre-clip HUC 8 directory to be a folder with both Alaska and CONUS HUCs.
- `src/check_huc_inputs.py`: Changed the `included_huc_list` variable to refer to a HUC list that includes Alaska.
- `src/derive_level_paths.py`: Add in logic to exclude different stream orders based on whether the HUC falls into the high or medium-high density HUC lists.
- `src/run_by_branch.sh`: Add in logic to check whether the HUC is in Alaska or not and to use the correct CRS accordingly.
- `src/run_unit_wb.sh`: Add in logic to check whether the HUC is in Alaska or not and to use the correct CRS and DEM domain filename accordingly.
- `src/utils/shared_variables.py`: Add the Alaska CRS, a list of high stream density HUCs, and a list of medium-high stream density HUCs.

<br/><br/>


## v4.4.13.3 - 2024-04-15 - [PR#1114](https://github.com/NOAA-OWP/inundation-mapping/pull/1114)

Two recent dependabot PR's came in, one for upgrading the `pillow` package and the other for upgrading idna. Both have been adjusted in this PR. 
In this PR, we also moved `openpyxl` package, which was part of an independent dockerfile, Pipfile and Pipefile.lock in the "dev" directory. This is now merged into the parent standard docker image.

Covers [PR 1111](https://github.com/NOAA-OWP/inundation-mapping/pull/1111) and 
Covers [PR 1119](https://github.com/NOAA-OWP/inundation-mapping/pull/1119)

A small update to the README.md was also updated for an unrelated topic (about AWS S3 credentials).

### Changes
- `Pipfile / Pipefile.lock`: As described above.
- `data/ble/ble_benchmark/README.md`: Updated notes to remove talking the specific ble docker image.

### Removals
- `data/ble/ble_benchmark`
   - `Dockerfile`: removed in favor the parent root Docker files.
   - `Pipfile`: removed in favor the parent root Docker files.
   - `Pipfile.lock` : removed in favor the parent root Docker files.

<br/><br/>

## v4.4.13.2 - 2024-04-04 - [PR#1110](https://github.com/NOAA-OWP/inundation-mapping/pull/1110)

This PR reflects upgrades for openJDK from 17.0.8 to something higher, minimum of 17.0.9. After some research, we can not upgrade all the way to the latest openJDK but can jump up to 19.0.  This limitation is related to version of our base docker image.  openJDK was identified as requiring an upgrade by a system wide security scan.

The "black" packages is also be upgraded from 23.7.0 to 24.3.

**NOTE: the update of "black" has change the rules slightly for formatting. This is why you see a bunch of files being changed but only for the formatting changes.**

### Files Change
- `Dockerfile`, `Pipfile`, `Pipefile.lock`
- `pre-commit-config.yaml` is also has Black upgraded for CI/CD tests for linting during GIT check ins.
- `many files`:
     - 19 files have had minor formatting changes related to the upgrade in the "black" package.

<br/><br/>


## v4.4.13.1 - 2024-03-11 - [PR#1086](https://github.com/NOAA-OWP/inundation-mapping/pull/1086)

Fixes bug where levee-protected areas were not being masked from branch 0 DEMs.

### Changes

`src/mask_dem.py`: Corrects indentation preventing masked branch 0 from overwriting existing DEM.

<br/><br/>

## v4.4.13.0 - 2024-03-11 - [PR#1006](https://github.com/NOAA-OWP/inundation-mapping/pull/1006)

Adds a new module that mitigates the branch outlet backpool error. In some HUCs, an overly-large catchment appears at the outlet of the branch (as in issue #985) which causes an artificially large amount of water to get routed to the smaller stream instead of the main stem. This issue is mitigated by trimming the levelpath just above the outlet and removing the offending pixel catchment from the pixel catchments and catchment reaches files. 

The branch outlet backpool issue is identified based on two criteria: 
  1. There is a pixel catchment that is abnormally large (more than two standard deviations above the mean.)
  2. The abnormally-large pixel catchment occurs at the outlet of the levelpath.

If both criteria are met for a branch, then the issue is mitigated by trimming the flowline to the third-to-last point.

### Additions

- `src/mitigate_branch_outlet_backpool.py`: Detects and mitigates the branch outlet backpool error. If both branch outlet backpool criteria are met, the snapped point is set to be the penultimate vertex and then the flowline is trimmed to that point (instead of the last point). Trims the `gw_catchments_pixels_<id>.tif` and `gw_catchments_reaches_<id>.tif` rasters by using `gdal_polygonize.py` to polygonize the `gw_pixel_catchments_<id>.tif` file, creating a mask that excludes the problematic pixel catchment, and then using that mask to trim the pixel catchment and catchment reaches rasters.

### Changes

- `src/delineate_hydros_and_produce_HAND.sh`: Adds the `mitigate_branch_outlet_backpool.py` module to run after the  `Gage Watershed for Pixels` step. 
- `src/split_flows.py`: Improves documentation and readability.

<br/><br/>

## v4.4.12.0 - 2024-03-11 - [PR#1078](https://github.com/NOAA-OWP/inundation-mapping/pull/1078)

Resolves issue #1033 by adding Alaska-specific data to the FIM input folders and updating the pre-clip vector process to use the proper data and CRS when an Alaska HUC is detected. The `-wbd` flag was removed from the optional arguments of `generate_pre_clip_fim_huc8`. The WBD file path will now only be sourced from the `bash_variables.env` file. The `bash_variables.env` file has been updated to include the new Alaska-specific FIM input files.

### Changes

- `/data/wbd/`
    - `clip_vectors_to_wbd.py`: Replaced all CRS inputs with the `huc_CRS` variable, which is input based on whether the HUC is Alaska or CONUS. Previously, the default FIM projection was automatically assigned as the CRS (which had been retrieved from `utils.shared_variables`).

    - `generate_pre_clip_fim_huc8.py`:
        - Added Alaska projection and links to the new Alaska data file paths that were added to `bash_variables.env`.
        - Removed the `wbd` argument from the `pre_clip_hucs_from_wbd` function and made it so that the code gets the WBD path from `bash_variables.env`.
        - Added logic to check whether the HUC is in Alaska and, if so, use the Alaska-specific HUC and input file paths.
        - Cleaned up the spelling and formatting of some comments
- `/src/`
    - `bash_variables.env`: Added the Alaska-specific projection (EPSG:3338) and file paths for Alaska-specific data (see data changelog for list of new input data)

<br/><br/>

## v4.4.11.1 - 2024-03-08 - [PR#1080](https://github.com/NOAA-OWP/inundation-mapping/pull/1080)

Fixes bug in bathymetric adjustment where `mask` is used with `geopandas.read_file`. The solution is to force `read_file` to use `fiona` instead of `pyogrio`.

### Changes

`src/bathymetric_adjustment.py`: Use `engine=fiona` instead of default `pyogrio` to use `mask=` with `geopandas.read_file`

<br/><br/>

## v4.4.11.0 - 2024-02-16 - [PR#1077](https://github.com/NOAA-OWP/inundation-mapping/pull/1077)

Replace `fiona` with `pyogrio` to improve I/O speed. `geopandas` will use `pyogrio` by default starting with version 1.0. `pyarrow` was also added as an environment variable to further speedup I/O. As a result of the changes in this PR, `fim_pipeline.sh` runs approximately 10% faster.

### Changes

- `Pipfile`: Upgraded `geopandas` from v0.12.2 to v0.14.3, added `pyogrio`, and fixed version of `pyflwdir`.
- `src/bash_variables.env`: Added environment variable for `pyogrio` to use `pyarrow`
- To all of the following files: Added `pyogrio` and `pyarrow`
    - `data/`
        - `bathymetry/preprocess_bathymetry.py`, `ble/ble_benchmark/create_flow_forecast_file.py`, `esri.py`, `nld/levee_download.py`, `usgs/acquire_and_preprocess_3dep_dems.py`, `wbd/clip_vectors_to_wbd.py`, `wbd/preprocess_wbd.py`, `write_parquet_from_calib_pts.py`
    - `src/`
        - `add_crosswalk.py`, `associate_levelpaths_with_levees.py`, `bathy_rc_adjust.py`, `bathymetric_adjustment.py`, `buffer_stream_branches.py`, `build_stream_traversal.py`, `crosswalk_nwm_demDerived.py`, `derive_headwaters.py`, `derive_level_paths.py`, `edit_points.py`, `filter_catchments_and_add_attributes.py`, `finalize_srcs.py`, `make_stages_and_catchlist.py`, `mask_dem.py`, `reachID_grid_to_vector_points.py`, `split_flows.py`, `src_adjust_spatial_obs.py`, `stream_branches.py`, `subset_catch_list_by_branch_id.py`, `usgs_gage_crosswalk.py`, `usgs_gage_unit_setup.py`, `utils/shared_functions.py`
    - `tools/`
        - `adjust_rc_with_feedback.py`, `check_deep_flooding.py`, `create_flow_forecast_file.py`, `eval_plots.py`, `evaluate_continuity.py`, `evaluate_crosswalk.py`, `fimr_to_benchmark.py`, `find_max_catchment_breadth.py`, `generate_categorical_fim.py`, `generate_categorical_fim_flows.py`, `generate_categorical_fim_mapping.py`, `generate_nws_lid.py`, `hash_compare.py`, `inundate_events.py`, `inundation.py`, `make_boxes_from_bounds.py`, `mosaic_inundation.py`, `overlapping_inundation.py`, `rating_curve_comparison.py`, `rating_curve_get_usgs_curves.py`, `test_case_by_hydro_id.py`, `tools_shared_functions.py`
        
<br/><br/>

## v4.4.10.1 - 2024-02-16 - [PR#1075](https://github.com/NOAA-OWP/inundation-mapping/pull/1075)

We recently added code to fim_pre_processing.sh that checks the CPU count. Earlier this test was being done in post-processing and was killing a pipeline that had already been running for a while.

Fix:
- Removed the CPU test from pre-processing. This puts us back to it possibly failing in post-processing but we have to leave it for now. 
- Exit status codes (non 0) are now returned in pre-processing and post-processing when an error has occurred.

Tested that the a non zero return exit from pre-processing shuts down the AWS step functions.

### Changes
- `fim_pre_processing.sh`: added non zero exit codes when in error, plus removed CPU test
- `fim_post_processing.sh`:  added non zero exit codes when in error

<br/><br/>

## v4.4.10.0 - 2024-02-02 - [PR#1054](https://github.com/NOAA-OWP/inundation-mapping/pull/1054)

Recent testing exposed a bug with the `acquire_and_preprocess_3dep_dems.py` script. It lost the ability to be re-run and look for files that were unsuccessful earlier attempts and try them again. It may have been lost due to confusion of the word "retry". Now "retry" means restart the entire run. A new flag called "repair"  has been added meaning fix what failed earlier.  This is a key feature it is common for communication failures when calling USGS to download DEMs.  And with some runs taking many hours, this feature becomes important.

Also used the opportunity to fix a couple of other minor issues:
1) Reduce log output
2) Add a test for ensuring the user does not submit job numbers (num of cpu requests) to exceed the system max cpus. This test exists in a number of places in the code but way later in the processing stack after alot of processing has been done. Now it is done at the start of the fim pipeline stack.
3) remove arguments for "isaws" which is no longer in use and has not been for a while.
4) quick upgrade to the tracker log that keeps track of duration of each unit being processed.

### Changes

- `data\usgs\`
    - `acquire_and_preprocess_3dep_dems.py`: Re-add a feature which allowed for restarting and redo missing outputs or partial outputs. System now named as a "repair" system.
- `fim_pipeline.sh`:  remove the parallel `--eta` flag to reduce logging. It was not needed, also removed "isaws" flag.
- `fim_pre_processing.sh`: Added validation tests for maximum CPU requests (job numbers)
- `fim_post_processing.sh`: Added a permissions updated as output folders were being locked due to permissions.
- `fim_process_unit_wb.sh`: Fixed a bug with output folders being locked due to permissions, but it was not recursive.
- `src`
    - `bash_functions.sh`: Added function so the unit timing logs would also have a time in percentage so it can easily be used to calculate averages.
    - `delineate_hydros_and_produce_HAND.sh`: Removed some unnecessary logging. Changed a few gdal calls to be less verbose.
    - `derive_level_paths.py`: Changed verbose to false to reduce  unnecessary logging.
    - `run_by_branch.sh`: Removed some unnecessary logging. Added a duration system so we know how long the branch took to process.
    - `run_unit_by_wb.sh`: Removed some unnecessary logging. Changed a few gdal calls to be less verbose.
    - `split_flows.py`: Removed progress bar which was unnecessary and was adding to logging.
  
<br/><br/>

## v4.4.9.2 - 2024-02-02 - [PR#1066](https://github.com/NOAA-OWP/inundation-mapping/pull/1066)

Adds an index to the aggregated `crosswalk_table.csv`. The index is a consecutive integer that starts at 1. Columns have been reordered, renamed, and sorted.

### Changes

`tools/combine_crosswalk_tables.py`: Adds index and sorts and renames columns

<br/><br/>

## v4.4.9.1 - 2024-02-02 - [PR#1073](https://github.com/NOAA-OWP/inundation-mapping/pull/1073)

Dependabot requested two fixes. One for an upgrade to pillow [#1068](https://github.com/NOAA-OWP/inundation-mapping/pull/1068) and the other for juypterlab #[1067 ](https://github.com/NOAA-OWP/inundation-mapping/pull/1067)

### Changes

- `src`
    - `Pipfile` and `Pipfile.lock`: Updated some packages.
    
<br/><br/>

## v4.4.9.0 - 2024-01-12 - [PR#1058](https://github.com/NOAA-OWP/inundation-mapping/pull/1058)

Upgrades base Docker image to GDAL v3.8.0. In order to upgrade past GDAL v.3.4.3 (see #1029), TauDEM's `aread8` was replaced with a module from the `pyflwdir` Python package.

### Additions

- `src/accumulate_headwaters.py`: Uses `pyflwdir` to accumulate headwaters and threshold and create stream pixels.

### Changes

- `Dockerfile`: Upgrade GDAL from v.3.4.3 to v.3.8.0; remove JDK 17 and TauDEM `aread8` and `threshold`.
- `Pipfile` and `Pipfile.lock`: Add `pyflwdir`, `pycryptodomex` and upgrade Python version.
- `src/delineate_hydros_and_produce_HAND.sh`: Add `src/accumulate_headwaters.py` and remove TauDEM `aread8` and `threshold`

<br/><br/>

## v4.4.8.4 - 2024-01-12 - [PR#1061](https://github.com/NOAA-OWP/inundation-mapping/pull/1061)

Adds a post-processing tool to compare crosswalked (conflated) `feature_id`s between NWM stream network to DEM-derived reaches. The tool is run if the `-x` flag is added to `fim_pipeline.sh`. Results are computed for branch 0 and saved in a summary file in the HUC output folder.

### Additions

- `tools/evaluate_crosswalk.py`: evaluates crosswalk accuracy using two methods:
    - intersections: the number of intersections between streamlines
    - network (or tree): compares the feature_ids of the immediate upstream segments

### Changes

- `Dockerfile`: added `toolsDir` environment variable
- `fim_pipeline.sh`: added `-x` flag to run crosswalk evaluation tool
- `fim_post_processing.sh`: changed hardcoded `/foss_fim/tools` to `toolsDir` environment variable
- `fim_pre_processing.sh`: added `evaluateCrosswalk` environment variable
- `src/`
    - `add_crosswalk.py`: fix bug
    - `delineate_hydros_and_produce_HAND.sh`: added a call to `verify_crosswalk.py` if evaluateCrosswalk is True.

<br/><br/>

## v4.4.8.3 - 2024-01-05 - [PR#1059](https://github.com/NOAA-OWP/inundation-mapping/pull/1059)

Fixes erroneous branch inundation in levee-protected areas.

Levees disrupt the natural hydrology and can create large catchments that contain low-lying areas in levee-protected areas that are subject to being inundated in the REM (HAND) grid. However, these low-lying areas are hydrologically disconnected from the stream associated with the catchment and can be erroneously inundated. Branch inundation in levee-protected areas is now confined to the catchment for the levelpath.

### Changes

- `src/`
    - `delineate_hydros_and_produce_HAND.sh`: Adds input argument for catchments.
    - `mask_dem.py`: Adds DEM masking for areas of levee-protected areas that are not in the levelpath catchment.

<br/><br/>


## v4.4.8.2 - 2023-12-12 - [PR#1052](https://github.com/NOAA-OWP/inundation-mapping/pull/1052)

The alpha test for v4.4.8.1 came back with a large degradation in skill and we noticed that the global manning's roughness file was changed in v4.4.7.1 - likely in error.

### Changes

- `src`/`bash_variables.env`: changed the global roughness file to `${inputsDir}/rating_curve/variable_roughness/mannings_global_06_12.csv`

<br/><br/>

## v4.4.8.1 - 2023-12-08 - [PR#1047](https://github.com/NOAA-OWP/inundation-mapping/pull/1047)

Upgrades JDK to v.17.0.9 in Docker image to address security vulnerabilities.

### Changes

- `Dockerfile`: Upgrades JDK to v.17.

<br/><br/>

## v4.4.8.0 - 2023-12-08 - [PR#1045](https://github.com/NOAA-OWP/inundation-mapping/pull/1045)

In order to avoid file system collisions on AWS, and keep the reads/writes from the same file on disk to a minimum, three files (`HUC6_dem_domain.gpkg`, `nws_lid.gpkg`, `reformat_ras_rating_curve_points_rel_101.gpkg`, & `usgs_gages.gpkg`) are now copied from disk into a scratch directory (temporary working directory), and removed after processing steps are completed.

### Changes

- `config`/`deny_unit.lst`: Add files to remove list - repetitive copies needed for processing step (`run_unit_wb.sh`)
- `src`
    - `bash_variables.env`: Add a new variable for the ras rating curve filename. It will be easier to track the filename in the `.env`, and pull into `run_unit_wb.sh`, rather than hardcode it.
    - `run_unit_wb.sh`: Copy files and update references from `$inputsDir` to `$tempHucDataDir`.

<br/><br/>

## v4.4.7.2 - 2023-12-08 - [PR#1026](https://github.com/NOAA-OWP/inundation-mapping/pull/1026)

A couple of directly related issues were fixed in this PR.
The initial problem came from Issue #[1025](https://github.com/NOAA-OWP/inundation-mapping/issues/1025) which was about a pathing issue for the outputs directory. In testing that fix, it exposed a few other pathing and file cleanup issues which are now fixed. We also added more console output to help view variables and pathing.

### Changes

- `config`/`params_template.env`:  Updated for a newer mannings global file. Changed and tested by Ryan Spies.
- `tools`
    - `inundate_mosiac_wrapper.py`:  Took out a misleading and non-required print statement.
    - `inundate_nation.py`: As mentioned above.

<br/><br/>

## v4.4.7.1 - 2023-12-01 - [PR#1036](https://github.com/NOAA-OWP/inundation-mapping/pull/1036)

Quick update to match incoming ras2fim calibration output files being feed into FIM was the initial change.

There is no FIM issue card for this, but this is related to a ras2fim [PR #205](https://github.com/NOAA-OWP/ras2fim/pull/205) which also made changes to ensure compatibility. New copies of both the `reformat_ras_rating_curve_table_rel_101.csv` and `reformat_ras_rating_curve_points_rel_101.gpkg` were generated from ras2fim but retained the version of `rel_101`.

Originally, was planning to update just the two locations for newer versions of the two `reformat_ras_rating_surve...` files. Both had been update to recognize the ras2release version rel_101.

In the process of doing that, we took the opportunity to move all inputs files from params_template.env and put them into bash_variables.env as per precedence set recently.

### Changes

- `config`/`params_template.env`: moved input variables into `src/bash_variables.env`
- `src`
    - `bash_variablles.env`: Added all input variables from `params_template.env` to here and added one new one from `run_unit_wb.sh` for ras_rating_curve_points_gpkg.
    - `run_unit_wb.sh`:   Updated an input param to the usgs_gage_unit_setup.py file to point the -ras param to the updated rel_101 value now in the `src/bash_variables.env`.
    - `usgs_gage_unit_setup.py`:  Changed to drop a column no longer going to be coming from ras2fim calibration files.

<br/><br/>

## v4.4.7.0 - 2023-11-13 - [PR#1030](https://github.com/NOAA-OWP/inundation-mapping/pull/1030)

This PR introduces the `.github/workflows/lint_and_format.yaml` file which serves as the first step in developing a Continuous Integration pipeline for this repository. 
The `flake8-pyproject` dependency is now used, as it works out of the box with the `pre-commit` GitHub Action in the GitHub Hosted Runner environment.
In switching to this package, a couple of `E721` errors appeared. Modifications were made to the appropriate files to resolve the `flake8` `E721` errors.
Also, updates to the `unit_tests` were necessary since Branch IDs have changed with the latest code.  

A small fix was also included where `src_adjust_ras2fim_rating.py` which sometimes fails with an encoding error when the ras2fim csv sometimes is created or adjsuted in windows.

### Changes
- `.pre-commit-config.yaml`: use `flake8-pyproject` package instead of `pyproject-flake8`.
- `Pipfile` and `Pipfile.lock`: updated to use `flake8-pyproject` package instead of `pyproject-flake8`, upgrade `pyarrow` version.
- `data`
    - `/wbd/generate_pre_clip_fim_huc8.py`: Add space between (-) operator line 134.
    - `write_parquet_from_calib_pts.py`: Add space between (-) operator line 234.
- `src`
    - `check_huc_inputs.py`: Change `== string` to `is str`, remove `import string`
    - `src_adjust_ras2fim_rating.py`: Fixed encoding error.
- `tools`
    - `eval_plots.py`: Add space after comma in lines 207 & 208
    - `generate_categorical_fim_mapping.py`: Use `is` instead of `==`, line 315
    - `hash_compare.py`: Add space after comma, line 153.
    - `inundate_mosaic_wrapper.py`: Use `is` instead of `==`, line 73.
    - `inundation_wrapper_nwm_flows.py`: Use `is not` instead of `!=`, line 76.
    - `mosaic_inundation.py`: Use `is` instead of `==`, line 181.
- `unit_tests`
    - `README.md`: Updated documentation, run `pytest` in `/foss_fim` directory.
    - `clip_vectors_to_wbd_test.py`: File moved to data/wbd directory, update import statement, skipped this test.
    - `filter_catchments_and_add_attributes_params.json`: Update Branch ID
    - `inundate_gms_params.json`: Moved to `unit_tests/` folder.
    - `inundate_gms_test.py`: Moved to `unit_tests/` folder.
    - `inundation_params.json`: Moved to `unit_tests/` folder.
    - `inundation_test.py`: Moved to `unit_tests/` folder.
    - `outputs_cleanup_params.json`: Update Branch ID
    - `outputs_cleanup_test.py`: Update import statement
    - `split_flows_params.json`: Update Branch ID
    - `usgs_gage_crosswalk_params.json`: Update Branch ID & update argument to gage_crosswalk.run_crosswalk
    - `usgs_gage_crosswalk_test.py`: Update params to gage_crosswalk.run_crosswalk

### Additions 
- `.github/workflows/`
    - `lint_and_format.yaml`: Add GitHub Actions Workflow file for Continuous Integration environment (lint and format test).

<br/><br/>

## v4.4.6.0 - 2023-11-17 - [PR#1031](https://github.com/NOAA-OWP/inundation-mapping/pull/1031)

Upgrade our acquire 3Dep DEMs script to pull down South Alaska HUCS with its own CRS.

The previous set of DEMs run for FIM and it's related vrt already included all of Alaska, and those have not been re-run. FIM code will be updated in the near future to detect if the HUC starts with a `19` with slight different logic, so it can preserve the CRS of EPSG:3338 all the way to final FIM outputs.  See [792 ](https://github.com/NOAA-OWP/inundation-mapping/issues/792)for new integration into FIM.

A new vrt for the new South Alaska DEMs was also run with no changes required.

This issue closes [1028](https://github.com/NOAA-OWP/inundation-mapping/issues/1028). 

### Additions
- `src/utils`
     - `shared_validators.py`: A new script where we can put in code to validate more complex arguments for python scripts. Currently has one for validating CRS values. It does valid if the CRS value is legitimate but does check a bunch of formatting including that it starts with either the name of `EPSG` or `ESRI`

### Changes
- `data/usgs` 
    - `aquire_and_preprocess_3dep_dems.py`: Changes include:
        - Add new input arg for desired target projection and logic to support an incoming CRS.
        - Updated logic for pre-existing output folders and `on-the-fly` question to users during execution if they want to overwrite the output folder (if applicable).
        - Changed date/times to utc.
        - Upgraded error handing for the gdal "processing" call.

<br/><br/>

## v4.4.5.0 - 2023-10-26 - [PR#1018](https://github.com/NOAA-OWP/inundation-mapping/pull/1018)

During a recent BED attempt which added the new pre-clip system, it was erroring out on a number of hucs. It was issuing an error in the add_crosswalk.py script. While a minor bug does exist there, after a wide number of tests, the true culprit is the memory profile system embedded throughout FIM. This system has been around for at least a few years but not in use. It is not 100% clear why it became a problem with the addition of pre-clip, but that changes how records are loaded which likely affected memory at random times.

This PR removes that system.

A couple of other minor updates were made:
- Update to the pip files (also carried forward changes from other current PRs)
- When a huc or huc list is provided to fim_pipeline, it goes to a script, check_huc_inputs.py, to ensure that the incoming HUCs are valid and in that list. In the previous code it looks for all files with the file name pattern of "included_huc*.lst". However, we now only want it to check against the file "included_huc8.list".

### Changes
- `CONTRIBUTING.md`: Text update.
- `Pipfile` and `Pipfile.lock`: updated to remove tghe memory-profiler package, update gval to 0.2.3 and update urllib3 to 1.26.18.
- `data/wbd`
    - `clip_vectors_to_wbd.py`: remove profiler
 - `src`
     - `add_crosswalk.py`: remove profiler
     - `add_thalweg_lateral.py`: remove profiler.
     - `aggregate_by_huc.py`: remove profiler and small text correction.
     - `agreedem.py`: remove profiler.
     - `bathy_src_adjust_topwidth.py`: remove profiler.
     - `burn_in_levees.py`: remove profiler.
     - `check_huc_inputs.py`: changed test pattern to just look against `included_huc8.lst`.
     - `delineate_hydros_and_produce_HAND.sh`: remove profiler.
     - `filter_catchments_and_add_attributes.py`: remove profiler.
     - `make_stages_and_catchlist.py` remove profiler.
     - `mask_dem.py`: remove profiler.
     - `reachID_grid_to_vector_points.py`: remove profiler.
     - `run_unit_wb.sh`: remove profiler.
     - `split_flows.py`: remove profiler.
     - `unique_pixel_and_allocation.py`: remove profiler.
     - `usgs_gage_crosswalk.py`: remove profiler.
     - `usgs_gage_unit_setup.py`: remove profiler.
     - `utils`
         - `shared_functions`: remove profiler.
      ` unit_tests`
          - `clip_vectors_to_wbd_tests.py`: Linting tools change order of the imports.

<br/><br/>

## v4.4.4.1 - 2023-10-26 - [PR#1007](https://github.com/NOAA-OWP/inundation-mapping/pull/1007)

Updates GVAL to address memory and performance issues associated with running synthesize test cases.

### Changes

- `tools/tools_shared_functions.py`
- `Pipfile`
- `pyproject.toml`
- `tools/run_test_case.py`
- `tools/synthesize_test_cases.py`
- `tools/inundate_mosaic_wrapper`
<br/><br/>

## v4.4.4.0 - 2023-10-20 - [PR#1012](https://github.com/NOAA-OWP/inundation-mapping/pull/1012)

The way in which watershed boundary data (WBD) is generated and processed has been modified. Instead of generating those files "on the fly" for every run, a script has been added that will take a huclist and create the .gpkg files per HUC in a specified directory (`$pre_clip_huc_dir`).  During a `fim_pipeline.sh` run, the pre-clipped staged vectors will be copied over to the containers' working directory. This reduces runtime and the repetitive computation needed to generate those files every run.

### Changes

- `src/`
    - `bash_variables.env`: Add pre_clip_huc_dir env variable. 
    - `clip_vectors_to_wbd.py`: Moved to `/data/wbd/clip_vectors_to_wbd.py`.
    - `src/run_unit_wb.sh`: Remove ogr2ogr calls to get & clip WBD, remove call to clip_vectors_to_wbd.py, and replace with copying staged .gpkg files. 

### Additions

- `data/wbd/`
    - `generate_pre_clip_fim_huc8.py`: This script generates the pre-clipped vectors at the huc level.

<br/><br/>

## v4.4.3.0 - 2023-10-10 - [PR#1005](https://github.com/NOAA-OWP/inundation-mapping/pull/1005)

Revise stream clipping to WBD by (1) reducing the buffer to clip streams away from the edge of the DEM (to prevent reverse flow issues) from 3 cells to 8 cells to account for the 70m AGREE buffer; (2) splitting MultiLineStrings formed by NWM streams being clipped by the DEM edge and then re-entering the DEM, and retaining only the lowest segment. Also changes the value of `input_WBD_gdb` to use the WBD clipped to the DEM domain.

### Changes

- `src/`
    - `bash_variables.env`: Update WBD to the WBD clipped to the DEM domain
    - `clip_vectors_to_wbd.py`: Decrease stream buffer from 3 to 8 cells inside of the WBD buffer; select the lowest segment of any incoming levelpaths that are split by the DEM edge.
    - `derive_level_paths.py`: Remove unused argument
    - `stream_branches.py`: Remove unused argument

<br/><br/>

## v4.4.2.3 - 2023-09-21 - [PR#998](https://github.com/NOAA-OWP/inundation-mapping/pull/998)

Removes exclude list for black formatter in `.pre-commit-config.yaml` as well as in `pyproject.toml`. Ran the `black` executable on the 
whole repository, the re-formatted files in `src/` & `tools/` are included.

### Changes

- `.pre-commit-config.yaml`
- `pyproject.toml`
- `src/add_crosswalk.py`
- `src/bathy_src_adjust_topwidth.py`
- `src/bathymetric_adjustment.py`
- `src/identify_src_bankfull.py`
- `src/src_roughness_optimization.py`
- `tools/vary_mannings_n_composite.py`

<br/><br/>

## v4.4.2.2 - 2023-09-21 - [PR#997](https://github.com/NOAA-OWP/inundation-mapping/pull/997)

Bug fix for an error related to reindexing in `StreamNetwork.drop()`.

### Changes

- `src/stream_branches.py`: Fixes reindexing error.

<br/><br/>

## v4.4.2.1 - 2023-09-20 - [PR#990](https://github.com/NOAA-OWP/inundation-mapping/pull/990)

Corrects a bug in `src/usgs_gage_unit_setup.py` caused by missing geometry field after `GeoDataFrame.update()`.

### Changes

- `src/usgs_gage_unit_setup.py`: Sets geometry field in `self.gages`.

<br/><br/>

## v4.4.2.0 - 2023-09-20 - [PR#993](https://github.com/NOAA-OWP/inundation-mapping/pull/993)

Resolves the causes of two warnings in pandas and geopandas: (1) `FutureWarning` from taking the `int()` of single-length Series and (2) `SettingWithCopyWarning` resulting from the use of `inplace=True`.

### Changes

Removed `inplace=True` from
- `data/`
    - `usgs/preprocess_ahps_usgs.py`
    - `write_parquet_from_calib_pts.py`
- `src/`
    - `add_crosswalk.py`
    - `bathy_src_adjust_topwidth.py`
    - `clip_vectors_to_wbd.py`
    - `crosswalk_nwm_demDerived.py`
    - `derive_level_paths.py`
    - `finalize_srcs.py`
    - `identify_src_bankfull.py`
    - `src_adjust_usgs_rating.py`
    - `src_roughness_optimization.py`
    - `stream_branches.py`
    - `subdiv_chan_obank_src.py`
    - `subset_catch_list_by_branch_id.py`
    - `usgs_gage_unit_setup.py`
    - `utils/shared_functions.py`
- `tools/`
    - `adjust_rc_with_feedback.py`
    - `aggregate_csv_files.py`
    - `combine_crosswalk_tables.py`
    - `eval_plots_stackedbar.py`
    - `inundation.py`
    - `make_boxes_from_bounds.py`
    - `mosaic_inundation.py`
    - `plots.py`
    - `rating_curve_comparison.py`
    - `vary_mannings_n_composite.py`

Fixed single-length Series in
- `src/`
    - `split_flows.py`
    - `stream_branches.py`

- ``src/stream_branches.py``: Fixed class methods

<br/><br/>

## v4.4.1.1 - 2023-09-20 - [PR#992](https://github.com/NOAA-OWP/inundation-mapping/pull/992)

Fixes errors caused when a GeoDataFrame contains a `MultiLineString` geometry instead of a `LineString`. Update black force-exclude list.

### Changes

- `src/`
    `split_flows.py` and `stream_branches.py`: Converts `MultiLineString` geometry into `LineString`s.
- `pyproject.toml` : Add three files in `src/` to exclude list.

<br/><br/>

## v4.4.1.0 - 2023-09-18 - [PR#988](https://github.com/NOAA-OWP/inundation-mapping/pull/988)

Format code using `black` formatter, incorporate `isort` package to sort import statements,
and adhere all code to PEP8 Style Guide using the `flake8` package. Remove deprecated files.
Set up git pre-commit hooks.

Not all files were modified, however, to avoid individually listing each file here, the `/*` convention
is used to denote that almost every file in those directories were formatted and linted.

### Changes

- `.gitattributes`: Add newline at EOF.
- `.github/*`: 
- `.gitignore`: Trim extra last line.
- `CONTRIBUTING.md`: Update contributing guidelines.
- `Dockerfile`: Update PYTHONPATH to point to correct `unit_tests` directory.
- `Pipfile`: Add flake8, black, pyproject-flake8, pre-commit, isort packages
- `Pipfile.lock`: Update to correspond with new packages in Pipfile 
- `README.md` : Update link to wiki, trim whitespace.
- `config/*`
- `data/*`
- `docs/*`
- `fim_pipeline.sh` : Clean up usage statement
- `fim_post_processing.sh`: Update usage statement
- `fim_pre_processing.sh`: Update usage statement.
- `fim_process_unit_wb.sh`: Make usage functional, combine usage and comments.
- `src/*`
- `tools/*`
- `unit_tests/*`: The directory name where the unit test data must reside was changed from
`fim_unit_test_data_do_not_remove` => `unit_test_data`

### Additions

- `pyproject.toml`: Configuration file
- `.pre-commit-config.yaml`: Initialize git pre-commit hooks
- `tools/hash_compare.py`: Carson's hash compare script added to compare files or directories 
in which we do not expect any changes.

### Removals

- `data/nws/preprocess_ahps_nws.py`
- `src/adjust_headwater_streams.py`
- `src/aggregate_vector_inputs.py`
- `src/utils/reproject_dem.py`
- `tools/code_standardizer/*`: Incorporated "code_standardizer" into base level Dockerfile.
- `tools/compile_comp_stats.py`
- `tools/compile_computational_stats.py`
- `tools/consolidate_metrics.py`
- `tools/copy_test_case_folders.py`
- `tools/cygnss_preprocessing.py`
- `tools/nesdis_preprocessing.py`
- `tools/plots/*`: Duplicate and unused directory.
- `.isort.cfg`: Incorporated into `pyproject.toml`

<br/><br/>

## v4.4.0.1 - 2023-09-06 - [PR#987](https://github.com/NOAA-OWP/inundation-mapping/pull/987)

Corrects a bug in `src/usgs_gage_unit_setup.py` that causes incorrect values to populate a table, generating an error in `src/usgs_gage_crosswalk.py`.

### Changes

- `src/usgs_gage_unit_setup.py`: Changes `self.gages.location_id.fillna(usgs_gages.nws_lid, inplace=True)` to `self.gages.location_id.fillna(self.gages.nws_lid, inplace=True)`

<br/><br/>

## v4.4.0.0 - 2023-09-01 - [PR#965](https://github.com/NOAA-OWP/inundation-mapping/pull/965)

This feature branch includes new functionality to perform an additional layer of HAND SRC calibration using ras2fim rating curve and point data. The calibration workflow for ras2fim data follows the same general logic as the existing USGS rating curve calibration routine.

### Additions

- `src/src_adjust_ras2fim_rating.py`: New python script to perform the data prep steps for running the SRC calibration routine:
1) merge the `ras_elev_table.csv` data and the ras2fim cross section rating curve data (`reformat_ras_rating_curve_table.csv`)
2) sample the ras2fim rating curve at NWM recurrence flow intervals (2, 5, 10, 25, 50, 100yr)
3) pass inputs to the `src_roughness_optimization.py` workflow

### Changes

- `config/deny_branches.lst`: Added `ras_elev_table.csv` to keep list. Needed for `fim_post_processing.sh`
- `config/deny_unit.lst`: Added `ras_elev_table.csv` to keep list. Needed for `fim_post_processing.sh`
- `config/params_template.env`: Added new block for ras2fim SRC calibration parameters (can turn on/off each of the three SRC calibration routines individually); also reconfigured docstrings for calibration parameters)
- `fim_post_processing.sh`: Added routines to create ras2fim calibration data and then run the SRC calibration workflow with ras2fim data
- `src/add_crosswalk.py`: Added placeholder variable (`calb_coef_ras2fim`) in all `hydrotable.csv` files
- `src/aggregate_by_huc.py`: Added new blocks to perform huc-branch aggregation for all `ras_elev_table.csv` files
- `src/run_by_branch.sh`: Revised input variable (changed from csv file to directory) for `usgs_gage_crosswalk.py` to facilitate both `usgs_elev_table.csv` and ras_elev_table.csv` outputs
- `src/run_unit_wb.sh`: Revised inputs and output variables for `usgs_gage_unit_setup.py` and `usgs_gage_crosswalk.py`
- `src/src_roughness_optimization.py`: Added code blocks to ingest ras2fim rating curve data; added new attributes/renamed output variables to catchments gpkg output
- `src/usgs_gage_crosswalk.py`: Added code block to process ras2fim point locations alongside existing USGS gage point locations; outputs a separate csv if ras2fim points exist within the huc
- `src/usgs_gage_unit_setup.py`: Added code block to ingest and process raw ras2fim point locations gpkg file (same general workflow to usgs gages); all valid points (USGS and RAS2FIM) are exported to the huc level `usgs_subset_gages.gpkg`
- `tools/inundate_nation.py`: Added functionality to allow user to pass in a single HUC for faster spot checking of NWM recurr inundation maps

<br/><br/>

## v4.3.15.6 - 2023-09-01 - [PR#972](https://github.com/NOAA-OWP/inundation-mapping/pull/972)

Adds functionality to `tools/inundate_mosaic_wrapper.py` and incorporates functionality into existing `inundation-mapping` scripts.

### Changes

- `tools/`
    - `inundate_mosaic_wrapper.py`: Refactors to call `Inundate_gms` only once; adds functionality to produce a mosaicked polygon from `depths_raster` without needing to generate the `inundation_raster`; removes `log_file` and `output_fileNames` as variables and input arguments; updates the help description for `keep_intermediate`.
    - `composite_inundation.py`, 'inundate_nation.py`, and `run_test_case.py`: Implements `produce_mosaicked_inundation()` from `tools/inundate_mosaic_wrapper.py`.
    - `inundate_gms.py`: Adds back `Inundate_gms(**vars(parser.parse_args()))` command-line function call.
    - `mosaic_inundation.py` and `overlapping_inundation.py`: Removes unused import(s).
    - `tools_shared_variables.py`: Changes hardcoded `INPUT_DIR` to environment variable.

<br/><br/>

## v4.3.15.5 - 2023-09-01 - [PR#970](https://github.com/NOAA-OWP/inundation-mapping/pull/970)

Fixes an issue where the stream network was clipped inside the DEM resulting in a burned stream channel that was then filled by the DEM depression filling process so that all pixels in the burned channel had the same elevation which was the elevation at the spill point (which wasn't necessarily at the HUC outlet). The stream network is now extended from the WBD to the buffered WBD and all streams except the outlet are clipped to the streams buffer inside the WBD (WBD - (3 x cell_size)). This also prevents reverse flow issues.

### Changes

- `src/`
    - `clip_vectors_to_wbd.py`: Clip NWM streams to buffered WBD and clip non-outlet streams to WBD streams buffer (WBD - (3 x cell_size)).
    - `derive_level_paths.py`: Add WBD input argument
    - `run_unit_wb.py`: Add WBD input argument
    - `src_stream_branches.py`: Ignore branches outside HUC
- `unit_tests/`
    - `derive_level_paths_params.json`: Add WBD parameter value
    - `derive_level_paths_test.py`: Add WBD parameter

<br/><br/>

## v4.3.15.4 - 2023-09-01 - [PR#977](https://github.com/NOAA-OWP/inundation-mapping/pull/977)

Fixes incorrect `nodata` value in `src/burn_in_levees.py` that was responsible for missing branches (Exit code: 61). Also cleans up related files.

### Changes

- `src/`
    - `buffer_stream_branches.py`: Moves script functionality into a function.
    - `burn_in_levees.py`: Corrects `nodata` value. Adds context managers for reading rasters.
    - `generate_branch_list.py`: Removes unused imports.
    - `mask_dem.py`: Removes commented code.

<br/><br/>

## v4.3.15.3 - 2023-09-01 - [PR#983](https://github.com/NOAA-OWP/inundation-mapping/pull/983)

This hotfix addresses some bugs introduced in the pandas upgrade.

### Changes

- `/tools/eval_plots_stackedbar.py`: 2 lines were changed to work with the pandas upgrade. Added an argument for a `groupby` median call and fixed a bug with the pandas `query`. Also updated with Black compliance.

<br/><br/>

## v4.3.15.2 - 2023-07-18 - [PR#948](https://github.com/NOAA-OWP/inundation-mapping/pull/948)

Adds a script to produce inundation maps (extent TIFs, polygons, and depth grids) given a flow file and hydrofabric outputs. This is meant to make it easier to team members and external collaborators to produce inundation maps.

### Additions
- `data/`
    - `/tools/inundate_mosaic_wrapper.py`: The script that performs the inundation and mosaicking processes.
    - `/tools/mosaic_inundation.py`: Add function (mosaic_final_inundation_extent_to_poly).

<br/><br/>

## v4.3.15.1 - 2023-08-08 - [PR#960](https://github.com/NOAA-OWP/inundation-mapping/pull/960)

Provides a scripted procedure for updating BLE benchmark data including downloading, extracting, and processing raw BLE data into benchmark inundation files (inundation rasters and discharge tables).

### Additions

- `data/ble/ble_benchmark/`
    - `Dockerfile`, `Pipfile`, and `Pipfile.lock`: creates a new Docker image with necessary Python packages
    - `README.md`: contains installation and usage information
    - `create_ble_benchmark.py`: main script to generate BLE benchmark data

### Changes

- `data/ble/ble_benchmark/`
    - `create_flow_forecast_file.py` and `preprocess_benchmark.py`: moved from /tools

<br/><br/>

## v4.3.15.0 - 2023-08-08 - [PR#956](https://github.com/NOAA-OWP/inundation-mapping/pull/956)

Integrating GVAL in to the evaluation of agreement maps and contingency tables.

- `Dockerfile`: Add dependencies for GVAL
- `Pipfile`: Add GVAL and update related dependencies
- `Pipfile.lock`: Setup for Docker Image builds
- `run_test_case.py`: Remove unused arguments and cleanup
- `synthesize_test_cases.py`: Fix None comparisons and cleanup
- `tools/shared_functions.py`: Add GVAL crosswalk function, add rework create_stats_from_raster, create and create_stats_from_contingency_table
- `unit_tests/tools/inundate_gms_test.py`: Bug fix

<br/><br/>

## v4.3.14.2 - 2023-08-08 - [PR#959](https://github.com/NOAA-OWP/inundation-mapping/pull/959)

The enhancements in this PR include the new modules for pre-processing bathymetric data from the USACE eHydro dataset and integrating the missing hydraulic geometry into the HAND synthetic rating curves.

### Changes
- `data/bathymetry/preprocess_bathymetry.py`: added data source column to output geopackage attribute table.
- `fim_post_processing.sh`: changed -bathy input reference location.
- `config/params_template.env`: added export to bathymetry_file

<br/><br/>

## v4.3.14.1 - 2023-07-13 - [PR#946](https://github.com/NOAA-OWP/inundation-mapping/pull/946)

ras2fim product had a need to run the acquire 3dep script to pull down some HUC8 DEMs. The old script was geared to HUC6 but could handle HUC8's but needed a few enhancements. ras2fim also did not need polys made from the DEMs, so a switch was added for that.

The earlier version on the "retry" feature would check the file size and if it was smaller than a particular size, it would attempt to reload it.  The size test has now been removed. If a file fails to download, the user will need to look at the log out, then remove the file before attempting again. Why? So the user can see why it failed and decide action from there.

Note: later, as needed, we might upgrade it to handle more than just 10m (which it is hardcoded against).

Additional changes to README to reflect how users can access ESIP's S3 as well as a one line addition to change file permissions in fim_process_unit_wb.sh.

### Changes
- `data`
    - `usgs`
        - `acquire_and_preprocess_3dep_dems.py`:  As described above.
 - `fim_pipeline.sh`:  a minor styling fix (added a couple of lines for readability)
 - `fim_pre_processing.sh`: a user message was incorrect & chmod 777 $outputDestDir.
 - `fim_process_unit_wb.sh`: chmod 777 for /output/<run_name> directory.
 - `README.md`: --no-sign-request instead of --request-payer requester for ESIP S3 access.

<br/><br/>

## v4.3.14.0 - 2023-08-03 - [PR#953](https://github.com/NOAA-OWP/inundation-mapping/pull/953)

The enhancements in this PR include the new modules for pre-processing bathymetric data from the USACE eHydro dataset and integrating the missing hydraulic geometry into the HAND synthetic rating curves.

### Additions

- `data/bathymetry/preprocess_bathymetry.py`: preprocesses the eHydro datasets.
- `src/bathymetric_adjustment.py`: adjusts synthetic rating curves for HUCs where preprocessed bathymetry is available.

### Changes

- `config/params_template.env`: added a toggle for the bathymetric adjustment routine: `bathymetry_adjust`
- `fim_post_processing.sh`: added the new `bathymetric_adjustment.py` to the postprocessing lineup
- `src/`
    - `add_crosswalk.py`, `aggregate_by_huc.py`, & `subdiv_chan_obank_src.py`: accounting for the new Bathymetry_source field in SRCs

<br/><br/>

## v4.3.13.0 - 2023-07-26 - [PR#952](https://github.com/NOAA-OWP/inundation-mapping/pull/952)

Adds a feature to manually calibrate rating curves for specified NWM `feature_id`s using a CSV of manual coefficients to output a new rating curve. Manual calibration is applied after any/all other calibrations. Coefficient values between 0 and 1 increase the discharge value (and decrease inundation) for each stage in the rating curve while values greater than 1 decrease the discharge value (and increase inundation).

Manual calibration is performed if `manual_calb_toggle="True"` and the file specified by `man_calb_file` (with `HUC8`, `feature_id`, and `calb_coef_manual` fields) exists. The original HUC-level `hydrotable.csv` (after calibration) is saved with a suffix of `_pre-manual` before the new rating curve is written.

### Additions

- `src/src_manual_calibration.py`: Adds functionality for manual calibration by CSV file

### Changes

- `config/params_template.env`: Adds `manual_calb_toggle` and `man_calb_file` parameters
- `fim_post_processing.sh`: Adds check for toggle and if `man_calb_file` exists before running manual calibration

<br/><br/>

## v4.3.12.1 - 2023-07-21 - [PR#950](https://github.com/NOAA-OWP/inundation-mapping/pull/950)

Fixes a couple of bugs that prevented inundation using HUC-level hydrotables. Update associated unit tests.

### Changes

- `tools/inundate_gms.py`: Fixes a file path error and Pandas DataFrame indexing error.
- `unit_tests/tools/inundate_gms_test.py`: Do not skip this test, refactor to check that all branch inundation rasters exist.
- `unit_tests/tools/inundate_gms_params.json`: Only test 1 HUC, update forecast filepath, use 4 'workers'.

### Removals

- `unit_tests/tools/inundate_gms_unittests.py`: No longer used. Holdover from legacy unit tests.

<br/><br/>


## v4.3.12.0 - 2023-07-05 - [PR#940](https://github.com/NOAA-OWP/inundation-mapping/pull/940)

Refactor Point Calibration Database for synthetic rating curve adjustment to use `.parquet` files instead of a PostgreSQL database.

### Additions
- `data/`
    -`write_parquet_from_calib_pts.py`: Script to write `.parquet` files based on calibration points contained in a .gpkg file.

### Changes
- `src/`
    - `src_adjust_spatial_obs.py`: Refactor to remove PostgreSQL and use `.parquet` files.
    - `src_roughness_optimization.py`: Line up comments and add newline at EOF.
    - `bash_variables.env`: Update formatting, and add `{}` to inherited `.env` variables for proper variable expansion in Python scripts.
- `/config`
    - `params_template.env`: Update comment.
- `fim_pre_processing.sh`: In usage statement, remove references to PostGRES calibration tool.
- `fim_post_processing.sh`: Remove connection to and loading of PostgreSQL database.
- `.gitignore`: Add newline.
- `README.md`: Remove references to PostGRES calibration tool.

### Removals
- `config/`
    - `calb_db_keys_template.env`: No longer necessary without PostGRES Database.

- `/tools/calibration-db` : Removed directory including files below.
    - `README.md`
    - `docker-compose.yml`
    - `docker-entrypoint-enitdb.d/init-db.sh`

<br/><br/>

## v4.3.11.7 - 2023-06-12 - [PR#932](https://github.com/NOAA-OWP/inundation-mapping/pull/932)

Write to a csv file with processing time of `run_unit_wb.sh`, update PR Template, add/update bash functions in `bash_functions.env`, and modify error handling in `src/check_huc_inputs.py`. Update unit tests to throw no failures, `25 passed, 3 skipped`.

### Changes
- `.github/`
    - `PULL_REQUEST_TEMPLATE.md` : Update PR Checklist into Issuer Checklist and Merge Checklist
- `src/`
    - `run_unit_wb.sh`: Add line to log processing time to `$outputDestDir/logs/unit/total_duration_run_by_unit_all_HUCs.csv`
    - `check_huc_inputs.py`: Modify error handling. Correctly print HUC number if it is not valid (within `included_huc*.lst`)
    - `bash_functions.env`: Add `Calc_Time` function, add `local` keyword to functionally scoped variables in `Calc_Duration`
- `unit_tests/`
    - `derive_level_paths_test.py`: Update - new parameter (`buffer_wbd_streams`)
    - `derive_level_paths_params.json`: Add new parameter (`buffer_wbd_streams`)
    - `clip_vectors_to_wbd_test.py`: Update - new parameter (`wbd_streams_buffer_filename`)
    - `clip_vectors_to_wbd_params.json`: Add new parameter (`wbd_streams_buffer_filename`) & Fix pathing for `nwm_headwaters`

<br/><br/>

## v4.3.11.6 - 2023-05-26 - [PR#919](https://github.com/NOAA-OWP/inundation-mapping/pull/919)

Auto Bot asked for the python package of `requests` be upgraded from 2.28.2 to 2.31.0. This has triggered a number of packages to upgrade.

### Changes
- `Pipfile.lock`: as described.

<br/><br/>

## v4.3.11.5 - 2023-05-30 - [PR#911](https://github.com/NOAA-OWP/inundation-mapping/pull/911)

This fix addresses bugs found when using the recently added functionality in `tools/synthesize_test_cases.py` along with the `PREV` argument. The `-pfiles` argument now performs as expected for both `DEV` and `PREV` processing. Addresses #871

### Changes
`tools/synthesize_test_cases.py`: multiple changes to enable all expected functionality with the `-pfiles` and `-pcsv` arguments

<br/><br/>

## v4.3.11.4 - 2023-05-18 - [PR#917](https://github.com/NOAA-OWP/inundation-mapping/pull/917)

There is a growing number of files that need to be pushed up to HydroVis S3 during a production release, counting the new addition of rating curve comparison reports.

Earlier, we were running a number of aws cli scripts one at a time. This tool simplies it and pushes all of the QA and supporting files. Note: the HAND files from a release, will continue to be pushed by `/data/aws/s3.py` as it filters out files to be sent to HV s3.

### Additions

- `data\aws`
     - `push-hv-data-support-files.sh`: As described above. See file for command args.

<br/><br/>


## v4.3.11.3 - 2023-05-25 - [PR#920](https://github.com/NOAA-OWP/inundation-mapping/pull/920)

Fixes a bug in CatFIM script where a bracket was missing on a pandas `concat` statement.

### Changes
- `/tools/generate_categorical_fim.py`: fixes `concat` statement where bracket was missing.

<br/><br/>

## v4.3.11.2 - 2023-05-19 - [PR#918](https://github.com/NOAA-OWP/inundation-mapping/pull/918)

This fix addresses a bug that was preventing `burn_in_levees.py` from running. The if statement in run_unit_wb.sh preceeding `burn_in_levees.py` was checking for the existence of a filepath that doesn't exist.

### Changes
- `src/run_unit_wb.sh`: fixed the if statement filepath to check for the presence of levee features to burn into the DEM

<br/><br/>

## v4.3.11.1 - 2023-05-16 - [PR#904](https://github.com/NOAA-OWP/inundation-mapping/pull/904)

`pandas.append` was deprecated in our last Pandas upgrade (v4.3.9.0). This PR updates the remaining instances of `pandas.append` to `pandas.concat`.

The file `tools/thalweg_drop_check.py` had an instance of `pandas.append` but was deleted as it is no longer used or necessary.

### Changes

The following files had instances of `pandas.append` changed to `pandas.concat`:
- `data/`
    - `nws/preprocess_ahps_nws.py`
    - `usgs/`
        - `acquire_and_preprocess_3dep_dems.py`
        - `preprocess_ahps_usgs.py`
- `src/`
    - `add_crosswalk.py`
    - `adjust_headwater_streams.py`
    - `aggregate_vector_inputs.py`
    - `reset_mannings.py`
- `tools/`
    - `aggregate_mannings_calibration.py`
    - `eval_plots.py`
    - `generate_categorical_fim.py`
    - `generate_categorical_fim_flows.py`
    - `plots/`
        - `eval_plots.py`
        - `utils/shared_functions.py`
    - `rating_curve_comparison.py`
    - `rating_curve_get_usgs_curves.py`
    - `tools_shared_functions.py`

### Removals

- `tools/thalweg_drop_check.py`

<br/><br/>

## v4.3.11.0 - 2023-05-12 - [PR#903](https://github.com/NOAA-OWP/inundation-mapping/pull/903)

These changes address some known issues where the DEM derived flowlines follow the incorrect flow path (address issues with stream order 1 and 2 only). The revised code adds a new workflow to generate a new flow direction raster separately for input to the `run_by_branch.sh` workflow (branch 0 remains unchanged). This modification helps ensure that the DEM derived flowlines follow the desired NWM flow line when generating the DEM derived flowlines at the branch level.

### Changes
- `config/deny_branch_zero.lst`: removed `LandSea_subset_{}.tif` and `flowdir_d8_burned_filled_{}.tif` from the "keep" list as these files are now kept in the huc root folder.
- `config/deny_unit.lst`: added file cleanups for newly generated branch input files stored in the huc root folder (`dem_burned.tif`, `dem_burned_filled.tif`, `flowdir_d8_burned_filled.tif`, `flows_grid_boolean.tif`, `wbd_buffered_streams.gpkg`)
- `src/clip_vectors_to_wbd.py`: saving the `wbd_streams_buffer` as an output gpkg for input to `derive_level_paths.py`
- `src/derive_level_paths.py`: added a new step to clip the `out_stream_network_dissolved` with the `buffer_wbd_streams` polygon. this resolves errors with the edge case scenarios where a NWM flow line intersects the WBD buffer polygon
- `src/run_unit_wb.sh`: Introduce new processing steps to generate separate outputs for input to branch 0 vs. all other branches. Remove the branch zero `outputs_cleanup.py` as the branches are no longer pointing to files stored in the branch 0 directory (stored in huc directory)
   - Rasterize reach boolean (1 & 0) for all branches (not branch 0): using the `nwm_subset_streams_levelPaths_dissolved.gpkg` to define the branch levelpath flow lines
   - AGREEDEM reconditioning for all branches (not branch 0)
   - Pit remove burned DEM for all branches (not branch 0)
   - D8 flow direction generation for all branches (not branch 0)
- `src/run_by_branch.sh`: changed `clip_rasters_to_branches.py` input file location for `$tempHucDataDir/flowdir_d8_burned_filled.tif` (newly created file)

<br/><br/>

## v4.3.10.0 - 2023-05-12 - [PR#888](https://github.com/NOAA-OWP/inundation-mapping/pull/888)

`aggregate_by_huc.py` was taking a long time to process. Most HUCs can aggregate their branches into one merged hydrotable.csv in just 22 seconds, but a good handful took over 2 mins and a few took over 7 mins. When multiplied by 2,138 HUCs it was super slow. Multi-proc has not been added and it now takes appx 40 mins at 80 cores.

An error logging system was also added to track errors that may have occurred during processing.

### Changes
- `fim_pipeline.sh` - added a duration counter at the end of processing HUCs
- `fim_post_processing.sh` - added a job limit (number of procs), did a little cleanup, and added a warning note about usage of job limits in this script,
- `src`
    - `aggregate_by_huc.py`: Added multi proc, made it useable for non external script calls, added a logging system for errors only.
    - `indentify_src_bankful.py`: typo fix.

<br/><br/>

## v4.3.9.2 - 2023-05-12 - [PR#902](https://github.com/NOAA-OWP/inundation-mapping/pull/902)

This merge fixes several sites in Stage-Based CatFIM sites that showed overinundation. The cause was found to be the result of Stage-Based CatFIM code pulling the wrong value from the `usgs_elev_table.csv`. Priority is intended to go to the `dem_adj_elevation` value that is not from branch 0, however there was a flaw in the prioritization logic. Also includes a change to `requests` usage that is in response to an apparent IT SSL change. This latter change was necessary in order to run CatFIM. Also added a check to make sure the `dem_adj_thalweg` is not too far off the official elevation, and continues if it is.

### Changes
- `/tools/generate_categorical_fim.py`: fixed pandas bug where the non-branch zero `dem_adj_elevation` value was not being properly indexed. Also added a check to make sure the `dem_adj_thalweg` is not too far off the official elevation, and continues if it is.
- ` /tools/tools_shared_functions.py`: added `verify=False` to `requests` library calls because connections to WRDS was being refused (likely because of new IT protocols).

<br/><br/>

## v4.3.9.1 - 2023-05-12 - [PR#893](https://github.com/NOAA-OWP/inundation-mapping/pull/893)

Fix existing unit tests, remove unwanted behavior in `check_unit_errors_test.py`, update `unit_tests/README.md`

### Changes

- `unit_tests/`
    - `README.md` : Split up headings for setting up unit tests/running unit tests & re-formatted code block.
    - `check_unit_errors_test.py`: Fixed unwanted behavior of test leaving behind `sample_n.txt` files in `unit_errors/`
    - `clip_vectors_to_wbd_params.json`: Update parameters
    - `clip_vectors_to_wbd_test.py`: Update arguments
    - `pyproject.toml`: Ignore RuntimeWarning, to suppress pytest failure.
    - `usgs_gage_crosswalk_test.py`: Enhance readability of arguments in `gage_crosswalk.run_crosswalk` call

<br/><br/>

## v4.3.9.0 - 2023-04-19 - [PR#889](https://github.com/NOAA-OWP/inundation-mapping/pull/889)

Updates GDAL in base Docker image from 3.1.2 to 3.4.3 and updates all Python packages to latest versions, including Pandas v.2.0.0. Fixes resulting errors caused by deprecation and/or other changes in dependencies.

NOTE: Although the most current GDAL is version 3.6.3, something in 3.5 causes an issue in TauDEM `aread8` (this has been submitted as https://github.com/dtarb/TauDEM/issues/254)

### Changes

- `Dockerfile`: Upgrade package versions and fix `tzdata`
- `fim_post_processing.sh`: Fix typo
- `Pipfile` and `Pipfile.lock`: Update Python versions
- `src/`
    - `add_crosswalk.py`, `aggregate_by_huc.py`, `src_adjust_usgs_rating.py`, and `usgs_gage_unit_setup.py`: Change `df1.append(df2)` (deprecated) to `pd.concat([df1, df2])`
    - `build_stream_traversal.py`: Add `dropna=True` to address change in NaN handling
    - `getRasterInfoNative.py`: Replace `import gdal` (deprecated) with `from osgeo import gdal`
    - `stream_branches.py`: Change deprecated indexing to `.iloc[0]` and avoid `groupby.max()` over geometry
- `tools`
    - `inundation.py`: Cleans unused `from gdal`
    - `eval_plots.py`: deprecated dataframe.append fixed and deprecated python query pattern fixed.

<br/><br/>

## v4.3.8.0 - 2023-04-07 - [PR#881](https://github.com/NOAA-OWP/inundation-mapping/pull/881)

Clips branch 0 to terminal segments of NWM streams using the `to` attribute of NWM streams (where `to=0`).

### Changes

- `src/`
    - `delineate_hydros_and_produce_HAND.sh`: Added input arguments to `src/split_flows.py`
    - `split_flows.py`: Added functionality to snap and trim branch 0 flows to terminal NWM streamlines

<br/><br/>

## v4.3.7.4 - 2023-04-10 - [PR#882](https://github.com/NOAA-OWP/inundation-mapping/pull/882)

Bug fix for empty `output_catchments` in `src/filter_catchments_and_add_attributes.py`

### Changes

- `src/filter_catchments_and_add_attributes.py`: Adds check for empty `output_catchments` and exits with Status 61 if empty.

<br/><br/>

## v4.3.7.3 - 2023-04-14 - [PR#880](https://github.com/NOAA-OWP/inundation-mapping/pull/880)

Hotfix for addressing an error during the NRMSE calculation/aggregation step within `tools/rating_curve_comparison.py`. Also added the "n" variable to the agg_nwm_recurr_flow_elev_stats table. Addresses #878

### Changes

- `tools/rating_curve_comparison.py`: address error for computing nrmse when n=1; added the "n" variable (sample size) to the output metrics table

<br/><br/>

## v4.3.7.2 - 2023-04-06 - [PR#879](https://github.com/NOAA-OWP/inundation-mapping/pull/879)

Replaces `os.environ` with input arguments in Python files that are called from bash scripts. The bash scripts now access the environment variables and pass them to the Python files as input arguments. In addition to adapting some Python scripts to a more modular structure which allows them to be run individually, it also allows Visual Studio Code debugger to work properly. Closes #875.

### Changes

- `fim_pre_processing.sh`: Added `-i $inputsDir` input argument to `src/check_huc_inputs.py`
- `src/`
    - `add_crosswalk.py`: Changed `min_catchment_area` and `min_stream_length` environment variables to input arguments
    - `check_huc_inputs.py`: Changed `inputsDir` environment variable to input argument
    - `delineate_hydros_and_produce_HAND.sh`: Added `-m $max_split_distance_meters -t $slope_min -b $lakes_buffer_dist_meters` input arguments to `src/split_flows.py`
    - `split_flows.py`: Changed `max_split_distance_meters`, `slope_min`, and `lakes_buffer_dist_meters` from environment variables to input arguments

<br/><br/>

## v4.3.7.1 - 2023-04-06 - [PR#874](https://github.com/NOAA-OWP/inundation-mapping/pull/874)

Hotfix to `process_branch.sh` because it wasn't removing code-61 branches on exit. Also removes the current run from the new fim_temp directory.

### Changes

- `fim_pipeline.sh`: removal of current run from fim_temp directory
- `src/process_branch.sh`: switched the exit 61 block to use the temp directory instead of the outputs directory

<br/><br/>

## v4.3.7.0 - 2023-03-02 - [PR#868](https://github.com/NOAA-OWP/inundation-mapping/pull/868)

This pull request adds a new feature to `fim_post_processing.sh` to aggregate all of the hydrotables for a given HUC into a single HUC-level `hydrotable.csv` file. Note that the aggregation step happens near the end of `fim_post_processing.sh` (after the subdivision and calibration routines), and the branch hydrotable files are preserved in the branch directories for the time being.

### Changes

- `fim_pipeline.sh`: created a new variable `$jobMaxLimit` that multiplies the `$jobHucLimit` and the `$jobBranchLimit`
- `fim_post_processing.sh`: added new aggregation/concatenation step after the SRC calibration routines; passing the new `$jobMaxLimit` to the commands that accept a multiprocessing job number input; added `$skipcal` argument to the USGS rating curve calibration routine
- `src/add_crosswalk.py`: changed the default value for `calb_applied` variable to be a boolean
- `src/aggregate_by_huc.py`: file renamed (previous name: `src/usgs_gage_aggregate.py`); updated to perform branch to huc file aggregation for `hydroTable_{branch_id}.csv` and `src_full_crosswalked_{branch_id}.csv` files; note that the input arguments ask you to specify which file types to aggregate using the flags: `-elev`, `-htable`, and `-src`
- `tools/inundate_gms.py`: added check to use the aggregated HUC-level `hydrotable.csv` if it exists, otherwise continue to use the branch hydroTable files
- `tools/inundation.py`: added `usecols` argument to the `pd.read_csv` commands to improve read time for hydrotables
- `src/subdiv_chan_obank_src.py`: add dtype to hydrotable pd.read_csv to resolve pandas dtype interpretation warnings

<br/><br/>

## v4.3.6.0 - 2023-03-23 - [PR#803](https://github.com/NOAA-OWP/inundation-mapping/pull/803)

Clips Watershed Boundary Dataset (WBD) to DEM domain for increased efficiency. Essentially, this is a wrapper for `geopandas.clip()` and moves clipping from `src/clip_vectors_to_wbd.py` to `data/wbd/preprocess_wbd.py`.

### Additions

- `data/wbd/preprocess_wbd.py`: Clips WBD to DEM domain polygon

### Changes

- `src/`
    - `bash_variables.env`: Updates `input_WBD_gdb` environment variable
    - `clip_vectors_to_wbd.py`: Removes clipping to DEM domain

<br/><br/>

## v4.3.5.1 - 2023-04-01 - [PR#867](https://github.com/NOAA-OWP/inundation-mapping/pull/867)

outputs_cleanup.py was throwing an error saying that the HUC source directory (to be cleaned up), did not exist. This was confirmed in a couple of environments. The src path in run_unit_wb.sh was sending in the "outputs" directory and not the "fim_temp" directory. This might have been a merge issue.

The log file was moved to the unit_errors folder to validate the error, as expected.

### Changes

- `src/run_unit_wb.sh`: Change the source path being submitted to `outputs_cleanup.py` from the `outputs` HUC directory to the `fim_temp` HUC directory.
- `fim_process_unit_wb.sh`: Updated the phrase "Copied temp directory" to "Moved temp directory"

<br/><br/>

## v4.3.5.0 - 2023-03-02 - [PR#857](https://github.com/NOAA-OWP/inundation-mapping/pull/857)

Addresses changes to function calls needed to run upgraded Shapely library plus other related library upgrades. Upgraded libraries include:
- shapely
- geopandas
- pandas
- numba
- rasterstats
- numpy
- rtree
- tqdm
- pyarrow
- py7zr

Pygeos is removed because its functionality is incorporated into the upgraded shapely library.

### Changes

- `Dockerfile`
- `Pipfile and Pipfile.lock`
- `src/`
	- `associate_levelpaths_with_levees.py`
    - `build_stream_traversal.py`
	- `add_crosswalk.py`
	- `adjust_headwater_streams.py`
	- `aggregate_vector_inputs.py`
	- `clip_vectors_to_wbd.py`
	- `derive_headwaters.py`
	- `stream_branches.py`
	- `split_flows.py`
- `tools/`
	- `fimr_to_benchmark.py`
	- `tools_shared_functions.py`

<br/><br/>

## v4.3.4.0 - 2023-03-16-23 [PR#847](https://github.com/NOAA-OWP/inundation-mapping/pull/847)

### Changes

Create a 'working directory' in the Docker container to run processes within the container's non-persistent filesystem. Modify variables in scripts that process HUCs and branches to use the temporary working directory, and then copy temporary directory (after trimming un-wanted files) over to output directory (persistent filesystem).  Roll back changes to `unit_tests/` to use `/data/outputs` (contains canned data), as the volume mounted `outputs/` most likely will not contain the necessary unit test data.

- `Dockerfile` - create a `/fim_temp` working directory, update `projectDir` to an `ENV`, rename inputs and outputs directory variables
- `fim_pipeline.sh` - remove `projectDir=/foss_fim`, update path of `logFile`, remove indentation
- `fim_pre_processing.sh` - change `$outputRunDataDir` => `$outputDestDir` & add `$tempRunDir`
- `fim_post_processing.sh` - change `$outputRunDataDir` => `$outputDestDir`
- `fim_process_unit_wb.sh` - change `$outputRunDataDir` => `$outputDestDir`, add vars & export `tempRunDir`, `tempHucDataDir`, & `tempBranchDataDir` to `run_unit_wb.sh`
- `README.md` - add linebreaks to codeblocks

- `src/`
  - `bash_variables.env` - `$inputDataDir` => `$inputsDir`
  - `check_huc_inputs.py` - `$inputDataDir` => `$inputsDir`
  - `delineate_hydros_and_produce_HAND.py` - `$outputHucDataDir` => `$tempHucDataDir`, `$outputCurrentBranchDataDir` => `$tempCurrentBranchDataDir`
  - `process_branch.sh` - `$outputRunDataDir` => `$outputsDestDir`
  - `run_by_branch.sh` - `$outputCurrentBranchDataDir` => `$tempCurrentBranchDataDir`, `$outputHucDataDir` => `$tempHucDataDir`
  - `run_unit_wb.sh` - `$outputRunDataDir` => `$outputDestDir`, `$outputHucDataDir` => `$tempHucDataDir`
  - `utils/`
    - `shared_functions.py` - `$inputDataDir` => `$inputsDir`

- `tools/`
  - `inundation_wrapper_custom_flow.py` - `$outputDataDir` => `$outputsDir`
  - `inundation_wrapper_nwm_flows.py`  - `$outputDataDir` => `$outputsDir`
  - `tools_shared_variables.py` - `$outputDataDir` => `$outputsDir`

- `unit_tests/`
  - `README.md` - add linebreaks to code blocks, `/outputs/` => `/data/outputs/`
  - `*_params.json` - `/outputs/` => `/data/outputs/` & `$outputRunDataDir` => `$outputDestDir`
  - `derive_level_paths_test.py` - `$outputRunDataDir` => `$outputDestDir`
  - `check_unit_errors_test.py` - `/outputs/` => `/data/outputs/`
  - `shared_functions_test.py` - `$outputRunDataDir` => `$outputDestDir`
  - `split_flows_test.py`  - `/outputs/` => `/data/outputs/`
  - `tools/`
    - `*_params.json` - `/outputs/` => `/data/outputs/` & `$outputRunDataDir` => `$outputDestDir`

<br/><br/>

## v4.3.3.7 - 2023-03-22 - [PR#856](https://github.com/NOAA-OWP/inundation-mapping/pull/856)

Simple update to the `PULL_REQUEST_TEMPLATE.md` to remove unnecessary/outdated boilerplate items, add octothorpe (#) in front of Additions, Changes, Removals to mirror `CHANGELOG.md` format, and clean up the PR Checklist.

### Changes
- `docs/`
  - `PULL_REQUEST_TEMPLATE.md`

<br/><br/>

## v4.3.3.6 - 2023-03-30 - [PR#859](https://github.com/NOAA-OWP/inundation-mapping/pull/859)

Addresses the issue of output storage space being taken up by output files from branches that did not run. Updates branch processing to remove the extraneous branch file if a branch gets an error code of 61.

### Changes

- `src/process_branch.sh`: added line 41, which removes the outputs and output folder if Error 61 occurs.

<br/><br/>

## v4.3.3.5 - 2023-03-23 - [PR#848](https://github.com/NOAA-OWP/inundation-mapping/pull/848)

Introduces two new arguments (`-pcsv` and `-pfiles`) and improves the documentation of  `synthesize_test_cases.py`. The new arguments allow the user to provide a CSV of previous metrics (`-pcsv`) and to specity whether or not metrics should pulled from previous directories (`-pfiles`).

The dtype warning was suppressed through updates to the `read_csv` function in `hydrotable.py` and additional comments were added throughout script to improve readability.

### Changes
- `tools/inundation.py`: Add data types to the section that reads in the hydrotable (line 483).

- `tools/synthesize_test_cases.py`: Improved formatting, spacing, and added comments. Added two new arguments: `pcsv` and `pfiles` along with checks to verify they are not being called concurrently (lines 388-412). In `create_master_metrics_csv`, creates an `iteration_list` that only contains `['comparison']` if `pfiles` is not true, reads in the previous metric csv `prev_metrics_csv` if it is provided and combine it with the compiled metrics (after it is converted to dataframe), and saves the metrics dataframe (`df_to_write`) to CSV.

<br/><br/>

## v4.3.3.4 - 2023-03-17 - [PR#849](https://github.com/NOAA-OWP/inundation-mapping/pull/849)

This hotfix addresses an error in inundate_nation.py relating to projection CRS.

### Changes

- `tools/inundate_nation.py`: #782 CRS projection change likely causing issue with previous projection configuration

<br/><br/>

## v4.3.3.3 - 2023-03-20 - [PR#854](https://github.com/NOAA-OWP/inundation-mapping/pull/854)

At least one site (e.g. TRYM7) was not been getting mapped in Stage-Based CatFIM, despite having all of the acceptable accuracy codes. This was caused by a data type issue in the `acceptable_coord_acc_code_list` in `tools_shared_variables.py` having the accuracy codes of 5 and 1 as a strings instead of an integers.

### Changes

- `/tools/tools_shared_variables.py`: Added integers 5 and 1 to the acceptable_coord_acc_code_list, kept the '5' and '1' strings as well.

<br/><br/>

## v4.3.3.2 - 2023-03-20 - [PR#851](https://github.com/NOAA-OWP/inundation-mapping/pull/851)

Bug fix to change `.split()` to `os.path.splitext()`

### Changes

- `src/stream_branches.py`: Change 3 occurrences of `.split()` to `os.path.splitext()`

<br/><br/>

## v4.3.3.1 - 2023-03-20 - [PR#855](https://github.com/NOAA-OWP/inundation-mapping/pull/855)

Bug fix for KeyError in `src/associate_levelpaths_with_levees.py`

### Changes

- `src/associate_levelpaths_with_levees.py`: Adds check if input files exist and handles empty GeoDataFrame(s) after intersecting levee buffers with leveed areas.

<br/><br/>

## v4.3.3.0 - 2023-03-02 - [PR#831](https://github.com/NOAA-OWP/inundation-mapping/pull/831)

Addresses bug wherein multiple CatFIM sites in the flow-based service were displaying the same NWS LID. This merge also creates a workaround solution for a slowdown that was observed in the WRDS location API, which may be a temporary workaround, until WRDS addresses the slowdown.

### Changes

- `tools/generate_categorical_fim_mapping.py`: resets the list of tifs to format for each LID within the loop that does the map processing, instead of only once before the start of the loop.
- `tools/tools_shared_functions.py`:
  - adds a try-except block around code that attempted to iterate on an empty list when the API didn't return relevant metadata for a given feature ID (this is commented out, but may be used in the future once WRDS slowdown is addressed).
  - Uses a passed NWM flows geodataframe to determine stream order.
- `/tools/generate_categorical_fim_flows.py`:
  - Adds multiprocessing to flows generation and uses `nwm_flows.gpkg` instead of the WRDS API to determine stream order of NWM feature_ids.
  - Adds duration print messages.
- `/tools/generate_categorical_fim.py`:
  - Refactor to allow for new NWM filtering scheme.
  - Bug fix in multiprocessing calls for interval map production.
  - Adds duration print messages.

<br/><br/>

## v4.3.2.0 - 2023-03-15 - [PR#845](https://github.com/NOAA-OWP/inundation-mapping/pull/845)

This merge revises the methodology for masking levee-protected areas from inundation. It accomplishes two major tasks: (1) updates the procedure for acquiring and preprocessing the levee data to be burned into the DEM and (2) revises the way levee-protected areas are masked from branches.

(1) There are now going to be two different levee vector line files in each HUC. One (`nld_subset_levees_burned.gpkg`) for the levee elevation burning and one (`nld_subset_levees.gpkg`) for the levee-level-path assignment and masking workflow.

(2) Levee-protected areas are masked from inundation based on a few methods:
  - Branch 0: All levee-protected areas are masked.
  - Other branches: Levee-protected areas are masked from the DEMs of branches for level path(s) that the levee is protecting against by using single-sided buffers alongside each side of the levee to determine which side the levee is protecting against (the side opposite the associated levee-protected area).

### Additions

- `.gitignore`: Adds `.private` folder for unversioned code.
- `data/`
    - `esri.py`: Class for querying and downloading ESRI feature services.
    - `nld/`
        - `levee_download.py`: Module that handles downloading and preprocessing levee lines and protected areas from the National Levee Database.
- `src/associate_levelpaths_with_levees.py`: Associates level paths with levees using single-sided levee buffers and writes to CSV to be used by `src/mask_dem.py`

### Changes

- `.config/`
    - `deny_branch_zero.lst`: Adds `dem_meters_{}.tif`.
    - `deny_branches.lst`: Adds `levee_levelpaths.csv` and removes `nld_subset_levees_{}.tif`.
    - `deny_unit.lst`: Adds `dem_meters.tif`.
    - `params_template.env`: Adds `levee_buffer` parameter for levee buffer size/distance in meters and `levee_id_attribute`.
- `src/`
    - `bash_variables.env`: Updates `input_nld_levee_protected_areas` and adds `input_NLD` (moved from `run_unit_wb.sh`) and `input_levees_preprocessed` environment. .variables
    - `burn_in_levees.py`: Removed the unit conversion from feet to meters because it's now being done in `levee_download.py`.
    - `clip_vectors_to_wbd.py`: Added the new levee lines for the levee-level-path assignment and masking workflow.
    - `delineate_hydros_and_produce_HAND.sh`: Updates input arguments.
    - `mask_dem.py`: Updates to use `levee_levelpaths.csv` (output from `associate_levelpaths_with_levees.py`) to mask branch DEMs.
    - `run_by_branch.sh`: Clips `dem_meters.tif` to use for branches instead of `dem_meters_0.tif` since branch 0 is already masked.
    - `run_unit_wb.sh`: Added inputs to `clip_vectors_to_wbd.py`. Added `associate_levelpaths_with_levees.py`. Processes `dem_meters.tif` and then makes a copy for branch 0. Moved `deny_unit.lst` cleanup to after branch processing.

### Removals
- `data/nld/preprocess_levee_protected_areas.py`: Deprecated.

<br/><br/>

## v4.3.1.0 - 2023-03-10 - [PR#834](https://github.com/NOAA-OWP/inundation-mapping/pull/834)

Change all occurances of /data/outputs to /outputs to honor the correct volume mount directory specified when executing docker run.

### Changes

- `Dockerfile` - updated comments in relation to `projectDir=/foss_fim`
- `fim_pipeline.sh` - updated comments in relation to `projectDir=/foss_fim`
- `fim_pre_processing.sh` -updated comments in relation to `projectDir=/foss_fim`
- `fim_post_processing.sh` - updated comments in relation to `projectDir=/foss_fim`
- `README.md` - Provide documentation on starting the Docker Container, and update docs to include additional command line option for calibration database tool.

- `src/`
  - `usgs_gage_crosswalk.py` - added newline character to shorten commented example usage
  - `usgs_gage_unit_setup.py` - `/data/outputs/` => `/outputs/`

- `tools/`
  - `cache_metrics.py` -  `/data/outputs/` => `/outputs/`
  - `copy_test_case_folders.py`  - `/data/outputs/` => `/outputs/`
  - `run_test_case.py` - `/data/outputs/` => `/outputs/`

- `unit_tests/*_params.json`  - `/data/outputs/` => `/outputs/`

- `unit_tests/split_flows_test.py`  - `/data/outputs/` => `/outputs/`

<br/><br/>

## v4.3.0.1 - 2023-03-06 - [PR#841](https://github.com/NOAA-OWP/inundation-mapping/pull/841)

Deletes intermediate files generated by `src/agreedem.py` by adding them to `config/deny_*.lst`

- `config/`
    - `deny_branch_zero.lst`, `deny_branches.lst`, `deny_branch_unittests.lst`: Added `agree_binary_bufgrid.tif`, `agree_bufgrid_zerod.tif`, and `agree_smogrid_zerod.tif`
    - `deny_unit.lst`: Added `agree_binary_bufgrid.tif`, `agree_bufgrid.tif`, `agree_bufgrid_allo.tif`, `agree_bufgrid_dist.tif`,  `agree_bufgrid_zerod.tif`, `agree_smogrid.tif`, `agree_smogrid_allo.tif`, `agree_smogrid_dist.tif`, `agree_smogrid_zerod.tif`

<br/><br/>

## v4.3.0.0 - 2023-02-15 - [PR#814](https://github.com/NOAA-OWP/inundation-mapping/pull/814)

Replaces GRASS with Whitebox. This addresses several issues, including Windows permissions and GRASS projection issues. Whitebox also has a slight performance benefit over GRASS.

### Removals

- `src/r_grow_distance.py`: Deletes file

### Changes

- `Dockerfile`: Removes GRASS, update `$outputDataDir` from `/data/outputs` to `/outputs`
- `Pipfile` and `Pipfile.lock`: Adds Whitebox and removes GRASS
- `src/`
    - `agreedem.py`: Removes `r_grow_distance`; refactors to use with context and removes redundant raster reads.
    - `adjust_lateral_thalweg.py` and `agreedem.py`: Refactors to use `with` context and removes redundant raster reads
    - `unique_pixel_and_allocation.py`: Replaces GRASS with Whitebox and remove `r_grow_distance`
    - `gms/`
        - `delineate_hydros_and_produce_HAND.sh` and `run_by_unit.sh`: Removes GRASS parameter
        - `mask_dem.py`: Removes unnecessary line

<br/><br/>

## v4.2.1.0 - 2023-02-21 - [PR#829](https://github.com/NOAA-OWP/inundation-mapping/pull/829)

During the merge from remove-fim3 PR into dev, merge conflicts were discovered in the unit_tests folders and files. Attempts to fix them at that time failed, so some files were removed, other renamed, other edited to get the merge to work.  Here are the fixes to put the unit tests system back to par.

Note: some unit tests are now temporarily disabled due to dependencies on other files / folders which may not exist in other environments.

Also.. the Changelog.md was broken and is being restored here.

Also.. a minor text addition was added to the acquire_and_preprocess_3dep_dems.py files (not directly related to this PR)

For file changes directly related to unit_test folder and it's file, please see [PR#829](https://github.com/NOAA-OWP/inundation-mapping/pull/829)

Other file changes:

### Changes
- `Pipfile.lock` : rebuilt and updated as a safety pre-caution.
- `docs`
    - `CHANGELOG.md`: additions to this file for FIM 4.2.0.0 were not merged correctly.  (re-added just below in the 4.2.0.0 section)
- `data`
    - `usgs`
        - `acquire_and_preprocess_3dep_dems.py`: Added text on data input URL source.

<br/><br/>

## v4.2.0.1 - 2023-02-16 - [PR#827](https://github.com/NOAA-OWP/inundation-mapping/pull/827)

FIM 4.2.0.0. was throwing errors for 14 HUCs that did not have any level paths. These are HUCs that have only stream orders 1 and 2 and are covered under branch zero, but no stream orders 3+ (no level paths).  This has now been changed to not throw an error but continue to process of the HUC.

### Changes

- `src`
    - `run_unit_wb.sh`: Test if branch_id.lst exists, which legitimately might not. Also a bit of text cleanup.

<br/><br/>

## v4.2.0.0 - 2023-02-16 - [PR#816](https://github.com/NOAA-OWP/inundation-mapping/pull/816)

This update removes the remaining elements of FIM3 code.  It further removes the phrases "GMS" as basically the entire FIM4 model. FIM4 is GMS. With removing FIM3, it also means remove concepts of "MS" and "FR" which were no longer relevant in FIM4.  There are only a few remaining places that will continue with the phrase "GMS" which is in some inundation files which are being re-evaluated.  Some deprecated files have been removed and some subfolders removed.

There are a lot of duplicate explanations for some of the changes, so here is a shortcut system.

- desc 1:  Remove or rename values based on phrase "GMS, MS and/or FR"
- desc 2:  Moved file from the /src/gms folder to /src  or /tools/gms_tools to /tools
- desc 3:  No longer needed as we now use the `fim_pipeline.sh` processing model.

### Removals

- `data`
    - `acquire_and_preprocess_inputs.py`:  No longer needed
- `gms_pipeline.sh` : see desc 3
- `gms_run_branch.sh` : see desc 3
- `gms_run_post_processing.sh` : see desc 3
- `gms_run_unit.sh` : see desc 3
- `src`
    - `gms`
        - `init.py` : folder removed, no longer needed.
        - `aggregate_branch_lists.py`: no longer needed.  Newer version already exists in src directory.
        - `remove_error_branches.py` :  see desc 3
        - `run_by_unit.sh` : see desc 3
        - `test_new_crosswalk.sh` : no longer needed
        - `time_and_tee_run_by_branch.sh` : see desc 3
        - `time_and_tee_run_by_unit.sh` : see desc 3
    - `output_cleanup.py` : see desc 3
 - `tools/gms_tools`
     - `init.py` : folder removed, no longer needed.

### Changes

- `config`
   - `deny_branch_unittests.lst` :  renamed from `deny_gms_branch_unittests.lst`
   - `deny_branch_zero.lst` : renamed from `deny_gms_branch_zero.lst`
   - `deny_branches.lst` :  renamed from `deny_gms_branches.lst`
   - `deny_unit.lst`  : renamed from `deny_gms_unit.lst`
   - `params_template.env` : see desc 1

- `data`
    - `nws`
        - `preprocess_ahps_nws.py`:   Added deprecation note: If reused, it needs review and/or upgrades.
    - `acquire_and_preprocess_3dep_dems.py` : see desc 1
 - `fim_post_processing.sh` : see desc 1, plus a small pathing change.
 - `fim_pre_processing.sh` : see desc 1
 - ` src`
     - `add_crosswalk.py` : see desc 1. Also cleaned up some formatting and commented out a code block in favor of a better way to pass args from "__main__"
     - `bash_variables.env` : see desc 1
     - `buffer_stream_branches.py` : see desc 2
     - `clip_rasters_to_branches.py` : see desc 2
     - `crosswalk_nwm_demDerived.py` :  see desc 1 and desc 2
     - `delineate_hydros_and_produce_HAND.sh` : see desc 1 and desc 2
     - `derive_level_paths.py`  :  see desc 1 and desc 2
     - `edit_points.py` : see desc  2
     - `filter_inputs_by_huc.py`: see desc 1 and desc 2
     - `finalize_srcs.py`:  see desc 2
     - `generate_branch_list.py` : see desc 1
     - `make_rem.py` : see desc 2
     - `make_dem.py` : see desc  2
     - `outputs_cleanup.py`:  see desc 1
     - `process_branch.sh`:  see desc 1
     - `query_vectors_by_branch_polygons.py`: see desc 2
     - `reset_mannings.py` : see desc 2
     - `run_by_branch.sh`:  see desc 1
     - `run_unit_wb.sh`: see desc 1
     - `stream_branches.py`:  see desc 2
     - `subset_catch_list_by_branch_id.py`: see desc 2
     - `toDo.md`: see desc 2
     - `usgs_gage_aggregate.py`:  see desc 1
     - `usgs_gage_unit_setup.py` : see desc 1
     - `utils`
         - `fim_enums.py` : see desc 1

- `tools`
    - `combine_crosswalk_tables.py` : see desc 2
    - `compare_ms_and_non_ms_metrics.py` : see desc 2
    - `compile_comp_stats.py`: see desc 2  and added note about possible deprecation.
    - `compile_computation_stats.py` : see desc 2  and added note about possible deprecation.
    - `composite_inundation.py` : see desc 1 : note.. references a file called inundate_gms which retains it's name for now.
    - `consolidate_metrics.py`: added note about possible deprecation.
    - `copy_test_case_folders.py`: see desc 1
    - `eval_plots.py` : see desc 1
    - `evaluate_continuity.py`: see desc 2
    - `find_max_catchment_breadth.py` : see desc 2
    - `generate_categorical_fim_mapping.py` : see desc 1
    - `inundate_gms.py`: see desc 1 and desc 2. Note: This file has retained its name with the phrase "gms" in it as it might be upgraded later and there are some similar files with similar names.
    - `inundate_nation.py` : see desc 1
    - `inundation.py`:  text styling change
    - `make_boxes_from_bounds.py`: text styling change
    - `mosaic_inundation.py`:  see desc 1 and desc 2
    - `overlapping_inundation.py`: see desc 2
    - `plots.py` : see desc 2
    - `run_test_case.py`:  see desc 1
    - `synthesize_test_cases.py`: see desc 1

- `unit_tests`
    - `README.md`: see desc 1
    - `__template_unittests.py`: see desc 1
    - `check_unit_errors_params.json`  and `check_unit_errors_unittests.py` : see desc 1
    - `derive_level_paths_params.json` and `derive_level_paths_unittests.py` : see desc 1 and desc 2
    - `filter_catchments_and_add_attributes_unittests.py`: see desc 1
    - `outputs_cleanup_params.json` and `outputs_cleanup_unittests.py`: see desc 1 and desc 2
    - `split_flows_unittests.py` : see desc 1
    - `tools`
        - `inundate_gms_params.json` and `inundate_gms_unittests.py`: see desc 1 and desc 2

<br/><br/>

## v4.1.3.0 - 2023-02-13 - [PR#812](https://github.com/NOAA-OWP/inundation-mapping/pull/812)

An update was required to adjust host name when in the AWS environment

### Changes

- `fim_post_processing.sh`: Added an "if isAWS" flag system based on the input command args from fim_pipeline.sh or

- `tools/calibration-db`
    - `README.md`: Minor text correction.

<br/><br/>

## v4.1.2.0 - 2023-02-15 - [PR#808](https://github.com/NOAA-OWP/inundation-mapping/pull/808)

Add `pytest` package and refactor existing unit tests. Update parameters to unit tests (`/unit_tests/*_params.json`) to valid paths. Add leading slash to paths in `/config/params_template.env`.

### Additions

- `/unit_tests`
  - `__init__.py`  - needed for `pytest` command line executable to pick up tests.
  - `pyproject.toml`  - used to specify which warnings are excluded/filtered.
  - `/gms`
    - `__init__.py` - needed for `pytest` command line executable to pick up tests.
  - `/tools`
    - `__init__.py`  - needed for `pytest` command line executable to pick up tests.
    - `inundate_gms_params.json` - file moved up into this directory
    - `inundate_gms_test.py`     - file moved up into this directory
    - `inundation_params.json`   - file moved up into this directory
    - `inundation_test.py`       - file moved up into this directory

### Removals

- `/unit_tests/tools/gms_tools/` directory removed, and files moved up into `/unit_tests/tools`

### Changes

- `Pipfile` - updated to include pytest as a dependency
- `Pipfile.lock` - updated to include pytest as a dependency

- `/config`
  - `params_template.env` - leading slash added to paths

- `/unit_tests/` - All of the `*_test.py` files were refactored to follow the `pytest` paradigm.
  - `*_params.json` - valid paths on `fim-dev1` provided
  - `README.md`  - updated to include documentation on pytest.
  - `unit_tests_utils.py`
  - `__template_unittests.py` -> `__template.py` - exclude the `_test` suffix to remove from test suite. Updated example on new format for pytest.
  - `check_unit_errors_test.py`
  - `clip_vectors_to_wbd_test.py`
  - `filter_catchments_and_add_attributes_test.py`
  - `rating_curve_comparison_test.py`
  - `shared_functions_test.py`
  - `split_flow_test.py`
  - `usgs_gage_crosswalk_test.py`
  - `aggregate_branch_lists_test.py`
  - `generate_branch_list_test.py`
  - `generate_branch_list_csv_test.py`
  - `aggregate_branch_lists_test.py`
  - `generate_branch_list_csv_test.py`
  - `generate_branch_list_test.py`
    - `/gms`
      - `derive_level_paths_test.py`
      - `outputs_cleanup_test.py`
    - `/tools`
      - `inundate_unittests.py` -> `inundation_test.py`
      - `inundate_gms_test.py`


<br/><br/>

## v4.1.1.0 - 2023-02-16 - [PR#809](https://github.com/NOAA-OWP/inundation-mapping/pull/809)

The CatFIM code was updated to allow 1-foot interval processing across all stage-based AHPS sites ranging from action stage to 5 feet above major stage, along with restart capability for interrupted processing runs.

### Changes

- `tools/generate_categorical_fim.py` (all changes made here)
    - Added try-except blocks for code that didn't allow most sites to actually get processed because it was trying to check values of some USGS-related variables that most of the sites didn't have
    - Overwrite abilities of the different outputs for the viz team were not consistent (i.e., one of the files had the ability to be overwritten but another didn't), so that has been made consistent to disallow any overwrites of the existing final outputs for a specified output folder.
    - The code also has the ability to restart from an interrupted run and resume processing uncompleted HUCs by first checking for a simple "complete" file for each HUC. If a HUC has that file, then it is skipped (because it already completed processing during a run for a particular output folder / run name).
    - When a HUC is successfully processed, an empty "complete" text file is created / touched.

<br/><br/>

## v4.1.0.0 - 2023-01-30 - [PR#806](https://github.com/NOAA-OWP/inundation-mapping/pull/806)

As we move to Amazon Web Service, AWS, we need to change our processing system. Currently, it is `gms_pipeline.sh` using bash "parallel" as an iterator which then first processes all HUCs, but not their branches. One of `gms_pipeline.sh`'s next steps is to do branch processing which is again iterated via "parallel". AKA. Units processed as one step, branches processed as second independent step.

**Note:** While we are taking steps to move to AWS, we will continue to maintain the ability of doing all processing on a single server using a single docker container as we have for a long time. Moving to AWS is simply taking portions of code from FIM and adding it to AWS tools for performance of large scale production runs.

Our new processing system, starting with this PR,  is to allow each HUC to process it's own branches.

A further requirement was to split up the overall processing flow to independent steps, with each step being able to process itself without relying on "export" variables from other files. Note: There are still a few exceptions.  The basic flow now becomes
- `fim_pre_processing.sh`,
- one or more calls to `fim_process_unit_wb.sh` (calling this file for each single HUC to be processed).
- followed by a call to `fim_post_processing.sh`.


Note: This is a very large, complex PR with alot of critical details. Please read the details at [PR 806](https://github.com/NOAA-OWP/inundation-mapping/pull/806).

### CRITICAL NOTE
The new `fim_pipeline.sh` and by proxy `fim_pre_processing.sh` has two new key input args, one named **-jh** (job HUCs) and one named **-jb** (job branches).  You can assign the number of cores/CPU's are used for processing a HUC versus the number of branches.  For the -jh number arg, it only is used against the `fim_pipeline.sh` file when it is processing more than one HUC or a list of HUCs as it is the iterator for HUCs.   The -jb flag says how many cores/CPU's can be used when processing branches (note.. the average HUC has 26 branches).

BUT.... you have to be careful not to overload your system.  **You need to multiply the -jh and the -jb values together, but only when using the `fim_pipeline.sh` script.**  Why? _If you have 16 CPU's available on your machine, and you assign -jh as 10 and -jb as 26, you are actually asking for 126 cores (10 x 26) but your machine only has 16 cores._   If you are not using `fim_pipeline.sh` but using the three processing steps independently, then the -jh value has not need to be anything but the number of 1 as each actual HUC can only be processed one at a time. (aka.. no iterator).
</br>

### Additions

- `fim_pipeline.sh` :  The wrapper for the three new major "FIM" processing steps. This script allows processing in one command, same as the current tool of `gms_pipeline.sh`.
- `fim_pre_processing.sh`: This file handles all argument input from the user, validates those inputs and sets up or cleans up folders. It also includes a new system of taking most input parameters and some key enviro variables and writing them out to a files called `runtime_args.env`.  Future processing steps need minimal input arguments as it can read most values it needs from this new `runtime_args.env`. This allows the three major steps to work independently from each other. Someone can now come in, run `fim_pre_processing.sh`, then run `fim_process_unit_wb.sh`, each with one HUC, as many time as they like, each adding just its own HUC folder to the output runtime folder.
- `fim_post_processing.sh`: Scans all HUC folders inside the runtime folders to handle a number of processing steps which include (to name a few):
    - aggregating errors
    - aggregating to create a single list (gms_inputs.csv) for all valid HUCs and their branch ids
    - usgs gage aggregation
    - adjustments to SRV's
    - and more
- `fim_process_unit_wb.sh`: Accepts only input args of runName and HUC number. It then sets up global variable, folders, etc to process just the one HUC. The logic for processing the HUC is in `run_unit_wb.sh` but managed by this `fim_process_unit_wb.sh` file including all error trapping.
- `src`
    - `aggregate_branch_lists.py`:  When each HUC is being processed, it creates it's own .csv file with its branch id's. In post processing we need one master csv list and this file aggregates them. Note: This is a similar file already in the `src/gms` folder but that version operates a bit different and will be deprecated soon.
    - `generate_branch_list.py`: This creates the single .lst for a HUC defining each branch id. With this list, `run_unit_wb.sh` can do a parallelized iteration over each of its branches for processing. Note: This is also similar to the current `src/gms` file of the same name and the gms folder version will also be deprecated soon.
    - `generate_branch_list_csv.py`. As each branch, including branch zero, has processed and if it was successful, it will add to a .csv list in the HUC directory. At the end, it becomes a list of all successful branches. This file will be aggregates with all similar .csv in post processing for future processing.
    - `run_unit_wb.sh`:  The actual HUC processing logic. Note: This is fundamentally the same as the current HUC processing logic that exists currently in `src/gms/run_by_unit.sh`, which will be removed in the very near future. However, at the end of this file, it creates and manages a parallelized iterator for processing each of it's branches.
    - `process_branch.sh`:  Same concept as `process_unit_wb.sh` but this one is for processing a single branch. This file manages the true branch processing file of `src/gms/run_by_branch.sh`.  It is a wrapper file to `src/gms/run_by_branch.sh` and catches all error and copies error files as applicable. This allows the parent processing files to continue despite branch errors. Both the new fim processing system and the older gms processing system currently share the branch processing file of `src/gms/run_by_branch.sh`. When the gms processing file is removed, this file will likely not change, only moved one directory up and be no longer in the `gms` sub-folder.
- `unit_tests`
    - `aggregate_branch_lists_unittests.py' and `aggregate_branch_lists_params.json`  (based on the newer `src` directory edition of `aggregate_branch_lists.py`).
    - `generate_branch_list_unittest.py` and `generate_branch_list_params.json` (based on the newer `src` directory edition of `generate_branch_list.py`).
    -  `generate_branch_list_csv_unittest.py` and `generate_branch_list_csv_params.json`

### Changes

- `config`
    - `params_template.env`: Removed the `default_max_jobs` value and moved the `startDiv` and `stopDiv` to the `bash_variables.env` file.
    - `deny_gms_unit.lst` : Renamed from `deny_gms_unit_prod.lst`
    - `deny_gms_branches.lst` : Renamed from `deny_gms_branches_prod.lst`

- `gms_pipeline.sh`, `gms_run_branch.sh`, `gms_run_unit.sh`, and `gms_post_processing.sh` :  Changed to hardcode the `default_max_jobs` to the value of 1. (we don't want this to be changed at all). They were also changed for minor adjustments for the `deny` list files names.

- `src`
    - `bash_functions.env`: Fix error with calculating durations.
    - `bash_variables.env`:  Adds the two export lines (stopDiv and startDiv) from `params_template.env`
    - `clip_vectors_to_wbd.py`: Cleaned up some print statements for better output traceability.
    - `check_huc_inputs.py`: Added logic to ensure the file was an .lst file. Other file formats were not be handled correctly.
    - `gms`
        - `delineate_hydros_and_produce_HAND.sh`: Removed all `stopDiv` variable to reduce log and screen output.
        - `run_by_branch.sh`: Removed an unnecessary test for overriding outputs.

### Removed

- `config`
    - `deny_gms_branches_dev.lst`

<br/><br/>

## v4.0.19.5 - 2023-01-24 - [PR#801](https://github.com/NOAA-OWP/inundation-mapping/pull/801)

When running tools/test_case_by_hydroid.py, it throws an error of local variable 'stats' referenced before assignment.

### Changes

- `tools`
    - `pixel_counter.py`: declare stats object and remove the GA_Readonly flag
    - `test_case_by_hydroid_id_py`: Added more logging.

<br/><br/>

## v4.0.19.4 - 2023-01-25 - [PR#802](https://github.com/NOAA-OWP/inundation-mapping/pull/802)

This revision includes a slight alteration to the filtering technique used to trim/remove lakeid nwm_reaches that exist at the upstream end of each branch network. By keeping a single lakeid reach at the branch level, we can avoid issues with the branch headwater point starting at a lake boundary. This ensures the headwater catchments for some branches are properly identified as a lake catchment (no inundation produced).

### Changes

- `src/gms/stream_branches.py`: New changes to the `find_upstream_reaches_in_waterbodies` function: Added a step to create a list of nonlake segments (lakeid = -9999) . Use the list of nonlake reaches to allow the filter to keep a the first lakeid reach that connects to a nonlake segment.

<br/><br/>

## v4.0.19.3 - 2023-01-17 - [PR#794](https://github.com/NOAA-OWP/inundation-mapping/pull/794)

Removing FIM3 files and references.  Anything still required for FIM 3 are held in the dev-fim3 branch.

### Removals

- `data`
    - `preprocess_rasters.py`: no longer valid as it is for NHD DEM rasters.
- `fim_run.sh`
- ` src`
    - `aggregate_fim_outputs.sh`
    - `fr_to_ms_raster.mask.py`
    - `get_all_huc_in_inputs.py`
    - `reduce_nhd_stream_density.py`
    - `rem.py`:  There are two files named `rem.py`, one in the src directory and one in the gms directory. This version in the src directory is no longer valid. The `rem.py` in the gms directory is being renamed to avoid future enhancements of moving files.
    - `run_by_unit.sh`:  There are two files named `run_by_unit.sh`, one in the src directory and one in the gms directory. This version in the src directory is for fim3. For the remaining `run_by_unit.sh`, it is NOT being renamed at this time as it will likely be renamed in the near future.
    - `time_and_tee_run_by_unit.sh`:  Same not as above for `run_by_unit.sh`.
    - `utils`
        - `archive_cleanup.py`
 - `tools`
     - `compare_gms_srcs_to_fr.py`
     - `preprocess_fimx.py`

### Changes

- `src`
    - `adjust_headwater_streams.py`: Likely deprecated but kept for safety reason. Deprecation note added.
- `tools`
    - `cygnss_preprocess.py`: Likely deprecated but kept for safety reason. Deprecation note added.
    - `nesdis_preprocess.py`: Likely deprecated but kept for safety reason. Deprecation note added.

<br/><br/>

## v4.0.19.2 - 2023-01-17 - [PR#797](https://github.com/NOAA-OWP/inundation-mapping/pull/797)

Consolidates global bash environment variables into a new `src/bash_variables.env` file. Additionally, Python environment variables have been moved into this file and `src/utils/shared_variables.py` now references this file. Hardcoded projections have been replaced by an environment variable. This also replaces the Manning's N file in `config/params_template.env` with a constant and updates relevant code. Unused environment variables have been removed.

### Additions

- `src/bash_variables.env`: Adds file for global environment variables

### Removals

- `config/`
    - `mannings_default.json`
    - `mannings_default_calibrated.json`

### Changes

- `config/params_template.env`: Changes manning_n from filename to default value of 0.06
- `gms_run_branch.sh`: Adds `bash_variables.env`
- `gms_run_post_processing.sh`: Adds `bash_variables.env` and changes projection from hardcoded to environment variable
- `gms_run_unit.sh`: Adds `bash_variables.env`
- `src/`
    - `add_crosswalk.py`: Assigns default manning_n value and removes assignments by stream orders
    - `aggregate_vector_inputs.py`: Removes unused references to environment variables and function
    - `gms/run_by_unit.sh`: Removes environment variable assignments and uses projection from environment variables
    - `utils/shared_variables.py`: Removes environment variables and instead references src/bash_variables.env

<br/><br/>

## v4.0.19.1 - 2023-01-17 - [PR#796](https://github.com/NOAA-OWP/inundation-mapping/pull/796)

### Changes

- `tools/gms_tools/combine_crosswalk_tables.py`: Checks length of dataframe list before concatenating

<br/><br/>

## v4.0.19.0 - 2023-01-06 - [PR#782](https://github.com/NOAA-OWP/inundation-mapping/pull/782)

Changes the projection of HAND processing to EPSG 5070.

### Changes

- `gms_run_post_processing.sh`: Adds target projection for `points`
- `data/nld/preprocess_levee_protected_areas.py`: Changed to use `utils.shared_variables.DEFAULT_FIM_PROJECTION_CRS`
- `src/`
    - `clip_vectors_to_wbd.py`: Save intermediate outputs in EPSG:5070
    - `src_adjust_spatial_obs.py`: Changed to use `utils.shared_variables.DEFAULT_FIM_PROJECTION_CRS`
    - `utils/shared_variables.py`: Changes the designated projection variables
    - `gms/`
        - `stream_branches.py`: Checks the projection of the input streams and changes if necessary
        - `run_by_unit.py`: Changes the default projection crs variable and added as HUC target projection
- `tools/inundate_nation.py`: Changed to use `utils.shared_variables.PREP_PROJECTION`

<br/><br/>

## v4.0.18.2 - 2023-01-11 - [PR#790](https://github.com/NOAA-OWP/inundation-mapping/pull/790)

Remove Great Lakes clipping

### Changes

- `src/`
    - `clip_vectors_to_wbd.py`: Removes Great Lakes clipping and references to Great Lakes polygons and lake buffer size

    - `gms/run_by_unit.sh`: Removes Great Lakes polygon and lake buffer size arguments to `src/clip_vectors_to_wbd.py`

<br/><br/>

## v4.0.18.1 - 2022-12-13 - [PR #760](https://github.com/NOAA-OWP/inundation-mapping/pull/760)

Adds stacked bar eval plots.

### Additions

- `/tools/eval_plots_stackedbar.py`: produces stacked bar eval plots in the same manner as `eval_plots.py`.

<br/><br/>

## v4.0.18.0 - 2023-01-03 - [PR#780](https://github.com/NOAA-OWP/inundation-mapping/pull/780)

Clips WBD and stream branch buffer polygons to DEM domain.

### Changes

- `src/`
    - `clip_vectors_to_wbd.py`: Clips WBD polygon to DEM domain

    - `gms/`
        - `buffer_stream_branches.py`: Clips branch buffer polygons to DEM domain
        - `derive_level_paths.py`: Stop processing if no branches exist
        - `mask_dem.py`: Checks if stream file exists before continuing
        - `remove_error_branches.py`: Checks if error_branches has data before continuing
        - `run_by_unit.sh`: Adds DEM domain as bash variable and adds it as an argument to calling `clip_vectors_to_wbd.py` and `buffer_stream_branches.py`

<br/><br/>


## v4.0.17.4 - 2023-01-06 - [PR#781](https://github.com/NOAA-OWP/inundation-mapping/pull/781)

Added crosswalk_table.csv from the root output folder as being a file push up to Hydrovis s3 bucket after FIM BED runs.

### Changes

- `config`
    - `aws_s3_put_fim4_hydrovis_whitelist.lst`:  Added crosswalk_table.csv to whitelist.


<br/><br/>

## v4.0.17.3 - 2022-12-23 - [PR#773](https://github.com/NOAA-OWP/inundation-mapping/pull/773)

Cleans up REM masking of levee-protected areas and fixes associated error.

### Removals

- `src/gms/`
    - `delineate_hydros_and_produce_HAND.sh`: removes rasterization and masking of levee-protected areas from the REM
    - `rasterize_by_order`: removes this file
- `config/`
    - `deny_gms_branch_zero.lst`, `deny_gms_branches_dev.lst`, and `deny_gms_branches_prod.lst`: removes `LeveeProtectedAreas_subset_{}.tif`

### Changes

- `src/gms/rem.py`: fixes an error where the nodata value of the DEM was overlooked

<br/><br/>

## v4.0.17.2 - 2022-12-29 - [PR #779](https://github.com/NOAA-OWP/inundation-mapping/pull/779)

Remove dependency on `other` folder in `test_cases`. Also updates ESRI and QGIS agreement raster symbology label to include the addition of levee-protected areas as a mask.

### Removals

- `tools/`
    - `aggregate_metrics.py` and `cache_metrics.py`: Removes reference to test_cases/other folder

### Changes

- `config/symbology/`
    - `esri/agreement_raster.lyr` and `qgis/agreement_raster.qml`: Updates label from Waterbody mask to Masked since mask also now includes levee-protected areas
- `tools/`
    - `eval_alt_catfim.py` and `run_test_case.py`: Updates waterbody mask to dataset located in /inputs folder

<br/><br/>

## v4.0.17.1 - 2022-12-29 - [PR #778](https://github.com/NOAA-OWP/inundation-mapping/pull/778)

This merge fixes a bug where all of the Stage-Based intervals were the same.

### Changes
- `/tools/generate_categorical_fim.py`: Changed `stage` variable to `interval_stage` variable in `produce_stage_based_catfim_tifs` function call.

<br/><br/>

## v4.0.17.0 - 2022-12-21 - [PR #771](https://github.com/NOAA-OWP/inundation-mapping/pull/771)

Added rysnc to docker images. rysnc can now be used inside the images to move data around via docker mounts.

### Changes

- `Dockerfile` : added rsync

<br/><br/>

## v4.0.16.0 - 2022-12-20 - [PR #768](https://github.com/NOAA-OWP/inundation-mapping/pull/768)

`gms_run_branch.sh` was processing all of the branches iteratively, then continuing on to a large post processing portion of code. That has now be split to two files, one for branch iteration and the other file for just post processing.

Other minor changes include:
- Removing the system where a user could override `DropStreamOrders` where they could process streams with stream orders 1 and 2 independently like other GMS branches.  This option is now removed, so it will only allow stream orders 3 and higher as gms branches and SO 1 and 2 will always be in branch zero.

- The `retry` flag on the three gms*.sh files has been removed. It did not work correctly and was not being used. Usage of it would have created unreliable results.

### Additions

- `gms_run_post_processing.sh`
   - handles all tasks from after `gms_run_branch.sh` to this file, except for output cleanup, which stayed in `gms_run_branch.sh`.
   - Can be run completely independent from `gms_run_unit.sh` or gms_run_branch.sh` as long as all of the files are in place. And can be re-run if desired.

### Changes

- `gms_pipeline.sh`
   - Remove "retry" system.
   - Remove "dropLowStreamOrders" system.
   - Updated for newer reusable output date/time/duration system.
   - Add call to new `gms_run_post_processing.sh` file.

- `gms_run_branch.sh`
   - Remove "retry" system.
   - Remove "dropLowStreamOrders" system.
   - Updated for newer reusable output date/time/duration system.
   - Removed most code from below the branch iterator to the new `gms_run_post_processing.sh` file. However, it did keep the branch files output cleanup and non-zero exit code checking.

- `gms_run_unit.sh`
   - Remove "retry" system.
   - Remove "dropLowStreamOrders" system.
   - Updated for newer reusable output date/time/duration system.

- `src`
    - `bash_functions.env`:  Added a new method to make it easier / simpler to calculation and display duration time.
    - `filter_catchments_and_add_attributes.py`:  Remove "dropLowStreamOrders" system.
    - `split_flows.py`: Remove "dropLowStreamOrders" system.
    - `usgs_gage_unit_setup.py`:  Remove "dropLowStreamOrders" system.

- `gms`
    - `delineate_hydros_and_produced_HAND.sh` : Remove "dropLowStreamOrders" system.
    - `derive_level_paths.py`: Remove "dropLowStreamOrders" system and some small style updates.
    - `run_by_unit.sh`: Remove "dropLowStreamOrders" system.

- `unit_tests/gms`
    - `derive_level_paths_params.json` and `derive_level_paths_unittests.py`: Remove "dropLowStreamOrders" system.

<br/><br/>

## v4.0.15.0 - 2022-12-20 - [PR #758](https://github.com/NOAA-OWP/inundation-mapping/pull/758)

This merge addresses feedback received from field users regarding CatFIM. Users wanted a Stage-Based version of CatFIM, they wanted maps created for multiple intervals between flood categories, and they wanted documentation as to why many sites are absent from the Stage-Based CatFIM service. This merge seeks to address this feedback. CatFIM will continue to evolve with more feedback over time.

## Changes
- `/src/gms/usgs_gage_crosswalk.py`: Removed filtering of extra attributes when writing table
- `/src/gms/usgs_gage_unit_setup.py`: Removed filter of gages where `rating curve == yes`. The filtering happens later on now.
- `/tools/eval_plots.py`: Added a post-processing step to produce CSVs of spatial data
- `/tools/generate_categorical_fim.py`:
  - New arguments to support more advanced multiprocessing, support production of Stage-Based CatFIM, specific output directory pathing, upstream and downstream distance, controls on how high past "major" magnitude to go when producing interval maps for Stage-Based, the ability to run a single AHPS site.
- `/tools/generate_categorical_fim_flows.py`:
  - Allows for flows to be retrieved for only one site (useful for testing)
  - More logging
  - Filtering stream segments according to stream order
- `/tools/generate_categorical_fim_mapping.py`:
  - Support for Stage-Based CatFIM production
  - Enhanced multiprocessing
  - Improved post-processing
- `/tools/pixel_counter.py`: fixed a bug where Nonetypes were being returned
- `/tools/rating_curve_get_usgs_rating_curves.py`:
  - Removed filtering when producing `usgs_gages.gpkg`, but adding attribute as to whether or not it meets acceptance criteria, as defined in `gms_tools/tools_shared_variables.py`.
  - Creating a lookup list to filter out unacceptable gages before they're written to `usgs_rating_curves.csv`
  - The `usgs_gages.gpkg` now includes two fields indicating whether or not gages pass acceptance criteria (defined in `tools_shared_variables.py`. The fields are `acceptable_codes` and `acceptable_alt_error`
- `/tools/tools_shared_functions.py`:
  - Added `get_env_paths()` function to retrieve environmental variable information used by CatFIM and rating curves scripts
  - `Added `filter_nwm_segments_by_stream_order()` function that uses WRDS to filter out NWM feature_ids from a list if their stream order is different than a desired stream order.
- `/tools/tools_shared_variables.py`: Added the acceptance criteria and URLS for gages as non-constant variables. These can be modified and tracked through version changes. These variables are imported by the CatFIM and USGS rating curve and gage generation scripts.
- `/tools/test_case_by_hydroid.py`: reformatting code, recommend adding more comments/docstrings in future commit

<br/><br/>

## v4.0.14.2 - 2022-12-22 - [PR #772](https://github.com/NOAA-OWP/inundation-mapping/pull/772)

Added `usgs_elev_table.csv` to hydrovis whitelist files.  Also updated the name to include the word "hydrovis" in them (anticipating more s3 whitelist files).

### Changes

- `config`
    - `aws_s3_put_fim4_hydrovis_whitelist.lst`:  File name updated and added usgs_elev_table.csv so it gets push up as well.
    - `aws_s3_put_fim3_hydrovis_whitelist.lst`: File name updated

- `data/aws`
   - `s3.py`: added `/foss_fim/config/aws_s3_put_fim4_hydrovis_whitelist.lst` as a default to the -w param.

<br/><br/>

## v4.0.14.1 - 2022-12-03 - [PR #753](https://github.com/NOAA-OWP/inundation-mapping/pull/753)

Creates a polygon of 3DEP DEM domain (to eliminate errors caused by stream networks with no DEM data in areas of HUCs that are outside of the U.S. border) and uses the polygon layer to clip the WBD and stream network (to a buffer inside the WBD).

### Additions
- `data/usgs/acquire_and_preprocess_3dep_dems.py`: Adds creation of 3DEP domain polygon by polygonizing all HUC6 3DEP DEMs and then dissolving them.
- `src/gms/run_by_unit.sh`: Adds 3DEP domain polygon .gpkg as input to `src/clip_vectors_to_wbd.py`

### Changes
- `src/clip_vectors_to_wbd.py`: Clips WBD to 3DEP domain polygon and clips streams to a buffer inside the clipped WBD polygon.

<br/><br/>

## v4.0.14.0 - 2022-12-20 - [PR #769](https://github.com/NOAA-OWP/inundation-mapping/pull/769)

Masks levee-protected areas from the DEM in branch 0 and in highest two stream order branches.

### Additions

- `src/gms/`
    - `mask_dem.py`: Masks levee-protected areas from the DEM in branch 0 and in highest two stream order branches
    - `delineate_hydros_and_produce_HAND.sh`: Adds `src/gms/mask_dem.py`

<br/><br/>

## v4.0.13.2 - 2022-12-20 - [PR #767](https://github.com/NOAA-OWP/inundation-mapping/pull/767)

Fixes inundation of nodata areas of REM.

### Changes

- `tools/inundation.py`: Assigns depth a value of `0` if REM is less than `0`

<br/><br/>

## v4.0.13.1 - 2022-12-09 - [PR #743](https://github.com/NOAA-OWP/inundation-mapping/pull/743)

This merge adds the tools required to generate Alpha metrics by hydroid. It summarizes the Apha metrics by branch 0 catchment for use in the Hydrovis "FIM Performance" service.

### Additions

- `pixel_counter.py`:  A script to perform zonal statistics against raster data and geometries
- `pixel_counter_functions.py`: Supporting functions
- `pixel_counter_wrapper.py`: a script that wraps `pixel_counter.py` for batch processing
- `test_case_by_hydroid.py`: the main script to orchestrate the generation of alpha metrics by catchment

<br/><br/>

## v4.0.13.0 - 2022-11-16 - [PR #744](https://github.com/NOAA-OWP/inundation-mapping/pull/744)

Changes branch 0 headwaters data source from NHD to NWS to be consistent with branches. Removes references to NHD flowlines and headwater data.

### Changes

- `src/gms/derive_level_paths.py`: Generates headwaters before stream branch filtering

### Removals

- Removes NHD flowlines and headwater references from `gms_run_unit.sh`, `config/deny_gms_unit_prod.lst`, `src/clip_vectors_to_wbd.py`, `src/gms/run_by_unit.sh`, `unit_tests/__template_unittests.py`, `unit_tests/clip_vectors_to_wbd_params.json`, and `unit_tests/clip_vectors_to_wbd_unittests.py`

<br/><br/>

## V4.0.12.2 - 2022-12-04 - [PR #754](https://github.com/NOAA-OWP/inundation-mapping/pull/754)

Stop writing `gms_inputs_removed.csv` if no branches are removed with Error status 61.

### Changes

- `src/gms/remove_error_branches.py`: Checks if error branches is not empty before saving gms_inputs_removed.csv

<br/><br/>

## v4.0.12.1 - 2022-11-30 - [PR #751](https://github.com/NOAA-OWP/inundation-mapping/pull/751)

Updating a few deny list files.

### Changes

- `config`:
    - `deny_gms_branches_dev.lst`, `deny_gms_branches_prod.lst`, and `deny_gms_unit_prod.lst`

<br/><br/>


## v4.0.12.0 - 2022-11-28 - [PR #736](https://github.com/NOAA-OWP/inundation-mapping/pull/736)

This feature branch introduces a new methodology for computing Manning's equation for the synthetic rating curves. The new subdivision approach 1) estimates bankfull stage by crosswalking "bankfull" proxy discharge data to the raw SRC discharge values 2) identifies in-channel vs. overbank geometry values 3) applies unique in-channel and overbank Manning's n value (user provided values) to compute Manning's equation separately for channel and overbank discharge and adds the two components together for total discharge 4) computes a calibration coefficient (where benchmark data exists) that applies to the  calibrated total discharge calculation.

### Additions

- `src/subdiv_chan_obank_src.py`: new script that performs all subdiv calculations and then produce a new (modified) `hydroTable.csv`. Inputs include `src_full_crosswalked.csv` for each huc/branch and a Manning's roughness csv file (containing: featureid, channel n, overbank n; file located in the `/inputs/rating_curve/variable_roughness/`). Note that the `identify_src_bankfull.py` script must be run prior to running the subdiv workflow.

### Changes

- `config/params_template.env`: removed BARC and composite roughness parameters; added new subdivision parameters; default Manning's n file set to `mannings_global_06_12.csv`
- `gms_run_branch.sh`: moved the PostgreSQL database steps to occur immediately before the SRC calibration steps; added new subdivision step; added condition to SRC calibration to ensure subdivision routine is run
- `src/add_crosswalk.py`: removed BARC function call; update placeholder value list (removed BARC and composite roughness variables) - these placeholder variables ensure that all hydrotables have the same dimensions
- `src/identify_src_bankfull.py`: revised FIM3 starting code to work with FIM4 framework; stripped out unnecessary calculations; restricted bankfull identification to stage values > 0
- `src/src_adjust_spatial_obs.py`: added huc sort function to help user track progress from console outputs
- `src/src_adjust_usgs_rating.py`: added huc sort function to help user track progress from console outputs
- `src/src_roughness_optimization.py`: reconfigured code to compute a calibration coefficient and apply adjustments using the subdivision variables; renamed numerous variables; simplified code where possible
- `src/utils/shared_variables.py`: increased `ROUGHNESS_MAX_THRESH` from 0.6 to 0.8
- `tools/vary_mannings_n_composite.py`: *moved this script from /src to /tools*; updated this code from FIM3 to work with FIM4 structure; however, it is not currently implemented (the subdivision routine replaces this)
- `tools/aggregate_csv_files.py`: helper tool to search for csv files by name/wildcard and concatenate all found files into one csv (used for aggregating previous calibrated roughness values)
- `tools/eval_plots.py`: updated list of metrics to plot to also include equitable threat score and mathews correlation coefficient (MCC)
- `tools/synthesize_test_cases.py`: updated the list of FIM version metrics that the `PREV` flag will use to create the final aggregated metrics csv; this change will combine the dev versions provided with the `-dc` flag along with the existing `previous_fim_list`

<br/><br/>

## v4.0.11.5 - 2022-11-18 - [PR #746](https://github.com/NOAA-OWP/inundation-mapping/pull/746)

Skips `src/usgs_gage_unit_setup.py` if no level paths exist. This may happen if a HUC has no stream orders > 2. This is a bug fix for #723 for the case that the HUC also has USGS gages.

### Changes

- `src/gms/run_by_unit.sh`: Adds check for `nwm_subset_streams_levelPaths.gpkg` before running `usgs_gage_unit_setup.py`

<br/><br/>

## v4.0.11.4 - 2022-10-12 - [PR #709](https://github.com/NOAA-OWP/inundation-mapping/pull/709)

Adds capability to produce single rating curve comparison plots for each gage.

### Changes

- `tools/rating_curve_comparison.py`
    - Adds generate_single_plot() to make a single rating curve comparison plot for each gage in a given HUC
    - Adds command line switch to generate single plots

<br/><br/>

## v4.0.11.3 - 2022-11-10 - [PR #739](https://github.com/NOAA-OWP/inundation-mapping/pull/739)

New tool with instructions of downloading levee protected areas and a tool to pre-process it, ready for FIM.

### Additions

- `data`
    - `nld`
         - `preprocess_levee_protected_areas.py`:  as described above

### Changes

- `data`
     - `preprocess_rasters.py`: added deprecation note. It will eventually be replaced in it's entirety.
- `src`
    - `utils`
        - `shared_functions.py`: a few styling adjustments.

<br/><br/>

## v4.0.11.2 - 2022-11-07 - [PR #737](https://github.com/NOAA-OWP/inundation-mapping/pull/737)

Add an extra input args to the gms_**.sh files to allow for an override of the branch zero deny list, same as we can do with the unit and branch deny list overrides. This is needed for debugging purposes.

Also, if there is no override for the deny branch zero list and is not using the word "none", then use the default or overridden standard branch deny list.  This will keep the branch zero's and branch output folders similar but not identical for outputs.

### Changes

- `gms_pipeline.sh`:  Add new param to allow for branch zero deny list override. Plus added better logic for catching bad deny lists earlier.
- `gms_run_branch.sh`:  Add new param to allow for branch zero deny list override.  Add logic to cleanup all branch zero output folders with the default branch deny list (not the branch zero list), UNLESS an override exists for the branch zero deny list.
- `gms_run_unit.sh`: Add new param to allow for branch zero deny list override.
- `config`
    - `deny_gms_branch_zero.lst`: update to keep an additional file in the outputs.
- `src`
    - `output_cleanup.py`: added note saying it is deprecated.
    - `gms`
        - `run_by_branch.sh`: variable name change (matching new names in related files for deny lists)
        - `run_by_unit.sh`: Add new param to allow for branch zero deny list override.

<br/><br/>

## v4.0.11.1 - 2022-11-01 - [PR #732](https://github.com/NOAA-OWP/inundation-mapping/pull/732)

Due to a recent IT security scan, it was determined that Jupyter-core needed to be upgraded.

### Changes

- `Pipfile` and `Pipfile.lock`:  Added a specific version of Jupyter Core that is compliant with IT.

<br/><br/>

## v4.0.11.0 - 2022-09-21 - [PR #690](https://github.com/NOAA-OWP/inundation-mapping/pull/690)

Masks levee-protected areas from Relative Elevation Model if branch 0 or if branch stream order exceeds a threshold.

### Additions

- `src/gms/`
   - `delineate_hydros_and_produce_HAND.sh`
      - Reprojects and creates HUC-level raster of levee-protected areas from polygon layer
      - Uses that raster to mask/remove those areas from the Relative Elevation Model
   - `rasterize_by_order.py`: Subsets levee-protected area branch-level raster if branch 0 or if order exceeds a threshold (default threshold: max order - 1)
- `config/`
   - `deny_gms_branches_default.lst`, and `deny_gms_branches_min.lst`: Added LeveeProtectedAreas_subset_{}.tif
   - `params_template.env`: Adds mask_leveed_area_toggle

### Changes

- `src/gms/delineate_hydros_and_produce_HAND.sh`: Fixes a bug in ocean/Great Lakes masking
- `tools/`
    - `eval_alt_catfim.py` and `run_test_case.py`: Changes the levee mask to the updated inputs/nld_vectors/Levee_protected_areas.gpkg

<br/><br/>

## v4.0.10.5 - 2022-10-21 - [PR #720](https://github.com/NOAA-OWP/inundation-mapping/pull/720)

Earlier versions of the acquire_and_preprocess_3dep_dems.py did not have any buffer added when downloading HUC6 DEMs. This resulted in 1 pixel nodata gaps in the final REM outputs in some cases at HUC8 sharing a HUC6 border. Adding the param of cblend 6 to the gdalwarp command meant put a 6 extra pixels all around perimeter. Testing showed that 6 pixels was plenty sufficient as the gaps were never more than 1 pixel on borders of no-data.

### Changes

- `data`
    - `usgs`
        - `acquire_and_preprocess_3dep_dems.py`: Added the `cblend 6` param to the gdalwarp call for when the dem is downloaded from USGS.
    - `create_vrt_file.py`:  Added sample usage comment.
 - `src`
     - `gms`
         `run_by_unit.sh`: Added a comment about gdal as it relates to run_by_unit.

Note: the new replacement inputs/3dep_dems/10m_5070/ files can / will be copied before PR approval as the true fix was replacment DEM's. There is zero risk of overwriting prior to code merge.

<br/><br/>

## v4.0.10.4 - 2022-10-27 - [PR #727](https://github.com/NOAA-OWP/inundation-mapping/pull/727)

Creates a single crosswalk table containing HUC (huc8), BranchID, HydroID, feature_id (and optionally LakeID) from branch-level hydroTables.csv files.

### Additions

- `tools/gms_tools/combine_crosswalk_tables.py`: reads and concatenates hydroTable.csv files, writes crosswalk table
- `gms_run_branch.sh`: Adds `tools/gms_tools/make_complete_hydrotable.py` to post-processing

<br/><br/>

## v4.0.10.3 - 2022-10-19 - [PR #718](https://github.com/NOAA-OWP/inundation-mapping/pull/718)

Fixes thalweg notch by clipping upstream ends of the stream segments to prevent the stream network from reaching the edge of the DEM and being treated as outlets when pit filling the burned DEM.

### Changes

- `src/clip_vectors_to_wbd.py`: Uses a slightly smaller buffer than wbd_buffer (wbd_buffer_distance-2*(DEM cell size)) to clip stream network inside of DEM extent.

<br/><br/>

## v4.0.10.2 - 2022-10-24 - [PR #723](https://github.com/NOAA-OWP/inundation-mapping/pull/723)

Runs branch 0 on HUCs with no other branches remaining after filtering stream orders if `drop_low_stream_orders` is used.

### Additions

- `src/gms`
    - `stream_branches.py`: adds `exclude_attribute_values()` to filter out stream orders 1&2 outside of `load_file()`

### Changes

- `src/gms`
    - `buffer_stream_branches.py`: adds check for `streams_file`
    - `derive_level_paths.py`: checks length of `stream_network` before filtering out stream orders 1&2, then filters using `stream_network.exclude_attribute_values()`
    - `generate_branch_list.py`: adds check for `stream_network_dissolved`

<br/><br/>

## v4.0.10.1 - 2022-10-5 - [PR #695](https://github.com/NOAA-OWP/inundation-mapping/pull/695)

This hotfix address a bug with how the rating curve comparison (sierra test) handles the branch zero synthetic rating curve in the comparison plots. Address #676

### Changes

- `tools/rating_curve_comparison.py`
  - Added logging function to print and write to log file
  - Added new filters to ignore AHPS only sites (these are sites that we need for CatFIM but do not have a USGS gage or USGS rating curve available for sierra test analysis)
  - Added functionality to identify branch zero SRCs
  - Added new plot formatting to distinguish branch zero from other branches

<br/><br/>

## v4.0.10.0 - 2022-10-4 - [PR #697](https://github.com/NOAA-OWP/inundation-mapping/pull/697)

Change FIM to load DEM's from the new USGS 3Dep files instead of the original NHD Rasters.

### Changes

- `config`
    - `params_template.env`: Change default of the calib db back to true:  src_adjust_spatial back to "True". Plus a few text updates.
- `src`
    - `gms`
        - `run_by_unit.sh`: Change input_DEM value to the new vrt `$inputDataDir/3dep_dems/10m_5070/fim_seamless_3dep_dem_10m_5070.vrt` to load the new 3Dep DEM's. Note: The 3Dep DEM's are projected as CRS 5070, but for now, our code is using ESRI:102039. Later all code and input will be changed to CRS:5070. We now are defining the FIM desired projection (102039), so we need to reproject on the fly from 5070 to 102039 during the gdalwarp cut.
        - `run_by_branch.sh`: Removed unused lines.
    - `utils`
        - `shared_variables.py`: Changes to use the new 3Dep DEM rasters instead of the NHD rasters. Moved some values (grouped some variables). Added some new variables for 3Dep. Note: At this time, some of these new enviro variables for 3Dep are not used but are expected to be used shortly.
- `data`
    - `usgs`
        - `acquire_and_preprocess_3dep_dems.py`: Minor updates for adjustments of environmental variables. Adjustments to ensure the cell sizes are fully defined as 10 x 10 as source has a different resolution. The data we downloaded to the new `inputs/3dep_dems/10m_5070` was loaded as 10x10, CRS:5070 rasters.

### Removals

- `lib`
    - `aggregate_fim_outputs.py` : obsolete. Had been deprecated for a while and replaced by other files.
    - `fr_to_mr_raster_mask.py` : obsolete. Had been deprecated for a while and replaced by other files.

<br/><br/>

## v4.0.9.8 - 2022-10-06 - [PR #701](https://github.com/NOAA-OWP/inundation-mapping/pull/701)

Moved the calibration tool from dev-fim3 branch into "dev" (fim4) branch. Git history not available.

Also updated making it easier to deploy, along with better information for external contributors.

Changed the system so the calibration database name is configurable. This allows test databases to be setup in the same postgres db / server system. You can have more than one calb_db_keys.env running in different computers (or even more than one on one server) pointing to the same actual postgres server and service. ie) multiple dev machine can call a single production server which hosts the database.

For more details see /tools/calibration-db/README.md

### Changes

- `tools`
    - `calibration-db`
        - `docker-compose.yml`: changed to allow for configurable database name. (allows for more then one database in a postgres database system (one for prod, another for test if needed))

### Additions

- `config`
    - `calb_db_keys_template.env`: a new template verison of the required config values.

### Removals

- `tools`
    - `calibration-db`
        - `start_db.sh`: Removed as the command should be run on demand and not specifically scripted because of its configurable location of the env file.

<br/><br/>

## v4.0.9.7 - 2022-10-7 - [PR #703](https://github.com/NOAA-OWP/inundation-mapping/pull/703)

During a recent release of a FIM 3 version, it was discovered that FIM3 has slightly different AWS S3 upload requirements. A new s3 whitelist file has been created for FIM3 and the other s3 file was renamed to include the phrase "fim4" in it.

This is being added to source control as it might be used again and we don't want to loose it.

### Additions

- `config`
   - `aws_s3_put_fim3_whitelist.lst`

### Renamed

- `config`
   - `aws_s3_put_fim4_whitelist.lst`: renamed from aws_s3_put_whitelist.lst

<br/><br/>

## v4.0.9.6 - 2022-10-17 - [PR #711](https://github.com/NOAA-OWP/inundation-mapping/pull/711)

Bug fix and formatting upgrades. It was also upgraded to allow for misc other inundation data such as high water data.

### Changes

- `tools`
    - `inundate_nation.py`:  As stated above.

### Testing

- it was run in a production model against fim 4.0.9.2 at 100 yr and 2 yr as well as a new High Water dataset.

<br/><br/>

## v4.0.9.5 - 2022-10-3 - [PR #696](https://github.com/NOAA-OWP/inundation-mapping/pull/696)

- Fixed deny_gms_unit_prod.lst to comment LandSea_subset.gpkg, so it does not get removed. It is needed for processing in some branches
- Change default for params_template.env -> src_adjust_spatial="False", back to default of "True"
- Fixed an infinite loop when src_adjust_usgs_rating.py was unable to talk to the calib db.
- Fixed src_adjsust_usgs_rating.py for when the usgs_elev_table.csv may not exist.

### Changes

- `gms_run_branch.sh`:  removed some "time" command in favour of using fim commands from bash_functions.sh which give better time and output messages.

- `config`
    - `deny_gms_unit_prod.lst`: Commented out LandSea_subset.gpkg as some HUCs need that file in place.
    - `params_template.env`: Changed default src_adjust_spatial back to True

- `src`
    - `src_adjust_spatial_obs.py`:  Added code to a while loop (line 298) so it is not an indefinite loop that never stops running. It will now attempts to contact the calibration db after 6 attempts. Small adjustments to output and logging were also made and validation that a connection to the calib db was actually successful.
    - `src_adjust_usgs_rating.py`: Discovered that a usgs_elev_df might not exist (particularly when processing was being done for hucs that have no usgs guage data). If the usgs_elev_df does not exist, it no longer errors out.

<br/><br/>

## v4.0.9.4 - 2022-09-30 - [PR #691](https://github.com/NOAA-OWP/inundation-mapping/pull/691)

Cleanup Branch Zero output at the end of a processing run. Without this fix, some very large files were being left on the file system. Adjustments and cleanup changed the full BED output run from appx 2 TB output to appx 1 TB output.

### Additions

- `unit_tests`
    - `gms`
        - `outputs_cleanup_params.json` and `outputs_cleanup_unittests.py`: The usual unit test files.

### Changes

- `gms_pipeline.sh`: changed variables and text to reflect the renamed default `deny_gms_branchs_prod.lst` and `deny_gms_unit_prod.lst` files. Also tells how a user can use the word 'none' for the deny list parameter (both or either unit or branch deny list) to skip output cleanup(s).

- `gms_run_unit.sh`: changed variables and text to reflect the renamed default `deny_gms_unit_prod.lst` files. Also added a bit of minor output text (styling). Also tells how a user can use the word 'none' for the deny list parameter to skip output cleanup.

- `gms_run_branch.sh`:
       ... changed variables and text to reflect the renamed default `deny_gms_branches.lst` files.
       ... added a bit of minor output text (styling).
       ... also tells how a user can use the word 'none' for the deny list parameter to skip output cleanup.
       ... added a new section that calls the `outputs_cleanup.py` file and will do post cleanup on branch zero output files.

- `src`
    - `gms`
        - `outputs_cleanup.py`: pretty much rewrote it in its entirety. Now accepts a manditory branch id (can be zero) and can recursively search subdirectories. ie) We can submit a whole output directory with all hucs and ask to cleanup branch 0 folder OR cleanup files in any particular directory as we did before (per branch id).

          - `run_by_unit.sh`:  updated to pass in a branch id (or the value of "0" meaning branch zero) to outputs_cleanup.py.
          - `run_by_branch.sh`:  updated to pass in a branch id to outputs_cleanup.py.

- `unit_tests`
    - `README.md`: updated to talk about the specific deny list for unit_testing.
    - `__template_unittests.py`: updated for the latest code standards for unit tests.

- `config`
    - `deny_gms_branch_unittest.lst`: Added some new files to be deleted, updated others.
    - `deny_gms_branch_zero.lst`: Added some new files to be deleted.
    - `deny_gms_branches_dev.lst`:  Renamed from `deny_gms_branches_default.lst` and some new files to be deleted, updated others. Now used primarily for development and testing use.
    - `deny_gms_branches_prod.lst`:  Renamed from `deny_gms_branches_min` and some new files to be deleted, updated others. Now used primarily for when releasing a version to production.
    - `deny_gms_unit_prod.lst`: Renamed from `deny_gms_unit_default.lst`, yes... there currently is no "dev" version.  Added some new files to be deleted.

<br/><br/>

## v4.0.9.3 - 2022-09-13 - [PR #681](https://github.com/NOAA-OWP/inundation-mapping/pull/681)

Created a new tool to downloaded USGS 3Dep DEM's via their S3 bucket.

Other changes:
 - Some code file re-organization in favour of the new `data` folder which is designed for getting, setting, and processing data from external sources such as AWS, WBD, NHD, NWM, etc.
 - Added tmux as a new tool embedded inside the docker images.

### Additions

- `data`
   - `usgs`
      - `acquire_and_preprocess_3dep_dems.py`:  The new tool as described above. For now it is hardcoded to a set path for USGS AWS S3 vrt file but may change later for it to become parameter driven.
 - `create_vrt_file.py`: This is also a new tool that can take a directory of geotiff files and create a gdal virtual file, .vrt extention, also called a `virtual raster`. Instead of clipping against HUC4, 6, 8's raster files, and run risks of boundary issues, vrt's actual like all of the tif's are one giant mosaiced raster and can be clipped as one.

### Removals

- 'Dockerfile.prod`:  No longer being used (never was used)

### Changes

- `Dockerfile`:  Added apt install for tmux. This tool will now be available in docker images and assists developers.

- `data`
   - `acquire_and_preprocess_inputs.py`:  moved from the `tools` directory but not other changes made. Note: will required review/adjustments before being used again.
   - `nws`
      - `preprocess_ahps_nws.py`:  moved from the `tools` directory but not other changes made. Note: will required review/adjustments before being used again.
      - `preprocess_rasters.py`: moved from the `tools` directory but not other changes made. Note: will required review/adjustments before being used again.
    - `usgs`
         - `preprocess_ahps_usgs.py`:  moved from the `tools` directory but not other changes made. Note: will required review/adjustments before being used again.
         - `preprocess_download_usgs_grids.py`: moved from the `tools` directory but not other changes made. Note: will required review/adjustments before being used again.

 - `src`
     - `utils`
         - `shared_functions.py`:  changes made were
              - Cleanup the "imports" section of the file (including a change to how the utils.shared_variables file is loaded.
              - Added `progress_bar_handler` function which can be re-used by other code files.
              - Added `get_file_names` which can create a list of files from a given directory matching a given extension.
              - Modified `print_current_date_time` and `print_date_time_duration` and  methods to return the date time strings. These helper methods exist to help with standardization of logging and output console messages.
              - Added `print_start_header` and `print_end_header` to help with standardization of console and logging output messages.
          - `shared_variables.py`: Additions in support of near future functionality of having fim load DEM's from USGS 3DEP instead of NHD rasters.

<br/><br/>

## v4.0.9.2 - 2022-09-12 - [PR #678](https://github.com/NOAA-OWP/inundation-mapping/pull/678)

This fixes several bugs related to branch definition and trimming due to waterbodies.

### Changes

- `src/gms/stream_branches.py`
   - Bypasses erroneous stream network data in the to ID field by using the Node attribute instead.
   - Adds check if no nwm_lakes_proj_subset.gpkg file is found due to no waterbodies in the HUC.
   - Allows for multiple upstream branches when stream order overrides arbolate sum.

<br/><br/>

## v4.0.9.1 - 2022-09-01 - [PR #664](https://github.com/NOAA-OWP/inundation-mapping/pull/664)

A couple of changes:
1) Addition of a new tool for pushing files / folders up to an AWS (Amazon Web Service) S3 bucket.
2) Updates to the Docker image creation files to include new packages for boto3 (for AWS) and also added `jupyter`, `jupterlab` and `ipympl` to make it easier to use those tools during development.
3) Correct an oversight of `logs\src_optimization` not being cleared upon `overwrite` run.

### Additions

- `src`
   - `data`
       - `README.md`: Details on how the new system for `data` folders (for communication for external data sources/services).
       - `aws`
           - `aws_base.py`:  A file using a class and inheritance system (parent / child). This file has properties and a method that all child class will be expected to use and share. This makes it quicker and easier to added new AWS tools and helps keep consistant patterns and standards.
           - `aws_creds_template.env`: There are a number of ways to validate credentials to send data up to S3. We have chosen to use an `.env` file that can be passed into the tool from any location. This is the template for that `.env` file. Later versions may be changed to use AWS profile security system.
           - `s3.py`: This file pushes file and folders up to a defined S3 bucket and root folder. Note: while it is designed only for `puts` (pushing to S3), hooks were added in case functional is added later for `gets` (pull from S3).


### Changes

- `utils`
   - `shared_functions.py`:  A couple of new features
       -  Added a method which accepts a path to a .lst or .txt file with a collection of data and load it into a  python list object. It can be used for a list of HUCS, file paths, or almost anything.
       - A new method for quick addition of current date/time in output.
       - A new method for quick calculation and formatting of time duration in hours, min and seconds.
       - A new method for search for a string in a given python list. It was designed with the following in mind, we already have a python list loaded with whitelist of files to be included in an S3 push. As we iterate through files from the file system, we can use this tool to see if the file should be pushed to S3. This tool can easily be used contexts and there is similar functionality in other FIM4 code that might be able to this method.

- `Dockerfile` : Removed a line for reloading Shapely in recent PRs, which for some reason is no longer needed after adding the new BOTO3 python package. Must be related to python packages dependencies. This removed Shapely warning seen as a result of another recent PR. Also added AWS CLI for bash commands.

- `Pipfile` and `Pipfile.lock`:  Updates for the four new python packages, `boto3` (for AWS), `jupyter`, `jupyterlab` and `ipympl`. We have some staff that use Jupyter in their dev actitivies. Adding this package into the base Docker image will make it easier for them.

<br/><br/>

## 4.0.9.0 - 2022-09-09 - [PR #672](https://github.com/NOAA-OWP/inundation-mapping/pull/672)

When deriving level paths, this improvement allows stream order to override arbolate sum when selecting the proper upstream segment to continue the current branch.

<br/><br/>

## 4.0.8.0 - 2022-08-26 - [PR #671](https://github.com/NOAA-OWP/inundation-mapping/pull/671)

Trims ends of branches that are in waterbodies; also removes branches if they are entirely in a waterbody.

## Changes

- `src/gms/stream_branches.py`: adds `trim_branches_in_waterbodies()` and `remove_branches_in_waterbodies()` to trim and prune branches in waterbodies.

<br/><br/>

## v4.0.7.2 - 2022-08-11 - [PR #654](https://github.com/NOAA-OWP/inundation-mapping/pull/654)

`inundate_nation.py` A change to switch the inundate nation function away from refrences to `inundate.py`, and rather use `inundate_gms.py` and `mosaic_inundation.py`

### Changes

- `inundate_gms`:  Changed `mask_type = 'filter'`

<br/><br/>

## v4.0.7.1 - 2022-08-22 - [PR #665](https://github.com/NOAA-OWP/inundation-mapping/pull/665)

Hotfix for addressing missing input variable when running `gms_run_branch.sh` outside of `gms_pipeline.sh`.

### Changes
- `gms_run_branch.sh`: defining path to WBD HUC input file directly in ogr2ogr call rather than using the $input_WBD_gdb defined in `gms_run_unit.sh`
- `src/src_adjust_spatial_obs.py`: removed an extra print statement
- `src/src_roughness_optimization.py`: removed a log file write that contained sensitive host name

<br/><br/>

## v4.0.7.0 - 2022-08-17 - [PR #657](https://github.com/NOAA-OWP/inundation-mapping/pull/657)

Introduces synthetic rating curve calibration workflow. The calibration computes new Manning's coefficients for the HAND SRCs using input data: USGS gage locations, USGS rating curve csv, and a benchmark FIM extent point database stored in PostgreSQL database. This addresses [#535].

### Additions

- `src/src_adjust_spatial_obs.py`: new synthetic rating curve calibration routine that prepares all of the spatial (point data) benchmark data for ingest to the Manning's coefficient calculations performed in `src_roughness_optimization.py`
- `src/src_adjust_usgs_rating.py`: new synthetic rating curve calibration routine that prepares all of the USGS gage location and observed rating curve data for ingest to the Manning's coefficient calculations performed in `src_roughness_optimization.py`
- `src/src_roughness_optimization.py`: new SRC post-processing script that ingests observed data and HUC/branch FIM output data to compute optimized Manning's coefficient values and update the discharge values in the SRCs. Outputs a new hydroTable.csv.

### Changes

- `config/deny_gms_branch_zero.lst`: added `gw_catchments_reaches_filtered_addedAttributes_crosswalked_{}.gpkg` to list of files to keep (used in calibration workflow)
- `config/deny_gms_branches_min.lst`: added `gw_catchments_reaches_filtered_addedAttributes_crosswalked_{}.gpkg` to list of files to keep (used in calibration workflow)
- `config/deny_gms_unit_default.lst`: added `usgs_elev_table.csv` to list of files to keep (used in calibration workflow)
- `config/params_template.env`: added new variables for user to control calibration
  - `src_adjust_usgs`: Toggle to run src adjustment routine (True=on; False=off)
  - `nwm_recur_file`: input file location with nwm feature_id and recurrence flow values
  - `src_adjust_spatial`: Toggle to run src adjustment routine (True=on; False=off)
  - `fim_obs_pnt_data`: input file location with benchmark point data used to populate the postgresql database
  - `CALB_DB_KEYS_FILE`: path to env file with sensitive paths for accessing postgres database
- `gms_run_branch.sh`: includes new steps in the workflow to connect to the calibration PostgreSQL database, run SRC calibration w/ USGS gage rating curves, run SRC calibration w/ benchmark point database
- `src/add_crosswalk.py`: added step to create placeholder variables to be replaced in post-processing (as needed). Created here to ensure consistent column variables in the final hydrotable.csv
- `src/gms/run_by_unit.sh`: added new steps to workflow to create the `usgs_subset_gages.gpkg` file for branch zero and then perform crosswalk and create `usgs_elev_table.csv` for branch zero
- `src/make_stages_and_catchlist.py`: Reconcile flows and catchments hydroids
- `src/usgs_gage_aggregate.py`: changed streamorder data type from integer to string to better handle missing values in `usgs_gage_unit_setup.py`
- `src/usgs_gage_unit_setup.py`: added new inputs and function to populate `usgs_elev_table.csv` for branch zero using all available gages within the huc (not filtering to a specific branch)
- `src/utils/shared_functions.py`: added two new functions for calibration workflow
  - `check_file_age`: check the age of a file (use for flagging potentially outdated input)
  - `concat_huc_csv`: concatenate huc csv files to a single dataframe/csv
- `src/utils/shared_variables.py`: defined new SRC calibration threshold variables
  - `DOWNSTREAM_THRESHOLD`: distance in km to propogate new roughness values downstream
  - `ROUGHNESS_MAX_THRESH`: max allowable adjusted roughness value (void values larger than this)
  - `ROUGHNESS_MIN_THRESH`: min allowable adjusted roughness value (void values smaller than this)

<br/><br/>

## v4.0.6.3 - 2022-08-04 - [PR #652](https://github.com/NOAA-OWP/inundation-mapping/pull/652)

Updated `Dockerfile`, `Pipfile` and `Pipfile.lock` to add the new psycopg2 python package required for a WIP code fix for the new FIM4 calibration db.

<br/><br/>

## v4.0.6.2 - 2022-08-16 - [PR #639](https://github.com/NOAA-OWP/inundation-mapping/pull/639)

This file converts USFIMR remote sensed inundation shapefiles into a raster that can be used to compare to the FIM data. It has to be run separately for each shapefile. This addresses [#629].

### Additions

- `/tools/fimr_to_benchmark.py`: This file converts USFIMR remote sensed inundation shapefiles into a raster that can be used to compare to the FIM data. It has to be run separately for each shapefile.

<br/><br/>

## v4.0.6.1 - 2022-08-12 - [PR #655](https://github.com/NOAA-OWP/inundation-mapping/pull/655)

Prunes branches that fail with NO_FLOWLINES_EXIST (Exit code: 61) in `gms_run_branch.sh` after running `split_flows.py`

### Additions
- Adds `remove_error_branches.py` (called from `gms_run_branch.sh`)
- Adds `gms_inputs_removed.csv` to log branches that have been removed across all HUCs

### Removals
- Deletes branch folders that fail
- Deletes branch from `gms_inputs.csv`

<br/><br/>

## v4.0.6.0 - 2022-08-10 - [PR #614](https://github.com/NOAA-OWP/inundation-mapping/pull/614)

Addressing #560, this fix in run_by_branch trims the DEM derived streamline if it extends past the end of the branch streamline. It does this by finding the terminal point of the branch stream, snapping to the nearest point on the DEM derived stream, and cutting off the remaining downstream portion of the DEM derived stream.

### Changes

- `/src/split_flows.py`: Trims the DEM derived streamline if it flows past the terminus of the branch (or level path) streamline.
- `/src/gms/delineate_hydros_and_produce_HAND.sh`: Added branch streamlines as an input to `split_flows.py`.

<br/><br/>

## v4.0.5.4 - 2022-08-01 - [PR #642](https://github.com/NOAA-OWP/inundation-mapping/pull/642)

Fixes bug that causes [Errno2] No such file or directory error when running synthesize_test_cases.py if testing_versions folder doesn't exist (for example, after downloading test_cases from ESIP S3).

### Additions

- `run_test_case.py`: Checks for testing_versions folder in test_cases and adds it if it doesn't exist.

<br/><br/>

## v4.0.5.3 - 2022-07-27 - [PR #630](https://github.com/NOAA-OWP/inundation-mapping/issues/630)

A file called gms_pipeline.sh already existed but was unusable. This has been updated and now can be used as a "one-command" execution of the fim4/gms run. While you still can run gms_run_unit.sh and gms_run_branch.sh as you did before, you no longer need to. Input arguments were simplified to allow for more default and this simplification was added to `gms_run_unit.sh` and `gms_run_branch.sh` as well.

A new feature was added that is being used for `gms_pipeline.sh` which tests the percent and number of errors after hucs are processed before continuing onto branch processing.

New FIM4/gms usability is now just (at a minumum): `gms_pipeline.sh -n <output name> -u <HUC(s) or HUC list path>`

`gms_run_branch.sh` and `gms_run_branch.sh` have also been changed to add the new -a flag and default to dropping stream orders 1 and 2.

### Additions

- `src`
    - `check_unit_errors.py`: as described above.
- `unit_tests`
    - `check_unit_errors_unittests.py` and `check_unit_errors_params.json`: to match new file.

### Changes

- `README.md`:  Updated text for FIM4, gms_pipeline, S3 input updates, information about updating dependencies, misc link updates and misc text verbage.
- `gms_pipeline.sh`: as described above.
- `gms_run_unit.sh`: as described above. Also small updates to clean up folders and files in case of an overwrite.
- `gms_run_branch.sh`: as described above.
- `src`
     - `utils`
         - `fim_enums.py`:  FIM_system_exit_codes renamed to FIM_exit_codes.
         - `shared_variables.py`: added configurable values for minimum number and percentage of unit errors.
    - `bash_functions.env`:   Update to make the cumulative time screen outputs in mins/secs instead of just seconds.
    - `check_huc_inputs.py`:  Now returns the number of HUCs being processed, needed by `gms_pipeline.sh` (Note: to get the value back to a bash file, it has to send it back via a "print" line and not a "return" value.  Improved input validation,
- `unit_tests`
   - `README.md`: Misc text and link updates.

### Removals

- `config\params_template_calibrated.env`: No longer needed. Has been removed already from dev-fim3 and confirmed that it is not needed.
<br><br>

## v4.0.5.2 - 2022-07-25 - [PR #622](https://github.com/NOAA-OWP/inundation-mapping/pull/622)

Updates to unit tests including a minor update for outputs and loading in .json parameter files.
<br><br>


## v4.0.5.1 - 2022-06-27 - [PR #612](https://github.com/NOAA-OWP/inundation-mapping/pull/612)

`Alpha Test Refactor` An upgrade was made a few weeks back to the dev-fim3 branch that improved performance, usability and readability of running alpha tests. Some cleanup in other files for readability, debugging verbosity and styling were done as well. A newer, cleaner system for printing lines when the verbose flag is enabled was added.

### Changes

- `gms_run_branch.sh`:  Updated help instructions to about using multiple HUCs as command arguments.
- `gms_run_unit.sh`:  Updated help instructions to about using multiple HUCs as command arguments.
- `src/utils`
    - `shared_functions.py`:
       - Added a new function called `vprint` which creates a simpler way (and better readability) for other python files when wanting to include a print line when the verbose flag is on.
       - Added a new class named `FIM_Helpers` as a wrapper for the new `vprint` method.
       - With the new `FIM_Helpers` class, a previously existing method named `append_id_to_file_name` was moved into this class making it easier and quicker for usage in other classes.

- `tools`
    - `composite_inundation.py`: Updated its usage of the `append_id_to_file_name` function to now call the`FIM_Helpers` method version of it.
    - `gms_tools`
       - `inundate_gms.py`: Updated for its adjusted usage of the `append_id_to_file_name` method, also removed its own `def __vprint` function in favour of the `FIM_Helpers.vprint` method.
       - `mosaic_inundation.py`:
          - Added adjustments for use of `append_id_to_file_name` and adjustments for `fh.vprint`.
          - Fixed a bug for the variable `ag_mosaic_output` which was not pre-declared and would fail as using an undefined variable in certain conditions.
    - `run_test_case.py`: Ported `test_case` class from FIM 3 and tweaked slightly to allow for GMS FIM. Also added more prints against the new fh.vprint method. Also added a default print line for progress / traceability for all alpha test regardless if the verbose flag is set.
    - `synthesize_test_cases.py`: Ported `test_case` class from FIM 3.
- `unit_tests`
   - `shared_functions_unittests.py`: Update to match moving the `append_id_to_file_name` into the `FIM_Helpers` class. Also removed all "header print lines" for each unit test method (for output readability).

<br/><br/>

## v4.0.5.0 - 2022-06-16 - [PR #611](https://github.com/NOAA-OWP/inundation-mapping/pull/611)

'Branch zero' is a new branch that runs the HUCs full stream network to make up for stream orders 1 & 2 being skipped by the GMS solution and is similar to the FR extent in FIM v3. This new branch is created during `run_by_unit.sh` and the processed DEM is used by the other GMS branches during `run_by_branch.sh` to improve efficiency.

### Additions

- `src/gms/delineate_hydros_and_produce_HAND.sh`: Runs all of the modules associated with delineating stream lines and catchments and building the HAND relative elevation model. This file is called once during `gms_run_unit` to produce the branch zero files and is also run for every GMS branch in `gms_run_branch`.
- `config/deny_gms_branch_zero.lst`: A list specifically for branch zero that helps with cleanup (removing unneeded files after processing).

### Changes

- `src/`
    - `output_cleanup.py`: Fixed bug for viz flag.
    - `gms/`
        - `run_by_unit.sh`: Added creation of "branch zero", DEM pre-processing, and now calls.
        -  `delineate_hydros_and_produce_HAND.sh` to produce HAND outputs for the entire stream network.
        - `run_by_branch.sh`: Removed DEM processing steps (now done in `run_by_unit.sh`), moved stream network delineation and HAND generation to `delineate_hydros_and_produce_HAND.sh`.
        - `generate_branch_list.py`: Added argument and parameter to sure that the branch zero entry was added to the branch list.
- `config/`
     - `params_template.env`: Added `zero_branch_id` variable.
- `tools`
     - `run_test_case.py`: Some styling / readability upgrades plus some enhanced outputs.  Also changed the _verbose_ flag to _gms_verbose_ being passed into Mosaic_inundation function.
     - `synthesize_test_cases.py`: arguments being passed into the _alpha_test_args_ from being hardcoded from flags to verbose (effectively turning on verbose outputs when applicable. Note: Progress bar was not affected.
     - `tools_shared_functions.py`: Some styling / readability upgrades.
- `gms_run_unit.sh`: Added export of extent variable, dropped the -s flag and added the -a flag so it now defaults to dropping stream orders 1 and 2.
- `gms_run_branch.sh`: Fixed bug when using overwrite flag saying branch errors folder already exists, dropped the -s flag and added the -a flag so it now defaults to dropping stream orders 1 and 2.

### Removals

- `tests/`: Redundant
- `tools/shared_variables`: Redundant

<br/><br/>

## v4.0.4.3 - 2022-05-26 - [PR #605](https://github.com/NOAA-OWP/inundation-mapping/pull/605)

We needed a tool that could composite / mosaic inundation maps for FIM3 FR and FIM4 / GMS with stream orders 3 and higher. A tool previously existed named composite_fr_ms_inundation.py and it was renamed to composite_inundation.py and upgraded to handle any combination of 2 of 3 items (FIM3 FR, FIM3 MS and/or FIM4 GMS).

### Additions

- `tools/composite_inundation.py`: Technically it is a renamed from composite_ms_fr_inundation.py, and is based on that functionality, but has been heavily modified. It has a number of options, but primarily is designed to take two sets of output directories, inundate the files, then composite them into a single mosiac'd raster per huc. The primary usage is expected to be compositing FIM3 FR with FIM4 / GMS with stream orders 3 and higher.

- `unit_tests/gms/inundate_gms_unittests.py and inundate_gms_params.json`: for running unit tests against `tools/gms_tools/inunundate_gms.py`.
- `unit_tests/shared_functions_unittests.py and shared_functions_params.json`: A new function named `append_id_to_file_name_single_identifier` was added to `src/utils/shared_functions.py` and some unit tests for that function was created.

### Removed

- `tools/composite_ms_fr_inundation.py`: replaced with upgraded version named `composite_inundation.py`.

### Changes

- `tools/gms_tools/inundate_gms.py`: some style, readabilty cleanup plus move a function up to `shared_functions.py`.
- `tools/gms_tools/mosaic_inundation.py`: some style, readabilty cleanup plus move a function up to `shared_functions.py`.
- `tools/inundation.py`: some style, readabilty cleanup.
- `tools/synthesize_test_cases.py`: was updated primarily for sample usage notes.

<br/><br/>

## v4.0.4.2 - 2022-05-03 - [PR #594](https://github.com/NOAA-OWP/inundation-mapping/pull/594)

This hotfix includes several revisions needed to fix/update the FIM4 area inundation evaluation scripts. These changes largely migrate revisions from the FIM3 evaluation code to the FIM4 evaluation code.

### Changes

- `tools/eval_plots.py`: Copied FIM3 code revisions to enable RAS2FIM evals and PND plots. Replaced deprecated parameter name for matplotlib grid()
- `tools/synthesize_test_cases.py`: Copied FIM3 code revisions to assign FR, MS, COMP resolution variable and addressed magnitude list variable for IFC eval
- `tools/tools_shared_functions.py`: Copied FIM3 code revisions to enable probability not detected (PND) metric calculation
- `tools/tools_shared_variables.py`: Updated magnitude dictionary variables for RAS2FIM evals and PND plots

<br/><br/>

## v4.0.4.1 - 2022-05-02 - [PR #587](https://github.com/NOAA-OWP/inundation-mapping/pull/587)

While testing GMS against evaluation and inundation data, we discovered some challenges for running alpha testing at full scale. Part of it was related to the very large output volume for GMS which resulted in outputs being created on multiple servers and folders. Considering the GMS volume and processing, a tool was required to extract out the ~215 HUC's that we have evaluation data for. Next, we needed isolate valid HUC output folders from original 2,188 HUC's and its 100's of thousands of branches. The first new tool allows us to point to the `test_case` data folder and create a list of all HUC's that we have validation for.

Now that we have a list of relavent HUC's, we need to consolidate output folders from the previously processed full CONUS+ output data. The new `copy_test_case_folders.py` tool extracts relavent HUC (gms unit) folders, based on the list created above, into a consolidated folder. The two tools combine result in significantly reduced overall processing time for running alpha tests at scale.

`gms_run_unit.sh` and `aggregated_branch_lists.py` were adjusted to make a previously hardcoded file path and file name to be run-time parameters. By adding the two new arguments, the file could be used against the new `copy_test_case_folders.py`. `copy_test_case_folders.py` and `gms_run_unit.sh` can now call `aggregated_branch_lists.py` to create a key input file called `gms_inputs.csv` which is a key file required for alpha testing.

A few other small adjustments were made for readability and traceability as well as a few small fixes discovered when running at scale.

### Additions

- `tools/find_test_case_folders.py`: A new tool for creating a list of HUC's that we have test/evaluation data for.
- `tools/copy_test_case_folders.py`: A new tool for using the list created above, to scan through other fully processed output folders and extract only the HUC's (gms units) and it's branches into a consolidated folder, ready for alpha test processing (or other needs).

### Changes

- `src/gms/aggregate_branch_lists.py`: Adjusted to allow two previously hardcoded values to now be incoming arguments. Now this file can be used by both `gms_run_unit.sh` and `copy_test_case_folders.py`.
- `tools/synthesize_test_cases.py`: Adjustments for readability and progress status. The embedded progress bars are not working and will be addressed later.
- `tools/run_test_case.py`: A print statement was added to help with processing progess was added.
- `gms_run_unit.sh`: This was adjusted to match the new input parameters for `aggregate_branch_lists.py` as well as additions for progress status. It now will show the entire progress period start datetime, end datetime and duration.
- `gms_run_branch.sh`: Also was upgraded to show the entire progress period start datetime, end datetime and duration.

<br/><br/>

## v4.0.4.0 - 2022-04-12 - [PR #557](https://github.com/NOAA-OWP/inundation-mapping/pull/557)

During large scale testing of the new **filtering out stream orders 1 and 2** feature [PR #548](https://github.com/NOAA-OWP/inundation-mapping/pull/548), a bug was discovered with 14 HUCS that had no remaining streams after removing stream orders 1 and 2. This resulted in a number of unmanaged and unclear exceptions. An exception may be still raised will still be raised in this fix for logging purposes, but it is now very clear what happened. Other types of events are logged with clear codes to identify what happened.

Fixes were put in place for a couple of new logging behaviors.

1. Recognize that for system exit codes, there are times when an event is neither a success (code 0) nor a failure (code 1). During processing where stream orders are dropped, some HUCs had no remaining reaches, others had mismatched reaches and others as had missing flowlines (reaches) relating to dissolved level paths (merging individual reaches as part of GMS). When these occur, we want to abort the HUC (unit) or branch processing, identify that they were aborted for specific reasons and continue. A new custom system exit code system was adding using python enums. Logging was enhanced to recognize that some exit codes were not a 0 or a 1 and process them differently.

2. Pathing and log management became an issue. It us not uncommon for tens or hundreds of thousands of branches to be processed. A new feature was to recognize what is happening with each branch or unit and have them easily found and recognizable. Futher, processing for failure (sys exit code of 1) are now copied into a unique folder as the occur to help with visualization of run time errors. Previously errors were not extracted until the end of the entire run which may be multiple days.

3. A minor correction was made when dissolved level paths were created with the new merged level path not always having a valid stream order value.

### File Additions

- `src/`
   - `utils/`
      - `fim_enums.py`:
         - A new class called `FIM_system_exit_codes` was added. This allows tracking and blocking of duplicate system exit codes when a custom system code is required.


### Changes

- `fim_run.sh`: Added the gms `non-zero-exit-code` system to `fim_run` to help uncover and isolate errors during processing. Errors recorded in log files within in the logs/unit folder are now copied into a new folder called `unit_errors`.

- `gms_run_branch.sh`:
    -  Minor adjustments to how the `non-zero-exit code` logs were created. Testing uncovered that previous versions were not always reliable. This is now stablized and enhanced.
    - In previous versions, only the `gms_unit.sh` was aware that **stream order filtering** was being done. Now all branch processing is also aware that filtering is in place. Processing in child files and classes can now make adjustments as/if required for stream order filtering.
    - Small output adjustments were made to help with overall screen and log readability.

- `gms_run_unit.sh`:
    - Minor adjustments to how the `non-zero-exit-code` logs were created similar to `gms_run_branch.sh.`
    - Small text corrections, formatting and output corrections were added.
    - A feature removing all log files at the start of the entire process run were added if the `overwrite` command line argument was added.

- `src/`
   - `filter_catchments_and_add_attributes.py`:
      - Some minor formatting and readability adjustments were added.
      - Additions were made to help this code be aware and responding accordingly if that stream order filtering has occurred. Previously recorded as bugs coming from this class, are now may recorded with the new custom exit code if applicable.

   - `run_by_unit.sh` (supporting fim_run.sh):
         - As a change was made to sub-process call to `filter_catchments_and_add_attributes.py` file, which is shared by gms, related to reach errors / events.

   - `split_flows.py`:
      - Some minor formatting and readability adjustments were added.
      - Additions were made to recognize the same type of errors as being described in other files related to stream order filtering issues.
      - A correction was made to be more precise and more explicit when a gms branch error existed. This was done to ensure that we were not letting other exceptions be trapped that were NOT related to stream flow filtering.

   - `time_and_tee_run_by_unit.sh`:
      - The new custom system exit codes was added. Note that the values of 61 (responding system code) are hardcoded instead of using the python based `Fim_system_exit_code` system. This is related to limited communication between python and bash.

   - `gms/`
      - `derive_level_paths.py`:
          - Was upgraded to use the new fim_enums.Fim_system_exit_codes system. This occurs when no streams / flows remain after filtering.  Without this upgrade, standard exceptions were being issued with minimal details for the error.
          - Minor adjustments to formatting for readability were made.

      - `generate_branch_list.py` :  Minor adjustments to formatting for readability were made.

      - `run_by_branch.sh`:
         - Some minor formatting and readability adjustments were added.
         - Additions to the subprocess call to `split_flows.py` were added so it was aware that branch filtering was being used. `split_flows.py` was one of the files that was throwing errors related to stream order filtering. A subprocess call to `filter_catchments_and_add_attributes.py` adjustment was also required for the same reason.

      - `run_by_unit.sh`:
         - Some minor formatting and readability adjustments were added.
         - An addition was made to help trap errors that might be triggered by `derive_level_paths.py` for `stream order filtering`.

      - `time_and_tee_run_by_branch.sh`:
         - A system was added recognize if an non successful system exit code was sent back from `run_by_branch`. This includes true errors of code 1 and other new custom system exit codes. Upon detection of non-zero-exit codes, log files are immediately copied into special folders for quicker and easier visibility. Previously errors were not brought forth until the entire process was completed which ranged fro hours up to 18 days. Note: System exit codes of 60 and 61 were hardcoded instead of using the values from the new  `FIM_system_exit_codes` due to limitation of communication between python and bash.

      - `time_and_tee_run_by_unit.sh`:
         - The same upgrade as described above in `time_and_tee_run_by_branch.sh` was applied here.
         - Minor readability and output formatting changes were made.

      - `todo.md`
         - An entry was removed from this list which talked about errors due to small level paths exactly as was fixed in this pull request set.

- `unit_tests/`
   - `gms/`
      - `derive_level_paths_unittests.py` :  Added a new unit test specifically testing this type of condition with a known HUC that triggered the branch errors previously described..
      - `derive_level_paths_params.json`:
           - Added a new node with a HUC number known to fail.
           - Changed pathing for unit test data pathing from `/data/outputs/gms_example_unit_tests` to `/data/outputs/fim_unit_test_data_do_not_remove`. The new folder is intended to be a more permanent folder for unit test data.
           - Some additional tests were added validating the argument for dropping stream orders.

### Unit Test File Additions:

- `unit_tests/`
   - `filter_catchments_and_add_attributes_unittests.py` and `filter_catchments_and_add_attributes_params.json`:

   - `split_flows_unittests.py' and `split_flows_params.json`

<br/><br/>

## v4.0.3.1 - 2022-03-10 - [PR #561](https://github.com/NOAA-OWP/inundation-mapping/pull/561)

Bug fixes to get the Alpha Test working in FIM 4.

### Changes

- `tools/sythesize_test_cases.py`: Fixed bugs that prevented multiple benchmark types in the same huc from running `run_test_case.py`.
- `tools/run_test_case.py`: Fixed mall bug for IFC benchmark.
- `tools/eval_plots.py`: Fixed Pandas query bugs.

<br/><br/>

## v4.0.3.0 - 2022-03-03 - [PR #550](https://github.com/NOAA-OWP/inundation-mapping/pull/550)

This PR ports the functionality of `usgs_gage_crosswalk.py` and `rating_curve_comparison.py` to FIM 4.

### Additions

- `src/`:
    - `usgs_gage_aggregate.py`: Aggregates all instances of `usgs_elev_table.csv` to the HUC level. This makes it easier to view the gages in each HUC without having to hunt through branch folders and easier for the Sierra Test to run at the HUC level.
    - `usgs_gage_unit_setup.py`: Assigns a branch to each USGS gage within a unit. The output of this module is `usgs_subset_gages.gpkg` at the HUC level containing the `levpa_id` attribute.

### Changes

- `gms_run_branch.sh`: Added a line to aggregate all `usgs_elev_table.csv` into the HUC directory level using `src/usgs_gage_aggregate.py`.
- `src/`:
    -  `gms/`
        - `run_by_branch.sh`: Added a block to run `src/usgs_gage_crosswalk.py`.
        - `run_by_unit.sh`: Added a block to run `src/usgs_gage_unit_setup.py`.
    - `usgs_gage_crosswalk.py`: Similar to it's functionality in FIM 3, this module snaps USGS gages to the stream network, samples the underlying DEMs, and writes the attributes to `usgs_elev_table.csv`. This CSV is later aggregated to the HUC level and eventually used in `tools/rating_curve_comparison.py`. Addresses #539
- `tools/rating_curve_comparison.py`: Updated Sierra Test to work with FIM 4 data structure.
- `unit_tests/`:
    - `rating_curve_comparison_unittests.py` & `rating_curve_comparison_params.json`: Unit test code and parameters for the Sierra Test.
    - `usgs_gage_crosswalk_unittests.py` & `usgs_gage_crosswalk_params.json`: Unit test code and parameters for `usgs_gage_crosswalk.py`
- `config/`:
    - `deny_gms_branches_default.lst` & `config/deny_gms_branches_min.lst`: Add `usgs_elev_table.csv` to the lists as a comment so it doesn't get deleted during cleanup.
    - `deny_gms_unit_default.lst`: Add `usgs_subset_gages.gpkg` to the lists as a comment so it doesn't get deleted during cleanup.

<br/><br/>

## v4.0.2.0 - 2022-03-02 - [PR #548](https://github.com/NOAA-OWP/inundation-mapping/pull/548)

Added a new optional system which allows an argument to be added to the `gms_run_unit.sh` command line to filter out stream orders 1 and 2 when calculating branches.

### Changes

- `gms_run_unit.sh`: Add the new optional `-s` command line argument. Inclusion of this argument means "drop stream orders 1 and 2".

- `src/gms`
   - `run_by_unit.sh`: Capture and forward the drop stream orders flag to `derive_level_paths.py`

   - `derive_level_paths.py`: Capture the drop stream order flag and working with `stream_branches.py` to include/not include loading nwm stream with stream orders 1 and 2.

   - `stream_branchs.py`: A correction was put in place to allow for the filter of branch attributes and values to be excluded. The `from_file` method has the functionality but was incomplete. This was corrected and how could accept the values from `derive_level_paths.py` to use the branch attribute of "order_" (gkpg field) and values excluded of [1,2] when optionally desired.

- `unit_tests/gms`
    - `derive_level_paths_unittests.py` and `derive_level_paths_params.py`: Updated for testing for the new "drop stream orders 1 and 2" feature. Upgrades were also made to earlier existing incomplete test methods to test more output conditions.

<br/><br/>

## v4.0.1.0 - 2022-02-02 - [PR #525](https://github.com/NOAA-OWP/cahaba/pull/525)

The addition of a very simple and evolving unit test system which has two unit tests against two py files.  This will set a precendence and will grow over time and may be automated, possibly during git check-in triggered. The embedded README.md has more details of what we currently have, how to use it, how to add new unit tests, and expected future enhancements.

### Additions

- `/unit_tests/` folder which has the following:

   - `clip_vectors_to_wbd_params.json`: A set of default "happy path" values that are expected to pass validation for the clip_vectors_to_wbd.py -> clip_vectors_to_wbd (function).

   - `clip_vectors_to_wbd_unittests.py`: A unit test file for src/clip_vectors_to_wbd.py. Incomplete but evolving.

   - `README.md`: Some information about how to create unit tests and how to use them.

   - `unit_tests_utils.py`: A python file where methods that are common to all unit tests can be placed.

   - `gms/derive_level_paths_params.json`: A set of default "happy path" values that are expected to pass validation for the derive_level_paths_params.py -> Derive_level_paths (function).

   - `gms/derive_level_paths_unittests.py`: A unit test file for `src/derive_level_paths.py`. Incomplete but evolving.

<br/><br/>

## v4.0.0.0 - 2022-02-01 - [PR #524](https://github.com/NOAA-OWP/cahaba/pull/524)

FIM4 builds upon FIM3 and allows for better representation of inundation through the reduction of artificial restriction of inundation at catchment boundaries.

More details will be made available through a publication by Aristizabal et. al. and will be included in the "Credits and References" section of the README.md, titled "Reducing Horton-Strahler Stream Order Can Enhance Flood Inundation Mapping Skill with Applications for the U.S. National Water Model."

### Additions

- `/src/gms`: A new directory containing scripts necessary to produce the FIM4 Height Above Nearest Drainage grids and synthetic rating curves needed for inundation mapping.
- `/tools/gms_tools`: A new directory containing scripts necessary to generate and evaluate inundation maps produced from FIM4 Height Above Nearest Drainage grids and synthetic rating curves.

<br/><br/>

## v3.0.24.3 - 2021-11-29 - [PR #488](https://github.com/NOAA-OWP/cahaba/pull/488)

Fixed projection issue in `synthesize_test_cases.py`.

### Changes

- `Pipfile`: Added `Pyproj` to `Pipfile` to specify a version that did not have the current projection issues.

<br/><br/>

## v3.0.24.2 - 2021-11-18 - [PR #486](https://github.com/NOAA-OWP/cahaba/pull/486)

Adding a new check to keep `usgs_elev_table.csv`, `src_base.csv`, `small_segments.csv` for runs not using the `-viz` flag. We unintentionally deleted some .csv files in `vary_mannings_n_composite.py` but need to maintain some of these for non `-viz` runs (e.g. `usgs_elev_table.csv` is used for sierra test input).

### Changes

- `fim_run.sh`: passing `-v` flag to `vary_mannings_n_composite.py` to determine which csv files to delete. Setting `$viz` = 0 for non `-v` runs.
- `src/vary_mannings_n_composite.py`: added `-v` input arg and if statement to check which .csv files to delete.
- `src/add_crosswalk.py`: removed deprecated barc variables from input args.
- `src/run_by_unit.sh`: removed deprecated barc variables from input args to `add_crosswalk.py`.

<br/><br/>

## v3.0.24.1 - 2021-11-17 - [PR #484](https://github.com/NOAA-OWP/cahaba/pull/484)

Patch to clean up unnecessary files and create better names for intermediate raster files.

### Removals

- `tools/run_test_case_gms.py`: Unnecessary file.

### Changes

- `tools/composite_ms_fr_inundation.py`: Clean up documentation and intermediate file names.
- `tools/run_test_case.py`: Remove unnecessary imports.

<br/><br/>

## v3.0.24.0 - 2021-11-08 - [PR #482](https://github.com/NOAA-OWP/cahaba/pull/482)

Adds `composite_ms_fr_inundation.py` to allow for the generation of an inundation map given a "flow file" CSV and full-resolution (FR) and mainstem (MS) relative elevation models, synthetic rating curves, and catchments rasters created by the `fim_run.sh` script.

### Additions
- `composite_ms_fr_inundation.py`: New module that is used to inundate both MS and FR FIM and composite the two inundation rasters.
- `/tools/gms_tools/`: Three modules (`inundate_gms.py`, `mosaic_inundation.py`, `overlapping_inundation.py`) ported from the GMS branch used to composite inundation rasters.

### Changes
- `inundation.py`: Added 2 exception classes ported from the GMS branch.

<br/><br/>

## v3.0.23.3 - 2021-11-04 - [PR #481](https://github.com/NOAA-OWP/cahaba/pull/481)
Includes additional hydraulic properties to the `hydroTable.csv`: `Number of Cells`, `SurfaceArea (m2)`, `BedArea (m2)`, `Volume (m3)`, `SLOPE`, `LENGTHKM`, `AREASQKM`, `Roughness`, `TopWidth (m)`, `WettedPerimeter (m)`. Also adds `demDerived_reaches_split_points.gpkg`, `flowdir_d8_burned_filled.tif`, and `dem_thalwegCond.tif` to `-v` whitelist.

### Changes
- `run_by_unit.sh`: Added `EXIT FLAG` tag and previous non-zero exit code tag to the print statement to allow log lookup.
- `add_crosswalk.py`: Added extra attributes to the hydroTable.csv. Includes a default `barc_on` and `vmann_on` (=False) attribute that is overwritten (=True) if SRC post-processing modules are run.
- `bathy_src_adjust_topwidth.py`: Overwrites the `barc_on` attribute where applicable and includes the BARC-modified Volume property.
- `vary_mannings_n_composite.py`: Overwrites the `vmann_on` attribute where applicable.
- `output_cleanup.py`: Adds new files to the `-v` whitelist.

<br/><br/>

## v3.0.23.2 - 2021-11-04 - [PR #480](https://github.com/NOAA-OWP/cahaba/pull/480)
Hotfix for `vary_manning_n_composite.py` to address null discharge values for non-CONUS hucs.

### Changes
- `vary_manning_n_composite.py`: Add numpy where clause to set final discharge value to the original value if `vmann=False`

<br/><br/>

## v3.0.23.1 - 2021-11-03 - [PR #479](https://github.com/NOAA-OWP/cahaba/pull/479)
Patches the API updater. The `params_calibrated.env` is replaced with `params_template.env` because the BARC and Multi-N modules supplant the calibrated values.

### Changes
- `api/node/updater/updater.py`: Changed `params_calibrated.env` to `params_template.env`

<br/><br/>

## v3.0.23.0 - 2021-10-31 - [PR #475](https://github.com/NOAA-OWP/cahaba/pull/475)

Moved the synthetic rating curve (SRC) processes from the `\tools` directory to `\src` directory to support post-processing in `fim_run.sh`. These SRC post-processing modules will now run as part of the default `fim_run.sh` workflow. Reconfigured bathymetry adjusted rating curve (BARC) module to use the 1.5yr flow from NWM v2 recurrence flow data in combination with the Bieger et al. (2015) regression equations with bankfull discharge predictor variable input.

### Additions
- `src/bathy_src_adjust_topwidth.py` --> New version of bathymetry adjusted rating curve (BARC) module that is configured to use the Bieger et al. (2015) regression equation with input bankfull discharge as the predictor variable (previous version used the drainage area version of the regression equations). Also added log output capability, added reconfigured output content in `src_full_crosswalked_BARC.csv` and `hydroTable.csv`, and included modifications to allow BARC to run as a post-processing step in `fim_run.sh`. Reminder: BARC is only configured for MS extent.

### Removals
- `config/params_calibrated.env` --> deprecated the calibrated roughness values by stream order with the new introduction of variable/composite roughness module
- `src/bathy_rc_adjust.py` --> deprecated the previous BARC version

### Changes
- `src/identify_src_bankfull.py` --> Moved this script from /tools to /src, added more doc strings, cleaned up output log, and reconfigured to allow execution from fim_run.sh post-processing.
- `src/vary_mannings_n_composite.py` --> Moved this script from /tools to /src, added more doc strings, cleaned up output log, added/reconfigured output content in src_full_crosswalked_vmann.csv and hydroTable.csv, and reconfigured to allow execution from fim_run.sh post-processing.
- `config/params_template.env` --> Added additional parameter/variables for input to `identify_src_bankfull.py`, `vary_mannings_n_composite.py`, and `bathy_src_adjust_topwidth.py`.
      - default BARC input: bankfull channel geometry derived from the Bieger et al. (2015) bankfull discharge regression equations
      - default bankfull flow input: NWM v2 1.5-year recurrence flows
      - default variable roughness input: global (all NWM feature_ids) roughness values of 0.06 for in-channel and 0.11 for max overbank
- `fim_run.sh` --> Added SRC post-processing calls after the `run_by_unit.sh` workflow
- `src/add_crosswalk.py` --> Removed BARC module call (moved to post-processing)
- `src/run_by_unit.sh` --> Removed old/unnecessary print statement.
      - **Note: reset exit codes to 0 for unnecessary processing flags.** Non-zero error codes in `run_by_unit.sh` prevent the `fim_run.sh` post-processing steps from running. This error handling issue will be more appropriately handled in a soon to be release enhancement.
- `tools/run_test_case.py` --> Reverted changes used during development process

<br/><br/>

## v3.0.22.8 - 2021-10-26 - [PR #471](https://github.com/NOAA-OWP/cahaba/pull/471)

Manually filtering segments from stream input layer to fix flow reversal of the MS River (HUC 08030100).

### Changes
- `clip_vectors_to_wbd.py`: Fixes bug where flow direction is reversed for HUC 08030100. The issue is resolved by filtering incoming stream segments that intersect with the elevation grid boundary.

<br/><br/>

## v3.0.22.7 - 2021-10-08 - [PR #467](https://github.com/NOAA-OWP/cahaba/pull/467)

These "tool" enhancements 1) delineate in-channel vs. out-of-channel geometry to allow more targeted development of key physical drivers influencing the SRC calculations (e.g. bathymetry & Manning’s n) #418 and 2) applies a variable/composite Manning’s roughness (n) using user provided csv with in-channel vs. overbank roughness values #419 & #410.

### Additions
- `identify_src_bankfull.p`: new post-processing tool that ingests a flow csv (e.g. NWM 1.5yr recurr flow) to approximate the bankfull STG and then calculate the channel vs. overbank proportions using the volume and hydraulic radius variables
- `vary_mannings_n_composite.py`: new post-processing tool that ingests a csv containing feature_id, channel roughness, and overbank roughness and then generates composite n values via the channel ratio variable

### Changes
- `eval_plots.py`: modified the plot legend text to display full label for development tests
- `inundation.py`: added new optional argument (-n) and corresponding function to produce a csv containing the stage value (and SRC variables) calculated from the flow to stage interpolation.

<br/><br/>

## v3.0.22.6 - 2021-09-13 - [PR #462](https://github.com/NOAA-OWP/cahaba/pull/462)

This new workflow ingests FIM point observations from users and “corrects” the synthetic rating curves to produce the desired FIM extent at locations where feedback is available (locally calibrate FIM).

### Changes
- `add_crosswalk.py`: added `NextDownID` and `order_` attributes to the exported `hydroTable.csv`. This will potentially be used in future enhancements to extend SRC changes to upstream/downstream catchments.
- `adjust_rc_with_feedback.py`: added a new workflow to perform the SRC modifications (revised discharge) using the existing HAND geometry variables combined with the user provided point location flow and stage data.
- `inundation_wrapper_custom_flow.py`: updated code to allow for huc6 processing to generate custom inundation outputs.

<br/><br/>

## v3.0.22.5 - 2021-09-08 - [PR #460](https://github.com/NOAA-OWP/cahaba/pull/460)

Patches an issue where only certain benchmark categories were being used in evaluation.

### Changes
- In `tools/tools_shared_variables.py`, created a variable `MAGNITUDE_DICT` to store benchmark category magnitudes.
- `synthesize_test_cases.py` imports `MAGNITUDE_DICT` and uses it to assign magnitudes.

<br/><br/>

## v3.0.22.4 - 2021-08-30 - [PR #456](https://github.com/NOAA-OWP/cahaba/pull/456)

Renames the BARC modified variables that are exported to `src_full_crosswalked.csv` to replace the original variables. The default/original variables are renamed with `orig_` prefix. This change is needed to ensure downstream uses of the `src_full_crosswalked.csv` are able to reference the authoritative version of the channel geometry variables (i.e. BARC-adjust where available).

### Changes
- In `src_full_crosswalked.csv`, default/original variables are renamed with `orig_` prefix and `SA_div` is renamed to `SA_div_flag`.

<br/><br/>

## v3.0.22.3 - 2021-08-27 - [PR #457](https://github.com/NOAA-OWP/cahaba/pull/457)

This fixes a bug in the `get_metadata()` function in `/tools/tools_shared_functions.py` that arose because of a WRDS update. Previously the `metadata_source` response was returned as independent variables, but now it is returned a list of strings. Another issue was observed where the `EVALUATED_SITES_CSV` variable was being misdefined (at least on the development VM) through the OS environmental variable setting.

### Changes
- In `tools_shared_functions.py`, changed parsing of WRDS `metadata_sources` to account for new list type.
- In `generate_categorical_fim_flows.py`, changed the way the `EVALUATED_SITES_CSV` path is defined from OS environmental setting to a relative path that will work within Docker container.

<br/><br/>

## v3.0.22.2 - 2021-08-26 - [PR #455](https://github.com/NOAA-OWP/cahaba/pull/455)

This merge addresses an issues with the bathymetry adjusted rating curve (BARC) calculations exacerbating single-pixel inundation issues for the lower Mississippi River. This fix allows the user to specify a stream order value that will be ignored in BARC calculations (reverts to using the original/default rating curve). If/when the "thalweg notch" issue is addressed, this change may be unmade.

### Changes
- Added new env variable `ignore_streamorders` set to 10.
- Added new BARC code to set the bathymetry adjusted cross-section area to 0 (reverts to using the default SRC values) based on the streamorder env variable.

<br/><br/>

## v3.0.22.1 - 2021-08-20 - [PR #447](https://github.com/NOAA-OWP/cahaba/pull/447)

Patches the minimum stream length in the template parameters file.

### Changes
- Changes `max_split_distance_meters` in `params_template.env` to 1500.

<br/><br/>

## v3.0.22.0 - 2021-08-19 - [PR #444](https://github.com/NOAA-OWP/cahaba/pull/444)

This adds a script, `adjust_rc_with_feedback.py`, that will be expanded  in future issues. The primary function that performs the HAND value and hydroid extraction is ingest_points_layer() but this may change as the overall synthetic rating curve automatic update machanism evolves.

### Additions
- Added `adjust_rc_with_feedback.py` with `ingest_points_layer()`, a function to extract HAND and hydroid values for use in an automatic synthetic rating curve updating mechanism.

<br/><br/>

## v3.0.21.0 - 2021-08-18 - [PR #433](https://github.com/NOAA-OWP/cahaba/pull/433)

General repository cleanup, made memory-profiling an optional flag, API's release feature now saves outputs.

### Changes
- Remove `Dockerfile.prod`, rename `Dockerfile.dev` to just `Dockerfile`, and remove `.dockerignore`.
- Clean up `Dockerfile` and remove any unused* packages or variables.
- Remove any unused* Python packages from the `Pipfile`.
- Move the `CHANGELOG.md`, `SECURITY.md`, and `TERMS.md` files to the `/docs` folder.
- Remove any unused* scripts in the `/tools` and `/src` folders.
- Move `tools/preprocess` scripts into `tools/`.
- Ensure all scripts in the `/src` folder have their code in functions and are being called via a `__main__` function (This will help with implementing memory profiling fully).
- Changed memory-profiling to be an option flag `-m` for `fim_run.sh`.
- Updated FIM API to save all outputs during a "release" job.

<br/><br/>

## v3.0.20.2 - 2021-08-13 - [PR #443](https://github.com/NOAA-OWP/cahaba/pull/443)

This merge modifies `clip_vectors_to_wbd.py` to check for relevant input data.

### Changes
- `clip_vectors_to_wbd.py` now checks that there are NWM stream segments within the buffered HUC boundary.
- `included_huc8_ms.lst` has several additional HUC8s.

<br/><br/>

## v3.0.20.1 - 2021-08-12 - [PR #442](https://github.com/NOAA-OWP/cahaba/pull/442)

This merge improves documentation in various scripts.

### Changes
This PR better documents the following:

- `inundate_nation.py`
- `synthesize_test_cases.py`
- `adjust_thalweg_lateral.py`
- `rem.py`

<br/><br/>

## v3.0.20.0 - 2021-08-11 - [PR #440](https://github.com/NOAA-OWP/cahaba/pull/440)

This merge adds two new scripts into `/tools/` for use in QAQC.

### Additions
- `inundate_nation.py` to produce inundation maps for the entire country for use in QAQC.
- `check_deep_flooding.py` to check for depths of inundation greater than a user-supplied threshold at specific areas defined by a user-supplied shapefile.

<br/><br/>

## v3.0.19.5 - 2021-07-19

Updating `README.md`.

<br/><br/>

## v3.0.19.4 - 2021-07-13 - [PR #431](https://github.com/NOAA-OWP/cahaba/pull/431)

Updating logging and fixing bug in vector preprocessing.

### Additions
- `fim_completion_check.py` adds message to docker log to log any HUCs that were requested but did not finish `run_by_unit.sh`.
- Adds `input_data_edits_changelog.txt` to the inputs folder to track any manual or version/location specific changes that were made to data used in FIM 3.

### Changes
- Provides unique exit codes to relevant domain checkpoints within `run_by_unit.sh`.
- Bug fixes in `reduce_nhd_stream_density.py`, `mprof plot` call.
- Improved error handling in `add_crosswalk.py`.

<br/><br/>

## v3.0.19.3 - 2021-07-09

Hot fix to `synthesize_test_cases`.

### Changes
- Fixed if/elif/else statement in `synthesize_test_cases.py` that resulted in only IFC data being evaluated.

<br/><br/>

## v3.0.19.2 - 2021-07-01 - [PR #429](https://github.com/NOAA-OWP/cahaba/pull/429)

Updates to evaluation scripts to allow for Alpha testing at Iowa Flood Center (IFC) sites. Also, `BAD_SITES` variable updates to omit sites not suitable for evaluation from metric calculations.

### Changes
- The `BAD_SITES` list in `tools_shared_variables.py` was updated and reasons for site omission are documented.
- Refactored `run_test_case.py`, `synthesize_test_cases.py`, `tools_shared_variables.py`, and `eval_plots.py` to allow for IFC comparisons.

<br/><br/>

## v3.0.19.1 - 2021-06-17 - [PR #417](https://github.com/NOAA-OWP/cahaba/pull/417)

Adding a thalweg profile tool to identify significant drops in thalweg elevation. Also setting lateral thalweg adjustment threshold in hydroconditioning.

### Additions
- `thalweg_drop_check.py` checks the elevation along the thalweg for each stream path downstream of MS headwaters within a HUC.

### Removals
- Removing `dissolveLinks` arg from `clip_vectors_to_wbd.py`.

### Changes
- Cleaned up code in `split_flows.py` to make it more readable.
- Refactored `reduce_nhd_stream_density.py` and `adjust_headwater_streams.py` to limit MS headwater points in `agg_nhd_headwaters_adj.gpkg`.
- Fixed a bug in `adjust_thalweg_lateral.py` lateral elevation replacement threshold; changed threshold to 3 meters.
- Updated `aggregate_vector_inputs.py` to log intermediate processes.

<br/><br/>

## v3.0.19.0 - 2021-06-10 - [PR #415](https://github.com/NOAA-OWP/cahaba/pull/415)

Feature to evaluate performance of alternative CatFIM techniques.

### Additions
- Added `eval_catfim_alt.py` to evaluate performance of alternative CatFIM techniques.

<br/><br/>

## v3.0.18.0 - 2021-06-09 - [PR #404](https://github.com/NOAA-OWP/cahaba/pull/404)

To help analyze the memory consumption of the Fim Run process, the python module `memory-profiler` has been added to give insights into where peak memory usage is with in the codebase.

In addition, the Dockerfile was previously broken due to the Taudem dependency removing the version that was previously being used by FIM. To fix this, and allow new docker images to be built, the Taudem version has been updated to the newest version on the Github repo and thus needs to be thoroughly tested to determine if this new version has affected the overall FIM outputs.

### Additions
- Added `memory-profiler` to `Pipfile` and `Pipfile.lock`.
- Added `mprof` (memory-profiler cli utility) call to the `time_and_tee_run_by_unit.sh` to create overall memory usage graph location in the `/logs/{HUC}_memory.png` of the outputs directory.
- Added `@profile` decorator to all functions within scripts used in the `run_by_unit.sh` script to allow for memory usage tracking, which is then recorded in the `/logs/{HUC}.log` file of the outputs directory.

### Changes
- Changed the Taudem version in `Dockerfile.dev` to `98137bb6541a0d0077a9c95becfed4e56d0aa0ac`.
- Changed all calls of python scripts in `run_by_unit.s` to be called with the `-m memory-profiler` argument to allow scripts to also track memory usage.

<br/><br/>

## v3.0.17.1 - 2021-06-04 - [PR #395](https://github.com/NOAA-OWP/cahaba/pull/395)

Bug fix to the `generate_nws_lid.py` script

### Changes
- Fixes incorrectly assigned attribute field "is_headwater" for some sites in the `nws_lid.gpkg` layer.
- Updated `agg_nhd_headwaters_adj.gpkg`, `agg_nhd_streams_adj.gpkg`, `nwm_flows.gpkg`, and `nwm_catchments.gpkg` input layers using latest NWS LIDs.

<br/><br/>

## v3.0.17.0 - 2021-06-04 - [PR #393](https://github.com/NOAA-OWP/cahaba/pull/393)
BARC updates to cap the bathy calculated xsec area in `bathy_rc_adjust.py` and allow user to choose input bankfull geometry.

### Changes

- Added new env variable to control which input file is used for the bankfull geometry input to bathy estimation workflow.
- Modified the bathymetry cross section area calculation to cap the additional area value so that it cannot exceed the bankfull cross section area value for each stream segment (bankfull value obtained from regression equation dataset).
- Modified the `rating_curve_comparison.py` plot output to always put the FIM rating curve on top of the USGS rating curve (avoids USGS points covering FIM).
- Created a new aggregate csv file (aggregates for all hucs) for all of the `usgs_elev_table.csv` files (one per huc).
- Evaluate the FIM Bathymetry Adjusted Rating Curve (BARC) tool performance using the estimated bankfull geometry dataset derived for the NWM route link dataset.

<br/><br/>

## v3.0.16.3 - 2021-05-21 - [PR #388](https://github.com/NOAA-OWP/cahaba/pull/388)

Enhancement and bug fixes to `synthesize_test_cases.py`.

### Changes
- Addresses a bug where AHPS sites without benchmark data were receiving a CSI of 0 in the master metrics CSV produced by `synthesize_test_cases.py`.
- Includes a feature enhancement to `synthesize_test_cases.py` that allows for the inclusion of user-specified testing versions in the master metrics CSV.
- Removes some of the print statements used by `synthesize_test_cases.py`.

<br/><br/>

## v3.0.16.2 - 2021-05-18 - [PR #384](https://github.com/NOAA-OWP/cahaba/pull/384)

Modifications and fixes to `run_test_case.py`, `eval_plots.py`, and AHPS preprocessing scripts.

### Changes
- Comment out return statement causing `run_test_case.py` to skip over sites/hucs when calculating contingency rasters.
- Move bad sites list and query statement used to filter out bad sites to the `tools_shared_variables.py`.
- Add print statements in `eval_plots.py` detailing the bad sites used and the query used to filter out bad sites.
- Update AHPS preprocessing scripts to produce a domain shapefile.
- Change output filenames produced in ahps preprocessing scripts.
- Update workarounds for some sites in ahps preprocessing scripts.

<br/><br/>

## v3.0.16.1 - 2021-05-11 - [PR #380](https://github.com/NOAA-OWP/cahaba/pull/380)

The current version of Eventlet used in the Connector module of the FIM API is outdated and vulnerable. This update bumps the version to the patched version.

### Changes
- Updated `api/node/connector/requirements.txt` to have the Eventlet version as 0.31.0

<br/><br/>

## v3.0.16.0 - 2021-05-07 - [PR #378](https://github.com/NOAA-OWP/cahaba/pull/378)

New "Release" feature added to the FIM API. This feature will allow for automated FIM, CatFIM, and relevant metrics to be generated when a new FIM Version is released. See [#373](https://github.com/NOAA-OWP/cahaba/issues/373) for more detailed steps that take place in this feature.

### Additions
- Added new window to the UI in `api/frontend/gui/templates/index.html`.
- Added new job type to `api/node/connector/connector.py` to allow these release jobs to run.
- Added additional logic in `api/node/updater/updater.py` to run the new eval and CatFIM scripts used in the release feature.

### Changes
- Updated `api/frontend/output_handler/output_handler.py` to allow for copying more broad ranges of file paths instead of only the `/data/outputs` directory.

<br/><br/>

## v3.0.15.10 - 2021-05-06 - [PR #375](https://github.com/NOAA-OWP/cahaba/pull/375)

Remove Great Lakes coastlines from WBD buffer.

### Changes
- `gl_water_polygons.gpkg` layer is used to mask out Great Lakes boundaries and remove NHDPlus HR coastline segments.

<br/><br/>

## v3.0.15.9 - 2021-05-03 - [PR #372](https://github.com/NOAA-OWP/cahaba/pull/372)

Generate `nws_lid.gpkg`.

### Additions
- Generate `nws_lid.gpkg` with attributes indicating if site is a headwater `nws_lid` as well as if it is co-located with another `nws_lid` which is referenced to the same `nwm_feature_id` segment.

<br/><br/>

## v3.0.15.8 - 2021-04-29 - [PR #371](https://github.com/NOAA-OWP/cahaba/pull/371)

Refactor NHDPlus HR preprocessing workflow. Resolves issue #238

### Changes
- Consolidate NHD streams, NWM catchments, and headwaters MS and FR layers with `mainstem` column.
- HUC8 intersections are included in the input headwaters layer.
- `clip_vectors_to_wbd.py` removes incoming stream segment from the selected layers.

<br/><br/>

## v3.0.15.7 - 2021-04-28 - [PR #367](https://github.com/NOAA-OWP/cahaba/pull/367)

Refactor synthesize_test_case.py to handle exceptions during multiprocessing. Resolves issue #351

### Changes
- refactored `inundation.py` and `run_test_case.py` to handle exceptions without using `sys.exit()`.

<br/><br/>

## v3.0.15.6 - 2021-04-23 - [PR #365](https://github.com/NOAA-OWP/cahaba/pull/365)

Implement CatFIM threshold flows to Sierra test and add AHPS benchmark preprocessing scripts.

### Additions
- Produce CatFIM flows file when running `rating_curve_get_usgs_gages.py`.
- Several scripts to preprocess AHPS benchmark data. Requires numerous file dependencies not available through Cahaba.

### Changes
- Modify `rating_curve_comparison.py` to ingest CatFIM threshold flows in calculations.
- Modify `eval_plots.py` to save all site specific bar plots in same parent directory instead of in subdirectories.
- Add variables to `env.template` for AHPS benchmark preprocessing.

<br/><br/>

## v3.0.15.5 - 2021-04-20 - [PR #363](https://github.com/NOAA-OWP/cahaba/pull/363)

Prevent eval_plots.py from erroring out when spatial argument enabled if certain datasets not analyzed.

### Changes
- Add check to make sure analyzed dataset is available prior to creating spatial dataset.

<br/><br/>

## v3.0.15.4 - 2021-04-20 - [PR #356](https://github.com/NOAA-OWP/cahaba/pull/356)

Closing all multiprocessing Pool objects in repo.

<br/><br/>

## v3.0.15.3 - 2021-04-19 - [PR #358](https://github.com/NOAA-OWP/cahaba/pull/358)

Preprocess NHDPlus HR rasters for consistent projections, nodata values, and convert from cm to meters.

### Additions
- `preprocess_rasters.py` reprojects raster, converts to meters, and updates nodata value to -9999.
- Cleaned up log messages from `bathy_rc_adjust.py` and `usgs_gage_crosswalk.py`.
- Outputs paths updated in `generate_categorical_fim_mapping.py` and `generate_categorical_fim.py`.
- `update_raster_profile` cleans up raster crs, blocksize, nodata values, and converts elevation grids from cm to meters.
- `reproject_dem.py` imports gdal to reproject elevation rasters because an error was occurring when using rasterio.

### Changes
- `burn_in_levees.py` replaces the `gdal_calc.py` command to resolve inconsistent outputs with burned in levee values.

<br/><br/>

## v3.0.15.2 - 2021-04-16 - [PR #359](https://github.com/NOAA-OWP/cahaba/pull/359)

Hotfix to preserve desired files when production flag used in `fim_run.sh`.

### Changes

- Fixed production whitelisted files.

<br/><br/>

## v3.0.15.1 - 2021-04-13 - [PR #355](https://github.com/NOAA-OWP/cahaba/pull/355)

Sierra test considered all USGS gage locations to be mainstems even though many actually occurred with tributaries. This resulted in unrealistic comparisons as incorrect gages were assigned to mainstems segments. This feature branch identifies gages that are on mainstems via attribute field.

### Changes

- Modifies `usgs_gage_crosswalk.py` to filter out gages from the `usgs_gages.gpkg` layer such that for a "MS" run, only consider gages that contain rating curve information (via `curve` attribute) and are also mainstems gages (via `mainstems` attribute).
- Modifies `usgs_gage_crosswalk.py` to filter out gages from the `usgs_gages.gpkg` layer such that for a "FR" run, only consider gages that contain rating curve information (via `curve` attribute) and are not mainstems gages (via `mainstems` attribute).
- Modifies how mainstems segments are determined by using the `nwm_flows_ms.gpkg` as a lookup to determine if the NWM segment specified by WRDS for a gage site is a mainstems gage.

### Additions

- Adds a `mainstem` attribute field to `usgs_gages.gpkg` that indicates whether a gage is located on a mainstems river.
- Adds `NWM_FLOWS_MS` variable to the `.env` and `.env.template` files.
- Adds the `extent` argument specified by user when running `fim_run.sh` to `usgs_gage_crosswalk.py`.

<br/><br/>

## v3.0.15.0 - 2021-04-08 - [PR #340](https://github.com/NOAA-OWP/cahaba/pull/340)

Implementing a prototype technique to estimate the missing bathymetric component in the HAND-derived synthetic rating curves. The new Bathymetric Adjusted Rating Curve (BARC) function is built within the `fim_run.sh` workflow and will ingest bankfull geometry estimates provided by the user to modify the cross section area used in the synthetic rating curve generation.

### Changes
 - `add_crosswalk.py` outputs the stream order variables to `src_full_crosswalked.csv` and calls the new `bathy_rc_adjust.py` if bathy env variable set to True and `extent=MS`.
 - `run_by_unit.sh` includes a new csv outputs for reviewing BARC calculations.
 - `params_template.env` & `params_calibrated.env` contain new BARC function input variables and on/off toggle variable.
 - `eval_plots.py` now includes additional AHPS eval sites in the list of "bad_sites" (flagged issues with MS flowlines).

### Additions
 - `bathy_rc_adjust.py`:
    - Imports the existing synthetic rating curve table and the bankfull geometry input data (topwidth and cross section area per COMID).
    - Performs new synthetic rating curve calculations with bathymetry estimation modifications.
    - Flags issues with the thalweg-notch artifact.

<br/><br/>

## v3.0.14.0 - 2021-04-05 - [PR #338](https://github.com/NOAA-OWP/cahaba/pull/338)

Create tool to retrieve rating curves from USGS sites and convert to elevation (NAVD88). Intended to be used as part of the Sierra Test.

### Changes
 - Modify `usgs_gage_crosswalk.py` to:
    1) Look for `location_id` instead of `site_no` attribute field in `usgs_gages.gpkg` file.
    2) Filter out gages that do not have rating curves included in the `usgs_rating_curves.csv`.
 - Modify `rating_curve_comparison.py` to perform a check on the age of the user specified `usgs_rating_curves.csv` and alert user to the age of the file and recommend updating if file is older the 30 days.

### Additions
 - Add `rating_curve_get_usgs_curves.py`. This script will generate the following files:
     1) `usgs_rating_curves.csv`: A csv file that contains rating curves (including converted to NAVD88 elevation) for USGS gages in a format that is compatible with  `rating_curve_comparisons.py`. As it is is currently configured, only gages within CONUS will have rating curve data.
     2) `log.csv`: A log file that records status for each gage and includes error messages.
     3) `usgs_gages.gpkg`: A geospatial layer (in FIM projection) of all active USGS gages that meet a predefined criteria. Additionally, the `curve` attribute indicates whether a rating curve is found in the `usgs_rating_curves.csv`. This spatial file is only generated if the `all` option is passed with the `-l` argument.

<br/><br/>

## v3.0.13.0 - 2021-04-01 - [PR #332](https://github.com/NOAA-OWP/cahaba/pull/332)

Created tool to compare synthetic rating curve with benchmark rating curve (Sierra Test).

### Changes
 - Update `aggregate_fim_outputs.py` call argument in `fim_run.sh` from 4 jobs to 6 jobs, to optimize API performance.
 - Reroutes median elevation data from `add_crosswalk.py` and `rem.py` to new file (depreciating `hand_ref_elev_table.csv`).
 - Adds new files to `viz_whitelist` in `output_cleanup.py`.

### Additions
 - `usgs_gage_crosswalk.py`: generates `usgs_elev_table.csv` in `run_by_unit.py` with elevation and additional attributes at USGS gages.
 - `rating_curve_comparison.py`: post-processing script to plot and calculate metrics between synthetic rating curves and USGS rating curve data.

<br/><br/>

## v3.0.12.1 - 2021-03-31 - [PR #336](https://github.com/NOAA-OWP/cahaba/pull/336)

Fix spatial option in `eval_plots.py` when creating plots and spatial outputs.

### Changes
 - Removes file dependencies from spatial option. Does require the WBD layer which should be specified in `.env` file.
 - Produces outputs in a format consistent with requirements needed for publishing.
 - Preserves leading zeros in huc information for all outputs from `eval_plots.py`.

### Additions
 - Creates `fim_performance_points.shp`: this layer consists of all evaluated ahps points (with metrics). Spatial data retrieved from WRDS on the fly.
 - Creates `fim_performance_polys.shp`: this layer consists of all evaluated huc8s (with metrics). Spatial data retrieved from WBD layer.

<br/><br/>

## v3.0.12.0 - 2021-03-26 - [PR #327](https://github.com/NOAA-OWP/cahaba/pull/237)

Add more detail/information to plotting capabilities.

### Changes
 - Merge `plot_functions.py` into `eval_plots.py` and move `eval_plots.py` into the tools directory.
 - Remove `plots` subdirectory.

### Additions
 - Optional argument to create barplots of CSI for each individual site.
 - Create a csv containing the data used to create the scatterplots.

<br/><br/>

## v3.0.11.0 - 2021-03-22 - [PR #319](https://github.com/NOAA-OWP/cahaba/pull/298)

Improvements to CatFIM service source data generation.

### Changes
 - Renamed `generate_categorical_fim.py` to `generate_categorical_fim_mapping.py`.
 - Updated the status outputs of the `nws_lid_sites layer` and saved it in the same directory as the `merged catfim_library layer`.
 - Additional stability fixes (such as improved compatability with WRDS updates).

### Additions
 - Added `generate_categorical_fim.py` to wrap `generate_categorical_fim_flows.py` and `generate_categorical_fim_mapping.py`.
 - Create new `nws_lid_sites` shapefile located in same directory as the `catfim_library` shapefile.

<br/><br/>

## v3.0.10.1 - 2021-03-24 - [PR #320](https://github.com/NOAA-OWP/cahaba/pull/320)

Patch to synthesize_test_cases.py.

### Changes
 - Bug fix to `synthesize_test_cases.py` to allow comparison between `testing` version and `official` versions.

<br/><br/>

## v3.0.10.0 - 2021-03-12 - [PR #298](https://github.com/NOAA-OWP/cahaba/pull/298)

Preprocessing of flow files for Categorical FIM.

### Additions
 - Generate Categorical FIM flow files for each category (action, minor, moderate, major).
 - Generate point shapefile of Categorical FIM sites.
 - Generate csv of attribute data in shapefile.
 - Aggregate all shapefiles and csv files into one file in parent directory.
 - Add flood of record category.

 ### Changes
 - Stability fixes to `generate_categorical_fim.py`.

<br/><br/>

## v3.0.9.0 - 2021-03-12 - [PR #297](https://github.com/NOAA-OWP/cahaba/pull/297)

Enhancements to FIM API.

### Changes
 - `fim_run.sh` can now be run with jobs in parallel.
 - Viz post-processing can now be selected in API interface.
 - Jobs table shows jobs that end with errors.
 - HUC preset lists can now be selected in interface.
 - Better `output_handler` file writing.
 - Overall better restart and retry handlers for networking problems.
 - Jobs can now be canceled in API interface.
 - Both FR and MS configs can be selected for a single job.

<br/><br/>

## v3.0.8.2 - 2021-03-11 - [PR #296](https://github.com/NOAA-OWP/cahaba/pull/296)

Enhancements to post-processing for Viz-related use-cases.

### Changes
 - Aggregate grids are projected to Web Mercator during `-v` runs in `fim_run.sh`.
 - HUC6 aggregation is parallelized.
 - Aggregate grid blocksize is changed from 256 to 1024 for faster postprocessing.

<br/><br/>

## v3.0.8.1 - 2021-03-10 - [PR #302](https://github.com/NOAA-OWP/cahaba/pull/302)

Patched import issue in `tools_shared_functions.py`.

### Changes
 - Changed `utils.` to `tools_` in `tools_shared_functions.py` after recent structural change to `tools` directory.

<br/><br/>

## v3.0.8.0 - 2021-03-09 - [PR #279](https://github.com/NOAA-OWP/cahaba/pull/279)

Refactored NWS Flood Categorical HAND FIM (CatFIM) pipeline to open source.

### Changes
 - Added `VIZ_PROJECTION` to `shared_variables.py`.
 - Added missing library referenced in `inundation.py`.
 - Cleaned up and converted evaluation scripts in `generate_categorical_fim.py` to open source.
 - Removed `util` folders under `tools` directory.

<br/><br/>

## v3.0.7.1 - 2021-03-02 - [PR #290](https://github.com/NOAA-OWP/cahaba/pull/290)

Renamed benchmark layers in `test_cases` and updated variable names in evaluation scripts.

### Changes
 - Updated `run_test_case.py` with new benchmark layer names.
 - Updated `run_test_case_calibration.py` with new benchmark layer names.

<br/><br/>

## v3.0.7.0 - 2021-03-01 - [PR #288](https://github.com/NOAA-OWP/cahaba/pull/288)

Restructured the repository. This has no impact on hydrological work done in the codebase and is simply moving files and renaming directories.

### Changes
 - Moved the contents of the `lib` folder to a new folder called `src`.
 - Moved the contents of the `tests` folder to the `tools` folder.
 - Changed any instance of `lib` or `libDir` to `src` or `srcDir`.

<br/><br/>

## v3.0.6.0 - 2021-02-25 - [PR #276](https://github.com/NOAA-OWP/cahaba/pull/276)

Enhancement that creates metric plots and summary statistics using metrics compiled by `synthesize_test_cases.py`.

### Additions
 - Added `eval_plots.py`, which produces:
    - Boxplots of CSI, FAR, and POD/TPR
    - Barplot of aggregated CSI scores
    - Scatterplot of CSI comparing two FIM versions
    - CSV of aggregated statistics (CSI, FAR, POD/TPR)
    - CSV of analyzed data and analyzed sites

<br/><br/>

## v3.0.5.3 - 2021-02-23 - [PR #275](https://github.com/NOAA-OWP/cahaba/pull/275)

Bug fixes to new evaluation code.

### Changes

 - Fixed a bug in `synthesize_test_cases.py` where the extent (MS/FR) was not being written to merged metrics file properly.
 - Fixed a bug in `synthesize_test_cases.py` where only BLE test cases were being written to merged metrics file.
 - Removed unused imports from `inundation.py`.
 - Updated README.md

<br/><br/>

## v3.0.5.2 - 2021-02-23 - [PR #272](https://github.com/NOAA-OWP/cahaba/pull/272)

Adds HAND synthetic rating curve (SRC) datum elevation values to `hydroTable.csv` output.

### Changes

 - Updated `add_crosswalk.py` to included "Median_Thal_Elev_m" variable outputs in `hydroTable.csv`.
 - Renamed hydroid attribute in `rem.py` to "Median" in case we want to include other statistics in the future (e.g. min, max, range etc.).

<br/><br/>
## v3.0.5.1 - 2021-02-22

Fixed `TEST_CASES_DIR` path in `tests/utils/shared_variables.py`.

### Changes

 - Removed `"_new"` from `TEST_CASES_DIR` variable.

<br/><br/>

## v3.0.5.0 - 2021-02-22 - [PR #267](https://github.com/NOAA-OWP/cahaba/pull/267)

Enhancements to allow for evaluation at AHPS sites, the generation of a query-optimized metrics CSV, and the generation of categorical FIM. This merge requires that the `/test_cases` directory be updated for all machines performing evaluation.

### Additions

 - `generate_categorical_fim.py` was added to allow production of NWS Flood Categorical HAND FIM (CatFIM) source data. More changes on this script are to follow in subsequent branches.

### Removals

 - `ble_autoeval.sh` and `all_ble_stats_comparison.py` were deleted because `synthesize_test_cases.py` now handles the merging of metrics.
 - The code block in `run_test_case.py` that was responsible for printing the colored metrics to screen has been commented out because of the new scale of evaluations (formerly in `run_test_case.py`, now in `shared_functions.py`)
 - Remove unused imports from inundation wrappers in `/tools`.

### Changes

 - Updated `synthesize_test_cases.py` to allow for AHPS site evaluations.
 - Reorganized `run_test_case.py` by moving more functions into `shared_functions.py`.
 - Created more shared variables in `shared_variables.py` and updated import statements in relevant scripts.

<br/><br/>

## v3.0.4.4 - 2021-02-19 - [PR #266](https://github.com/NOAA-OWP/cahaba/pull/266)

Rating curves for short stream segments are replaced with rating curves from upstream/downstream segments.

### Changes

 - Short stream segments are identified and are reassigned the channel geometry from upstream/downstream segment.
 - `fossid` renamed to `fimid` and the attribute's starting value is now 1000 to avoid HydroIDs with leading zeroes.
 - Addresses issue where HydroIDs were not included in final hydrotable.
 - Added `import sys` to `inundation.py` (missing from previous feature branch).
 - Variable names and general workflow are cleaned up.

<br/><br/>

## v3.0.4.3 - 2021-02-12 - [PR #254](https://github.com/NOAA-OWP/cahaba/pull/254)

Modified `rem.py` with a new function to output HAND reference elev.

### Changes

 - Function `make_catchment_hydroid_dict` creates a df of pixel catchment ids and overlapping hydroids.
 - Merge hydroid df and thalweg minimum elevation df.
 - Produces new output containing all catchment ids and min thalweg elevation value named `hand_ref_elev_table.csv`.
 - Overwrites the `demDerived_reaches_split.gpk` layer by adding additional attribute `Min_Thal_Elev_meters` to view the elevation value for each hydroid.

<br/><br/>

## v3.0.4.2 - 2021-02-12 - [PR #255](https://github.com/NOAA-OWP/cahaba/pull/255)

Addresses issue when running on HUC6 scale.

### Changes

 - `src.json` should be fixed and slightly smaller by removing whitespace.
 - Rasters are about the same size as running fim as huc6 (compressed and tiled; aggregated are slightly larger).
 - Naming convention and feature id attribute are only added to the aggregated hucs.
 - HydroIDs are different for huc6 vs aggregated huc8s mostly due to forced split at huc boundaries (so long we use consistent workflow it shouldn't matter).
 - Fixed known issue where sometimes an incoming stream is not included in the final selection will affect aggregate outputs.

<br/><br/>

## v3.0.4.1 - 2021-02-12 - [PR #261](https://github.com/NOAA-OWP/cahaba/pull/261)

Updated MS Crosswalk method to address gaps in FIM.

### Changes

 - Fixed typo in stream midpoint calculation in `split_flows.py` and `add_crosswalk.py`.
 - `add_crosswalk.py` now restricts the MS crosswalk to NWM MS catchments.
 - `add_crosswalk.py` now performs a secondary MS crosswalk selection by nearest NWM MS catchment.

<br/><br/>

## v3.0.4.0 - 2021-02-10 - [PR #256](https://github.com/NOAA-OWP/cahaba/pull/256)

New python script "wrappers" for using `inundation.py`.

### Additions

 - Created `inundation_wrapper_nwm_flows.py` to produce inundation outputs using NWM recurrence flows: 1.5 year, 5 year, 10 year.
 - Created `inundation_wrapper_custom_flow.py` to produce inundation outputs with user-created flow file.
 - Created new `tools` parent directory to store `inundation_wrapper_nwm_flows.py` and  `inundation_wrapper_custom_flow.py`.

<br/><br/>

## v3.0.3.1 - 2021-02-04 - [PR #253](https://github.com/NOAA-OWP/cahaba/pull/253)

Bug fixes to correct mismatched variable name and file path.

### Changes

 - Corrected variable name in `fim_run.sh`.
 - `acquire_and_preprocess_inputs.py` now creates `huc_lists` folder and updates file path.

<br/><br/>

## v3.0.3.0 - 2021-02-04 - [PR #227](https://github.com/NOAA-OWP/cahaba/pull/227)

Post-process to aggregate FIM outputs to HUC6 scale.

### Additions

 - Viz outputs aggregated to HUC6 scale; saves outputs to `aggregate_fim_outputs` folder.

### Changes

 - `split_flows.py` now splits streams at HUC8 boundaries to ensure consistent catchment boundaries along edges.
 - `aggregate_fim_outputs.sh` has been depreciated but remains in the repo for potential FIM 4 development.
 - Replaced geopandas driver arg with getDriver throughout repo.
 - Organized parameters in environment files by group.
 - Cleaned up variable names in `split_flows.py` and `build_stream_traversal.py`.
 - `build_stream_traversal.py` is now assigning HydroID by midpoint instead centroid.
 - Cleanup of `clip_vectors_to_wbd.py`.

<br/><br/>

## v3.0.2.0 - 2021-01-25 - [PR #218](https://github.com/NOAA-OWP/cahaba/pull/218)

Addition of an API service to schedule, run and manage `fim_run` jobs through a user-friendly web interface.

### Additions

 - `api` folder that contains all the codebase for the new service.

<br/><br/>

## v3.0.1.0 - 2021-01-21 - [PR #206](https://github.com/NOAA-OWP/cahaba/pull/206)

Preprocess MS and FR stream networks

### Changes

 - Headwater stream segments geometries are adjusted to align with with NWM streams.
 - Incoming streams are selected using intersection points between NWM streams and HUC4 boundaries.
 - `clip_vectors_to_wbd.py` handles local headwaters.
 - Removes NHDPlus features categorized as coastline and underground conduit.
 - Added streams layer to production whitelist.
 - Fixed progress bar in `lib/acquire_and_preprocess_inputs.py`.
 - Added `getDriver` to shared `functions.py`.
 - Cleaned up variable names and types.

<br/><br/>

## v3.0.0.4 - 2021-01-20 - [PR #230](https://github.com/NOAA-OWP/cahaba/pull/230)

Changed the directory where the `included_huc*.lst` files are being read from.

### Changes

 - Changed the directory where the `included_huc*.lst` files are being read from.

<br/><br/>

## v3.0.0.3 - 2021-01-14 - [PR #210](https://github.com/NOAA-OWP/cahaba/pull/210)

Hotfix for handling nodata value in rasterized levee lines.

### Changes

 - Resolves bug for HUCs where `$ndv > 0` (Great Lakes region).
 - Initialize the `nld_rasterized_elev.tif` using a value of `-9999` instead of `$ndv`.

 <br/><br/>

## v3.0.0.2 - 2021-01-06 - [PR #200](https://github.com/NOAA-OWP/cahaba/pull/200)

Patch to address AHPSs mapping errors.

### Changes

 - Checks `dtype` of `hydroTable.csv` columns to resolve errors caused in `inundation.py` when joining to flow forecast.
 - Exits `inundation.py` when all hydrotable HydroIDs are lake features.
 - Updates path to latest AHPs site layer.
 - Updated [readme](https://github.com/NOAA-OWP/cahaba/commit/9bffb885f32dfcd95978c7ccd2639f9df56ff829)

<br/><br/>

## v3.0.0.1 - 2020-12-31 - [PR #184](https://github.com/NOAA-OWP/cahaba/pull/184)

Modifications to build and run Docker image more reliably. Cleanup on some pre-processing scripts.

### Changes

 - Changed to noninteractive install of GRASS.
 - Changed some paths from relative to absolute and cleaned up some python shebang lines.

### Notes
 - `aggregate_vector_inputs.py` doesn't work yet. Need to externally download required data to run fim_run.sh

 <br/><br/>

## v3.0.0.0 - 2020-12-22 - [PR #181](https://github.com/NOAA-OWP/cahaba/pull/181)

The software released here builds on the flood inundation mapping capabilities demonstrated as part of the National Flood Interoperability Experiment, the Office of Water Prediction's Innovators Program and the National Water Center Summer Institute. The flood inundation mapping software implements the Height Above Nearest Drainage (HAND) algorithm and incorporates community feedback and lessons learned over several years. The software has been designed to meet the requirements set by stakeholders interested in flood prediction and has been developed in partnership with several entities across the water enterprise.
