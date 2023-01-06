All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

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
