All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.
<br/><br/>

## v3.0.22.6 - 2021-09-13 - [PR #462](https://github.com/NOAA-OWP/cahaba/pull/462)

This new workflow ingests FIM point observations from users and “corrects” the synthetic rating curves to produce the desired FIM extent at locations where feedback is available (locally calibrate FIM).

## Changes
- `add_crosswalk.py`: added `NextDownID` and `order_` attributes to the exported `hydroTable.csv`. This will potentially be used in future enhancements to extend SRC changes to upstream/downstream catchments.
- `adjust_rc_with_feedback.py`: added a new workflow to perform the SRC modifications (revised discharge) using the existing HAND geometry variables combined with the user provided point location flow and stage data.
- `inundation_wrapper_custom_flow.py`: updated code to allow for huc6 processing to generate custom inundation outputs.

<br/><br/>

## v3.0.22.5 - 2021-09-08 - [PR #460](https://github.com/NOAA-OWP/cahaba/pull/460)

Patches an issue where only certain benchmark categories were being used in evaluation.

## Changes
- In `tools/tools_shared_variables.py`, created a variable `MAGNITUDE_DICT` to store benchmark category magnitudes.
- `synthesize_test_cases.py` imports `MAGNITUDE_DICT` and uses it to assign magnitudes.

<br/><br/>

## v3.0.22.4 - 2021-08-30 - [PR #456](https://github.com/NOAA-OWP/cahaba/pull/456)

Renames the BARC modified variables that are exported to `src_full_crosswalked.csv` to replace the original variables. The default/original variables are renamed with `orig_` prefix. This change is needed to ensure downstream uses of the `src_full_crosswalked.csv` are able to reference the authoritative version of the channel geometry variables (i.e. BARC-adjust where available).

## Changes
- In `src_full_crosswalked.csv`, default/original variables are renamed with `orig_` prefix and `SA_div` is renamed to `SA_div_flag`.

<br/><br/>

## v3.0.22.3 - 2021-08-27 - [PR #457](https://github.com/NOAA-OWP/cahaba/pull/457)

This fixes a bug in the `get_metadata()` function in `/tools/tools_shared_functions.py` that arose because of a WRDS update. Previously the `metadata_source` response was returned as independent variables, but now it is returned a list of strings. Another issue was observed where the `EVALUATED_SITES_CSV` variable was being misdefined (at least on the development VM) through the OS environmental variable setting.

## Changes
- In `tools_shared_functions.py`, changed parsing of WRDS `metadata_sources` to account for new list type.
- In `generate_categorical_fim_flows.py`, changed the way the `EVALUATED_SITES_CSV` path is defined from OS environmental setting to a relative path that will work within Docker container.

<br/><br/>

## v3.0.22.2 - 2021-08-26 - [PR #455](https://github.com/NOAA-OWP/cahaba/pull/455)

This merge addresses an issues with the bathymetry adjusted rating curve (BARC) calculations exacerbating single-pixel inundation issues for the lower Mississippi River. This fix allows the user to specify a stream order value that will be ignored in BARC calculations (reverts to using the original/default rating curve). If/when the "thalweg notch" issue is addressed, this change may be unmade.

## Changes
- Added new env variable `ignore_streamorders` set to 10.
- Added new BARC code to set the bathymetry adjusted cross-section area to 0 (reverts to using the default SRC values) based on the streamorder env variable.

<br/><br/>

## v3.0.22.1 - 2021-08-20 - [PR #447](https://github.com/NOAA-OWP/cahaba/pull/447)

Patches the minimum stream length in the template parameters file.

## Changes
- Changes `max_split_distance_meters` in `params_template.env` to 1500.

<br/><br/>

## v3.0.22.0 - 2021-08-19 - [PR #444](https://github.com/NOAA-OWP/cahaba/pull/444)

This adds a script, `adjust_rc_with_feedback.py`, that will be expanded  in future issues. The primary function that performs the HAND value and hydroid extraction is ingest_points_layer() but this may change as the overall synthetic rating curve automatic update machanism evolves.

## Additions
- Added `adjust_rc_with_feedback.py` with `ingest_points_layer()`, a function to extract HAND and hydroid values for use in an automatic synthetic rating curve updating mechanism.

<br/><br/>

## v3.0.21.0 - 2021-08-18 - [PR #433](https://github.com/NOAA-OWP/cahaba/pull/433)

General repository cleanup, made memory-profiling an optional flag, API's release feature now saves outputs.

## Changes
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

## Changes
- `clip_vectors_to_wbd.py` now checks that there are NWM stream segments within the buffered HUC boundary.
- `included_huc8_ms.lst` has several additional HUC8s.

<br/><br/>

## v3.0.20.1 - 2021-08-12 - [PR #442](https://github.com/NOAA-OWP/cahaba/pull/442)

This merge improves documentation in various scripts.

## Changes
This PR better documents the following:

- `inundate_nation.py`
- `synthesize_test_cases.py`
- `adjust_thalweg_lateral.py`
- `rem.py`

<br/><br/>

## v3.0.20.0 - 2021-08-11 - [PR #440](https://github.com/NOAA-OWP/cahaba/pull/440)

This merge adds two new scripts into `/tools/` for use in QAQC.

## Additions
- `inundate_nation.py` to produce inundation maps for the entire country for use in QAQC.
- `check_deep_flooding.py` to check for depths of inundation greater than a user-supplied threshold at specific areas defined by a user-supplied shapefile.

<br/><br/>

## v3.0.19.5 - 2021-07-19

Updating `README.md`.

<br/><br/>

## v3.0.19.4 - 2021-07-13 - [PR #431](https://github.com/NOAA-OWP/cahaba/pull/431)

Updating logging and fixing bug in vector preprocessing.

## Additions
- `fim_completion_check.py` adds message to docker log to log any HUCs that were requested but did not finish `run_by_unit.sh`.
- Adds `input_data_edits_changelog.txt` to the inputs folder to track any manual or version/location specific changes that were made to data used in FIM 3.

## Changes
- Provides unique exit codes to relevant domain checkpoints within `run_by_unit.sh`.
- Bug fixes in `reduce_nhd_stream_density.py`, `mprof plot` call.
- Improved error handling in `add_crosswalk.py`.

<br/><br/>

## v3.0.19.3 - 2021-07-09

Hot fix to `synthesize_test_cases`.

## Changes
- Fixed if/elif/else statement in `synthesize_test_cases.py` that resulted in only IFC data being evaluated.

<br/><br/>

## v3.0.19.2 - 2021-07-01 - [PR #429](https://github.com/NOAA-OWP/cahaba/pull/429)

Updates to evaluation scripts to allow for Alpha testing at Iowa Flood Center (IFC) sites. Also, `BAD_SITES` variable updates to omit sites not suitable for evaluation from metric calculations.

## Changes
- The `BAD_SITES` list in `tools_shared_variables.py` was updated and reasons for site omission are documented.
- Refactored `run_test_case.py`, `synthesize_test_cases.py`, `tools_shared_variables.py`, and `eval_plots.py` to allow for IFC comparisons.

<br/><br/>

## v3.0.19.1 - 2021-06-17 - [PR #417](https://github.com/NOAA-OWP/cahaba/pull/417)

Adding a thalweg profile tool to identify significant drops in thalweg elevation. Also setting lateral thalweg adjustment threshold in hydroconditioning.

## Additions
- `thalweg_drop_check.py` checks the elevation along the thalweg for each stream path downstream of MS headwaters within a HUC.

## Removals
- Removing `dissolveLinks` arg from `clip_vectors_to_wbd.py`.

## Changes
- Cleaned up code in `split_flows.py` to make it more readable.
- Refactored `reduce_nhd_stream_density.py` and `adjust_headwater_streams.py` to limit MS headwater points in `agg_nhd_headwaters_adj.gpkg`.
- Fixed a bug in `adjust_thalweg_lateral.py` lateral elevation replacement threshold; changed threshold to 3 meters.
- Updated `aggregate_vector_inputs.py` to log intermediate processes.

<br/><br/>

## v3.0.19.0 - 2021-06-10 - [PR #415](https://github.com/NOAA-OWP/cahaba/pull/415)

Feature to evaluate performance of alternative CatFIM techniques.

## Additions
- Added `eval_catfim_alt.py` to evaluate performance of alternative CatFIM techniques.

<br/><br/>
## v3.0.18.0 - 2021-06-09 - [PR #404](https://github.com/NOAA-OWP/cahaba/pull/404)

To help analyze the memory consumption of the Fim Run process, the python module `memory-profiler` has been added to give insights into where peak memory usage is with in the codebase.

In addition, the Dockerfile was previously broken due to the Taudem dependency removing the version that was previously being used by FIM. To fix this, and allow new docker images to be built, the Taudem version has been updated to the newest version on the Github repo and thus needs to be thoroughly tested to determine if this new version has affected the overall FIM outputs.

## Additions
- Added `memory-profiler` to `Pipfile` and `Pipfile.lock`.
- Added `mprof` (memory-profiler cli utility) call to the `time_and_tee_run_by_unit.sh` to create overall memory usage graph location in the `/logs/{HUC}_memory.png` of the outputs directory.
- Added `@profile` decorator to all functions within scripts used in the `run_by_unit.sh` script to allow for memory usage tracking, which is then recorded in the `/logs/{HUC}.log` file of the outputs directory.

## Changes
- Changed the Taudem version in `Dockerfile.dev` to `98137bb6541a0d0077a9c95becfed4e56d0aa0ac`.
- Changed all calls of python scripts in `run_by_unit.s` to be called with the `-m memory-profiler` argument to allow scripts to also track memory usage.

<br/><br/>
## v3.0.17.1 - 2021-06-04 - [PR #395](https://github.com/NOAA-OWP/cahaba/pull/395)

Bug fix to the `generate_nws_lid.py` script

## Changes
- Fixes incorrectly assigned attribute field "is_headwater" for some sites in the `nws_lid.gpkg` layer.
- Updated `agg_nhd_headwaters_adj.gpkg`, `agg_nhd_streams_adj.gpkg`, `nwm_flows.gpkg`, and `nwm_catchments.gpkg` input layers using latest NWS LIDs.

<br/><br/>
## v3.0.17.0 - 2021-06-04 - [PR #393](https://github.com/NOAA-OWP/cahaba/pull/393)
BARC updates to cap the bathy calculated xsec area in `bathy_rc_adjust.py` and allow user to choose input bankfull geometry.

## Changes

- Added new env variable to control which input file is used for the bankfull geometry input to bathy estimation workflow.
- Modified the bathymetry cross section area calculation to cap the additional area value so that it cannot exceed the bankfull cross section area value for each stream segment (bankfull value obtained from regression equation dataset).
- Modified the `rating_curve_comparison.py` plot output to always put the FIM rating curve on top of the USGS rating curve (avoids USGS points covering FIM).
- Created a new aggregate csv file (aggregates for all hucs) for all of the `usgs_elev_table.csv` files (one per huc).
- Evaluate the FIM Bathymetry Adjusted Rating Curve (BARC) tool performance using the estimated bankfull geometry dataset derived for the NWM route link dataset.

<br/><br/>
## v3.0.16.3 - 2021-05-21 - [PR #388](https://github.com/NOAA-OWP/cahaba/pull/388)

Enhancement and bug fixes to `synthesize_test_cases.py`.

## Changes
- Addresses a bug where AHPS sites without benchmark data were receiving a CSI of 0 in the master metrics CSV produced by `synthesize_test_cases.py`.
- Includes a feature enhancement to `synthesize_test_cases.py` that allows for the inclusion of user-specified testing versions in the master metrics CSV.
- Removes some of the print statements used by `synthesize_test_cases.py`.

<br/><br/>
## v3.0.16.2 - 2021-05-18 - [PR #384](https://github.com/NOAA-OWP/cahaba/pull/384)

Modifications and fixes to `run_test_case.py`, `eval_plots.py`, and AHPS preprocessing scripts.

## Changes
- Comment out return statement causing `run_test_case.py` to skip over sites/hucs when calculating contingency rasters.
- Move bad sites list and query statement used to filter out bad sites to the `tools_shared_variables.py`.
- Add print statements in `eval_plots.py` detailing the bad sites used and the query used to filter out bad sites.
- Update AHPS preprocessing scripts to produce a domain shapefile.
- Change output filenames produced in ahps preprocessing scripts.
- Update workarounds for some sites in ahps preprocessing scripts.

<br/><br/>
## v3.0.16.1 - 2021-05-11 - [PR #380](https://github.com/NOAA-OWP/cahaba/pull/380)

The current version of Eventlet used in the Connector module of the FIM API is outdated and vulnerable. This update bumps the version to the patched version.

## Changes
- Updated `api/node/connector/requirements.txt` to have the Eventlet version as 0.31.0

<br/><br/>
## v3.0.16.0 - 2021-05-07 - [PR #378](https://github.com/NOAA-OWP/cahaba/pull/378)

New "Release" feature added to the FIM API. This feature will allow for automated FIM, CatFIM, and relevant metrics to be generated when a new FIM Version is released. See [#373](https://github.com/NOAA-OWP/cahaba/issues/373) for more detailed steps that take place in this feature.

## Additions
- Added new window to the UI in `api/frontend/gui/templates/index.html`.
- Added new job type to `api/node/connector/connector.py` to allow these release jobs to run.
- Added additional logic in `api/node/updater/updater.py` to run the new eval and CatFIM scripts used in the release feature.

## Changes
- Updated `api/frontend/output_handler/output_handler.py` to allow for copying more broad ranges of file paths instead of only the `/data/outputs` directory.

<br/><br/>
## v3.0.15.10 - 2021-05-06 - [PR #375](https://github.com/NOAA-OWP/cahaba/pull/375)

Remove Great Lakes coastlines from WBD buffer.

## Changes
- `gl_water_polygons.gpkg` layer is used to mask out Great Lakes boundaries and remove NHDPlus HR coastline segments.

<br/><br/>
## v3.0.15.9 - 2021-05-03 - [PR #372](https://github.com/NOAA-OWP/cahaba/pull/372)

Generate `nws_lid.gpkg`.

## Additions
- Generate `nws_lid.gpkg` with attributes indicating if site is a headwater `nws_lid` as well as if it is co-located with another `nws_lid` which is referenced to the same `nwm_feature_id` segment.

<br/><br/>
## v3.0.15.8 - 2021-04-29 - [PR #371](https://github.com/NOAA-OWP/cahaba/pull/371)

Refactor NHDPlus HR preprocessing workflow. Resolves issue #238

## Changes
- Consolidate NHD streams, NWM catchments, and headwaters MS and FR layers with `mainstem` column.
- HUC8 intersections are included in the input headwaters layer.
- `clip_vectors_to_wbd.py` removes incoming stream segment from the selected layers.

<br/><br/>
## v3.0.15.7 - 2021-04-28 - [PR #367](https://github.com/NOAA-OWP/cahaba/pull/367)

Refactor synthesize_test_case.py to handle exceptions during multiprocessing. Resolves issue #351

## Changes
- refactored `inundation.py` and `run_test_case.py` to handle exceptions without using `sys.exit()`.

<br/><br/>
## v3.0.15.6 - 2021-04-23 - [PR #365](https://github.com/NOAA-OWP/cahaba/pull/365)

Implement CatFIM threshold flows to Sierra test and add AHPS benchmark preprocessing scripts.

## Additions
- Produce CatFIM flows file when running `rating_curve_get_usgs_gages.py`.
- Several scripts to preprocess AHPS benchmark data. Requires numerous file dependencies not available through Cahaba.

## Changes
- Modify `rating_curve_comparison.py` to ingest CatFIM threshold flows in calculations.
- Modify `eval_plots.py` to save all site specific bar plots in same parent directory instead of in subdirectories.
- Add variables to `env.template` for AHPS benchmark preprocessing.

<br/><br/>
## v3.0.15.5 - 2021-04-20 - [PR #363](https://github.com/NOAA-OWP/cahaba/pull/363)

Prevent eval_plots.py from erroring out when spatial argument enabled if certain datasets not analyzed.

## Changes
- Add check to make sure analyzed dataset is available prior to creating spatial dataset.

<br/><br/>
## v3.0.15.4 - 2021-04-20 - [PR #356](https://github.com/NOAA-OWP/cahaba/pull/356)

Closing all multiprocessing Pool objects in repo.

<br/><br/>
## v3.0.15.3 - 2021-04-19 - [PR #358](https://github.com/NOAA-OWP/cahaba/pull/358)

Preprocess NHDPlus HR rasters for consistent projections, nodata values, and convert from cm to meters.

## Additions
- `preprocess_rasters.py` reprojects raster, converts to meters, and updates nodata value to -9999.
- Cleaned up log messages from `bathy_rc_adjust.py` and `usgs_gage_crosswalk.py`.
- Outputs paths updated in `generate_categorical_fim_mapping.py` and `generate_categorical_fim.py`.
- `update_raster_profile` cleans up raster crs, blocksize, nodata values, and converts elevation grids from cm to meters.
- `reproject_dem.py` imports gdal to reproject elevation rasters because an error was occurring when using rasterio.

## Changes
- `burn_in_levees.py` replaces the `gdal_calc.py` command to resolve inconsistent outputs with burned in levee values.

<br/><br/>
## v3.0.15.2 - 2021-04-16 - [PR #359](https://github.com/NOAA-OWP/cahaba/pull/359)

Hotfix to preserve desired files when production flag used in `fim_run.sh`.

## Changes

- Fixed production whitelisted files.

<br/><br/>
## v3.0.15.1 - 2021-04-13 - [PR #355](https://github.com/NOAA-OWP/cahaba/pull/355)

Sierra test considered all USGS gage locations to be mainstems even though many actually occurred with tributaries. This resulted in unrealistic comparisons as incorrect gages were assigned to mainstems segments. This feature branch identifies gages that are on mainstems via attribute field.

## Changes

- Modifies `usgs_gage_crosswalk.py` to filter out gages from the `usgs_gages.gpkg` layer such that for a "MS" run, only consider gages that contain rating curve information (via `curve` attribute) and are also mainstems gages (via `mainstems` attribute).
- Modifies `usgs_gage_crosswalk.py` to filter out gages from the `usgs_gages.gpkg` layer such that for a "FR" run, only consider gages that contain rating curve information (via `curve` attribute) and are not mainstems gages (via `mainstems` attribute).
- Modifies how mainstems segments are determined by using the `nwm_flows_ms.gpkg` as a lookup to determine if the NWM segment specified by WRDS for a gage site is a mainstems gage.

## Additions

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
