All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.
<br/><br/>
## v3.0.15.10 - 2021-05-04 - [PR #375](https://github.com/NOAA-OWP/cahaba/pull/375)

Remove Great Lakes coastlines from WBD buffer.

## Additions
- gl_water_polygons.gpkg layer is used to mask out Great Lakes boundaries and remove NHDPlus HR coastline segments.

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
