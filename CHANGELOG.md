All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

## v3.0.5.1 - 2021-02-22

Fixed `TEST_CASES_DIR` path in `tests/utils/shared_variables.py`.

### Changes

 - Removed `"_new"` from `TEST_CASES_DIR` variable.

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
