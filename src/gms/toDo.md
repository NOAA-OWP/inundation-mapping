# GMS To Do: 
## Data
- Setting NDV for rasters is exposed to the user in preprocess_raster.py. This should be set to value in src/shared_variables.py of elev_raster_ndv. This prevents issues. Make sure all references to NDV in entire pipeline are consistent.
- pipeline management: your user pipeline should be strengthened. Preprocess, hydrofab production, inundation, evaluation steps should be consolidated. For example: there are many preprocessing steps now. Also every major pipeline step should have meta-data associated with it in some sort of pandas friendly json format. Example: every fim_run/gms_run should have a meta-data json detailing the model used, parameters, date ran, user, and the inherited data from the preprocessing steps of the pipeline.

## Hydrofabric
- levelpath derivation doesn't handle divergences (eg 12020002).
    - it shortens the effective length of levelpaths thus reducing the rating curve height of the most upstream catchment
- nld lines are first being rasterized then burned.
    - the python script burn_levees could be avoided using gdal_calc and converting nld elevations to meters in preprocessing
    - if conversion to meters is done in preprocessing. It maybe possible to burn the nld elevations directly into the dem with gdal_rasterize all in one step
- update to src/rem.py from src/gms/rem.py
- filtering of HydroIDs could take place earlier in process thus removing the need to run many levelpaths.
- unique identifiers for HydroIDs with GMS. Maybe FIMID, then branch ID, then HydroID.

## Immediate
- GMS whitelist for FIM 3
- convenience wrapper for fim_run, gms_run_unit.sh, gms_run_branch.sh. Move gms_run_branch.sh and gms_run_unit.sh to src/gms dir
- consider running filter_catchments_and_add_attributes.py in run_by_branch.sh.


## integration
- git rerere save conflict resolutions

## optimize
- 

## Inundation
- recheck polygons and depths

## Evaluations
- synthesize test cases
    - only one item in list for inundate_gms
    - extra file copied to dirs
    - improper masking
- synthesize test cases is spaghetti code. There are 15 indentations at one point.
  The evaluation pipeline currently comprised of run_test_case.py and synthesize_test_cases.py should be completely rewritten to make more modular and flexible. There should be clear functions to run test cases and batches of test cases without dependency on certain file directory or naming structures. For example, run_test_case.py should handle batches of test cases. There should be a command line function to run a test case without hardcoded file paths.
    - having code in the "__main__" part of a script removes it from importing it into other functions. Only embed code specific to command line functionality (argparse, commandline input handling, and main function call) in the main part. Embed everything else in the functions to use in other scripts.
