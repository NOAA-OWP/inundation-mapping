# To Do List
## FIM 3 functionality
- *NDV for elevation rasters:* setting NDV for rasters is exposed to the user in preprocess_raster.py. This should be set to value in src/shared_variables.py of elev_raster_ndv. This prevents issues. Make sure all references to NDV in entire pipeline are consistent.
- *Pipeline Management:* the user pipeline should be strengthened. Preprocess, hydrofab production, inundation, evaluation steps should be consolidated. For example: there are many preprocessing steps now. Also every major pipeline step should have meta-data associated with it in some sort of pandas friendly json format. Example: every fim_run/gms_run should have a meta-data json detailing the model used, parameters, date ran, user, and the inherited data from the preprocessing steps of the pipeline.
- *NLD Lines:* nld lines are first being rasterized then burned.
    - the python script burn_levees could be avoided using gdal_calc and converting nld elevations to meters in preprocessing
    - if conversion to meters is done in preprocessing. It maybe possible to burn the nld elevations directly into the dem with gdal_rasterize all in one step
    - this could help both FIM 3 and GMS

## Eval
- *Testing Architecture & Modularity:* The test case functionality requires more modularity. There should be a clear set of tools that abstract away the model or test cases used. 
    - User tools should allow any predicted raster and any benchmark raster to be compared given proper encoding.
        - This function should be command line oriented and allow for a list or file to be evaluated using multiple CPUs
    - Eval tools for FIM 3, Lisflood, GMS, etc can then wrap around these core eval tools
    - Aggregation of metrics can be improved. The function create_metrics_metrics.py is spaghetti (15 indentations).See consolidate_metrics for modular tooling. Needs support for other test case formats.
    - Try statements should only include very specific lines. Having very large try blocks (see run_test_case.py creates alot of issues in debugging nested modules).
    - Any code put in if main == name part of a module is not exposed to the user if the main function is imported elsewhere in python. These code segments should modularized and grouped into functionality in main function.
- *NoForecastFound Error:* inundation.py throws a NoForecastFound exception when no matches are found between forecast and hydrotable. In levelpaths with no forecast this avoids creating an inundation raster for that area. This changes the spatial extent of GMS to exclude some areas that FR/MS would write as FNs (see for an example 13020102).
- *Evaluation extents:* The evaluation extents between FR, MS, GMS are not consistent. While this does have a small effect on CSI, MCC should be immune to it and generally shows a similar trend


## GMS 
- *NWM Divergences:* Levelpath derivation doesn't handle divergences (eg 12020002).
    - it shortens the effective length of levelpaths thus reducing the rating curve height of the most upstream catchment
    - it also creates more levelpaths and likely increases computational time
- *Update rem.py*: rem.py underwent some change. Update to latest src/rem.py from src/gms/rem.py
- *Unique Level Path and Hydro IDs:* unique identifiers for HydroIDs with GMS. Maybe FIMID, then branch ID, then HydroID.
- *convenience wrapper for gms_run...sh:* Make a convenience wrapper for gms_run_unit.sh and gms_run_branch.sh. Be mindful of the two different processing loads and expose two different job numbers to the user.
    - *Deny Listing for Units:* The files in the deny list for units is executed at the end of gms/run_by_unit.sh. This requires files used in run_by_branch.sh to be left while not necessary left behind. This should be moved later in the process possibly once the convenience wrapper is made.
- *Update clip_vectors_to_wbd.py:* Clipping way too many vectors for GMS purposes. This creates extra processing and storage requirements.
- *Small level paths just in the WBD area:* Small levelpaths just outside the wbd are being filtered out filter_catchments_and_add_attributes.py and lead to an exception. These cause known non-zero exit codes. To reduce non-zero exit codes to not known issues these levelpaths need to be removed in derive_level_paths.py. See 11140104 - 7057000003, 12020007 - 1475000002, 11110104 - 7756000488, 11110104 - 7756000505, 11090204 - 5128000164
- *Stream order filtering:* You would likely gain computational time if you filtered out lower stream orders in the NWM for GMS input (creating a nwm_streams_gms.gpkg). By doing this you would need to mosaic with FR streams at the lower removed stream orders. GMS likely does very little at stream orders 1s and 2s and maybe even 3s and 4s.*
- *Levees to run_by_unit*: burning in levees at the branch scale is likely less efficient than at the unit scale. Consider moving those two modules to the run_by_unit script for gms
