# To Do List

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
- *Unique Level Path and Hydro IDs:* unique identifiers for HydroIDs with GMS. Maybe FIMID, then branch ID, then HydroID.
- *convenience wrapper for gms_run...sh:* Make a convenience wrapper for gms_run_unit.sh and gms_run_branch.sh. Be mindful of the two different processing loads and expose two different job numbers to the user.
    - *Deny Listing for Units:* The files in the deny list for units is executed at the end of gms/run_by_unit.sh. This requires files used in run_by_branch.sh to be left while not necessary left behind. This should be moved later in the process possibly once the convenience wrapper is made.
- *Update clip_vectors_to_wbd.py:* Clipping way too many vectors for GMS purposes. This creates extra processing and storage requirements.
- *Stream order filtering:* You would likely gain computational time if you filtered out lower stream orders in the NWM for GMS input (creating a streams_gms.gpkg). By doing this you would need to mosaic with FR streams at the lower removed stream orders. GMS likely does very little at stream orders 1s and 2s and maybe even 3s and 4s.*
- *Levees to run_by_unit*: burning in levees at the branch scale is likely less efficient than at the unit scale. Consider moving those two modules to the run_by_unit script for gms
