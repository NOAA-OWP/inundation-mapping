# GMS To Do: 
## Immediate
- Mosaic branch inundation maps
    - Mosaic polygons vs rasters?
- Evaluate with test cases

## Longer term
- Derive levelpath
- Parallelize by unit. 
    - Needs a script comparable to fim_run or integration into fim_run
    - Need branch progress tracking or writing stdout/err to individual log file
- No data value issue?
- Implement attribute exclusion for branching functionality (stream order 1s)
- parameterize branch buffer distance by stream order
- derive branch_id.lst for all HydroID's in buffered units

# FIM 3 To Do for GMS
- rem.py is current bottleneck. Optimize
- demDerived_reaches_split_points.gpkg and flows_points_pixels.gpkg files should have HydroID attribute. Transition edit_points.py functionality to split_points.py
- rem.py can be zeroing out and possibly masking too?
- carrying over connectivity from demDerived_reaches.shp in split_flows.py

