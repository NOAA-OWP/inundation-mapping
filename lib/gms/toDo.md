# GMS To Do: 
## Immediate
- Fix inundate_gms??
- Mosaic branch inundation maps
    - Mosaic polygons vs rasters?
- Evaluate with test cases


## Longer term
- Derive levelpath
    - UNSORTED BRANCH IDs in dissolved files
    - fix issues with library for deriving levelpaths
    - subset other vectors based on this now (requires association of HydroIDs to levelpathIDs)
    - create branch id list from levelpathIDs
    - dissolving?
- levelpaths for nested segments: 
    - nested segments not allocated correctly in levelpath. 
    - May need to derive arbolate sum and levelpath on a non-HUC scale (entire FIM domain scale)
- Parallelize by unit. 
    - Needs a script comparable to fim_run or integration into fim_run
    - Need branch progress tracking or writing stdout/err to individual log file
- Implement attribute exclusion for branching functionality (stream order 1s)
- parameterize branch buffer distance by stream order
- why using filtered reaches causes issues? HydroIDs not present in boths polygons and reach vectors
- add compatibility for new rem.py

# FIM 3 To Do for GMS
- rem.py is current bottleneck. Optimize
- demDerived_reaches_split_points.gpkg and flows_points_pixels.gpkg files should have HydroID attribute. Transition edit_points.py functionality to split_points.py
- rem.py can be zeroing out and possibly masking too?
- carrying over connectivity from demDerived_reaches.shp in split_flows.py

