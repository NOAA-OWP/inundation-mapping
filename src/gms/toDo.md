# GMS To Do: 
## Immediate
- some levelpaths yielding errors (see #242). add back -e flag once resolved
    - run `grep -in "error" /data/outputs/fim3_20210301_a92212d/gms_test2_levelpath.log` for more info
- inundate_gms: "No forecast value found in passed .."
- Mosaic branch inundation maps: still have to do polygons and depths
- Make a singular inundate_gms and mosaic function
    - clean up level path files when doing this
- Evaluate with test cases
    - clean up outputs for inundation mapping


## Longer term
- Parallelize by unit. 
    - Needs a script comparable to fim_run or integration into fim_run
    - Need branch progress tracking or writing stdout/err to individual log file
- Implement attribute exclusion for branching functionality (stream order 1s)
- running branch modules separate from branch for loop
    - running branch modules at domain scale to avoid levelpath derivation issue at nesting points
- parameterize branch buffer distance by stream order
- why using filtered reaches causes issues? HydroIDs not present in boths polygons and reach vectors
- add compatibility for new rem.py
- do re-split reaches have unique ID's or does that not matter

# FIM 3 To Do for GMS
- rem.py is current bottleneck. Optimize
- demDerived_reaches_split_points.gpkg and flows_points_pixels.gpkg files should have HydroID attribute. Transition edit_points.py functionality to split_points.py
- rem.py can be zeroing out and possibly masking too?
- carrying over connectivity from demDerived_reaches.shp in split_flows.py

