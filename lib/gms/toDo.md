# GMS TO DO
- Derive full SRC and hydroTable.csv
- Make branch inundation maps
- Mosaic branch inundation maps
- Run FIM 3 latest dev and run_by_branch on that


- Derive levelpath
- Parallelize by unit. Needs a script comparable to fim_run or integration into fim_run
    - echo HUC and progress for each branch
- Set no data value from DEM
- implement attribute exclusion for branching functionality (stream order 1s)
- parameterize branch buffer distance by stream order


# Issues in FIM 3
- REM raster nodata not set properly
- rem.py optimization
- demDerived_reaches_split_points.gpkg and flows_points_pixels.gpkg files should have HydroID attribute


