# GMS To Do: 
## Immediate
- Check outputs prior to moving forward
- Make branch inundation maps
- Mosaic branch inundation maps

## Longer term
- Derive levelpath
- Parallelize by unit. Needs a script comparable to fim_run or integration into fim_run
    - echo HUC and progress for each branch
- Set no data value from DEM
- Implement attribute exclusion for branching functionality (stream order 1s)
- parameterize branch buffer distance by stream order

# FIM 3 To Do for GMS
- REM raster nodata not set properly
- rem.py is current bottleneck. Optimize
- demDerived_reaches_split_points.gpkg and flows_points_pixels.gpkg files should have HydroID attribute. Transition edit_points.py functionality to split_points.py


