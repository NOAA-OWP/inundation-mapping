#!/usr/bin/env python3

import geopandas as gpd

reach_points=gpd.read_file('/data/outputs/default_12090301/12090301/demDerived_reaches_split_points.gpkg')

reach_points['HydroID'] = reach_points['id'].copy()

reach_points.to_file('/data/temp/gms/test1/demDerived_reaches_split_points.gpkg',driver='GPKG',index=False)

pixel_points = reach_points.copy()

pixel_points['id'] = list(range(1,len(pixel_points)+1))

pixel_points.to_file('/data/temp/gms/test1/flows_points_pixels.gpkg',driver='GPKG',index=False)

