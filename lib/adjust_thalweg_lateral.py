#!/usr/bin/env python3


import argparse
from numba import njit, typed, types
import rasterio



def adjust_thalweg_laterally(elevation_raster, stream_raster, allocation_raster, cost_distance_raster, cost_distance_tolerance):
    

    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #
    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    # It updates the catchment_min_dict with this zonal minimum elevation value.
    @njit
    def make_zone_min_dict(elevation_window, zone_min_dict, zone_window, cost_window):
  
        for i,cm in enumerate(zone_window):
            # If the zone really exists in the dictionary, compare elevation values.
            if (cm in zone_min_dict):
                if (elevation_window[i] < zone_min_dict[cm]) and (cost_window[i] <= int(cost_distance_tolerance)):
                    # If the elevation_window's elevation value is less than the zone_min_dict min, update the zone_min_dict min.
                    zone_min_dict[cm] = elevation_window[i]                                                
            else:
                zone_min_dict[cm] = elevation_window[i]                
        return(zone_min_dict)
    
    # Open the masked gw_catchments_pixels_masked and dem_thalwegCond_masked.
    elevation_raster_object = rasterio.open(elevation_raster)
    allocation_zone_raster_object = rasterio.open(allocation_raster)
    cost_distance_raster_object = rasterio.open(cost_distance_raster)
    

    # -- Create zone_min_dict -- #
    zone_min_dict = typed.Dict.empty(types.int32,types.float32)  # Initialize an empty dictionary to store the catchment minimums.
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in elevation_raster_object.block_windows(1):  # Iterate over windows, using elevation_raster_object as template.
         elevation_window = elevation_raster_object.read(1,window=window).ravel()  # Define elevation_window.
         zone_window = allocation_zone_raster_object.read(1,window=window).ravel()  # Define zone_window.
         cost_window = cost_distance_raster_object.read(1, window=window).ravel()  # Define cost_window.

         # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
         zone_min_dict = make_zone_min_dict(elevation_window, zone_min_dict, zone_window, cost_window)
         
         
    # Find zonal mimimum in the allocation raster zones.
    
    # Update elevation along the thalweg .


if __name__ == '__main__':
    
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts the elevation of the thalweg to the lateral zonal minimum.')
    parser.add_argument('-e','--elevation_raster',help='Raster of elevation.',required=True)
    parser.add_argument('-s','--stream_raster',help='Raster of thalweg pixels (0=No Thalweg, 1=Thalweg)',required=True)
    parser.add_argument('-a','--allocation_raster',help='Raster of thalweg allocation zones.',required=True)
    parser.add_argument('-d','--cost_distance_raster',help='Raster of cost distances for the allocation raster.',required=True)
    parser.add_argument('-t','--cost_distance_tolerance',help='Tolerance in meters to use when searching for zonal minimum.',required=True)
    
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    
    
