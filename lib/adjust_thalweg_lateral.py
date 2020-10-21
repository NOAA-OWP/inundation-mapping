#!/usr/bin/env python3


import argparse
from numba import njit, typeof, typed, types
import rasterio
import numpy as np


def adjust_thalweg_laterally(elevation_raster, stream_raster, allocation_raster, cost_distance_raster, cost_distance_tolerance, dem_lateral_thalweg_adj):
    
    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #
    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    # It updates the catchment_min_dict with this zonal minimum elevation value.
    @njit
    def make_zone_min_dict(elevation_window, zone_min_dict, zone_window, cost_window):
        for i,cm in enumerate(zone_window):
#            print("Hey")
            # If the zone really exists in the dictionary, compare elevation values.
            i = int(i)
            cm = int(cm)
#            cost_distance_tolerance_float = float(cost_distance_tolerance)
            if (cm in zone_min_dict):
                if (elevation_window[i] < zone_min_dict[cm]):
                    if (cost_distance_raster <= cost_distance_tolerance):
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
    print("Create zone_min_dict")
    zone_min_dict = typed.Dict.empty(types.int32,types.float32)  # Initialize an empty dictionary to store the catchment minimums.
#    print("1")
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in elevation_raster_object.block_windows(1):  # Iterate over windows, using elevation_raster_object as template.
#        print("2")
        elevation_window = elevation_raster_object.read(1,window=window).ravel()  # Define elevation_window.
        zone_window = allocation_zone_raster_object.read(1,window=window).ravel()  # Define zone_window.
        cost_window = cost_distance_raster_object.read(1, window=window).ravel()  # Define cost_window.

        # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
#        print("5")
        zone_min_dict = make_zone_min_dict(elevation_window, zone_min_dict, zone_window, cost_window)
         
    # ------------------------------------------------------------------------------------------------------------------------ # 
    
    elevation_raster_object.close()
    allocation_zone_raster_object.close()
    cost_distance_raster_object.close()

    

    # ------------------------------------------- Assign zonal min to thalweg ------------------------------------------------ #
    @njit
    def minimize_thalweg_elevation(dem_window, zone_min_dict, zone_window, thalweg_window):
                
        # Copy elevation values into new array that will store the minimized elevation values.
        dem_window_to_return = np.empty_like (dem_window)
        dem_window_to_return[:] = dem_window
        

        for i,cm in enumerate(zone_window):
            i = int(i)
            cm = int(cm)
            thalweg_cell = thalweg_window[i]  # From flows_grid_boolean.tif (0s and 1s)
            if thalweg_cell == 1:  # Make sure thalweg cells are checked.
#                print(i)
#                print(cm)
                
                zone_min_elevation = zone_min_dict[cm]
                dem_thalweg_elevation = dem_window[i]
                
                elevation_difference = zone_min_elevation - dem_thalweg_elevation
                
                
#                print(zone_min_dict[cm])
#                print(dem_window[i])
#                print(elevation_difference)
#                print()

                if zone_min_elevation < dem_thalweg_elevation and elevation_difference <= 5:
                    dem_window_to_return[i] = zone_min_elevation
#
#                
#                # If the elevation_difference is less than 5 meters and if the original_thalweg_elevation is greater than the zonal
#                # minimum, update the thalweg elevation value to m atch the zonal minimum elevation.
#                if zonal_min_elevation < original_thalweg_elevation and elevation_difference < 5:
#                    dem_window_to_return[i] = zonal_min_elevation

        return(dem_window_to_return)
        
        
    print("Minimizing dem")
    # Specify raster object metadata.
    elevation_raster_object = rasterio.open(elevation_raster)
    allocation_zone_raster_object = rasterio.open(allocation_raster)
    thalweg_object = rasterio.open(stream_raster)
    
    meta = elevation_raster_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'
    
    
    dem_lateral_thalweg_adj_object = rasterio.open(dem_lateral_thalweg_adj, 'w', **meta)
    
    for ji, window in elevation_raster_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = elevation_raster_object.read(1,window=window)  # Define dem_window.
        window_shape = dem_window.shape
        dem_window = dem_window.ravel()
        
        zone_window = allocation_zone_raster_object.read(1,window=window).ravel()  # Define catchments_window.
        thalweg_window = thalweg_object.read(1,window=window).ravel()  # Define thalweg_window.
        
        # Call numba-optimized function to reassign thalweg cell values to catchment minimum value.
        minimized_dem_window = minimize_thalweg_elevation(dem_window, zone_min_dict, zone_window, thalweg_window)
        minimized_dem_window = minimized_dem_window.reshape(window_shape).astype(np.float32)


        dem_lateral_thalweg_adj_object.write(minimized_dem_window, window=window, indexes=1)    
    
    
if __name__ == '__main__':
    
    print("HEY")
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts the elevation of the thalweg to the lateral zonal minimum.')
    parser.add_argument('-e','--elevation_raster',help='Raster of elevation.',required=True)
    parser.add_argument('-s','--stream_raster',help='Raster of thalweg pixels (0=No Thalweg, 1=Thalweg)',required=True)
    parser.add_argument('-a','--allocation_raster',help='Raster of thalweg allocation zones.',required=True)
    parser.add_argument('-d','--cost_distance_raster',help='Raster of cost distances for the allocation raster.',required=True)
    parser.add_argument('-t','--cost_distance_tolerance',help='Tolerance in meters to use when searching for zonal minimum.',required=True)
    parser.add_argument('-o','--dem_lateral_thalweg_adj',help='Output elevation raster with adjusted thalweg.',required=True)
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    adjust_thalweg_laterally(**args)
    
    
    
