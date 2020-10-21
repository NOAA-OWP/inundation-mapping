#!/usr/bin/env python3


import os
import argparse



def adjust_thalweg_laterally(stream_raster, allocation_raster, cost_distance_raster, cost_distance_tolerance):
    
    pass

    # Find zonal mimimum in the allocation raster zones.
    
    # Update elevation along the thalweg .


if __name__ == '__main__':
    
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts the elevation of the thalweg to the lateral zonal minimum.')
    parser.add_argument('-s','--stream_raster',help='Raster of thalweg pixels (0=No Thalweg, 1=Thalweg)',required=True)
    parser.add_argument('-a','--allocation_raster',help='Raster of thalweg allocation zones.',required=True)
    parser.add_argument('-d','--cost_distance_raster',help='Raster of cost distances for the allocation raster.',required=True)
    parser.add_argument('-t','--cost_distance_tolerance',help='Tolerance in meters to use when searching for zonal minimum.',required=True)
    
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    
    
