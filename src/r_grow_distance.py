#!/usr/bin/env python3
from grass_session import Session
import os
import shutil 
import grass.script as gscript
import argparse


def r_grow_distance(input_raster, grass_workspace, proximity_dtype, allocation_dtype):
    '''
    Runs the r.grow.distance GRASS gis tool which given an input raster will produce an output proximity (or distance) and euclidian allocation tool.

    Parameters
    ----------
    input_raster : STR
        Path to input raster. For example, see flows_grid_boolean.tif
    grass_workspace : STR
        Path to TEMPORARY directory to store intermediate GRASS data. This directory is deleted upon completion of this function.
    proximity_dtype: STR
        Data type for the proximity output. Typically 'Float32'.    
    allocation_dtype: STR
        Data type for the allocation output. Typically 'Float32' (AGREE processing) or 'Float64' (thalweg adjustment processing)

    Returns
    -------
    output_proximity_path : STR
        The path to the output proximity (or distance) raster (in tif format).
    output_allocation_path : STR
        The path to the output euclidian allocation raster (in tif format).

    '''
    
    # Define parent directory of input raster and get input raster name
    input_raster_directory = os.path.dirname(input_raster)
    input_raster_name = os.path.splitext(os.path.basename(input_raster))[0]
    
    # Set up variables for use in GRASS
    grass_gisdb = grass_workspace
    grass_location = 'temporary_location'
    grass_mapset = 'temporary_mapset'
    projected_file = input_raster

    # Start and close PERMANENT session.
    PERMANENT = Session()
    PERMANENT.open(gisdb = grass_gisdb, location = grass_location, create_opts = projected_file)
    PERMANENT.close()

    # Open a temporary session.
    temporary_session = Session()
    temporary_session.open(gisdb = grass_gisdb, location = grass_location, mapset = grass_mapset, create_opts = projected_file)
    
    #Import input raster into temporary session.
    imported_grass_raster = input_raster_name + '@' + grass_mapset
    gscript.run_command('r.in.gdal', input = input_raster, output = imported_grass_raster, quiet = True)

    # Define names for proximity and allocation rasters. Run 
    # r.grow.distance tool.
    proximity_grass_name = 'proximity@' + grass_mapset
    allocation_grass_name = 'allocation@'+ grass_mapset
    gscript.run_command('r.grow.distance', flags = 'm', input = imported_grass_raster, distance = proximity_grass_name, value = allocation_grass_name, quiet = True)
    
    # Export proximity raster. Saved to same directory as input raster. 
    # Dtype for proximity always float32.
    proximity_filename = input_raster_name + '_dist.tif'
    output_proximity_path=os.path.join(input_raster_directory,proximity_filename)
    gscript.run_command('r.out.gdal', flags = 'cf', input = proximity_grass_name, output = output_proximity_path, format = 'GTiff', quiet = True, type = proximity_dtype, createopt = 'COMPRESS=LZW')

    # Export allocation raster. Saved to same directory as input raster. 
    # Dtype assigned via the allocation_dtype input.
    allocation_filename = input_raster_name + '_allo.tif'
    output_allocation_path = os.path.join(input_raster_directory, allocation_filename)
    gscript.run_command('r.out.gdal', flags = 'cf', input = allocation_grass_name, output = output_allocation_path, format = 'GTiff', quiet = True, type = allocation_dtype, createopt = 'COMPRESS=LZW')
    
    # Close down temporary session and remove temporary workspace.
    temporary_session.close()
    shutil.rmtree(grass_gisdb)
    
    return output_proximity_path,output_allocation_path


if __name__ == '__main__':

    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Calculate AGREE DEM')
    parser.add_argument('-i', '--in_raster', help = 'raster to perform r.grow.distance', required = True)
    parser.add_argument('-g', '--grass_workspace', help = 'Temporary GRASS workspace', required = True)
    parser.add_argument('-p', '--prox_dtype', help = 'Output proximity raster datatype', required = True)
    parser.add_argument('-a', '--allo_dtype', help = 'Output allocation raster datatype', required = True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Rename variable inputs
    input_raster = args['in_raster']
    grass_workspace = args['grass_workspace']
    proximity_dtype = args['prox_dtype']
    allocation_dtype = args['allo_dtype']
    
    # Run r_grow_distance
    r_grow_distance(input_raster, grass_workspace, proximity_dtype, allocation_dtype)
    