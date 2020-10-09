# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 08:24:51 2020

@author: trevor.grout
"""
from grass_session import Session
import os
import shutil 
import grass.script as gscript

workspace = 'data/temp/tsg/grassdata'
input_raster = '/data/temp/tsg/grass/data/vect_grid.tif'
def r_grow_distance(input_raster, workspace):
    input_raster_directory = os.path.dirname(input_raster)
    input_raster_name = os.path.splitext(os.path.basename(input_raster))[0]
    
    grass_gisdb = workspace
    grass_location = 'temporary_location'
    grass_mapset = 'temporary_mapset'
    projected_file = input_raster

    PERMANENT = Session()
    PERMANENT.open(gisdb = grass_gisdb, location = grass_location, create_opts = projected_file)
    PERMANENT.close()
    
    temporary_session = Session()
    temporary_session.open(gisdb = grass_gisdb, location = grass_location, mapset = grass_mapset, create_opts = projected_file)
    
    imported_grass_raster = input_raster_name + '@' + grass_mapset
    gscript.run_command('r.in.gdal', input = input_raster, output = imported_grass_raster)
    proximity_grass_name = 'proximity@' + grass_mapset
    allocation_grass_name = 'allocation@'+ grass_mapset
    gscript.run_command('r.grow.distance', flags = 'm', input = imported_grass_raster, distance = proximity_grass_name, value = allocation_grass_name)
    
    proximity_filename = input_raster_name + '_dist.tif'
    output_proximity_path=os.path.join(input_raster_directory,proximity_filename)
    gscript.run_command('r.out.gdal', input = proximity_grass_name, output = output_proximity_path, format = 'GTiff')
    allocation_filename = input_raster_name + '_allo.tif'
    output_allocation_path = os.path.join(input_raster_directory, allocation_filename)
    gscript.run_command('r.out.gdal', input = allocation_grass_name, output = output_allocation_path, format = 'GTiff')
    
    temporary_session.close()
    shutil.rmtree(grass_gisdb)
