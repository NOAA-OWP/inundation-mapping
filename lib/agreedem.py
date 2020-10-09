# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 16:20:28 2020

@author: trevor.grout
"""
import rasterio
import numpy as np
import os
import argparse
from r_grow_distance import r_grow_distance


# rivers_tf = '/data/temp/tsg/grass/data/flows_grid_boolean.tif'
# dem = '/data/temp/tsg/grass/data/dem.tif'
# workspace = '/data/temp/tsg/grass'

def agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace):
# Compute the vector grid (vectgrid). The cells in the vector grid corresponding to the lines in the vector coverage have data. All other cells have no data.
# vectgrid = linegrid ( %vectcov% )

    with rasterio.open(rivers_raster) as rivers:
        river_data = rivers.read(1)
    
    with rasterio.open(dem) as elev:
        dem_profile = elev.profile
        elev_data = elev.read(1)
        elev_mask = elev.read_masks(1).astype('bool')
    
    # Compute the smooth drop/raise grid (smogrid). The cells in the smooth drop/raise grid corresponding to the vector lines have an elevation equal to that of the original DEM (oelevgrid) plus a certain distance (smoothdist). All other cells have no data.
    # smogrid = int ( setnull ( isnull ( vectgrid ), ( %oelevgrid% + %smoothdist% ) ) )
    smooth_dist_m = -10
    smooth_dist = smooth_dist_m * 100 #Convert to cm
    smogrid = river_data*(elev_data + smooth_dist)
    
    smo_profile = dem_profile.copy()
    smo_profile.update(nodata = 0)
    smo_profile.update(dtype = 'int32')
    
    smo_output = os.path.join(workspace, 'agree_smogrid.tif')
    with rasterio.Env():
        with rasterio.open(smo_output, 'w', **smo_profile) as raster:
            raster.write(smogrid.astype('int32'),1)
    
    
    # Compute the vector distance grids (vectdist and vectallo). The cells in the vector distance grid (vectdist) store the distance to the closest vector cell. The cells in vector allocation grid (vectallo) store the elevation of the closest vector cell.
    # vectdist = eucdistance( smogrid, #, vectallo, #, # )
    
    vectdist_grid, vectallo_grid = r_grow_distance(smo_output, grass_workspace)
  
    # Compute the buffer grid (bufgrid2). The cells in the buffer grid outside the buffer distance (buffer) store the original elevation. The cells in the buffer grid inside the buffer distance have no data.
    # bufgrid1 = con ( ( vectdist > ( %buffer% - ( %cellsize% / 2 ) ) ), 1, 0)
    # bufgrid2 = int ( setnull ( bufgrid1 == 0, %oelevgrid% ) )
    
    with rasterio.open(vectdist_grid) as vectdist:
        vectdist_data = vectdist.read(1)
    with rasterio.open(vectallo_grid) as vectallo:
        vectallo_data = vectallo.read(1)
    
    buffer_dist = 50
    bufgrid = np.where(vectdist_data>buffer_dist,elev_data, 0)
    
    buf_output = os.path.join(workspace, 'agree_bufgrid.tif')
    buf_profile = dem_profile.copy()
    buf_profile.update(nodata = 0) #instead of dem no data value; valid data values outside huc on purpose.
    buf_profile.update(dtype = 'int32')
    with rasterio.Env():
        with rasterio.open(buf_output, 'w', **buf_profile) as raster:
            raster.write(bufgrid.astype('int32'),1)
    # Compute the buffer distance grids (bufdist and bufallo). The cells in the buffer distance grid (bufdist) store the distance to the closest valued buffer grid cell (bufgrid2). The cells in buffer allocation grid (bufallo) store the elevation of the closest valued buffer cell.
    # bufdist = eucdistance( bufgrid2, #, bufallo, #, # )
    bufdist_grid, bufallo_grid = r_grow_distance(buf_output, grass_workspace)

    with rasterio.open(bufdist_grid) as bufdist:
        bufdist_data = bufdist.read(1)
    with rasterio.open(bufallo_grid) as bufallo:
        bufallo_data = bufallo.read(1)
    
    # Compute the smooth modified elevation grid (smoelev). The cells in the smooth modified elevation grid store the results of the smooth surface reconditioning process. Note that for cells outside the buffer the the equation below assigns the original elevation.
    # smoelev = vectallo + ( ( bufallo - vectallo ) / ( bufdist + vectdist ) ) * vectdist
    
    smoelev = vectallo_data + ((bufallo_data - vectallo_data)/(bufdist_data + vectdist_data)) * vectdist_data
    
    # Compute the sharp drop/raise grid (shagrid). The cells in the sharp drop/raise grid corresponding to the vector lines have an elevation equal to that of the smooth modified elevation grid (smoelev) plus a certain distance (sharpdist). All other cells have no data.
    # shagrid = int ( setnull ( isnull ( vectgrid ), ( smoelev + %sharpdist% ) ) )
    sharp_dist_m = -1000
    sharp_dist = sharp_dist_m * 100 #convert to cm
    shagrid = (smoelev + sharp_dist) * river_data
    
    # Compute the modified elevation grid (elevgrid). The cells in the modified elevation grid store the results of the surface reconditioning process. Note that for cells outside the buffer the the equation below assigns the original elevation.
    # elevgrid = con ( isnull ( vectgrid ), smoelev, shagrid )
    elevgrid = np.where(river_data == 0, smoelev/100.0, shagrid/100.0)
    agree_dem = np.where(elev_mask == True, elevgrid, dem_profile['nodata'])
    
    agree_output = output_raster
    agree_profile = dem_profile.copy()
    agree_profile.update(dtype = 'float32')
    with rasterio.Env():
        with rasterio.open(agree_output, 'w', **agree_profile) as raster:
            raster.write(agree_dem.astype('float32'),1)
        
        
if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Calculate AGREE DEM')
    parser.add_argument('-r', '--rivers', help = 'flows grid boolean layer', required = True)
    parser.add_argument('-d', '--dem_cm',  help = 'DEM raster in cm', required = True)
    parser.add_argument('-w', '--workspace', help = 'Workspace', required = True)
    parser.add_argument('-g', '--grass_workspace', help = 'Temporary GRASS workspace', required = True)
    parser.add_argument('-o',  '--output', help = 'Path to output raster', required = True)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    rivers_raster = args['rivers']
    dem = args['dem_cm']
    workspace = args['workspace']
    grass_workspace = args['grass_workspace']
    output_raster = args['output']
    
    #Run agreedem
    agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace)
