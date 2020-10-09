# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 16:20:28 2020

@author: trevor.grout
"""
import rasterio
import numpy as np

# Compute the vector grid (vectgrid). The cells in the vector grid corresponding to the lines in the vector coverage have data. All other cells have no data.
# vectgrid = linegrid ( %vectcov% )
rivers_tf = r'D:\deleteme\GRASS\flows_grid_boolean.tif'
dem = r'D:\deleteme\GRASS\dem.tif'
with rasterio.open(rivers_tf) as rivers:
    rivers_profile = rivers.profile
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

out_profile = dem_profile.copy()
out_profile.update(nodata = 0)
out_profile.update(dtype = 'int32')

output = r'D:\deleteme\GRASS\vect_grid.tif'
with rasterio.Env():
    with rasterio.open(output, 'w', **out_profile) as raster:
        raster.write(smogrid.astype('int32'),1)


# Compute the vector distance grids (vectdist and vectallo). The cells in the vector distance grid (vectdist) store the distance to the closest vector cell. The cells in vector allocation grid (vectallo) store the elevation of the closest vector cell.
# vectdist = eucdistance( smogrid, #, vectallo, #, # )

vectdist_grid = r'D:\deleteme\GRASS\vectdist.tif'
vectallo_grid = r'D:\deleteme\GRASS\vectallo.tif' 

# Compute the buffer grid (bufgrid2). The cells in the buffer grid outside the buffer distance (buffer) store the original elevation. The cells in the buffer grid inside the buffer distance have no data.
# bufgrid1 = con ( ( vectdist > ( %buffer% - ( %cellsize% / 2 ) ) ), 1, 0)
# bufgrid2 = int ( setnull ( bufgrid1 == 0, %oelevgrid% ) )

with rasterio.open(vectdist_grid) as vectdist:
    vectdist_profile = vectdist.profile
    vectdist_data = vectdist.read(1)
with rasterio.open(vectallo_grid) as vectallo:
    vectallo_profile = vectallo.profile
    vectallo_data = vectallo.read(1)

buffer_dist = 50
bufgrid = np.where(vectdist_data>buffer_dist,elev_data, 0)
#bufgrid2 = np.where(bufgrid1 == dem_profile['nodata'],-99,bufgrid1) #only do if dem_elev no data = 0
output = r'D:\deleteme\GRASS\bufgrid.tif'
out_profile = dem_profile.copy()
out_profile.update(nodata = 0)
out_profile.update(dtype = 'int32')
with rasterio.Env():
    with rasterio.open(output, 'w', **out_profile) as raster:
        raster.write(bufgrid.astype('int32'),1)
# Compute the buffer distance grids (bufdist and bufallo). The cells in the buffer distance grid (bufdist) store the distance to the closest valued buffer grid cell (bufgrid2). The cells in buffer allocation grid (bufallo) store the elevation of the closest valued buffer cell.
# bufdist = eucdistance( bufgrid2, #, bufallo, #, # )
bufdist_grid = r'D:\deleteme\GRASS\bufdist.tif'
bufallo_grid = r'D:\deleteme\GRASS\bufallo.tif'
with rasterio.open(bufdist_grid) as bufdist:
    bufdist_profile = bufdist.profile
    bufdist_data = bufdist.read(1)
with rasterio.open(bufallo_grid) as bufallo:
    bufallo_profile = bufallo.profile
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

output = r'D:\deleteme\GRASS\agree_dem.tif'
agree_profile = dem_profile.copy()
agree_profile.update(dtype = 'float32')
with rasterio.Env():
    with rasterio.open(output, 'w', **agree_profile) as raster:
        raster.write(agree_dem.astype('float32'),1)