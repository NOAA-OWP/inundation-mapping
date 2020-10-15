#!/usr/bin/env python3
import rasterio
import numpy as np
import os
import argparse
from r_grow_distance import r_grow_distance


def agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace, buffer_dist, smooth_drop, sharp_drop):
    '''
    Produces a hydroconditioned raster using the AGREE DEM method.  Method follows workflow documented by Ferdi Hellweger (https://www.caee.utexas.edu/prof/maidment/gishydro/ferdi/research/agree/agree.html).
    This AGREE DEM method requires the calculation of euclidian allocation and euclidian distance rasters. The GRASS gis r.grow.distance tool is used for this.  All linear units assumed to be in meters. Input DEM assumed to be in meters (lateral and vertical). Output distance rasters from r.grow.distance are in meters. Input variables "buffer_dist", "smooth_drop", "sharp_drop" all in meters.

    Parameters
    ----------
    rivers_raster : STR
        Path to the river raster. River cells = 1 and non-river cells = 0. For example, see flows_grid_boolean.tif.
    dem : STR
        Elevation DEM (units assumed to be in meters). For example, see dem_meters.tif.
    output_raster : STR
        Path to output raster. For example, dem_burned.tif
    workspace : STR
        Path to workspace to save all intermediate files. 
    grass_workspace : STR
        Path to the temporary workspace for grass inputs. This temporary workspace is deleted once grass datasets are produced and exported to tif files.
    buffer_dist : FLOAT
        AGREE stream buffer distance (in meters) on either side of stream. 
    smooth_drop : FLOAT
        Smooth drop distance (in meters). Typically this has been 10m.
    sharp_drop : FLOAT
        Sharp drop distance (in meters). Typically this has been 1000m.
    Returns
    -------
    None.

    '''
    # 1. From Hellweger documentation: Compute the vector grid (vectgrid). The cells in the vector grid corresponding to the lines in the vector coverage have data. All other cells have no data.
  
    #Import dem layer and get profile as well as mask.
    with rasterio.open(dem) as elev:
        dem_profile = elev.profile
        elev_data = elev.read(1)
        elev_mask = elev.read_masks(1).astype('bool')

    # Import boolean river raster and apply same NODATA mask as dem layer. In case rivers extend beyond valid data regions of DEM.
    with rasterio.open(rivers_raster) as rivers:
        river_raw_data = rivers.read(1)
        river_data = np.where(elev_mask == True, river_raw_data, 0)
        
    # 2. From Hellweger documentation: Compute the smooth drop/raise grid (smogrid). The cells in the smooth drop/raise grid corresponding to the vector lines have an elevation equal to that of the original DEM (oelevgrid) plus a certain distance (smoothdist). All other cells have no data.

    # Assign smooth distance and calculate the smogrid.
    smooth_dist = -1 * smooth_drop # in meters.
    smogrid = river_data*(elev_data + smooth_dist)

    # Define smogrid properties and then export smogrid to tif file.
    smo_profile = dem_profile.copy()
    smo_profile.update(nodata = 0)
    smo_profile.update(dtype = 'float32')    
    smo_output = os.path.join(workspace, 'agree_smogrid.tif')
    with rasterio.Env():
        with rasterio.open(smo_output, 'w', **smo_profile) as raster:
            raster.write(smogrid.astype('float32'),1)
    
    
    # 3. From Hellweger documentation: Compute the vector distance grids (vectdist and vectallo). The cells in the vector distance grid (vectdist) store the distance to the closest vector cell. The cells in vector allocation grid (vectallo) store the elevation of the closest vector cell.

    # Compute allocation and proximity grid using r.grow.distance. Output distance grid in meters.
    vectdist_grid, vectallo_grid = r_grow_distance(smo_output, grass_workspace)
  
    # 4. From Hellweger documentation: Compute the buffer grid (bufgrid2). The cells in the buffer grid outside the buffer distance (buffer) store the original elevation. The cells in the buffer grid inside the buffer distance have no data.
    
    # Import distance and allocation grids.
    with rasterio.open(vectdist_grid) as vectdist:
        vectdist_data = vectdist.read(1)
    with rasterio.open(vectallo_grid) as vectallo:
        vectallo_data = vectallo.read(1)
    
    # Define buffer distance and calculate adjustment to comput the bufgrid.
    half_res = elev.res[0]/2 # adjustment equal to half distance of one cell 
    final_buffer = buffer_dist - half_res # all units in assumed to be in meters. 
    # Calculate bufgrid. Assign NODATA to areas where vectdist_data <= buffered value.
    bufgrid = np.where(vectdist_data > final_buffer, elev_data, dem_profile['nodata'])
    
    # Define bufgrid properties and export to tif file.
    buf_output = os.path.join(workspace, 'agree_bufgrid.tif')
    buf_profile = dem_profile.copy()
    #buf_profile.update(nodata = -12345.0) #Only areas within buffer distance will be classified as NODATA.
    buf_profile.update(dtype = 'float32')
    with rasterio.Env():
        with rasterio.open(buf_output, 'w', **buf_profile) as raster:
            raster.write(bufgrid.astype('float32'),1)

    # 5. From Hellweger documentation: Compute the buffer distance grids (bufdist and bufallo). The cells in the buffer distance grid (bufdist) store the distance to the closest valued buffer grid cell (bufgrid2). The cells in buffer allocation grid (bufallo) store the elevation of the closest valued buffer cell.

    # Compute allocation and proximity grid using r.grow.distance. Output distance grid in meters.
    bufdist_grid, bufallo_grid = r_grow_distance(buf_output, grass_workspace)

    # Import allocation and proximity grids.
    with rasterio.open(bufdist_grid) as bufdist:
        bufdist_data = bufdist.read(1)
    with rasterio.open(bufallo_grid) as bufallo:
        bufallo_data = bufallo.read(1)
    
    # 6. From Hellweger documentation: Compute the smooth modified elevation grid (smoelev). The cells in the smooth modified elevation grid store the results of the smooth surface reconditioning process. Note that for cells outside the buffer the equation below assigns the original elevation.
    
    # Calculate smoelev. 
    smoelev = vectallo_data + ((bufallo_data - vectallo_data)/(bufdist_data + vectdist_data)) * vectdist_data
    
    # 7. From Hellweger documentation: Compute the sharp drop/raise grid (shagrid). The cells in the sharp drop/raise grid corresponding to the vector lines have an elevation equal to that of the smooth modified elevation grid (smoelev) plus a certain distance (sharpdist). All other cells have no data.

    # Define sharp drop distance and calculate the sharp drop grid where only river cells are dropped by the sharp_dist amount.
    sharp_dist = -1 * sharp_drop # in meters.
    shagrid = (smoelev + sharp_dist) * river_data
    
    # 8. From Hellweger documentation: Compute the modified elevation grid (elevgrid). The cells in the modified elevation grid store the results of the surface reconditioning process. Note that for cells outside the buffer the the equation below assigns the original elevation.

    # Merge sharp drop grid with smoelev grid. Then apply the same NODATA mask as original elevation grid.
    elevgrid = np.where(river_data == 0, smoelev, shagrid)
    agree_dem = np.where(elev_mask == True, elevgrid, dem_profile['nodata'])

    #Define properties and export final AGREE DEM to tif.
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
    parser.add_argument('-b',  '--buffer', help = 'Buffer distance (m) on either side of channel', required = True)
    parser.add_argument('-sm', '--smooth', help = 'Smooth drop (m)', required = True)
    parser.add_argument('-sh', '---sharp', help = 'Sharp drop (m)', required = True)

    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    rivers_raster = args['rivers']
    dem = args['dem_cm']
    workspace = args['workspace']
    grass_workspace = args['grass_workspace']
    output_raster = args['output']
    buffer_dist = float(args['buffer'])
    smooth_drop = float(args['smooth'])
    sharp_drop =  float(args['sharp'])
    
    #Run agreedem
    agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace, buffer_dist, smooth_drop, sharp_drop)
