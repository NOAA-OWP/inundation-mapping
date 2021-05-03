#!/usr/bin/env python3

import rasterio
import numpy as np
import os
import argparse
from r_grow_distance import r_grow_distance


def agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace, buffer_dist, smooth_drop, sharp_drop, delete_intermediate_data):
    '''
    Produces a hydroconditioned raster using the AGREE DEM methodology as described by Ferdi Hellweger (https://www.caee.utexas.edu/prof/maidment/gishydro/ferdi/research/agree/agree.html). The GRASS gis tool r.grow.distance is used to calculate intermediate allocation and proximity rasters.

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
    delete_intermediate_data: BOOL
        If True all intermediate data is deleted, if False (default) no intermediate datasets are deleted.

    Returns
    -------
    None.

    '''

    '''
    ------------------------------------------------------------------
    1. From Hellweger documentation: Compute the vector grid (vectgrid).
    The cells in the vector grid corresponding to the lines in the vector
    coverage have data. All other cells have no data.
    '''

    # Import dem layer and river layer and get dem profile
    elev = rasterio.open(dem)
    dem_profile = elev.profile

    rivers = rasterio.open(rivers_raster)

    # Define smogrid profile and output file
    smo_profile = dem_profile.copy()
    smo_profile.update(nodata = 0)
    smo_profile.update(dtype = 'float32')
    smo_output = os.path.join(workspace, 'agree_smogrid.tif')

    # Windowed reading/calculating/writing
    with rasterio.Env():
        with rasterio.open(smo_output, 'w', **smo_profile) as raster:
            for ji, window in elev.block_windows(1):
                # read elevation data and mask information
                elev_data_window = elev.read(1, window = window)
                elev_mask_window = elev.read_masks(1, window = window).astype('bool')
                # Import boolean river raster and apply same NODATA mask as dem
                # layer. In case rivers extend beyond valid data regions of DEM.
                river_raw_data_window = rivers.read(1, window = window)
                river_data_window = np.where(elev_mask_window == True, river_raw_data_window, 0)

                '''
                ---------------------------------------------------------------
                2. From Hellweger documentation: Compute the smooth drop/raise
                grid (smogrid). The cells in the smooth drop/raise grid
                corresponding to the vector lines have an elevation equal to that
                of the original DEM (oelevgrid) plus a certain distance
                (smoothdist). All other cells have no data.
                '''

                # Assign smooth distance and calculate the smogrid
                smooth_dist = -1 * smooth_drop # in meters
                smogrid_window = river_data_window*(elev_data_window + smooth_dist)

                # Write out raster
                raster.write(smogrid_window.astype('float32'), indexes = 1, window = window)

    elev.close()
    rivers.close()
    raster.close()

    '''
    ------------------------------------------------------------------
    3. From Hellweger documentation: Compute the vector distance grids
    (vectdist and vectallo). The cells in the vector distance grid
    (vectdist) store the distance to the closest vector cell. The
    cells in vector allocation grid (vectallo) store the elevation of
    the closest vector cell.
    '''
    # Compute allocation and proximity grid using GRASS gis r.grow.distance tool.
    # Output distance grid in meters. Set datatype for output allocation and proximity grids to float32.
    vectdist_grid, vectallo_grid = r_grow_distance(smo_output, grass_workspace, 'Float32', 'Float32')

    '''
    ------------------------------------------------------------------
    4. From Hellweger documentation: Compute the buffer grid
    (bufgrid2). The cells in the buffer grid outside the buffer
    distance (buffer) store the original elevation. The cells in the
    buffer grid inside the buffer distance have no data.
    '''

    # Open distance, allocation, elevation grids.
    vectdist = rasterio.open(vectdist_grid)
    vectallo = rasterio.open(vectallo_grid)
    elev = rasterio.open(dem)

    # Define bufgrid profile and output file.
    buf_output = os.path.join(workspace, 'agree_bufgrid.tif')
    buf_profile = dem_profile.copy()
    buf_profile.update(dtype = 'float32')

    # Windowed reading/calculating/writing
    with rasterio.Env():
        with rasterio.open(buf_output, 'w', **buf_profile) as raster:
            for ji, window in elev.block_windows(1):
                # read distance, allocation, and elevation datasets
                vectdist_data_window = vectdist.read(1, window = window)
                vectallo_data_window = vectallo.read(1, window = window)
                elev_data_window = elev.read(1, window = window)

                # Define buffer distance and calculate adjustment to compute the bufgrid.
                # half_res adjustment equal to half distance of one cell
                half_res = elev.res[0]/2
                final_buffer = buffer_dist - half_res # assume all units in meters.

                # Calculate bufgrid. Assign NODATA to areas where vectdist_data <= buffered value.
                bufgrid_window = np.where(vectdist_data_window > final_buffer, elev_data_window, dem_profile['nodata'])

                # Write out raster
                raster.write(bufgrid_window.astype('float32'), indexes = 1, window = window)

    vectdist.close()
    vectallo.close()
    elev.close()

    '''
    ------------------------------------------------------------------
    5. From Hellweger documentation: Compute the buffer distance grids
    (bufdist and bufallo). The cells in the buffer distance grid
    (bufdist) store the distance to the closest valued buffer grid
    cell (bufgrid2). The cells in buffer allocation grid (bufallo)
    store the elevation of the closest valued buffer cell.
    '''

    # Compute allocation and proximity grid using GRASS gis r.grow.distance.
    # Output distance grid in meters. Set datatype for output allocation and proximity grids to float32.
    bufdist_grid, bufallo_grid = r_grow_distance(buf_output, grass_workspace, 'Float32', 'Float32')

    # Open distance, allocation, elevation grids
    bufdist = rasterio.open(bufdist_grid)
    bufallo = rasterio.open(bufallo_grid)
    vectdist = rasterio.open(vectdist_grid)
    vectallo = rasterio.open(vectallo_grid)
    rivers = rasterio.open(rivers_raster)
    elev = rasterio.open(dem)

    # Define profile output file
    agree_output = output_raster
    agree_profile = dem_profile.copy()
    agree_profile.update(dtype = 'float32')

    # Windowed reading/calculating/writing
    with rasterio.Env():
        with rasterio.open(agree_output, 'w', **agree_profile) as raster:
            for ji, window in elev.block_windows(1):
                # Read elevation data and mask, distance and allocation grids, and river data
                elev_data_window = elev.read(1, window = window)
                elev_mask_window = elev.read_masks(1, window = window).astype('bool')
                bufdist_data_window = bufdist.read(1, window = window)
                bufallo_data_window = bufallo.read(1, window = window)
                vectdist_data_window = vectdist.read(1, window = window)
                vectallo_data_window = vectallo.read(1, window = window)
                river_raw_data_window = rivers.read(1, window = window)


                river_data_window = np.where(elev_mask_window == True, river_raw_data_window, -20.0)

                '''
                ------------------------------------------------------------------
                6. From Hellweger documentation: Compute the smooth modified
                elevation grid (smoelev). The cells in the smooth modified
                elevation grid store the results of the smooth surface
                reconditioning process. Note that for cells outside the buffer the
                equation below assigns the original elevation.
                '''

                # Calculate smoelev
                smoelev_window = vectallo_data_window + ((bufallo_data_window - vectallo_data_window)/(bufdist_data_window + vectdist_data_window)) * vectdist_data_window

                '''
                ------------------------------------------------------------------
                7. From Hellweger documentation: Compute the sharp drop/raise grid
                (shagrid). The cells in the sharp drop/raise grid corresponding to
                the vector lines have an elevation equal to that of the smooth
                modified elevation grid (smoelev) plus a certain distance
                (sharpdist). All other cells have no data.
                '''

                # Define sharp drop distance and calculate the sharp drop grid where only river cells are dropped by the sharp_dist amount.
                sharp_dist = -1 * sharp_drop # in meters
                shagrid_window = (smoelev_window + sharp_dist) * river_data_window

                '''
                ------------------------------------------------------------------
                8. From Hellweger documentation: Compute the modified elevation
                grid (elevgrid). The cells in the modified elevation grid store
                the results of the surface reconditioning process. Note that for
                cells outside the buffer the the equation below assigns the
                original elevation.
                '''

                # Merge sharp drop grid with smoelev grid. Then apply the same NODATA mask as original elevation grid.
                elevgrid_window = np.where(river_data_window == 0, smoelev_window, shagrid_window)
                agree_dem_window = np.where(elev_mask_window == True, elevgrid_window, dem_profile['nodata'])

                # Write out to raster
                raster.write(agree_dem_window.astype('float32'), indexes = 1, window = window)

    bufdist.close()
    bufallo.close()
    vectdist.close()
    vectallo.close()
    rivers.close()
    elev.close()

    # If the '-t' flag is called, intermediate data is removed
    if delete_intermediate_data:
        os.remove(smo_output)
        os.remove(buf_output)
        os.remove(vectdist_grid)
        os.remove(vectallo_grid)
        os.remove(bufdist_grid)
        os.remove(bufallo_grid)


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Calculate AGREE DEM')
    parser.add_argument('-r', '--rivers', help = 'flows grid boolean layer', required = True)
    parser.add_argument('-d', '--dem_m',  help = 'DEM raster in meters', required = True)
    parser.add_argument('-w', '--workspace', help = 'Workspace', required = True)
    parser.add_argument('-g', '--grass_workspace', help = 'Temporary GRASS workspace', required = True)
    parser.add_argument('-o',  '--output', help = 'Path to output raster', required = True)
    parser.add_argument('-b',  '--buffer', help = 'Buffer distance (m) on either side of channel', required = True)
    parser.add_argument('-sm', '--smooth', help = 'Smooth drop (m)', required = True)
    parser.add_argument('-sh', '---sharp', help = 'Sharp drop (m)', required = True)
    parser.add_argument('-t',  '--del',  help = 'Optional flag to delete intermediate datasets', action = 'store_true')

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())

    rivers_raster = args['rivers']
    dem = args['dem_m']
    workspace = args['workspace']
    grass_workspace = args['grass_workspace']
    output_raster = args['output']
    buffer_dist = float(args['buffer'])
    smooth_drop = float(args['smooth'])
    sharp_drop =  float(args['sharp'])
    delete_intermediate_data = args['del']

    # Run agreedem
    agreedem(rivers_raster, dem, output_raster, workspace, grass_workspace, buffer_dist, smooth_drop, sharp_drop, delete_intermediate_data)
