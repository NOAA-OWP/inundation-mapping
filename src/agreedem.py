#!/usr/bin/env python3
import argparse
import os

import numpy as np
import rasterio
import whitebox


def agreedem(
    rivers_raster,
    dem,
    output_raster,
    workspace,
    buffer_dist,
    smooth_drop,
    sharp_drop,
    delete_intermediate_data,
):
    '''
    Produces a hydroconditioned raster using the AGREE DEM methodology as described by Ferdi Hellweger
    (https://www.caee.utexas.edu/prof/maidment/gishydro/ferdi/research/agree/agree.html).
    Whiteboxtools is used to calculate intermediate allocation and proximity rasters.

    Parameters
    ----------
    rivers_raster : STR
        Path to the river raster. River cells = 1 and non-river cells = 0.
        For example, see flows_grid_boolean.tif.
    dem : STR
        Elevation DEM (units assumed to be in meters). For example, see dem_meters.tif.
    output_raster : STR
        Path to output raster. For example, dem_burned.tif
    workspace : STR
        Path to workspace to save all intermediate files.
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
    # Set wbt envs
    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)
    wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))

    # ------------------------------------------------------------------
    # 1. From Hellweger documentation: Compute the vector grid
    # (vectgrid). The cells in the vector grid corresponding to the
    # lines in the vector coverage have data. All other cells have no
    # data.

    # Import dem layer and river layer and get dem profile.
    with rasterio.open(dem) as elev, rasterio.open(rivers_raster) as rivers:
        dem_profile = elev.profile

        # Define smogrid profile and output file
        smo_profile = dem_profile.copy()
        smo_profile.update(nodata=0)
        smo_profile.update(dtype='float32')
        smo_output = os.path.join(workspace, 'agree_smogrid.tif')
        vectdist_grid = os.path.join(workspace, 'agree_smogrid_dist.tif')
        vectallo_grid = os.path.join(workspace, 'agree_smogrid_allo.tif')

        # Windowed reading/calculating/writing
        with rasterio.Env():
            with rasterio.open(smo_output, 'w', **smo_profile) as raster:
                for ji, window in elev.block_windows(1):
                    # read elevation data and mask information
                    elev_data_window = elev.read(1, window=window)
                    elev_mask_window = elev.read_masks(1, window=window).astype('bool')
                    # Import boolean river raster and apply same NODATA mask as dem
                    # layer. In case rivers extend beyond valid data regions of DEM.
                    river_raw_data_window = rivers.read(1, window=window)
                    river_data_window = np.where(elev_mask_window == True, river_raw_data_window, 0)

                    # ---------------------------------------------------------------
                    # 2. From Hellweger documentation: Compute the smooth drop/raise
                    # grid (smogrid). The cells in the smooth drop/raise grid
                    # corresponding to the vector lines have an elevation equal to that
                    # of the original DEM (oelevgrid) plus a certain distance
                    # (smoothdist). All other cells have no data.

                    # Assign smooth distance and calculate the smogrid.
                    smooth_dist = -1 * smooth_drop  # in meters.
                    smogrid_window = river_data_window * (elev_data_window + smooth_dist)

                    # Write out raster
                    raster.write(smogrid_window.astype('float32'), indexes=1, window=window)

        # ------------------------------------------------------------------
        # 3. From Hellweger documentation: Compute the vector distance grids
        # (vectdist and vectallo). The cells in the vector distance grid
        # (vectdist) store the distance to the closest vector cell. The
        # cells in vector allocation grid (vectallo) store the elevation of
        # the closest vector cell.

        # Compute allocation and proximity grid using WhiteboxTools
        smo_output_zerod = os.path.join(workspace, 'agree_smogrid_zerod.tif')
        wbt.euclidean_distance(rivers_raster, vectdist_grid)

        assert os.path.exists(vectdist_grid), f'Vector distance grid not created: {vectdist_grid}'

        wbt.convert_nodata_to_zero(smo_output, smo_output_zerod)

        assert os.path.exists(smo_output_zerod), f'Vector allocation grid not created: {smo_output_zerod}'

        wbt.euclidean_allocation(smo_output_zerod, vectallo_grid)

        assert os.path.exists(vectallo_grid), f'Vector allocation grid not created: {vectallo_grid}'

        # ------------------------------------------------------------------
        # 4. From Hellweger documentation: Compute the buffer grid
        # (bufgrid2). The cells in the buffer grid outside the buffer
        # distance (buffer) store the original elevation. The cells in the
        # buffer grid inside the buffer distance have no data.

        # Open distance, allocation, elevation grids.
        with rasterio.open(vectdist_grid) as vectdist:
            # Define bufgrid profile and output file.
            buf_output = os.path.join(workspace, 'agree_bufgrid.tif')
            bufdist_grid = os.path.join(workspace, 'agree_bufgrid_dist.tif')
            bufallo_grid = os.path.join(workspace, 'agree_bufgrid_allo.tif')
            buf_profile = dem_profile.copy()
            buf_profile.update(dtype='float32')

            # Windowed reading/calculating/writing
            with rasterio.Env():
                with rasterio.open(buf_output, 'w', **buf_profile) as raster:
                    for ji, window in elev.block_windows(1):
                        # read distance, allocation, and elevation datasets
                        vectdist_data_window = vectdist.read(1, window=window)
                        elev_data_window = elev.read(1, window=window)

                        # Define buffer distance and calculate adjustment to compute the
                        # bufgrid.
                        # half_res adjustment equal to half distance of one cell
                        half_res = elev.res[0] / 2
                        final_buffer = buffer_dist - half_res  # assume all units in meters.

                        # Calculate bufgrid. Assign NODATA to areas where vectdist_data <=
                        # buffered value.
                        bufgrid_window = np.where(
                            vectdist_data_window > final_buffer, elev_data_window, dem_profile['nodata']
                        )

                        # Write out raster.
                        raster.write(bufgrid_window.astype('float32'), indexes=1, window=window)

            # ------------------------------------------------------------------
            # 5. From Hellweger documentation: Compute the buffer distance grids
            # (bufdist and bufallo). The cells in the buffer distance grid
            # (bufdist) store the distance to the closest valued buffer grid
            # cell (bufgrid2). The cells in buffer allocation grid (bufallo)
            # store the elevation of the closest valued buffer cell.

            # # Transform the buffer grid (bufgrid2) to binary raster
            bin_buf_output = os.path.join(workspace, 'agree_binary_bufgrid.tif')
            with rasterio.open(buf_output) as agree_bufgrid:
                agree_bufgrid_profile = agree_bufgrid.profile
                bin_buf_output_profile = agree_bufgrid_profile.copy()
                bin_buf_output_profile.update(dtype='float32')

                with rasterio.Env():
                    with rasterio.open(bin_buf_output, 'w', **bin_buf_output_profile) as raster:
                        for ji, window in agree_bufgrid.block_windows(1):
                            # read distance, allocation, and elevation datasets
                            agree_bufgrid_data_window = agree_bufgrid.read(1, window=window)

                            # Calculate bufgrid. Assign NODATA to areas where vectdist_data <=
                            agree_bufgrid_data_window = np.where(agree_bufgrid_data_window > -10000, 1, 0)

                            # Write out raster.
                            raster.write(
                                agree_bufgrid_data_window.astype('float32'), indexes=1, window=window
                            )

            # Compute allocation and proximity grid using WhiteboxTools
            buf_output_zerod = os.path.join(workspace, 'agree_bufgrid_zerod.tif')
            wbt.euclidean_distance(bin_buf_output, bufdist_grid)

            assert os.path.exists(bufdist_grid), f'Buffer allocation grid not created: {bufdist_grid}'

            wbt.convert_nodata_to_zero(buf_output, buf_output_zerod)

            assert os.path.exists(buf_output_zerod), f'Buffer allocation grid not created: {buf_output_zerod}'

            wbt.euclidean_allocation(buf_output_zerod, bufallo_grid)

            assert os.path.exists(bufallo_grid), f'Buffer allocation grid not created: {bufallo_grid}'

            # Open distance, allocation, elevation grids.
            with rasterio.open(bufdist_grid) as bufdist, rasterio.open(
                bufallo_grid
            ) as bufallo, rasterio.open(vectallo_grid) as vectallo:
                # Define profile output file.
                agree_output = output_raster
                agree_profile = dem_profile.copy()
                agree_profile.update(dtype='float32')

                # Windowed reading/calculating/writing
                with rasterio.Env():
                    with rasterio.open(agree_output, 'w', **agree_profile) as raster:
                        for ji, window in elev.block_windows(1):
                            # Read elevation data and mask, distance and allocation grids, and river data.
                            elev_data_window = elev.read(1, window=window)
                            elev_mask_window = elev.read_masks(1, window=window).astype('bool')
                            bufdist_data_window = bufdist.read(1, window=window)
                            bufallo_data_window = bufallo.read(1, window=window)
                            vectdist_data_window = vectdist.read(1, window=window)
                            vectallo_data_window = vectallo.read(1, window=window)
                            river_raw_data_window = rivers.read(1, window=window)

                            bufallo_data_window = np.where(
                                bufallo_data_window == -32768.0, elev_data_window, bufallo_data_window
                            )

                            vectallo_data_window = np.where(
                                vectallo_data_window == -32768.0, elev_data_window - 10, vectallo_data_window
                            )

                            river_raw_data_window = river_raw_data_window.astype(np.float32)

                            river_data_window = np.where(
                                elev_mask_window == True, river_raw_data_window, -20.0
                            )
                            # ------------------------------------------------------------------
                            # 6. From Hellweger documentation: Compute the smooth modified
                            # elevation grid (smoelev). The cells in the smooth modified
                            # elevation grid store the results of the smooth surface
                            # reconditioning process. Note that for cells outside the buffer the
                            # equation below assigns the original elevation.

                            # Calculate smoelev.
                            smoelev_window = (
                                vectallo_data_window
                                + (
                                    (bufallo_data_window - vectallo_data_window)
                                    / (bufdist_data_window + vectdist_data_window)
                                )
                                * vectdist_data_window
                            )

                            # ------------------------------------------------------------------
                            # 7. From Hellweger documentation: Compute the sharp drop/raise grid
                            # (shagrid). The cells in the sharp drop/raise grid corresponding to
                            # the vector lines have an elevation equal to that of the smooth
                            # modified elevation grid (smoelev) plus a certain distance
                            # (sharpdist). All other cells have no data.

                            # Define sharp drop distance and calculate the sharp drop grid where
                            # only river cells are dropped by the sharp_dist amount.
                            sharp_dist = -1 * sharp_drop  # in meters.
                            shagrid_window = (smoelev_window + sharp_dist) * river_data_window

                            # ------------------------------------------------------------------
                            # 8. From Hellweger documentation: Compute the modified elevation
                            # grid (elevgrid). The cells in the modified elevation grid store
                            # the results of the surface reconditioning process. Note that for
                            # cells outside the buffer the the equation below assigns the
                            # original elevation.

                            # Merge sharp drop grid with smoelev grid. Then apply the same
                            # NODATA mask as original elevation grid.
                            elevgrid_window = np.where(river_data_window == 0, smoelev_window, shagrid_window)
                            agree_dem_window = np.where(
                                elev_mask_window == True, elevgrid_window, dem_profile['nodata']
                            )

                            # Write out to raster
                            raster.write(agree_dem_window.astype('float32'), indexes=1, window=window)

    # If the '-t' flag is called, intermediate data is removed.
    if delete_intermediate_data:
        os.remove(smo_output)
        os.remove(buf_output)
        os.remove(vectdist_grid)
        os.remove(vectallo_grid)
        os.remove(bufdist_grid)
        os.remove(bufallo_grid)
        os.remove(bin_buf_output)
        os.remove(buf_output_zerod)
        os.remove(smo_output_zerod)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Calculate AGREE DEM')
    parser.add_argument('-r', '--rivers', help='flows grid boolean layer', required=True)
    parser.add_argument('-d', '--dem_m', help='DEM raster in meters', required=True)
    parser.add_argument('-w', '--workspace', help='Workspace', required=True)
    parser.add_argument('-o', '--output', help='Path to output raster', required=True)
    parser.add_argument('-b', '--buffer', help='Buffer distance (m) on either side of channel', required=True)
    parser.add_argument('-sm', '--smooth', help='Smooth drop (m)', required=True)
    parser.add_argument('-sh', '---sharp', help='Sharp drop (m)', required=True)
    parser.add_argument(
        '-t', '--del', help='Optional flag to delete intermediate datasets', action='store_true'
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    rivers_raster = args['rivers']
    dem = args['dem_m']
    workspace = args['workspace']
    output_raster = args['output']
    buffer_dist = float(args['buffer'])
    smooth_drop = float(args['smooth'])
    sharp_drop = float(args['sharp'])
    delete_intermediate_data = args['del']

    # Run agreedem
    agreedem(
        rivers_raster,
        dem,
        output_raster,
        workspace,
        buffer_dist,
        smooth_drop,
        sharp_drop,
        delete_intermediate_data,
    )
