#!/usr/bin/env python3


import argparse

import numpy as np
import rasterio
from numba import njit, typed, types


def adjust_thalweg_laterally(
    filled_dem,
    original_dem,
    stream_raster,
    allocation_raster,
    cost_distance_raster,
    cost_distance_tolerance,
    dem_lateral_thalweg_adj,
    lateral_elevation_threshold,
):
    # ------------------------------------ Get catchment_min_dict ----------------------------------------- #
    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    @njit
    def make_zone_min_dict(elevation_window, zone_min_dict, zone_window, cost_window, cost_tolerance, ndv):
        for i, elev_m in enumerate(zone_window):
            # If the zone really exists in the dictionary, compare elevation values.
            i = int(i)
            elev_m = types.int32(elev_m)

            if cost_window[i] <= cost_tolerance:
                if elevation_window[i] > 0:  # Don't allow bad elevation values
                    if elev_m in zone_min_dict:
                        # If the elevation_window's elevation value is less than the zone_min_dict min,
                        # update the zone_min_dict min.
                        if elevation_window[i] < zone_min_dict[elev_m]:
                            zone_min_dict[elev_m] = elevation_window[i]
                    else:
                        zone_min_dict[elev_m] = elevation_window[i]

        return zone_min_dict

    # ------------------------------------ Assign zonal min to thalweg ------------------------------------ #
    @njit
    def minimize_thalweg_elevation(dem_window, zone_min_dict, zone_window, thalweg_window):
        # Copy elevation values into new array that will store the minimized elevation values.
        dem_window_to_return = np.empty_like(dem_window)
        dem_window_to_return[:] = dem_window

        for i, elev_m in enumerate(zone_window):
            i = int(i)
            elev_m = types.int32(elev_m)
            thalweg_cell = thalweg_window[i]  # From flows_grid_boolean.tif (0s and 1s)
            if thalweg_cell == 1:  # Make sure thalweg cells are checked.
                if elev_m in zone_min_dict:
                    zone_min_elevation = zone_min_dict[elev_m]
                    dem_thalweg_elevation = dem_window[i]

                    elevation_difference = dem_thalweg_elevation - zone_min_elevation

                    if (zone_min_elevation < dem_thalweg_elevation) and (
                        elevation_difference <= lateral_elevation_threshold
                    ):
                        dem_window_to_return[i] = zone_min_elevation

        return dem_window_to_return

    # Open files.
    with rasterio.open(filled_dem) as filled_dem_obj, rasterio.open(
        allocation_raster
    ) as allocation_zone_raster_object:
        with rasterio.open(cost_distance_raster) as cost_distance_raster_object:
            meta = filled_dem_obj.meta.copy()
            meta['tiled'], meta['compress'] = True, 'lzw'
            ndv = meta['nodata']

            # -- Create zone_min_dict -- #
            zone_min_dict = typed.Dict.empty(
                types.int32, types.float32
            )  # Initialize an empty dictionary to store the catchment minimums
            # Update catchment_min_dict with pixel sheds minimum.
            for ji, window in filled_dem_obj.block_windows(
                1
            ):  # Iterate over windows, using elevation_raster_object as template
                elevation_window = filled_dem_obj.read(1, window=window).ravel()  # Define elevation_window
                zone_window = allocation_zone_raster_object.read(
                    1, window=window
                ).ravel()  # Define zone_window
                cost_window = cost_distance_raster_object.read(1, window=window).ravel()  # Define cost_window

                # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
                zone_min_dict = make_zone_min_dict(
                    elevation_window,
                    zone_min_dict,
                    zone_window,
                    cost_window,
                    int(cost_distance_tolerance),
                    ndv,
                )

                del elevation_window, zone_window, cost_window

            # --------------------------------------------------------------------------------------------- #

        # Specify raster object metadata.
        with rasterio.open(stream_raster) as thalweg_obj, rasterio.open(
            original_dem
        ) as orig_dem_obj, rasterio.open(dem_lateral_thalweg_adj, 'w', **meta) as output_obj:

            for ji, window in filled_dem_obj.block_windows(1):
                # Read window data (2D)
                dem_window_filled_2d = filled_dem_obj.read(1, window=window)
                dem_window_orig_2d = orig_dem_obj.read(1, window=window)
                zone_window_2d = allocation_zone_raster_object.read(1, window=window)
                thalweg_window_2d = thalweg_obj.read(1, window=window)

                # Flatten arrays for Numba
                dem_window_filled = dem_window_filled_2d.ravel()
                zone_window = zone_window_2d.ravel()
                thalweg_window = thalweg_window_2d.ravel()

                # Perform thalweg adjustment
                minimized_thalweg_flat = minimize_thalweg_elevation(
                    dem_window_filled, zone_min_dict, zone_window, thalweg_window
                )

                # Reshape back to 2D (use the shape of the window)
                minimized_thalweg_2d = minimized_thalweg_flat.reshape(dem_window_filled_2d.shape)

                # Combine adjusted thalweg with original DEM
                combined_window = np.where(thalweg_window_2d == 1, minimized_thalweg_2d, dem_window_orig_2d)

                # Write output (rasterio expects 2D input)
                output_obj.write(combined_window.astype(np.float32), window=window, indexes=1)

                del (
                    dem_window_filled_2d,
                    dem_window_orig_2d,
                    zone_window_2d,
                    thalweg_window,
                    minimized_thalweg_2d,
                )


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Adjusts the elevation of the thalweg to the lateral zonal minimum.'
    )
    parser.add_argument(
        '-f', '--filled_dem', help='Filled DEM raster used for elevation calculations.', required=True
    )
    parser.add_argument(
        '-e',
        '--original_dem',
        help='Original DEM raster used for non-thalweg elevation values.',
        required=True,
    )
    parser.add_argument(
        '-s', '--stream_raster', help='Raster of thalweg pixels (0=No Thalweg, 1=Thalweg)', required=True
    )
    parser.add_argument(
        '-a', '--allocation_raster', help='Raster of thalweg allocation zones.', required=True
    )
    parser.add_argument(
        '-d',
        '--cost_distance_raster',
        help='Raster of cost distances for the allocation raster.',
        required=True,
    )
    parser.add_argument(
        '-t',
        '--cost_distance_tolerance',
        help='Tolerance in meters to use when searching for zonal minimum.',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--dem_lateral_thalweg_adj',
        help='Output elevation raster with adjusted thalweg.',
        required=True,
    )
    parser.add_argument(
        '-th',
        '--lateral_elevation_threshold',
        help='Maximum difference between current thalweg elevation and lowest lateral elevation in meters.',
        required=True,
        type=int,
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    adjust_thalweg_laterally(**args)
