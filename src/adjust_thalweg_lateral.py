#!/usr/bin/env python3


import argparse
import math

import numpy as np
import rasterio
from numba import njit, typed, types
from rasterio.features import shapes
from scipy.ndimage import generate_binary_structure, label
from skimage.measure import regionprops


# --------------------------- pit detection function ----------------------------
def detect_pits(filled_dem_path, original_dem_path, save_mask=True):
    """Detects gravel pit–like depressions by comparing filled and original DEMs."""

    with rasterio.open(filled_dem_path) as fsrc, rasterio.open(original_dem_path) as osrc:
        filled = fsrc.read(1)
        orig = osrc.read(1)
        profile = fsrc.profile

    # Compute fill depth
    diff = filled - orig
    diff[diff < 0] = 0  # Ignore negatives

    # Label connected regions of positive fill depth
    structure = generate_binary_structure(2, 2)  # 2D, fully connected (8-connectivity)
    labeled, num = label(diff > 0, structure=structure)
    props = regionprops(labeled, intensity_image=diff)

    # Initialize mask
    pit_mask = np.zeros_like(diff, dtype=np.uint8)
    # pixel_area = abs(profile["transform"][0]) * abs(profile["transform"][4])

    for prop in props:
        area_pixels = prop.area
        # area_m2 = area_pixels * pixel_area
        max_depth = prop.max_intensity
        mean_depth = prop.mean_intensity
        perim = prop.perimeter if prop.perimeter > 0 else 1e-9
        circularity = (4 * math.pi * area_pixels) / (perim**2)

        # Two-tier detection logic
        pit_find = ((area_pixels >= 15) and (mean_depth >= 10) and (circularity >= 0.6)) or (
            (area_pixels >= 20) and (max_depth >= 20)
        )

        if pit_find:
            pit_mask[labeled == prop.label] = 1

    if save_mask:
        mask_path = filled_dem_path.replace(".tif", "_pitmask.tif")
        profile.update(dtype="uint8", nodata=0, compress="lzw")
        with rasterio.open(mask_path, "w", **profile) as dst:
            dst.write(pit_mask, 1)
        print(f"Saved pit mask to: {mask_path}")

    return pit_mask, profile


# ----------------------- thalweg adjustment function -----------------------------
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
    def minimize_thalweg_elevation(
        dem_window, zone_min_dict, zone_window, thalweg_window, lateral_elevation_threshold
    ):
        # Copy elevation values into new array that will store the minimized elevation values.
        dem_window_to_return = np.empty_like(dem_window)
        dem_window_to_return[:] = dem_window

        for i in range(len(zone_window)):
            elev_m = types.int32(zone_window[i])
            thalweg_cell = thalweg_window[i]  # From flows_grid_boolean.tif (0s and 1s)
            if thalweg_cell == 1 and elev_m in zone_min_dict:  # Make sure thalweg cells are checked.
                zone_min_elevation = zone_min_dict[elev_m]
                dem_thalweg_elevation = dem_window[i]
                elevation_difference = dem_thalweg_elevation - zone_min_elevation
                if (zone_min_elevation < dem_thalweg_elevation) and (
                    elevation_difference <= lateral_elevation_threshold
                ):
                    dem_window_to_return[i] = zone_min_elevation

        return dem_window_to_return

    # Detect pits first
    pit_mask, profile = detect_pits(filled_dem, original_dem)

    # Build combined DEM for lateral search
    with rasterio.open(filled_dem) as fsrc, rasterio.open(original_dem) as osrc:
        filled = fsrc.read(1)
        orig = osrc.read(1)
        combined_dem = np.where(pit_mask == 1, filled, orig)

    # Optional save for QA
    combined_path = filled_dem.replace(".tif", "_combined.tif")
    profile.update(dtype="float32", compress="lzw", nodata=None)
    with rasterio.open(combined_path, "w", **profile) as dst:
        dst.write(combined_dem.astype(np.float32), 1)
    print(f"Saved combined DEM to: {combined_path}")

    # Open necessary datasets
    with (
        rasterio.open(allocation_raster) as alloc_src,
        rasterio.open(cost_distance_raster) as cost_src,
        rasterio.open(filled_dem) as filled_src,
        rasterio.open(original_dem) as orig_src,
        rasterio.open(stream_raster) as thalweg_src,
    ):
        meta = filled_src.meta.copy()
        meta.update(tiled=True, compress="lzw")
        ndv = meta.get("nodata", -9999)

        # Creat zone min dictionary
        zone_min_dict = typed.Dict.empty(types.int32, types.float32)
        for ji, window in filled_src.block_windows(1):
            elevation_window = combined_dem[
                window.row_off : window.row_off + window.height,
                window.col_off : window.col_off + window.width,
            ].ravel()
            zone_window = alloc_src.read(1, window=window).ravel()
            cost_window = cost_src.read(1, window=window).ravel()

            zone_min_dict = make_zone_min_dict(
                elevation_window, zone_min_dict, zone_window, cost_window, int(cost_distance_tolerance), ndv
            )

        with rasterio.open(dem_lateral_thalweg_adj, "w", **meta) as out_dst:
            for ji, window in filled_src.block_windows(1):
                dem_window_filled_2d = filled_src.read(1, window=window)
                dem_window_orig_2d = orig_src.read(1, window=window)
                thalweg_window_2d = thalweg_src.read(1, window=window)
                pit_window = pit_mask[
                    window.row_off : window.row_off + window.height,
                    window.col_off : window.col_off + window.width,
                ]

                base_dem_window = np.where(pit_window == 1, dem_window_filled_2d, dem_window_orig_2d)
                dem_window_flat = base_dem_window.ravel()

                zone_window = alloc_src.read(1, window=window).ravel()
                thalweg_window = thalweg_window_2d.ravel()

                minimized_flat = minimize_thalweg_elevation(
                    dem_window_flat, zone_min_dict, zone_window, thalweg_window, lateral_elevation_threshold
                )
                minimized_2d = minimized_flat.reshape(base_dem_window.shape)

                combined_window = np.where(thalweg_window_2d == 1, minimized_2d, base_dem_window)
                out_dst.write(combined_window.astype(np.float32), window=window, indexes=1)

                del (dem_window_filled_2d, dem_window_orig_2d, thalweg_window_2d, minimized_2d)


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
