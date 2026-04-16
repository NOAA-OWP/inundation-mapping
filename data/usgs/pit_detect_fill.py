import argparse
import glob
import logging
import math
import multiprocessing
import os
import sys
import time
from functools import partial

import fiona
import geopandas as gpd
import numpy as np
import rasterio
import richdem as rd
from rasterio.features import rasterize
from rasterio.warp import transform_bounds
from scipy.ndimage import binary_opening, generate_binary_structure, label
from skimage.measure import regionprops


# ==========================================
# Pit Detection Parameters
# ==========================================
# TIER 1: OSM Guided Detection
OSM_MIN_PIXELS = 40
OSM_MIN_MED_DEPTH = 5.0
OSM_MIN_OVERLAP_RATIO = 0.50  # 50% or more of the pit must be inside the OSM polygon

# TIER 2: Restrictive Terrain Fallback
MIN_PIXELS = 50
MAX_PIXELS = 8000
MIN_MED_DEPTH = 8.0
MIN_CIRCULARITY = 0.75
MIN_MEDIAN_RATIO = 0.6  # mean_depth / max_depth


# ==========================================
# Processing Functions
# ==========================================
def process_single_dem(dem_path, output_dir, osm_path):
    filename = os.path.basename(dem_path)
    start_time = time.time()
    pits_detected = 0
    pit_logs = []

    try:
        # 1. Load Original DEM and get metadata
        with rasterio.open(dem_path) as src:
            orig = src.read(1)
            profile = src.profile
            nodata_val = src.nodata
            bounds = src.bounds
            transform = src.transform
            shape = orig.shape
            dem_crs = src.crs  # Grab the DEM's coordinate system

        # 2. Rasterize OSM Polygons for this DEM's extent
        # Peek at the OSM file to get its CRS without loading the whole file into memory
        with fiona.open(osm_path) as f:
            osm_crs = f.crs

        # Transform the DEM's bounding box to match the OSM file's CRS
        osm_bounds = transform_bounds(dem_crs, osm_crs, *bounds)

        # Read the OSM data using the correctly projected bounding box
        osm_gdf = gpd.read_file(osm_path, bbox=osm_bounds)

        if not osm_gdf.empty:
            # Reproject the loaded polygons to match the DEM so they align perfectly on the grid
            osm_gdf = osm_gdf.to_crs(dem_crs)

            # Create a generator of (geometry, 1) to burn into the raster
            osm_shapes = ((geom, 1) for geom in osm_gdf.geometry)
            # Create as uint8 for Rasterio, then cast to bool for NumPy logic
            osm_mask = rasterize(
                shapes=osm_shapes, out_shape=shape, transform=transform, fill=0, dtype='uint8'
            ).astype(bool)
        else:
            osm_mask = np.zeros(shape, dtype=bool)

        # 3. Perform Depression Filling (In-Memory)
        rd_orig = rd.rdarray(orig, no_data=nodata_val)
        rd_filled = rd_orig.copy()
        rd.FillDepressions(rd_filled, epsilon=False, in_place=True)
        filled = np.array(rd_filled)

        # 4. Calculate Difference Mask
        diff = filled - orig
        diff[diff < 0] = 0

        if nodata_val is not None:
            nodata_mask = orig == nodata_val
            diff[nodata_mask] = 0

        # Morphological Opening to trim stream arms
        struct = generate_binary_structure(2, 2)
        initial_fill_mask = diff > 0
        cleaned_mask = binary_opening(initial_fill_mask, structure=struct, iterations=1)
        diff[~cleaned_mask] = 0

        # Label regions
        labeled, num = label(diff > 0, structure=struct)
        props = regionprops(labeled, intensity_image=diff)

        pit_mask = np.zeros_like(diff, dtype=np.uint8)

        for prop in props:
            pixel_count = prop.area
            max_depth = prop.max_intensity
            mean_depth = prop.mean_intensity
            perim = prop.perimeter if prop.perimeter > 0 else 1e-9
            circularity = (4 * math.pi * pixel_count) / (perim**2)

            # --------------------------------------------------
            # NEW: Calculate Median Depth
            # --------------------------------------------------
            # 1. Get the rectangular bounding box of the pit from the main diff array
            local_diff = diff[prop.slice]

            # 2. prop.image is a boolean mask of the exact pit shape within that box
            local_region = prop.image

            # 3. Extract only the depth values that belong to this specific pit
            pit_pixels = local_diff[local_region]

            # 4. Calculate the median using NumPy
            median_depth = np.median(pit_pixels)

            # Calculate ratios (handle division by zero)
            # mean_ratio = (mean_depth / max_depth) if max_depth > 0 else 0
            median_ratio = (median_depth / max_depth) if max_depth > 0 else 0

            # --------------------------------------------------
            # TIER 1: OSM Overlap Check
            # --------------------------------------------------
            # We slice the OSM mask to the bounding box of the current depression
            local_osm = osm_mask[prop.slice]
            local_region = prop.image  # Boolean mask of the current depression shape

            # Count how many pixels of this specific pit overlap with the OSM mask
            overlap_pixels = np.sum(local_osm & local_region)
            overlap_ratio = overlap_pixels / pixel_count

            # To be an OSM match, it must meet the overlap ratio AND minimum size/depth
            osm_match = (
                (overlap_ratio >= OSM_MIN_OVERLAP_RATIO)
                and (pixel_count >= OSM_MIN_PIXELS)
                and (median_depth >= OSM_MIN_MED_DEPTH)
            )

            # --------------------------------------------------
            # TIER 2: Restrictive Terrain Fallback
            # --------------------------------------------------
            terrain_match = (
                (pixel_count >= MIN_PIXELS)
                and (pixel_count < MAX_PIXELS)
                and (median_depth >= MIN_MED_DEPTH)
                and (circularity >= MIN_CIRCULARITY)
                and (median_ratio >= MIN_MEDIAN_RATIO)
            )

            # Assign Pit and Log
            if osm_match or terrain_match:
                pit_mask[labeled == prop.label] = 1
                pits_detected += 1

                match_type = "OSM" if osm_match else "TERRAIN"

                # I added the Overlap Ratio to the log so you can see exactly how
                # much of the pit fell inside the polygon!
                pit_logs.append(
                    f"    -> Pit {pits_detected} [{match_type}]: "
                    f"Pixels={pixel_count}, MaxD={max_depth:.1f}, "
                    f"MeanD={mean_depth:.1f}, MedianD={median_depth:.1f}, Circ={circularity:.2f}, "
                    f"Ratio={median_ratio:.2f}, OSM_Overlap={overlap_ratio:.2f}"
                )

        # 5. Create Lean Output DEM Array
        lean_output = np.full_like(orig, nodata_val)
        is_pit = pit_mask == 1
        lean_output[is_pit] = filled[is_pit]

        # 6. Save the Lean DEM
        out_path = os.path.join(output_dir, filename.replace(".tif", "_pit_fills.tif"))
        profile.update(compress='deflate', tiled=True, blockxsize=256, blockysize=256, predictor=2)

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(lean_output, 1)

        duration = time.time() - start_time
        return {
            "filename": filename,
            "status": "SUCCESS",
            "pits_found": pits_detected,
            "duration_sec": duration,
            "error_msg": None,
            "pit_logs": pit_logs,
        }

    except Exception as e:
        duration = time.time() - start_time
        return {
            "filename": filename,
            "status": "FAILED",
            "pits_found": 0,
            "duration_sec": duration,
            "error_msg": str(e),
            "pit_logs": [],
        }


def main(args=None):
    parser = argparse.ArgumentParser(description="Batch process DEMs for pit detection and filling.")
    parser.add_argument("-i", "--input", required=True, help="Path to input DEM directory.")
    parser.add_argument("-o", "--output", required=True, help="Path to output DEM directory.")
    parser.add_argument(
        "-p", "--polygons", required=True, help="Path to OSM polygons file (e.g. .gpkg or .shp)."
    )
    parser.add_argument("-j", "--jobs", type=int, default=1, help="Number of concurrent processes.")

    parsed_args = parser.parse_args(args)

    if not os.path.exists(parsed_args.output):
        os.makedirs(parsed_args.output)

    log_file = os.path.join(parsed_args.output, "processing_run.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    dem_files = glob.glob(os.path.join(parsed_args.input, "*.tif"))
    if not dem_files:
        logging.warning(f"No .tif files found in {parsed_args.input}")
        return

    logging.info(f"Running {len(dem_files)} DEMs with {parsed_args.jobs} jobs.")

    # Pass the OSM path into the worker function
    worker_func = partial(process_single_dem, output_dir=parsed_args.output, osm_path=parsed_args.polygons)

    total_pits = 0
    total_failures = 0
    start_run_time = time.time()

    with multiprocessing.Pool(processes=parsed_args.jobs) as pool:
        for result in pool.imap_unordered(worker_func, dem_files):
            fname = result["filename"]
            t_sec = result["duration_sec"]
            pits = result["pits_found"]

            if result["status"] == "SUCCESS":
                logging.info(f"Processed {fname} in {t_sec:.2f}s | Pits: {pits}")
                for pit_log in result["pit_logs"]:
                    logging.info(pit_log)
                total_pits += pits
            else:
                logging.error(f"Failed {fname} after {t_sec:.2f}s | Error: {result['error_msg']}")
                total_failures += 1

    logging.info("-" * 40)
    logging.info(f"Total time: {time.time() - start_run_time:.2f} seconds")
    logging.info(f"Total pits detected: {total_pits} | Failures: {total_failures}")
    logging.info("-" * 40)


if __name__ == "__main__":
    main()
