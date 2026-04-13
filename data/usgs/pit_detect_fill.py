import argparse
import glob
import logging
import math
import multiprocessing
import os
import sys
import time
from functools import partial

import numpy as np
import rasterio
import richdem as rd
from scipy.ndimage import binary_opening, generate_binary_structure, label
from skimage.measure import regionprops


# ==========================================
# Pit Detection Parameters
# ==========================================
T1_MIN_PIXELS = 15
T1_MAX_PIXELS = 8000
T1_MIN_MEAN_DEPTH = 10
T1_MIN_CIRCULARITY = 0.6

T2_MIN_PIXELS = 15
T2_MAX_PIXELS = 20000
T2_MIN_MEAN_DEPTH = 5
T2_MIN_MAX_DEPTH = 20
# ==========================================


def process_single_dem(dem_path, output_dir):
    """
    Processes a single DEM file and returns a fit pilled DEM and pit logs.
    Note that we only save the pit detected filled elevation values. All
    other elevation values set to null.
    """
    filename = os.path.basename(dem_path)
    start_time = time.time()
    pits_detected = 0
    pit_logs = []  # Store individual pit details here

    try:
        # Load Original DEM and get metadata
        with rasterio.open(dem_path) as src:
            orig = src.read(1)
            profile = src.profile
            nodata_val = src.nodata

        # Perform Depression Filling (in memory)
        rd_orig = rd.rdarray(orig, no_data=nodata_val)
        rd_filled = rd_orig.copy()
        rd.FillDepressions(rd_filled, epsilon=False, in_place=True)
        filled = np.array(rd_filled)

        # Detect Pits by difference btw filled and original
        diff = filled - orig
        diff[diff < 0] = 0

        if nodata_val is not None:
            nodata_mask = orig == nodata_val
            diff[nodata_mask] = 0

        # Morphological opening to trim stream arms
        struct = generate_binary_structure(2, 2)
        initial_fill_mask = diff > 0
        cleaned_mask = binary_opening(initial_fill_mask, structure=struct, iterations=1)
        diff[~cleaned_mask] = 0

        labeled, num = label(diff > 0, structure=struct)
        props = regionprops(labeled, intensity_image=diff)

        pit_mask = np.zeros_like(diff, dtype=np.uint8)

        for prop in props:
            pixel_count = prop.area
            max_depth = prop.max_intensity
            mean_depth = prop.mean_intensity
            perim = prop.perimeter if prop.perimeter > 0 else 1e-9
            circularity = (4 * math.pi * pixel_count) / (perim**2)

            tier1_match = (
                (pixel_count >= T1_MIN_PIXELS)
                and (pixel_count < T1_MAX_PIXELS)
                and (mean_depth >= T1_MIN_MEAN_DEPTH)
                and (circularity >= T1_MIN_CIRCULARITY)
            )

            tier2_match = (
                (pixel_count >= T2_MIN_PIXELS)
                and (pixel_count < T2_MAX_PIXELS)
                and (mean_depth >= T2_MIN_MEAN_DEPTH)
                and (max_depth >= T2_MIN_MAX_DEPTH)
            )

            if tier1_match or tier2_match:
                pit_mask[labeled == prop.label] = 1
                pits_detected += 1

                # Format which tiers matched for the log
                matched_tiers = []
                if tier1_match:
                    matched_tiers.append("T1")
                if tier2_match:
                    matched_tiers.append("T2")
                tier_str = " & ".join(matched_tiers)

                # Append details to log list
                pit_logs.append(
                    f"    -> Pit {pits_detected} [{tier_str}]: "
                    f"Pixels={pixel_count}, MaxDepth={max_depth:.2f}, "
                    f"MeanDepth={mean_depth:.2f}, Circularity={circularity:.3f}"
                )

        # Create Output DEM filled Array
        lean_output = np.full_like(orig, nodata_val)
        is_pit = pit_mask == 1
        lean_output[is_pit] = filled[is_pit]

        # Save the pit filled only DEM
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
            "pit_logs": pit_logs,  # Pass the logs back to main
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
    parser.add_argument(
        "-i", "--input", required=True, help="Path to the directory containing input DEM .tif files."
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Path to the directory to save the output processed DEMs."
    )
    parser.add_argument(
        "-j", "--jobs", type=int, default=1, help="Number of concurrent processes to run (default: 1)."
    )

    parsed_args = parser.parse_args(args)

    input_dir = parsed_args.input
    output_dir = parsed_args.output
    num_jobs = parsed_args.jobs

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    log_file = os.path.join(output_dir, "processing_run.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    dem_files = glob.glob(os.path.join(input_dir, "*.tif"))

    if not dem_files:
        logging.warning(f"No .tif files found in {input_dir}")
        return

    logging.info(f"Found {len(dem_files)} DEMs to process.")
    logging.info(f"Running with {num_jobs} parallel jobs. Results will save to {output_dir}")

    worker_func = partial(process_single_dem, output_dir=output_dir)

    total_pits = 0
    total_failures = 0
    start_run_time = time.time()

    with multiprocessing.Pool(processes=num_jobs) as pool:
        for result in pool.imap_unordered(worker_func, dem_files):
            fname = result["filename"]
            t_sec = result["duration_sec"]
            pits = result["pits_found"]

            if result["status"] == "SUCCESS":
                logging.info(f"Processed {fname} in {t_sec:.2f}s | Pits detected: {pits}")
                # Print the detailed logs for this specific DEM
                for pit_log in result["pit_logs"]:
                    logging.info(pit_log)
                total_pits += pits
            else:
                logging.error(f"Failed {fname} after {t_sec:.2f}s | Error: {result['error_msg']}")
                total_failures += 1

    total_run_time = time.time() - start_run_time
    logging.info("-" * 40)
    logging.info("PROCESSING COMPLETE")
    logging.info(f"Total time: {total_run_time:.2f} seconds")
    logging.info(f"Total pits detected across all DEMs: {total_pits}")
    logging.info(f"Total failures: {total_failures}")
    logging.info("-" * 40)


if __name__ == "__main__":
    main()
