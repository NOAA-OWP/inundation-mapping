#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import os
import re
import shutil
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

import rasterio
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from rasterio.enums import Resampling
from rasterio.shutil import copy
from rio_vrt import build_vrt
from tqdm import tqdm

# from utils.shared_functions import FIM_Helpers as fh
import src.utils.shared_functions as sf


# INUN_REVIEW_DIR = r'/data/inputs/rating_curve/nwm_recur_flows/'
# INUN_OUTPUT_DIR = r'/data/inundation_review/inundate_nation/'
# INPUTS_DIR = r'/data/inputs'
# OUTPUT_BOOL_PARENT_DIR = '/data/inundation_review/inundate_nation/bool_temp/
# DEFAULT_OUTPUT_DIR = '/data/inundation_review/inundate_nation/mosaic_output/'


def inundate_nation(
    hand_run_dir,
    output_dir,
    magnitude_key,
    flow_file,
    huc_list,
    inc_mosaic,
    precalb,
    job_number,
    thread_number,
):
    assert os.path.exists(flow_file), f"ERROR: could not find the flow file: {flow_file}"

    available_cores = multiprocessing.cpu_count() - 2

    total_workers = job_number * thread_number
    if total_workers > available_cores:
        raise Exception("Job number arg * num threads can not exceed the max cores - 2."
                        f" max available is {available_cores}")

    hand_version = os.path.basename(os.path.normpath(hand_run_dir))
    output_base_file_name = magnitude_key + "_" + hand_version

    # =====================
    # Setup Logging and headers
    log_file_path = sf.setup_file_logger(output_dir, output_base_file_name)

    print("================================")
    logging.info(f"Start Inundate Nation : {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(f"Using hand version: {hand_version}")    
    overall_start_dt = datetime.now(timezone.utc)

    logging.info(f"Input FIM Directory: {hand_run_dir}")
    logging.info(f"output_dir: {output_dir}")
    logging.info(f"magnitude_key: {magnitude_key}")
    logging.info(f"flow_file: {flow_file}")
    logging.info(f"inc_mosaic: {str(inc_mosaic)}")
    logging.info(f"Precalibration Discharge: {str(precalb)}")
    print(f"Logs saved to: {log_file_path}")    
    logging.info("-----------------------------------")

    magnitude_output_dir = os.path.join(output_dir, output_base_file_name)

    if not os.path.exists(magnitude_output_dir):
        logging.info(
            "Removing previous output dir and creating new output dir for inunation wrapper files: "
            + magnitude_output_dir
        )
        os.mkdir(magnitude_output_dir)
    else:
        # we need to empty it. we will kill it and remake it (using rmtree to force it)
        shutil.rmtree(magnitude_output_dir, ignore_errors=True)
        os.mkdir(magnitude_output_dir)

    if huc_list == 'all' or len(huc_list) == 0:
        huc_list = []
        for huc in os.listdir(hand_run_dir):
            if re.match(r'\d{8}', huc):
                huc_list.append(huc)
    else:
        for huc in huc_list:
            huc_path = os.path.join(hand_run_dir, huc)
            assert os.path.isdir(huc_path), f'ERROR: could not find the input fim_dir location: {huc_path}'

    huc_list.sort()

    logging.info(f"Inundation mosaic wrapper outputs will saved here: {magnitude_output_dir}")

    try:

        if os.path.exists(output_bool_dir):
            # we need to empty it. we will kill it and remake it (using rmtree to force it)
            shutil.rmtree(output_bool_dir, ignore_errors=True)

        has_errors = run_inundation(hand_run_dir, huc_list, magnitude_key, magnitude_output_dir,
                        flow_file, thread_number, job_number, precalb)

        logging.info(f"Inudation complete: {sf.calculate_duration_msg(overall_start_dt)}")

        if huc_errors...... put up message and stop.

        # Perform mosaic operation
        if inc_mosaic:
            section_start_dt = datetime.now(timezone.utc)
            logging.info("-----------")
            logging.info("Performing bool mosaic process...")
            logging.info(datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
            output_bool_dir = os.path.join(output_dir, "bool_temp")
            logging.info(f"output_bool_dir is {output_bool_dir}")

            os.mkdir(output_bool_dir)

            procs_list = []
            for rasfile in os.listdir(magnitude_output_dir):
                if rasfile.endswith(".tif") and "extent" in rasfile:
                    # p = magnitude_output_dir + rasfile
                    procs_list.append([magnitude_output_dir, rasfile, output_bool_dir, hand_version])

            # Multiprocess --> create boolean inundation rasters for all hucs

            if len(procs_list) > 0:
                with Pool(processes=job_number) as pool:
                    pool.map(create_bool_rasters, procs_list)
            else:
                msg = f"Did not find any valid FIM extent rasters: {magnitude_output_dir}"
                print(msg)
                logging.info(msg)

            # Perform VRT creation and mosaic all of the huc rasters using boolean rasters
            vrt_raster_mosaic(output_bool_dir, output_dir, output_base_file_name, thread_number, precalb)

            # now cleanup the temp bool directory
            shutil.rmtree(output_bool_dir, ignore_errors=True)

            logging.info("-----------")
            logging.info("bool mosaic process complete...")
            logging.info(f"ended: {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
            logging.info(sf.calculate_duration_msg(section_start_dt))

        else:
            print("Skipping mosiaking")

    except KeyboardInterrupt:
        # Ctrl-C, just continue and shut down, no errors, warnings.
        logging.error("Keyboard Interrupt (likely Ctrl-C)")

    except Exception:
        # No need to reraise
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical("An exception has occurred")
        logging.critical(traceback.format_exc())
    finally:
        print("================================")
        logging.info("End Inundate Nation")
        logging.info(f"ended: {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(sf.calculate_duration_msg(overall_start_dt))
        print(f"Log files were saved to {log_file_path}")       
        print()


    # now cleanup the raw mosiac directories
    # comment this out if you want to see the individual huc rasters
    # shutil.rmtree(magnitude_output_dir, ignore_errors=True)


def run_inundation(hand_run_dir, huc_list, magnitude, magnitude_output_dir,
                   forecast, thread_number, job_number, precalb, log_file_path):
    """
    This script is a wrapper for the inundate function and is designed for multiprocessing.

    Args:
        args (list): [fim_run_dir (str), huc_list (list), magnitude (str),
            magnitude_output_dir (str), forecast (str), job_number (int)]

    """
    # Define file paths for use in inundate().

    final_inundation_raster = os.path.join(magnitude_output_dir, magnitude + "_inund_extent.tif")

    logging.info(
        "Running inundation wrapper for the NWM recurrence intervals for each huc using magnitude: "
        + str(magnitude)
    )
    print(
        "This will take a long time depending on the number of HUCs. Progress bar may not appear."
        " Once it gets to boolean/mosiacing (if applicable), screen output will exist. To see if the script has frozen,"
        " you should be able to watch the file system for some changes."
    )
    print()

    # =================================
    # Set up multiprocessor

    # Each log file created by each MP huc and mag pre-pended will start with the prefix
    # Each MP will add its own suffix to avoid log collisions
    # at the end of the process pool, we will aggregate the log files
    # which include this prefix.
    mp_log_prefix = f"{magnitude}_huc"
    # clear out any files that already pre-existed as mp files with this prefix.
    sf.remove_child_logs(log_file_path, mp_log_prefix)

    # try already set prior to calling run_inundation)


    # Build up the list of args
    # Note.. .do not turn this into a generator - important

    # July 2026: We no longer pass in num_workers as downstream no only uses multi-threading.
    # Some other scripts use num_workers to have their own MP before passing into produce_mosaicked_inundation
    # but others, just use high thread counts, such as this tool historically.
    # See more notes in produce_mosaicked_inundation and mosaic_inundation
    # In this case, it is processing multiple hucs at a time, so it will automatically use the
    # output_raster_path as a base name and location, appending the huc value to each one produced
    inun_arg_list = []
    for huc in huc_list:
        huc_mosaic_path = os.path.join(magnitude_output_dir, f"{magnitude}_{huc}_inund_extent.tif")
        args = {
            'hydrofabric_dir': hand_run_dir,
            'hucs': huc,
            'flow_file_path': forecast,
            'output_raster_path': huc_mosaic_path,
            # 'hydro_table_path': None  # let it pick it up from the huc level
            'verbose': False,
            'is_mosaic_for_branches': True,  # not really a good name, see produce_mosaicked_inundation for detail
            'num_threads': thread_number,
            # num_parent_workers - Used only for memory allocation management, not MT's
            'num_parent_workers': job_number,
            'precalb_option': precalb,
        }
        inun_arg_list.append(args)

    # TODO: Aug 13: Via a TON of testing through the synthesize_test_case.py, it is clear that there is a very tiny memory leak
    # still in play. I added MP here with MT downstream to emulate how synth does it which is a least close.
    # This tool previously more/less only used num threads (ish) for inundation. But that option is no longer avaialble as
    # it quickly overloads the system. The true memory leaks is somewhere in iundate_gms.py and inundate.py.
    # Now both Google Gemini and VSCode Copilot says there is no longer any true memory leaks but might be a volume issue
    # and object management. But.. simply dropping the workers does not quite seem to be enough, but more testing is required
    # to see if we can at least get by with lower thread numbers.
    # This tool is mid update and has not be tested at all with the new inundation code.

    # There continues to show evidence of memory leaks somewhere indirectly to gval and it really needs a re-evaluation
    # I have run a number of benchmark and various other tools and many point to it as well as some issues inside our
    # tools_shared_functions. It appears a ton of the functions in tools_shared_functions need upgrades and were not
    # written in the first place with memory management in place. Granted, almost all of our tools have not considered
    # best practises for memory and resource management. Now that we are needing more and more processing with more tools
    # and processing steps, that tech-debt is catching up to us.


    # Explicitly set the start method to fork
    # (Must be called before any pool is created)
    multiprocessing.set_start_method('spawn', force=True)
    has_errors = False
    with ProcessPoolExecutor(max_workers=job_number) as executor:

        # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
        executor_dict = {}

        pbar = tqdm(
            total=len(inun_arg_list),
            desc=f"Running Inundation per HUC with {job_number} workers",
        )
        try:
            for huc in huc_list:
                huc_mosaic_path = os.path.join(magnitude_output_dir, f"{magnitude}_{huc}_inund_extent.tif")
                args = {
                    'hydrofabric_dir': hand_run_dir,
                    'hucs': huc,
                    'flow_file_path': forecast,
                    'output_raster_path': huc_mosaic_path,
                    # 'hydro_table_path': None  # let it pick it up from the huc level
                    'verbose': False,
                    'is_mosaic_for_branches': True,  # not really a good name, see produce_mosaicked_inundation for detail
                    'num_threads': thread_number,
                    # num_parent_workers - Used only for memory allocation management, not MT's
                    'num_parent_workers': job_number,
                    'precalb_option': precalb,
                }
                future = executor.submit(produce_mosaicked_inundation, **args)
                executor_dict[future] = huc  # We can use the huc as a future ID

                for future in as_completed(executor_dict):

                    huc_num = executor_dict[future]
                    if future is not None:
                        if future.cancelled():  # for keyboard CTRL-C's generally
                            continue
                        if future.exception():  # Just reraise as is
                            has_errors = True
                            logging.critical(f"An exception has been returned by future: {huc_num}")
                            raise future.exception()
                        # We do not use the result at this time
                    num_successful_tests += 1
                    pbar.update(1)  # ✅ Progress update for each completed task

                    # helps release the memory faster
                    del executor_dict[future]

        except KeyboardInterrupt:
            has_errors = True
            # Ctrl-C, just continue and shut down, no errors, warnings.
            logging.error("Keyboard Interrupt (likely Ctrl-C)")
            pbar.close()  # aborts the progress bar
            executor.shutdown(wait=True, cancel_futures=True)  # yes.. need wait True for MT

        except Exception as ex:
            has_errors = True
            # this covers fails in the original call to produce_mosaicked_inundation such as
            # bad definition.
            logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
            logging.critical(f"*** Error: {ex}")
            logging.critical(traceback.format_exc())
            pbar.close()
            # Note: Even though we use the "wait" flag, most WIP processes can not be
            # aborted when using ProcessPool
            executor.shutdown(
                wait=True, cancel_futures=True
            )  # tells the ProcessPoolExecutor to stop accepting new tasks. Even cancel the running tasks as soon as possible
            # raise ex  Do not re-raise and do not sys.exit

        finally:
            # This will also merge -error.log and -warning.log files into the
            # respective parent error, warning files.
            # Granted.. putting it in "finally" will mean we get the logs a bit out of order
            # but all errors and criticals are in the logs at least twice, so look at
            # the last error messages and it will have context
            print("++++++++++++")
            logging.info(f"Merging child log files into parent logs. {log_file_path} - {mp_log_prefix}")
            print("This can take a bit, hang in there.")
            sf.merge_child_logs_into_parent_log(log_file_path, mp_log_prefix)
            
            # but it will be close enough to the actual progress.
    return has_errors  



def create_bool_rasters(args):
    in_raster_dir = args[0]
    rasfile = args[1]
    output_bool_dir = args[2]
    fim_version = args[3]

    print("Calculating boolean inundate raster: " + rasfile)
    p = in_raster_dir + os.sep + rasfile

    # TODO: Aug 2026: 
    # This whole section needs major memory and object/resource management upgrades. Lots of precendences
    # and examples in the synthesize_test_case and downstream inundation family of files.
    with rasterio.open(p) as raster:
        profile = raster.profile
        array = raster.read()

    array[array > 0] = 1
    array[array <= 0] = 0
    # And then change the band count to 1, set the
    # dtype to uint8, and specify LZW compression.
    profile.update(
        driver="GTiff",
        height=array.shape[1],
        width=array.shape[2],
        count=1,
        tiled=True,
        nodata=0,
        blockxsize=512,
        blockysize=512,
        dtype="uint8",
        compress="deflate",  # Switched to Deflate for better compression on large binary masks
        zlevel=9,  # Maximum compression level
        predictor=2,  # Horizontal differencing (great for 0/1 data)
        BIGTIFF="YES",
    )
    with rasterio.open(
        output_bool_dir + os.sep + rasfile[:-4] + '_' + fim_version + '.tif', "w", **profile
    ) as dst:
        dst.write(array.astype(rasterio.uint8))


def vrt_raster_mosaic(output_bool_dir, output_dir, fim_version_tag, threads, precalb):

    # TODO: Aug 13, 2026: Lots of review and update requirements needed here for memory and object/resource management
    crs_groups = defaultdict(list)

    # Group rasters by CRS
    for rasfile in os.listdir(output_bool_dir):
        if rasfile.endswith('.tif') and "extent" in rasfile:
            path = os.path.join(output_bool_dir, rasfile)
            try:
                with rasterio.open(path) as src:
                    crs = src.crs
                    if crs:
                        crs_groups[crs.to_epsg()].append(path)
                    else:
                        logging.warning(f"Raster has no CRS: {path}")
            except Exception as e:
                logging.warning(f"Could not read raster {path}: {e}")

    # Build VRTs and mosaics for each group
    for epsg_code, raster_list in crs_groups.items():
        if precalb:
            output_mosaic_name = f"{fim_version_tag}_EPSG{epsg_code}_precalb_mosaic.tif"
        else:
            output_mosaic_name = f"{fim_version_tag}_EPSG{epsg_code}_mosaic.tif"
        output_mosaic_raster = os.path.join(output_dir, output_mosaic_name)

        # Define COG creation options for the FINAL mosaic
        creation_options = {
            "driver": "GTiff",
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "compress": "deflate",
            "predictor": 2,
            "zlevel": 6,  # level 9 is too slow for a 200GB mosaic; 6 is the sweet spot
            "BIGTIFF": "YES",  # ABSOLUTELY REQUIRED for 200GB
        }

        if len(raster_list) == 1:
            # Just copy the raster
            logging.info(f"Only one raster found for EPSG:{epsg_code}, skipping VRT creation.")
            shutil.copyfile(raster_list[0], output_mosaic_raster)
            logging.info(f"Copied {raster_list[0]} to {output_mosaic_raster}")
        else:
            output_mosaic_vrt = os.path.join(output_bool_dir, f"{fim_version_tag}_EPSG{epsg_code}_merged.vrt")
            logging.info(f"Building VRT: {output_mosaic_vrt}")
            vrt_file = build_vrt(output_mosaic_vrt, raster_list)

            logging.info(f"Building raster mosaic: {output_mosaic_raster}")
            logging.info(f"Using {threads} threads for parallelizing")
            # Use rasterio.shutil.copy to apply the COG profile during the merge
            rasterio.shutil.copy(vrt_file, output_mosaic_raster, **creation_options)

            # build overviews on the final mosaic file
            logging.info("Building overviews for the final mosaic...")
            with rasterio.open(output_mosaic_raster, "r+") as dst:
                factors = [2, 4, 8, 16, 32, 64, 128]
                dst.build_overviews(factors, Resampling.nearest)
                dst.update_tags(ns='rio_overview', resampling='nearest')

            logging.info(f"Mosaic for EPSG:{epsg_code} completed and saved to {output_mosaic_raster}")
            vrt_file = None



if __name__ == "__main__":
    """
    Sample usage:
    python3 /foss_fim/tools/inundate_nation.py
        -r /outputs/fim_4_0_9_2 -m 100_0
        -f /data/inputs/rating_curve/nwm_recur_flows/nwm3_17C_recurr_10_0_cms.csv
        -s
        -j 10
        -t 8
    outputs become /data/inundation_review/inundate_nation/100_0_fim_4_0_9_2_mosiac.tif (.log, etc)

    python3 /foss_fim/tools/inundate_nation.py
        -r /outputs/fim_4_0_9_2
        -m hw
        -f /data/inputs/rating_curve/bankfull_flows/nwm3_high_water_threshold_cms.csv
        -s
        -j 10
        -t 8
    outputs become /data/inundation_review/inundate_nation/hw_fim_4_0_9_2_mosiac.tif (.log, etc)
    """

    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Inundation mapping for FOSS FIM using streamflow '
        'recurrence interflow data. Inundation outputs are stored in the '
        '/inundation_review/inundation_nwm_recurr/ directory.'
    )

    parser.add_argument(
        '-r',
        '--hand-run-dir',
        help='Name of directory containing outputs '
        'of fim_pipeline.sh (e.g. data/ouputs/dev_abc/12345678_dev_test)',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output-dir',
        help='Optional: The path to a directory to write the '
        'outputs. If not used, the inundation_nation directory is used by default '
        'ie) /data/inundation_review/inundate_nation/',
        default='/data/inundation_review/inundate_nation/',
        required=False,
    )

    parser.add_argument(
        '-m',
        '--magnitude_key',
        help='used in output folders names and temp files, '
        'added to output_file_name_key ie 100_0, 2_0, hw, etc)',
        required=True,
    )

    parser.add_argument(
        '-f',
        '--flow_file',
        help='the path and flow file to be used. '
        'ie /data/inputs/rating_curve/bankfull_flows/'
        'nwm3_high_water_threshold_cms.csv',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--huc-list',
        help='OPTIONAL: HUC list to run specified HUC(s).Specifiy multiple hucs single space delimited'
        '--> 12090301 12090302. Default (no huc list provided) will use hucs found in -r directory',
        required=False,
        default='all',
        nargs='+',
    )

    parser.add_argument(
        '-s',
        '--inc_mosaic',
        help='Optional flag to produce mosaic of FIM extent rasters',
        action='store_true',
    )

    parser.add_argument(
        '-p',
        '--precalb',
        help='Optional flag to use the pre-calibrated discharge from the SRCs',
        action='store_true',
        required=False,
        default=False,
    )

    parser.add_argument('-j', '--job-number', help='The number of jobs', required=False, default=1, type=int)

    parser.add_argument(
        '-t', '--thread-number', help='The number of threads', required=False, default=1, type=int
    )

    args = vars(parser.parse_args())

    inundate_nation(**args)
