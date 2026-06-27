#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import os
import re
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from multiprocessing import Pool

import pandas as pd
from run_test_case import Test_Case
from tools_shared_variables import (
    AHPS_BENCHMARK_CATEGORIES,
    MAGNITUDE_DICT,
    OUTPUTS_DIR,
    PREVIOUS_FIM_DIR,
    TEST_CASES_DIR,
)
from tqdm import tqdm

import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


# NOTE: Jun 2026: Now that we are fully using prev_metrics_csv
# many args are no longer relavent.
def synthesize_test_cases(
    config_type,
    precalb_option,
    hand_version,
    job_number_alpha_tests,  # combo of huc + benchmark type
    job_number_branch,
    benchmark_category,
    overwrite,
    master_metrics_csv,
    verbose,
    prev_metrics_csv,
):

    # Note: debug value of True, means when using logging.debug, it will go to the log files only

    # TODO: Jun 2026: We likely want to change this to accept a output path
    # versus calc it. There are pros/cons, mostly based in enforcement of "PREV" versus "DEV"
    # including test cases folder pathing
    # for now, lets just calc the hand output folder to put these logs in it.

    # =====================
    # Validation
    if hand_version == "":
        raise ValueError('hand version (-v) can not be empty')

    # Define whether or not to archive metrics in "official_versions" or "testing_versions" for each test_id.
    # and also setup logging
    log_folder = ""
    hand_path = ""
    if config_type == 'PREV':
        hand_path = os.path.join(PREVIOUS_FIM_DIR, hand_version)
        archive_results = True
    elif config_type == 'DEV':
        hand_path = os.path.join(OUTPUTS_DIR, hand_version)
        archive_results = False
    else:
        raise ValueError('Config (-c) option incorrectly set. Use "DEV" or "PREV"')

    if not os.path.exists(hand_path):
        raise ValueError(f"Calculated hand path of {hand_path} does not exist")
    logging.info(f"Source hand dataset is {hand_path}")

    if master_metrics_csv == "":
        raise ValueError("master metric path (-m) can not be empty")

    ___, ext = os.path.splitext(os.path.basename(master_metrics_csv))
    if ext.lower() != ".csv":
        raise ValueError("master metric path (-m) must end in .csv")

    if prev_metrics_csv == "":
        raise ValueError("previous metric file (-pcsv) can not be empty")

    if not os.path.exists(prev_metrics_csv):
        raise ValueError("previous metric file (-pcsv) does not exist")

    # =====================
    # Setup Logging and headers
    log_folder = os.path.join(hand_path, "logs", "alpha_logs")
    log_file_path = sf.setup_file_logger(log_folder, "synthesize_test_cases")

    print("================================")
    logging.info(f"Start synthesize test cases : {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
    overall_start_dt = datetime.now(timezone.utc)

    logging.info("***************************************************")
    logging.info(
        "***** Note about log files: Some warnings and errors will show up multiple times, and"
        " not necessarily in order, but last copy of a set of error messages will show find context info."
    )
    logging.info("***************************************************")
    logging.debug(f"inputs and locals = {locals()}")
    logging.debug("***************************************************")
    print("")

    # check job numbers
    # Jun 2026: Now that we have threading, we can have combo of huc/bench * branches that is much higher
    # as branches are used for multi-theading which has much higher capacity
    # But we still don't want it to go crazy
    total_cpus_requested = job_number_alpha_tests * job_number_branch
    total_cpus_available = os.cpu_count() - 1
    if total_cpus_requested > total_cpus_available:
        msg = f"\nIMPORTANT WARNING:\n   You have set the -ja (job_number_alpha_tests) at {job_number_alpha_tests}"
        f"\n   and the -jb (job_number_branch) at {job_number_branch}."
        f"\n   Multiplying the two gives you {total_cpus_requested} which is acceptable within reason."
        f"\n   Your machine has {total_cpus_available} available."
        "\n   While it is perfectly acceptable to go higher then the max cpus, due to the use of multi-threading,"
        "\n   very high values can risk overloading the server. The system can only go as fast as lowest of the"
        "\n   CPU/Memory/Network speeds. If you see long spikes in one of those areas, consider lowering your"
        "\n   your job numbers."
        "\n\n    Hit CTRL-C to abort."
        print(msg)
        # give them a few seconds to read it.
        time.sleep(5)  # gives the a min to read this.
        print("")

    try:
        # remove the old one
        if os.path.exists(master_metrics_csv):
            os.remove(master_metrics_csv)

        # =================================
        # Find valid test classes
        # Create a list of all test_cases for which we have validation data
        all_test_cases = Test_Case.list_all_test_cases(
            hand_version=hand_version,
            archive=archive_results,
            benchmark_categories=[] if benchmark_category == "all" else [benchmark_category],
        )

        if len(all_test_cases) == 0:
            raise Exception("Error: all_test_cases is empty and should not be")

        # looks in the hand dir for each huc that has the hydrotable file which means
        # the huc was processed by pipeline successfully.
        applicable_hand_huc_test_cases = [x for x in all_test_cases if x.is_valid_hand_huc]
        if len(applicable_hand_huc_test_cases) == 0:
            raise Exception(
                "Error: After filtering HUC folder looking for a hydrotable file"
                " which are assumed to be a valid HUC folder, there are no remaining valid HUC folders"
            )

        # Sort by huc (ascending)
        applicable_hand_huc_test_cases = sorted(applicable_hand_huc_test_cases, key=lambda t_case: t_case.huc)

        huc_list = [test_class_obj.huc for test_class_obj in applicable_hand_huc_test_cases]
        # drop dups
        huc_list = list(set(huc_list))

        logging.info(
            f"Processing alpha test cases for {len(applicable_hand_huc_test_cases)}"
            " records (Each alpha test is a huc/benchmark combo)"
        )

        # logging.debug(f"Processing hucs: {huc_list}")
        # =================================
        # Set up multiprocessor
        mp_log_prefix = "alpha_test"
        # clear out any files that already pre-existed as mp files with this prefix.
        sf.remove_child_logs(log_file_path, mp_log_prefix)

        # Each log file created by each MP alpha test will start with the prefix
        # alpha_test. Each MP will add its own suffix to avoid log collisions
        # at the end of the process pool, we will aggregate the log files
        # which include this prefix

        # By default, maxtasksperchild is set to None, meaning worker processes live as long as the process pool itself
        # If a memory leaks exist, it can overload the system

        num_successful_tests = 0
        with ProcessPoolExecutor(max_workers=job_number_alpha_tests) as executor:
            # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
            executor_dict = {}

            pbar = tqdm(
                total=len(applicable_hand_huc_test_cases),
                desc=f"Running alpha test cases with {job_number_alpha_tests} workers",
            )
            try:

                for test_case_class in applicable_hand_huc_test_cases:

                    # logging.info(f"test_case_class.test_id is {test_case_class.test_id}")
                    alpha_test_args = {
                        'overwrite': overwrite,
                        'verbose': verbose,
                        'branch_workers': job_number_branch,
                        'precalb_option': precalb_option,
                        'log_folder': log_folder,
                        'log_prefix': mp_log_prefix,
                    }

                    future = executor.submit(test_case_class.alpha_test, **alpha_test_args)
                    executor_dict[future] = test_case_class.test_id

                # Any one alpha_test class can fail in multiple ways. The original defination
                # call to test_case_class.alpha_test can fail which is covered by the try/except
                # and inside the running of test_case_class.alpha_test can also fail.
                # it may let the try catch come out or capture it itself. So.. as_complete
                # can get a future back that is future.exception()
                # By catching it better, we can shut down the pool if we need to
                # Remember.. you can't really stop each WIP child proc, but you can
                # catch the errors and stop new processes from starting up.

                for future in as_completed(executor_dict):

                    # for future in tqdm(
                    #     as_completed(executor_dict), total=len(executor_dict),
                    #     desc=f"Running alpha test cases with {job_number_alpha_tests} workers"
                    # ):
                    if future is not None:
                        if future.cancelled():  # for keyboard CTRL-C's generally
                            continue
                        if future.exception():  # Just reraise as is
                            raise future.exception()
                        # We do not use the result at this time
                    num_successful_tests += 1
                    pbar.update(1)  # ✅ Progress update for each completed task

            except Exception as ex:
                # this covers fails in the original call to test_case_class.alpha_test such as
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
                logging.debug(f"Merging child log files into parent logs. {log_file_path} - {mp_log_prefix}")
                sf.merge_child_logs_into_parent_log(log_file_path, mp_log_prefix)

        if num_successful_tests == 0:
            logging.warning("Skipping creating metrics file as there was not successful alpha tests")
        else:
            # Do aggregate_metrics.
            logging.info("Creating master metrics CSV...")
            create_master_metrics_csv(
                master_metrics_csv_output=master_metrics_csv,
                config_type=config_type,
                prev_metrics_csv=prev_metrics_csv,
                hand_version=hand_version,
                huc_list=huc_list,
            )
    except Exception:
        # No need to reraise
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")        
        logging.critical("An exception has occurred")
        logging.critical(traceback.format_exc())
    finally:
        print("================================")
        logging.info("End synthesize test cases")
        print(f"Log files were saved to {log_file_path}")

        logging.info(f"ended: {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(sf.calculate_duration_msg(overall_start_dt))
        print()


def create_master_metrics_csv(
    master_metrics_csv_output, config_type, prev_metrics_csv, hand_version, huc_list
):
    """
    This function searches for and collates metrics from the current hand_version and concats
    it to the prev_metrics_csv file

    """

    # Construct header
    metrics_to_write = [
        'true_negatives_count',
        'false_negatives_count',
        'true_positives_count',
        'false_positives_count',
        'contingency_tot_count',
        'cell_area_m2',
        'TP_area_km2',
        'FP_area_km2',
        'TN_area_km2',
        'FN_area_km2',
        'contingency_tot_area_km2',
        'predPositive_area_km2',
        'predNegative_area_km2',
        'obsPositive_area_km2',
        'obsNegative_area_km2',
        'positiveDiff_area_km2',
        'CSI',
        'FAR',
        'TPR',
        'TNR',
        'PND',
        'PPV',
        'NPV',
        'ACC',
        'Bal_ACC',
        'MCC',
        'EQUITABLE_THREAT_SCORE',
        'PREVALENCE',
        'BIAS',
        'F1_SCORE',
        'TP_perc',
        'FP_perc',
        'TN_perc',
        'FN_perc',
        'predPositive_perc',
        'predNegative_perc',
        'obsPositive_perc',
        'obsNegative_perc',
        'positiveDiff_perc',
        'masked_count',
        'masked_perc',
        'masked_area_km2',
    ]

    # Create table header
    additional_header_info_prefix = ['version', 'nws_lid', 'magnitude', 'huc']
    list_to_write = [
        additional_header_info_prefix
        + metrics_to_write
        + ['full_json_path']
        + ['flow']
        + ['benchmark_source']
        + ['extent_config']
    ]

    # logging.debug(f"Calculating metrics for huc list {huc_list}")

    # Note: Jun 2026:
    # the arg calibrated has had no logic value for a long time. It only was used to determin if eval_metadata.json
    # files are created, but those no longer have value with dropping FIM 3. This value will be directly entered
    # in the metrics file as before.

    # Iterate through 5 benchmark sources
    new_data_found = False
    for benchmark_source in ['ble', 'nws', 'usgs', 'ifc', 'ras2fim']:
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        if not os.path.exists(benchmark_test_case_dir):
            continue

        test_cases_folders = [d for d in os.listdir(benchmark_test_case_dir) if re.match(r'\d{8}_\w{3,7}', d)]
        test_cases_folders.sort()

        logging.debug(f"Processing metrics for benchmark source: {benchmark_test_case_dir}")

        if benchmark_source in ['ble', 'ifc', 'ras2fim']:
            magnitude_list = MAGNITUDE_DICT[benchmark_source]

            test_cases_folders.sort()
            # Iterate through available test cases
            for test_case_folder in test_cases_folders:
                try:
                    # Get HUC id
                    # int(each_test_case.split('_')[0])
                    huc = test_case_folder.split('_')[0]

                    if huc not in huc_list:  # No sense processing hucs that do not have that benchmark type
                        continue

                    # Update filepaths based on whether the official or dev versions should be included

                    if config_type == "PREV":
                        version_to_crawl = os.path.join(
                            benchmark_test_case_dir, test_case_folder, 'official_versions'
                        )
                        # versions_to_aggregate = prev_versions_to_include_list
                    else:
                        version_to_crawl = os.path.join(
                            benchmark_test_case_dir, test_case_folder, 'testing_versions'
                        )
                    version_to_crawl = os.path.join(version_to_crawl, hand_version)
                    # versions_to_aggregate = versions_to_include_list

                    logging.debug(f"Processing {version_to_crawl}")

                    # Pull version info from filepath
                    for magnitude in magnitude_list:

                        # TODO: May 29, 2026: We really don't need the "COMP" and "c" test anymore
                        extent_config = 'COMP'
                        magnitude_dir = os.path.join(version_to_crawl, magnitude)

                        # Add metrics from file to metrics table ('list_to_write')
                        if os.path.exists(magnitude_dir):
                            magnitude_dir_list = os.listdir(magnitude_dir)
                            for f in magnitude_dir_list:
                                if '.json' in f:
                                    flow = 'NA'
                                    nws_lid = ""
                                    sub_list_to_append = [hand_version, nws_lid, magnitude, huc]
                                    full_json_path = os.path.join(magnitude_dir, f)
                                    if os.path.exists(full_json_path):
                                        stats_dict = json.load(open(full_json_path))
                                        for metric in metrics_to_write:
                                            sub_list_to_append.append(stats_dict[metric])
                                        sub_list_to_append.append(full_json_path)
                                        sub_list_to_append.append(flow)
                                        sub_list_to_append.append(benchmark_source)
                                        sub_list_to_append.append(extent_config)
                                        new_data_found = True
                                        list_to_write.append(sub_list_to_append)
                except ValueError as ve:
                    # TODO: Is this really an error? it was just a pass. .lets see what we have
                    # Can we even get a valueerror?
                    logging.error(f"value error issued: {ve}")
                    logging.error(traceback.format_exc())
                    pass  # TODO: ??
                    # really? when we are missing a test if it is acceptable to catch a value error and continue

        # Iterate through AHPS benchmark data
        if benchmark_source in AHPS_BENCHMARK_CATEGORIES:  # nws, usgs
            test_cases_folders = os.listdir(benchmark_test_case_dir)
            # logging.debug(f"Start of reviewing benchmark data for AHPS Categories: {benchmark_source}")

            test_cases_folders.sort()
            for test_case_folder in test_cases_folders:
                try:
                    # Get HUC id
                    # int(each_test_case.split('_')[0])  # what this some sort of test to validate the test case?
                    huc = test_case_folder.split('_')[0]

                    if huc not in huc_list:  # No sense processing hucs that do not have that benchmark type
                        continue

                    version_to_crawl = os.path.join(benchmark_test_case_dir, test_case_folder)
                    if config_type == "PREV":
                        version_to_crawl = os.path.join(version_to_crawl, 'official_versions')
                    else:
                        version_to_crawl = os.path.join(version_to_crawl, 'testing_versions')
                    version_to_crawl = os.path.join(version_to_crawl, hand_version)

                    logging.debug(f"Processing {version_to_crawl}")

                    # Pull model info from filepath
                    for magnitude in ['action', 'minor', 'moderate', 'major']:

                        # TODO: May 29, 2026: We really don't need the "COMP" and "c" test anymore
                        extent_config = 'COMP'

                        # version_dir = os.path.join(versions_to_crawl, version)
                        magnitude_dir = os.path.join(version_to_crawl, magnitude)

                        if os.path.exists(magnitude_dir):
                            magnitude_dir_list = os.listdir(magnitude_dir)
                            for f in magnitude_dir_list:
                                if '.json' in f and 'total_area' not in f:
                                    nws_lid = f[:5]
                                    sub_list_to_append = [hand_version, nws_lid, magnitude, huc]
                                    full_json_path = os.path.join(magnitude_dir, f)
                                    flow = ''
                                    if os.path.exists(full_json_path):
                                        # Get flow used to map
                                        flow_file = os.path.join(
                                            benchmark_test_case_dir,
                                            'validation_data_' + benchmark_source,
                                            huc,
                                            nws_lid,
                                            magnitude,
                                            'ahps_'
                                            + nws_lid
                                            + '_huc_'
                                            + huc
                                            + '_flows_'
                                            + magnitude
                                            + '.csv',
                                        )
                                        if os.path.exists(flow_file):
                                            with open(flow_file, newline='') as csv_file:
                                                reader = csv.reader(csv_file)
                                                next(reader)
                                                for row in reader:
                                                    flow = row[1]

                                        # Add metrics from file to metrics table ('list_to_write')
                                        stats_dict = json.load(open(full_json_path))
                                        for metric in metrics_to_write:
                                            sub_list_to_append.append(stats_dict[metric])
                                        sub_list_to_append.append(full_json_path)
                                        sub_list_to_append.append(flow)
                                        sub_list_to_append.append(benchmark_source)
                                        sub_list_to_append.append(extent_config)
                                        new_data_found = True
                                        # logging.debug(
                                        #     f"list_to_write for {full_json_path} is {list_to_write}"
                                        # )

                                        list_to_write.append(sub_list_to_append)
                except ValueError as ve:
                    # TODO: Is this really an error? it was just a pass. .lets see what we have
                    logging.error(f"value error issued: {ve}")
                    logging.error(traceback.format_exc())
                    pass  # TODO: ??
                    # really? when we are missing a test if it is acceptable to catch a value error and continue

    print("")
    # If previous metrics are provided: read in previously compiled metrics and join to calcaulated metrics
    if not new_data_found:
        logging.warning(
            "****** There are no new metrics data available. Check arguments or log files for errors"
        )
    else:
        prev_metrics_df = pd.read_csv(prev_metrics_csv)

        # Put calculated metrics into a dataframe and set the headers
        df_to_write_calc = pd.DataFrame(list_to_write)
        df_to_write_calc.columns = df_to_write_calc.iloc[0]
        df_to_write_calc = df_to_write_calc[1:]

        # Join the calculated metrics and the previous metrics dataframe
        df_to_write = pd.concat([df_to_write_calc, prev_metrics_df], axis=0)

        # Save aggregated compiled metrics ('df_to_write') as a CSV
        # create the path if it does not already exist
        metrics_file_path, __ = os.path.split(master_metrics_csv_output)
        if not os.path.exists(metrics_file_path):
            os.makedirs(metrics_file_path, exist_ok=True)
        logging.info(f"Writing metrics file to {master_metrics_csv_output}")
        df_to_write.to_csv(master_metrics_csv_output, index=False)


if __name__ == '__main__':
    # Sample usage:
    '''

    python /foss_fim/tools/synthesize_test_cases.py
        -c DEV
        -v hand_4_9_13_0
        -ja 30 -jb 2
        -m /outputs/gms_test_synth_combined/gms_synth_metrics.csv
        -psv /data/previous_fim/hand_4_9_11_1
        -o

     Notes:
       - fim_input.csv MUST be in the folder suggested (-v)
       - the -v param is the name in the folder in the "outputs/" directory where the test hucs are at.
         It also becomes the folder names inside the test_case folders when done.
       - The -m can be any path and any name.

     To see your outputs in the test_case folder (hard coded path), you can check for outputs using
         (cd .... to your test_case folder), then command becomes  find . -name rob_test_* -type d (Notice the
         the -name can be a wildcard for your -v param (or the whole -v value))
     If you want to delete the test outputs, test the outputs as suggest immediately above, but this time your
         command becomes:  find . -name rob_test_* -type d  -exec rm -rdf {} +
    '''

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument(
        '-c',
        '--config-type',
        help='REQUIRED: Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',
        required=True,
        default='DEV',
    )
    parser.add_argument(
        '-p',
        '--precalb-option',
        help='Using this argument will use the precalb_discharge_cms in hydrotable. ',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-v',
        '--hand-version',
        help='REQUIRED: Name of hand version. It is the name of the pipeline folder.'
        'ie) for PREV it is hand_4_9_12_0 as in /data/previous_fim/hand_4_9_12_0; or for DEV /outputs/test_alpha_hand_data.'
        ' For PREV, the data must be the root folder of /data/previous_fim and for DEV it must be in /outputs.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-ja',
        '--job-number-alpha-tests',
        help='This number is used to manage how many jobs for huc + benchmark type can be processed at one time.'
        'Number of processes to use for HUC scale operations. Number of Alpha jobs and Batch job numbers should'
        ' multiply to no more than one less than the CPU count of the machine.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-jb',
        '--job-number-branch',
        help='Number of processes to use for processing branches for each huc/benchmark type.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-b',
        '--benchmark-category',
        help='A benchmark category to specify. Defaults to process all categories.',
        required=False,
        default="all",
    )
    parser.add_argument(
        '-o',
        '--overwrite',
        help='Overwrite all metrics or only fill in missing metrics.',
        required=False,
        action="store_true",
    )
    parser.add_argument(
        '-m',
        '--master-metrics-csv',
        help='REQUIRED: Define path for output master metrics CSV file.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-vr', '--verbose', help='Verbose output', required=False, default=False, action='store_true'
    )
    parser.add_argument(
        '-pcsv',
        '--prev-metrics-csv',
        help='REQUIRED: : Filepath for a CSV with previous metrics to concatenate with new '
        'metrics to form a final aggregated metrics csv.',
        required=True,
        default="",
    )
    # Assign variables from arguments.
    args = vars(parser.parse_args())

    synthesize_test_cases(**args)
