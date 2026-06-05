#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

import src.utils.shared_functions as sf
from run_test_case import Test_Case
from src.utils.shared_functions import FIM_Helpers as fh
from tools_shared_variables import (
     AHPS_BENCHMARK_CATEGORIES,
     MAGNITUDE_DICT,
     PREVIOUS_FIM_DIR,
     TEST_CASES_DIR,
     OUTPUTS_DIR)


def synthesize_test_cases(config,
                          calibrated,
                          precalb_option,
                          fim_version,
                          job_number_huc,
                          job_number_branch,
                          thread_number_branch,
                          benchmark_category,
                          overwrite,
                          dev_versions_to_compare,
                          master_metrics_csv,
                          master_metrics_only,
                          verbose,
                          prev_metrics_csv,
                          cycle_previous_files
):

    # TODO: Jun 2026: We likely want to change this to accept a output path
    # versus calc it. There are pros/cons, mostly based in enforcement of "PREV" versus "DEV"
    # including test cases folder pathing
    # for now, lets just calc the hand output folder to put these logs in it.

    # Define whether or not to archive metrics in "official_versions" or "testing_versions" for each test_id.
    # and also setup logging
    log_folder=""
    if config == 'PREV':
        archive_results = True
        log_folder = os.path.join(PREVIOUS_FIM_DIR, fim_version, "alpha_logs")
    elif config == 'DEV':
        archive_results = False
        log_folder = os.path.join(OUTPUTS_DIR, fim_version, "alpha_logs")
    else:
        print('Config (-c) option incorrectly set. Use "DEV" or "PREV"')

    if os.path.isdir(log_folder):
        shutil.rmtree(log_folder)

    # NOTE: Careful with this logger as you dont' want to use the same logger
    # inside MP processes as they will collide writing to the same log file.
    # Best to let each MP have its own logging object, then concat all at the end.
    # NOTE: logger does screen and log file
    log_file_path = sf.setup_file_logger(log_folder, "synthesize_test_cases")

    print("================================")
    logging.info("Start synthesize test cases")
    overall_start_dt = datetime.now(timezone.utc)
    logging.info(f"started: {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
    print()

    # Warn about the MP job huc to the MT (multi-thread) job number.
    # While inconsistent, it does pop up a fair bit. It is likely the usage
    # of those two values in relation to how inundation is using those values.
    # TODO: Research required.
    print("--------------------------")
    print(
        "Warning: If you see errors in the output of this tool with the phrase of BrokenProcessPool,"
        " lower the -jh (huc job number) to a lower value. You can adjust the -tb (branch job number)"
        " to offset the new -jh value."
    )
    print("This is a known code issue that will be fixed in a future release.")
    print("--------------------------")

    # check job numbers
    total_cpus_requested = job_number_huc * job_number_branch
    total_cpus_available = os.cpu_count() - 2
    if total_cpus_requested > total_cpus_available:
        logging.error("Error: CPU count invalid")
        raise ValueError(
            f'The HUC job number of {job_number_huc} (-jh)'
            f' multiplied by the branch job number of {job_number_branch} (-th),'
            f' exceeds your machine\'s available CPU count of {total_cpus_available} ({os.cpu_count()} - 2)\n'
            'Please lower the job_number_huc or job_number_branch to create a multipled value that is less'
            f' than {total_cpus_available} for this tool for this server.'
        )

    # Default to processing all possible versions in PREVIOUS_FIM_DIR.
    # Otherwise, process only the user-supplied version.
    prev_versions_to_include_list = []
    dev_versions_to_include_list = []
    if fim_version != "all" and cycle_previous_files is False:
        if config == 'PREV':  # official fim model results
            prev_versions_to_include_list = [fim_version]
        elif config == 'DEV':  # development fim model results
            dev_versions_to_include_list = [fim_version]
    else:
        prev_versions_to_include_list = os.listdir(PREVIOUS_FIM_DIR)
        if config == 'DEV':  # development fim model results
            dev_versions_to_include_list = [fim_version]

    try:
        # Create a list of all test_cases for which we have validation data
        all_test_cases = Test_Case.list_all_test_cases(
            version=fim_version,
            archive=archive_results,
            benchmark_categories=[] if benchmark_category == "all" else [benchmark_category],
        )

        # =================================
        # Validate data
        # logging.info('all test cases', all_test_cases)
        # Make sure cycle-previous-files and a previous metric CSV have not been concurrently selected
        if prev_metrics_csv is not None and cycle_previous_files is True:
            logging.critical(
                "Error: Cycle previous files and previous metric CSV functionality cannot be used concurrently."
            )
            sys.exit(1)

        # Check whether a previous metrics CSV has been provided and, if so, make sure the CSV exists
        if prev_metrics_csv is not None:
            if not os.path.exists(prev_metrics_csv):
                logging.critical(f"Error: File does not exist at {prev_metrics_csv}")
                sys.exit(1)
            else:
                logging.info(f"Metrics will be combined with previous metric CSV: {prev_metrics_csv}")
                print()
        else:
            logging.info("ALERT: A previous metric CSV has not been provided (-pcsv) - this is optional.")
            print()

        if len(all_test_cases) == 0:
            raise Exception("Error: all_test_cases is empty and should not be")

        if master_metrics_csv == "":
            raise ValueError("master metric path (-m) can not be empty")

        master_metrics_folder_path, ext = os.path.splitext(os.path.basename(master_metrics_csv))
        if ext.lower() != ".csv":
            raise ValueError("master metric path (-m) must end in .csv")

        logging.info(f"output master metrics file will be saved at {master_metrics_csv}")

        # Print whether the previous files will be cycled through
        if cycle_previous_files is True:
            logging.info("ALERT: Metrics from previous directories will be compiled.")
        else:
            logging.info(
                "ALERT: Metrics from previous directories will NOT be compiled (-pfiles not provided) \n"
                "   - pfiles is optional -"
            )
        print()

        # =================================
        # Set up multiprocessor
        if not master_metrics_only:

            # Each log file lcreated by each MP alpha test will start with the prefix
            # alpha_test. Each MP will add its own suffix to avoid log collisions
            # at the end of the process pool, we will aggregate the log files
            # which include this prefix
            mp_log_prefix="alpha_test"
            has_error = False
            # By default, maxtasksperchild is set to None, meaning worker processes live as long as the process pool itself
            # If a memory leaks exist, it can overload the system
            with ProcessPoolExecutor(max_workers=job_number_huc, max_tasks_per_child=job_number_huc) as executor:
                # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
                executor_dict = {}

                for test_case_class in all_test_cases:

                    if not os.path.exists(test_case_class.fim_dir):
                        continue

                    # fh.vprint(f"test_case_class.test_id is {test_case_class.test_id}", verbose)
                    # logging.info(f"test_case_class.test_id is {test_case_class.test_id}")

                    alpha_test_args = {
                        'calibrated': calibrated,
    #                    'model': model,
                        'mask_type': 'huc',
                        'overwrite': overwrite,
                        # 'verbose': gms_verbose if model == 'GMS' else verbose,
                        'verbose': verbose,
                        'branch_workers': job_number_branch,
                        'precalb_option': precalb_option,
                        'threads': thread_number_branch,
                        'log_folder': log_folder,
                        'log_prefix': mp_log_prefix
                    }

                    try:
                        future = executor.submit(test_case_class.alpha_test, **alpha_test_args)
                        executor_dict[future] = test_case_class.test_id

                        # TODO: May 2026: we also should catch the as_complete and look for exceptions
                        # as there are different types of exceptions. Some from the runtime child code execution
                        # and sometimes from errors in the code itself.
                        # see shared_functions.run_by_mp for examples of how to upgrade this
                        # or possibly even replace this with the run_by_mp code.
                    except Exception as ex:
                        has_error = True
                        logging.critical(f"*** Error: {ex}")
                        logging.critica(traceback.print_exc())
                        executor.shutdown(
                            wait=False, cancel_futures=True
                        )  # tells the ProcessPoolExecutor to stop accepting new tasks. Even cancel the running tasks as soon as possible
                        # sys.exit(1) # sys.exit does not work inside an MP. You have to rethrow after shutting down the executor
                        # there will be a delay in shutting it down though as it does not auto kill all wip workers, just 
                        # stops new ones.
                        raise ex

                # Send the executor to the progress bar and wait for all MS tasks to finish
                # TQDM has been found to keep process MP and thread open sometimse and not let them
                # shut down correctl. This is related to subprocesses of TQDM inside the process pool
                # Best to just skip callign it if an catestropic error has occurred.
                if not has_error:
                    progress_bar_handler(
                        executor_dict, True, f"Running alpha test cases with {job_number_huc} workers"
                    )
                # wait(executor_dict.keys())

        # This will also merge -error.log and -warning.log files into the
        # respective parent error, warning files.
        sf.merge_child_logs_into_parent_log(log_file_path, mp_log_prefix)

        '''
        if not master_metrics_only:
            if model == 'MS' and fr_run_dir:
                # Rebuild all test cases list with the FR version, loop through them and apply the alpha test
                all_test_cases = Test_Case.list_all_test_cases(
                    version=fr_run_dir,
                    archive=archive_results,
                    benchmark_categories=[] if benchmark_category == "all" else [benchmark_category],
                )

                with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
                    executor_dict = {}
                    for test_case_class in all_test_cases:
                        if not os.path.exists(test_case_class.fim_dir):
                            continue
                        alpha_test_args = {
                            'calibrated': calibrated,
                            'model': model,
                            'mask_type': 'huc',
                            'verbose': verbose,
                            'overwrite': overwrite,
                            'precalb_option': precalb_option,
                        }
                        try:
                            future = executor.submit(test_case_class.alpha_test, **alpha_test_args)
                            executor_dict[future] = test_case_class.test_id
                        except Exception as ex:
                            print(f"*** {ex}")
                            traceback.print_exc()
                            sys.exit(1)

                    # Send the executor to the progress bar and wait for all FR tasks to finish
                    progress_bar_handler(
                        executor_dict, True, f"Running FR test cases with {job_number_huc} workers"
                    )
                    # wait(executor_dict.keys())

                # Loop through FR test cases, build composite arguments, and
                #   submit the composite method to the process pool
                with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
                    executor_dict = {}
                    for test_case_class in all_test_cases:
                        composite_args = {
                            'version_2': fim_version,  # this is the MS version name since `all_test_cases` are FR
                            'calibrated': calibrated,
                            'overwrite': overwrite,
                            'verbose': verbose,
                        }

                        try:
                            future = executor.submit(test_case_class.alpha_test, **alpha_test_args)
                            executor_dict[future] = test_case_class.test_id
                        except Exception as ex:
                            print(f"*** {ex}")
                            # traceback.print_exc()
                            print(traceback.format_exc())
                            sys.exit(1)

                    # Send the executor to the progress bar
                    progress_bar_handler(
                        executor_dict, verbose, f"Compositing test cases with {job_number_huc} workers"
                    )
        '''

        ## if using DEV version, include the testing versions the user included with the "-dc" flag
        if dev_versions_to_compare is not None:
            dev_versions_to_include_list += dev_versions_to_compare

        # Specify which results to iterate through
        if config == 'DEV':
            iteration_list = [
                'official',
                'testing',
            ]  # iterating through official model results AND testing model(s)
        else:
            iteration_list = ['official']  # only iterating through official model results

        # Do aggregate_metrics.
        logging.info("Creating master metrics CSV...")

        # Note: This function is not compatible with GMS
        create_master_metrics_csv(
            master_metrics_csv_output=master_metrics_csv,
            dev_versions_to_include_list=dev_versions_to_include_list,
            prev_versions_to_include_list=prev_versions_to_include_list,
            iteration_list=iteration_list,
            prev_metrics_csv=prev_metrics_csv,
        )
    except Exception:
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
    master_metrics_csv_output,
    dev_versions_to_include_list,
    prev_versions_to_include_list,
    iteration_list,
    prev_metrics_csv,
):
    """
    This function searches for and collates metrics into a single CSV file that can queried database-style.
        The CSV is an input to eval_plots.py.
        This function automatically looks for metrics produced for official versions and loads them into
            memory to be written to the output CSV.

    Args:
        master_metrics_csv_output (str)    : Full path to CSV output.
                                                If a file already exists at this path, it will be overwritten.
        dev_versions_to_include_list (list): A list of non-official FIM version names.
                                                If a user supplied information on the command line using the
                                                -dc flag, then this function will search for metrics in the
                                                "testing_versions" library of metrics and include them in
                                                the CSV output.
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
        + ["calibrated"]
    ]

    # add in composite of versions (used for previous FIM3 versions)
    if "official" in iteration_list:
        composite_versions = [v.replace('_ms', '_comp') for v in prev_versions_to_include_list if '_ms' in v]
        prev_versions_to_include_list += composite_versions

    # Iterate through 5 benchmark sources
    for benchmark_source in ['ble', 'nws', 'usgs', 'ifc', 'ras2fim']:
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        if not os.path.exists(benchmark_test_case_dir):
            continue

        test_cases_list = [d for d in os.listdir(benchmark_test_case_dir) if re.match(r'\d{8}_\w{3,7}', d)]

        if benchmark_source in ['ble', 'ifc', 'ras2fim']:
            magnitude_list = MAGNITUDE_DICT[benchmark_source]

            # Iterate through available test cases
            for each_test_case in test_cases_list:
                try:
                    # Get HUC id
                    int(each_test_case.split('_')[0])
                    huc = each_test_case.split('_')[0]

                    # Update filepaths based on whether the official or dev versions should be included
                    for iteration in iteration_list:
                        if (
                            iteration == "official"
                        ):  # and str(cycle_previous_files) == "True": # "official" refers to previous finalized model versions
                            versions_to_crawl = os.path.join(
                                benchmark_test_case_dir, each_test_case, 'official_versions'
                            )
                            versions_to_aggregate = prev_versions_to_include_list

                        if (
                            iteration == "testing"
                        ):  # "testing" refers to the development model version(s) being evaluated
                            versions_to_crawl = os.path.join(
                                benchmark_test_case_dir, each_test_case, 'testing_versions'
                            )
                            versions_to_aggregate = dev_versions_to_include_list

                        # Pull version info from filepath
                        for magnitude in magnitude_list:
                            for version in versions_to_aggregate:
                                # if '_ms' in version:
                                #     extent_config = 'MS'
                                # elif ('_fr' in version) or (version == 'fim_2_3_3'):
                                #     extent_config = 'FR'
                                # else:
                                # TODO: May 29, 2026: We really don't need the "COMP" and "c" test anymore
                                extent_config = 'COMP'

                                if "_c" in version and version.split('_c')[1] == "":
                                    calibrated = "yes"
                                else:
                                    calibrated = "no"
                                version_dir = os.path.join(versions_to_crawl, version)
                                magnitude_dir = os.path.join(version_dir, magnitude)

                                # Add metrics from file to metrics table ('list_to_write')
                                if os.path.exists(magnitude_dir):
                                    magnitude_dir_list = os.listdir(magnitude_dir)
                                    for f in magnitude_dir_list:
                                        if '.json' in f:
                                            flow = 'NA'
                                            nws_lid = "NA"
                                            sub_list_to_append = [version, nws_lid, magnitude, huc]
                                            full_json_path = os.path.join(magnitude_dir, f)
                                            if os.path.exists(full_json_path):
                                                stats_dict = json.load(open(full_json_path))
                                                for metric in metrics_to_write:
                                                    sub_list_to_append.append(stats_dict[metric])
                                                sub_list_to_append.append(full_json_path)
                                                sub_list_to_append.append(flow)
                                                sub_list_to_append.append(benchmark_source)
                                                sub_list_to_append.append(extent_config)
                                                sub_list_to_append.append(calibrated)

                                                list_to_write.append(sub_list_to_append)
                except ValueError:
                    pass

        # Iterate through AHPS benchmark data
        if benchmark_source in AHPS_BENCHMARK_CATEGORIES:
            test_cases_list = os.listdir(benchmark_test_case_dir)

            for each_test_case in test_cases_list:
                try:
                    # Get HUC id
                    int(each_test_case.split('_')[0])
                    huc = each_test_case.split('_')[0]

                    # Update filepaths based on whether the official or dev versions should be included
                    for iteration in iteration_list:
                        if iteration == "official":  # "official" refers to previous finalized model versions
                            versions_to_crawl = os.path.join(
                                benchmark_test_case_dir, each_test_case, 'official_versions'
                            )
                            versions_to_aggregate = prev_versions_to_include_list

                        if (
                            iteration == "testing"
                        ):  # "testing" refers to the development model version(s) being evaluated
                            versions_to_crawl = os.path.join(
                                benchmark_test_case_dir, each_test_case, 'testing_versions'
                            )
                            versions_to_aggregate = dev_versions_to_include_list

                        # Pull model info from filepath
                        for magnitude in ['action', 'minor', 'moderate', 'major']:
                            for version in versions_to_aggregate:
                                # if '_ms' in version:
                                #     extent_config = 'MS'
                                # elif ('_fr' in version) or (version == 'fim_2_3_3'):
                                #     extent_config = 'FR'
                                # else:
                                # TODO: May 29, 2026: We really don't need the "COMP" and "c" test anymore
                                extent_config = 'COMP'

                                if "_c" in version and version.split('_c')[1] == "":
                                    calibrated = "yes"
                                else:
                                    calibrated = "no"

                                version_dir = os.path.join(versions_to_crawl, version)
                                magnitude_dir = os.path.join(version_dir, magnitude)

                                if os.path.exists(magnitude_dir):
                                    magnitude_dir_list = os.listdir(magnitude_dir)
                                    for f in magnitude_dir_list:
                                        if '.json' in f and 'total_area' not in f:
                                            nws_lid = f[:5]
                                            sub_list_to_append = [version, nws_lid, magnitude, huc]
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
                                                sub_list_to_append.append(calibrated)
                                                list_to_write.append(sub_list_to_append)
                except ValueError as ex:
                    logging.critical("A Value exception has occurred")
                    logging.critical(ex)
                    pass

    # If previous metrics are provided: read in previously compiled metrics and join to calcaulated metrics
    if prev_metrics_csv is not None:
        prev_metrics_df = pd.read_csv(prev_metrics_csv)

        # Put calculated metrics into a dataframe and set the headers
        df_to_write_calc = pd.DataFrame(list_to_write)
        df_to_write_calc.columns = df_to_write_calc.iloc[0]
        df_to_write_calc = df_to_write_calc[1:]

        # Join the calculated metrics and the previous metrics dataframe
        df_to_write = pd.concat([df_to_write_calc, prev_metrics_df], axis=0)

    else:
        df_to_write = pd.DataFrame(list_to_write)
        df_to_write.columns = df_to_write.iloc[0]
        df_to_write = df_to_write[1:]

    # Save aggregated compiled metrics ('df_to_write') as a CSV
    # create the path if it does not already exist
    metrics_file_path, __ = os.path.split(master_metrics_csv_output)
    if not os.path.exists(metrics_file_path):
        os.makedirs(metrics_file_path, exist_ok=True)
    df_to_write.to_csv(master_metrics_csv_output, index=False)


def progress_bar_handler(executor_dict, verbose, desc):
    for future in tqdm(
        as_completed(executor_dict), total=len(executor_dict), disable=(not verbose), desc=desc
    ):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


if __name__ == '__main__':
    # Sample usage:
    '''

    python /foss_fim/tools/synthesize_test_cases.py
        -c DEV
        -v hand_4_6_1_2
        -jh 9 -jb 5
        -m /outputs/gms_test_synth_combined/gms_synth_metrics.csv
        -vg -o

     Notes:
       - fim_input.csv MUST be in the folder suggested.
       - the -v param is the name in the folder in the "outputs/" directory where the test hucs are at.
         It also becomes the folder names inside the test_case folders when done.
       - the -vg param may not be working (will be assessed better on later releases).
       - Find a balance between -jh (number of jobs for hucs) versus -jb (number of jobs for branches)
         on quick tests on a 96 core machine, we tried [1 @ 80], [2 @ 40], and [3 @ 25] (and others).
       - The -m can be any path and any name.
       - Previous metric CSV (-pcsv) and the cycle previous files argument (-pfiles) will return an error
         if called at the same time. If neither are used, the alpha test metrics will only be compiled
         for the provided dev version to compare. You will need a copy of a recent metrics file to use
         the -pcsv argument.

     To see your outputs in the test_case folder (hard coded path), you can check for outputs using
         (cd .... to your test_case folder), then command becomes  find . -name gms_test_* -type d (Notice the
         the -name can be a wildcard for your -v param (or the whole -v value))
     If you want to delete the test outputs, test the outputs as suggest immediately above, but this time your
         command becomes:  find . -name gms_test_* -type d  -exec rm -rdf {} +
    '''

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument(
        '-c',
        '--config',
        help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',
        required=True,
        default='DEV',
    )
    parser.add_argument(
        '-l',
        '--calibrated',
        help='Denotes use of calibrated n values. This should be taken from meta-data from hydrofabric dir',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-p',
        '--precalb-option',
        help='Using this argument will use the precalb_discharge_cms in hydrotable. ',
        required=False,
        default=False,
        action='store_true',
    )
    # parser.add_argument(
    #     '-e',
    #     '--model',
    #     help='Denotes model used. Options: [FR, MS, or GMS]. '
    #     'This should be taken from meta-data in hydrofabric dir.',
    #     default='GMS',
    #     required=False,
    # )
    parser.add_argument(
        '-v', '--fim-version', help='REQUIRED: Name of fim version to cache.', required=True, default="all"
    )
    parser.add_argument(
        '-jh',
        '--job-number-huc',
        help='Number of processes to use for HUC scale operations. HUC and Batch job numbers should multiply '
        'to no more than one less than the CPU count of the machine.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-jb',
        '--job-number-branch',
        help='Number of processes to use for Branch scale operations. HUC and Batch job numbers should '
        'multiply to no more than one less than the CPU count of the machine.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-tb',
        '--thread-number-branch',
        help='Number of threads to use for Branch scale operations. HUC and Batch job numbers should '
        'multiply to no more than one less than the CPU count of the machine.',
        required=False,
        default=1,
        type=int,
    )

    # variable was not being used
    # parser.add_argument(
    #     '-s',
    #     '--special-string',
    #     help='Add a special name to the end of the branch.',
    #     required=False,
    #     default="",
    # )
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
        '-dc',
        '--dev-versions-to-compare',
        nargs='+',
        help='Specify the name(s) of a dev (testing) version to include in master '
        'metrics CSV. Pass a space-delimited list.',
        required=False,
    )
    parser.add_argument(
        '-m',
        '--master-metrics-csv',
        help='REQUIRED: Define path for output master metrics CSV file.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-mm',
        '--master-metrics-only',
        help='OPTIONAL: Adding this tag, will skip processing benchmark data and will compile'
        ' the master metrics (.csv) only.',
        required=False,
        default=False,
        action='store_true',
    )
    # parser.add_argument(
    #     '-d',
    #     '--fr-run-dir',
    #     help='Name of test case directory containing FIM for FR model',
    #     required=False,
    #     default=None,
    # )
    parser.add_argument(
        '-vr', '--verbose', help='Verbose output', required=False, default=False, action='store_true'
    )
    # parser.add_argument(
    #     '-vg',
    #     '--gms-verbose',
    #     help='GMS Verbose Progress Bar',
    #     required=False,
    #     default=None,
    #     action='store_true',
    # )
    parser.add_argument(
        '-pcsv',
        '--prev-metrics-csv',
        help='Optional: Filepath for a CSV with previous metrics to concatenate with new '
        'metrics to form a final aggregated metrics csv.',
        required=False,
        default=None,
    )

    # TODO: May 2026: Do we even want this anymore? we onlyi use pcsv now
    parser.add_argument(
        '-pfiles',
        '--cycle-previous-files',
        help='Optional: Specifies whether previous metrics should be compiled by cycling '
        'through files (True). Cannot be used if a previous metrics CSV is provided.',
        required=False,
        default=False,
        action="store_true",
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    # config = args['config']
    # fim_version = args['fim_version']
    # job_number_huc = args['job_number_huc']
    # job_number_branch = args['job_number_branch']
    # thread_number_branch = args['thread_number_branch']
    # special_string = args['special_string']
    # benchmark_category = args['benchmark_category']
    # overwrite = args['overwrite']
    # dev_versions_to_compare = args['dev_versions_to_compare']
    # master_metrics_csv = args['master_metrics_csv']
    # fr_run_dir = args['fr_run_dir']
    # calibrated = args['calibrated']
    # precalb_option = args['precalb_option']
    # # model = args['model']
    # verbose = bool(args['verbose'])
    # # gms_verbose = bool(args['gms_verbose'])
    # prev_metrics_csv = args['previous_metrics_csv']
    # pfiles = bool(args['cycle_previous_files'])
    # master_metrics_only = bool(args['master_metrics_only'])

    synthesize_test_cases(**args)


