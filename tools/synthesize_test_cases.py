#!/usr/bin/env python3

import os
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
import json
import csv
from tqdm import tqdm
import re

from run_test_case import test_case
from tools_shared_variables import TEST_CASES_DIR, PREVIOUS_FIM_DIR, OUTPUTS_DIR, AHPS_BENCHMARK_CATEGORIES, MAGNITUDE_DICT


def create_master_metrics_csv(master_metrics_csv_output, dev_versions_to_include_list):
    """
    This function searches for and collates metrics into a single CSV file that can queried database-style. The
    CSV is an input to eval_plots.py. This function automatically looks for metrics produced for official versions
    and loads them into memory to be written to the output CSV.
    
    Args:
        master_metrics_csv_output (str): Full path to CSV output. If a file already exists at this path, it will be overwritten.
        dev_versions_to_include_list (list): A list of non-official FIM version names. If a user supplied information on the command
                                            line using the -dc flag, then this function will search for metrics in the "testing_versions"
                                            library of metrics and include them in the CSV output.
    
    """
    
    # Construct header
    metrics_to_write = ['true_negatives_count',
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
                        'masked_area_km2'
                        ]

    additional_header_info_prefix = ['version', 'nws_lid', 'magnitude', 'huc']
    list_to_write = [additional_header_info_prefix + metrics_to_write + ['full_json_path'] + ['flow'] + ['benchmark_source'] + ['extent_config'] + ["calibrated"]]

    versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)

    if len(dev_versions_to_include_list) > 0:
        iteration_list = ['official', 'comparison']
    else:
        iteration_list = ['official']

    for benchmark_source in ['ble', 'nws', 'usgs', 'ifc', 'ras2fim']:
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        test_cases_list = [d for d in os.listdir(benchmark_test_case_dir) if re.match('\d{8}_\w{3,7}', d)]

        if benchmark_source in ['ble', 'ifc', 'ras2fim']:
            
            magnitude_list = MAGNITUDE_DICT[benchmark_source]

            for test_case in test_cases_list:

                huc = test_case.split('_')[0]

                for iteration in iteration_list:

                    if iteration == "official":
                        versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'official_versions')
                        versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)
                        # add in composite of versions
                        composite_versions = [v.replace('_ms', '_comp') for v in versions_to_aggregate if '_ms' in v]
                        versions_to_aggregate += composite_versions
                    if iteration == "comparison":
                        versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'testing_versions')
                        versions_to_aggregate = dev_versions_to_include_list

                    for magnitude in magnitude_list:
                        for version in versions_to_aggregate:
                            if '_ms' in version:
                                extent_config = 'MS'
                            elif ('_fr' in version) or (version == 'fim_2_3_3'):
                                extent_config = 'FR'
                            else:
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
        
        if benchmark_source in AHPS_BENCHMARK_CATEGORIES:

            for test_case in test_cases_list:

                huc = test_case.split('_')[0]

                for iteration in iteration_list:

                    if iteration == "official":
                        versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'official_versions')
                        versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)
                        # add in composite of versions
                        composite_versions = [v.replace('_ms', '_comp') for v in versions_to_aggregate if '_ms' in v]
                        versions_to_aggregate += composite_versions
                    if iteration == "comparison":
                        versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'testing_versions')
                        versions_to_aggregate = dev_versions_to_include_list

                    for magnitude in ['action', 'minor', 'moderate', 'major']:
                        for version in versions_to_aggregate:
                            if '_ms' in version:
                                extent_config = 'MS'
                            elif ('_fr' in version) or (version == 'fim_2_3_3'):
                                extent_config = 'FR'
                            else:
                                extent_config = 'COMP'
                            if "_c" in version and version.split('_c')[1] == "":
                                calibrated = "yes"
                            else:
                                calibrated = "no"

                            version_dir = os.path.join(versions_to_crawl, version)
                            magnitude_dir = os.path.join(version_dir, magnitude)
                            if os.path.exists(magnitude_dir):
                                magnitude_dir_list = [f for f in os.listdir(magnitude_dir) if f.endswith('.json') and 'total_area' not in f]
                                for f in magnitude_dir_list:
                                    nws_lid = f[:5]
                                    sub_list_to_append = [version, nws_lid, magnitude, huc]
                                    full_json_path = os.path.join(magnitude_dir, f)
                                    flow = ''
                                    if os.path.exists(full_json_path):

                                        # Get flow used to map.
                                        flow_file = os.path.join(benchmark_test_case_dir, 'validation_data_' + benchmark_source, huc, nws_lid, magnitude, 'ahps_' + nws_lid + '_huc_' + huc + '_flows_' + magnitude + '.csv')
                                        if os.path.exists(flow_file):
                                            with open(flow_file, newline='') as csv_file:
                                                reader = csv.reader(csv_file)
                                                next(reader)
                                                for row in reader:
                                                    flow = row[1]

                                        stats_dict = json.load(open(full_json_path))
                                        for metric in metrics_to_write:
                                            sub_list_to_append.append(stats_dict[metric])
                                        sub_list_to_append.append(full_json_path)
                                        sub_list_to_append.append(flow)
                                        sub_list_to_append.append(benchmark_source)
                                        sub_list_to_append.append(extent_config)
                                        sub_list_to_append.append(calibrated)

                                        list_to_write.append(sub_list_to_append)
    
    with open(master_metrics_csv_output, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)

def progress_bar_handler(executor_dict, verbose, desc):

    for future in tqdm(as_completed(executor_dict),
                    total=len(executor_dict),
                    disable=(not verbose),
                    desc=desc,
                    ):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future],exc.__class__.__name__,exc))

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=False,default='DEV')
    parser.add_argument('-l','--calibrated',help='Denotes use of calibrated n values. This should be taken from meta-data from hydrofabric dir',required=False, default=False,action='store_true')
    parser.add_argument('-e','--model',help='Denotes model used. FR, MS, or GMS allowed. This should be taken from meta-data in hydrofabric dir.',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
    parser.add_argument('-j','--job-number-huc',help='Number of processes to use for HUC scale operations. HUC and Batch job numbers should multiply to no more than one less than the CPU count of the machine.',required=False, default=1,type=int)
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='A benchmark category to specify. Defaults to process all categories.',required=False, default="all")
    parser.add_argument('-o','--overwrite',help='Overwrite all metrics or only fill in missing metrics.',required=False, action="store_true")
    parser.add_argument('-dc', '--dev-version-to-compare', nargs='+', help='Specify the name(s) of a dev (testing) version to include in master metrics CSV. Pass a space-delimited list.',required=False)
    parser.add_argument('-m','--master-metrics-csv',help='Define path for master metrics CSV file.',required=False,default=None)
    parser.add_argument('-d','--fr-run-dir',help='Name of test case directory containing FIM for FR model',required=False,default=None)
    parser.add_argument('-vr','--verbose',help='Verbose',required=False,default=None,action='store_true')

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    config = args['config']
    fim_version = args['fim_version']
    job_number_huc = args['job_number_huc']
    special_string = args['special_string']
    benchmark_category = args['benchmark_category']
    overwrite = args['overwrite']
    dev_versions_to_compare = args['dev_version_to_compare']
    master_metrics_csv = args['master_metrics_csv']
    fr_run_dir = args['fr_run_dir']
    calibrated = args['calibrated']
    model = args['model']
    verbose = args['verbose']

    # check job numbers
    total_cpus_available = os.cpu_count() - 1
    if job_number_huc > total_cpus_available:
        raise ValueError('The HUC job number, {}, multiplied by the branch job number, {}, '\
                          'exceeds your machine\'s available CPU count minus one. '\
                          'Please lower the job_number_huc or job_number_branch'\
                          'values accordingly.'.format(job_number_huc,job_number_huc)
                        )

    # Default to processing all possible versions in PREVIOUS_FIM_DIR. Otherwise, process only the user-supplied version.
    if fim_version != "all":
        previous_fim_list = [fim_version]
    else:
        if config == 'PREV':
            previous_fim_list = os.listdir(PREVIOUS_FIM_DIR)
        elif config == 'DEV':
            previous_fim_list = os.listdir(OUTPUTS_DIR)

    # Define whether or not to archive metrics in "official_versions" or "testing_versions" for each test_id.
    if config == 'PREV':
        archive_results = True
    elif config == 'DEV':
        archive_results = False
    else:
        print('Config (-c) option incorrectly set. Use "DEV" or "PREV"')

    # Create a list of all test_cases for which we have validation data
    all_test_cases = test_case.list_all_test_cases(version=fim_version, archive=archive_results, 
            benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])
    
    # Set up multiprocessor
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:

        ## Loop through all test cases, build the alpha test arguments, and submit them to the process pool
        executor_dict = {}
        for test_case_class in all_test_cases:
            
            if not os.path.exists(test_case_class.fim_dir):
                continue

            alpha_test_args = { 
                                'calibrated': calibrated,
                                'model': model,
                                'mask_type': 'huc',
                                'overwrite': overwrite
                                }
            executor_dict[executor.submit(test_case_class.alpha_test, **alpha_test_args)] = test_case_class.test_id
        # Send the executor to the progress bar and wait for all MS tasks to finish
        progress_bar_handler(executor_dict, verbose, f"Running MS test cases with {job_number_huc} workers")
        wait(executor_dict.keys())

        ## Composite alpha test run is initiated by a MS `model` and providing a `fr_run_dir`
        if model == 'MS' and fr_run_dir:

            ## Rebuild all test cases list with the FR version, loop through them and apply the alpha test
            all_test_cases = test_case.list_all_test_cases(version=fr_run_dir, archive=archive_results, 
                    benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])
            executor_dict = {}
            for test_case_class in all_test_cases:
                if not os.path.exists(test_case_class.fim_dir):
                    continue
                alpha_test_args = { 
                                    'calibrated': calibrated,
                                    'model': model,
                                    'mask_type': 'huc',
                                    'overwrite': overwrite
                                    }
                executor_dict[executor.submit(test_case_class.alpha_test, **alpha_test_args)] = test_case_class.test_id
            # Send the executor to the progress bar and wait for all FR tasks to finish
            progress_bar_handler(executor_dict, verbose, f"Running FR test cases with {job_number_huc} workers")
            wait(executor_dict.keys())

            ## Loop through FR test cases, build composite arguments, and submit the composite method to the process pool
            executor_dict = {}
            for test_case_class in all_test_cases:
                composite_args = { 
                                    'version_2': fim_version, # this is the MS version name since `all_test_cases` are FR
                                    'calibrated': calibrated,
                                    'overwrite': overwrite
                                    }
                executor_dict[executor.submit(test_case_class.composite, **composite_args)] = test_case_class.test_id
            # Send the executor to the progress bar
            progress_bar_handler(executor_dict, verbose, f"Compositing test cases with {job_number_huc} workers")

    if config == 'DEV':
        if dev_versions_to_compare != None:
            dev_versions_to_include_list = dev_versions_to_compare + previous_fim_list
        else:
            dev_versions_to_include_list = previous_fim_list
        # Include FR and Composite metrics if running composite alpha
        if model == 'MS' and fr_run_dir:
            dev_versions_to_include_list += [fr_run_dir, fr_run_dir.replace('_fr', '_comp')]
    if config == 'PREV':
        dev_versions_to_include_list = []

    if master_metrics_csv is not None:
        # Do aggregate_metrics.
        print("Creating master metrics CSV...")

        # this function is not compatible with GMS
        create_master_metrics_csv(master_metrics_csv_output=master_metrics_csv, dev_versions_to_include_list=dev_versions_to_include_list)
