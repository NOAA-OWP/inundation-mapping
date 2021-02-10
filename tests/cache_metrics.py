#!/usr/bin/env python3

import os
import argparse
import traceback

from run_test_case import run_alpha_test
from multiprocessing import Pool

TEST_CASES_DIR = r'/data/test_cases_ahps_testing/'
PREVIOUS_FIM_DIR = r'/data/previous_fim'
OUTPUTS_DIR = r'/data/outputs'


def process_alpha_test(args):
    
    fim_run_dir = args[0]
    branch_name = args[1]
    test_id = args[2]
    magnitude = args[3]
    archive_results = args[4]
    
    mask_type = 'huc'    
    
    if archive_results == False:
        compare_to_previous = True
    else:
        compare_to_previous = False

    try:
        run_alpha_test(fim_run_dir, branch_name, test_id, magnitude, compare_to_previous=compare_to_previous, archive_results=archive_results, mask_type=mask_type)
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='Options include ble or ahps. Defaults to process both.',required=False, default=['ble', 'ahps'])
        
    test_cases_dir_list = os.listdir(TEST_CASES_DIR)
    
    args = vars(parser.parse_args())

    config = args['config']
    fim_version = args['fim_version']
    job_number = int(args['job_number'])
    special_string = args['special_string']
    benchmark_category = args['benchmark_category']
    
    if fim_version != "all":
        previous_fim_list = [fim_version]
    else:
        previous_fim_list = os.listdir(PREVIOUS_FIM_DIR)    
    
    if config == 'PREV':
        archive_results = True
    elif config == 'DEV':
        archive_results = False
    else:
        print('Config (-c) option incorrectly set. Use "DEV" or "PREV"')
    
    if type(benchmark_category) != list:
        benchmark_category = [benchmark_category]
    
    procs_list = []
    for test_id in test_cases_dir_list:
        if 'validation' and 'other' not in test_id:
                        
            current_huc = test_id.split('_')[0]
            
            if test_id.split('_')[1] in benchmark_category:
            
                for branch_name in previous_fim_list:
                    
                    if config == 'DEV':
                        fim_run_dir = os.path.join(OUTPUTS_DIR, branch_name, current_huc)
                    elif config == 'PREV':
                        fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, branch_name, current_huc)
                                            
                    if not os.path.exists(fim_run_dir):
                        fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, branch_name, current_huc[:6])  # For previous versions of HAND computed at HUC6 scale
                    
                    print(fim_run_dir)
                    if os.path.exists(fim_run_dir):
                        print("Hello")
                        if special_string != "":
                            branch_name = branch_name + '_' + special_string
                        
                        if 'ble' in test_id:
                            magnitude = ['100yr', '500yr']
                        elif 'ahps' in test_id:
                            magnitude = ['action', 'minor', 'moderate', 'major']
                        else:
                            continue
                    
                        print("Adding " + test_id + " to list of test_ids to process...")
                        if job_number > 1:
                            procs_list.append([fim_run_dir, branch_name, test_id, magnitude, archive_results])
                        else:                            
                            process_alpha_test([fim_run_dir, branch_name, test_id, magnitude, archive_results])
            else:
                print("No test_ids were found for the provided benchmark category: " + str(benchmark_category))

    if job_number > 1:
        pool = Pool(job_number)
        pool.map(process_alpha_test, procs_list)