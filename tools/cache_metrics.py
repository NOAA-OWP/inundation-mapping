#!/usr/bin/env python3

import os
import argparse
import traceback

from run_test_case import run_alpha_test
from multiprocessing import Pool

TEST_CASES_DIR = r'/data/test_cases_new/'  #TODO remove "_new"
PREVIOUS_FIM_DIR = r'/data/previous_fim'
OUTPUTS_DIR = r'/data/outputs'


def process_alpha_test(args):
    
    fim_run_dir = args[0]
    version = args[1]
    test_id = args[2]
    magnitude = args[3]
    archive_results = args[4]
    
    mask_type = 'huc'    
    
    if archive_results == False:
        compare_to_previous = True
    else:
        compare_to_previous = False

    try:
        run_alpha_test(fim_run_dir, version, test_id, magnitude, compare_to_previous=compare_to_previous, archive_results=archive_results, mask_type=mask_type)
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='Options include ble or ahps. Defaults to process both.',required=False, default=None)
        
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
    
    benchmark_category_list = []
    
    if benchmark_category == None:
        for d in test_cases_dir_list:
            if 'test_cases' in d:
                benchmark_category_list.append(d.replace('_test_cases', ''))
    else:
        benchmark_category_list = [benchmark_category]

    procs_list = []
    for bench_cat in benchmark_category_list:
        bench_cat_test_case_dir = os.path.join(TEST_CASES_DIR, bench_cat + '_test_cases')
        
        bench_cat_test_case_list = os.listdir(bench_cat_test_case_dir)
    
        for test_id in bench_cat_test_case_list:
            if 'validation' and 'other' not in test_id:
                            
                current_huc = test_id.split('_')[0]
                if test_id.split('_')[1] in bench_cat:
                
                    for version in previous_fim_list:
                        
                        if config == 'DEV':
                            fim_run_dir = os.path.join(OUTPUTS_DIR, version, current_huc)
                        elif config == 'PREV':
                            fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc)
                                                
                        if not os.path.exists(fim_run_dir):
                            fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc[:6])  # For previous versions of HAND computed at HUC6 scale
                        
                        if os.path.exists(fim_run_dir):
                            if special_string != "":
                                version = version + '_' + special_string
                            
                            if 'ble' in test_id:
                                magnitude = ['100yr', '500yr']
                            elif 'usgs' or 'nws' in test_id:
                                magnitude = ['action', 'minor', 'moderate', 'major']
                            else:
                                continue
                        
                            print("Adding " + test_id + " to list of test_ids to process...")
                            if job_number > 1:
                                procs_list.append([fim_run_dir, version, test_id, magnitude, archive_results])
                            else:                            
                                process_alpha_test([fim_run_dir, version, test_id, magnitude, archive_results])

    if job_number > 1:
        pool = Pool(job_number)
        pool.map(process_alpha_test, procs_list)