#!/usr/bin/env python3


import os
import argparse
from multiprocessing import Pool

from run_test_case import run_alpha_test
from all_ble_stats_comparison import subset_vector_layers
from aggregate_metrics import aggregate_metrics

TEST_CASES_DIR = r'/data/test_cases_new/'  # TODO remove "_new"
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
    except Exception as e:
        print(e)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=True)
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='Options include ble or ahps. Defaults to process both.',required=False, default=['ble', 'ahps'])
    parser.add_argument('-l','--huc8-list',help='A list of HUC8s to synthesize.',required=True)
    parser.add_argument('-d','--current-dev',help='The current dev id.',required=True)
    parser.add_argument('-o','--output-folder',help='The directory where synthesis outputs will be written.',required=True)
        
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



    
    
    
    
#    for test_id in test_cases_dir_list:
#        if not any(x in test_id for x in ['validation','other','.lst']):  #if 'validation' and 'other' not in test_id:
#                        
#            current_huc = test_id.split('_')[0]
#            print(current_huc)
#            if test_id.split('_')[1] in benchmark_category:
#            
#                
#                if config == 'DEV':
#                    fim_run_dir = os.path.join(OUTPUTS_DIR, branch_name, current_huc)
#                elif config == 'PREV':
#                    fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, branch_name, current_huc)
#                    
#                if os.path.exists(fim_run_dir):    
#                    
#                    if special_string != "":
#                        branch_name = branch_name + '_' + special_string
#                    
#                    if 'ble' in test_id:
#                        magnitude = ['100yr', '500yr']
#                    elif 'ahps' in test_id:
#                        magnitude = ['action', 'minor', 'moderate', 'major']
#                    else:
#                        continue
#                
#                    print("Adding " + test_id + " to list of test_ids to process...")
#                    if job_number > 1:
#                        procs_list.append([fim_run_dir, branch_name, test_id, magnitude, archive_results])
#                    else:
#                        process_alpha_test([fim_run_dir, branch_name, test_id, magnitude, archive_results])
#                        
#            else:
#                print("No test_ids were found for the provided benchmark category: " + str(test_id.split('_')[1]))

    # Multiprocess alpha test runs.
    if job_number > 1:
        pool = Pool(job_number)
        pool.map(process_alpha_test, procs_list)
        
    # Do all_ble_stats_comparison.
    subset_vector_layers(args['huc8_list'], fim_version, args['current_dev'], args['output_folder'])
    
    # Do aggregate_metrics.
    aggregate_metrics(config=config, branch=fim_version, hucs=args['huc8_list'], special_string=args['special_string'], outfolder=args['output_folder'])
    