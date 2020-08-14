# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 13:10:51 2020

@author: bradford.bates
"""

import os
import argparse

from run_test_case import run_alpha_test
from multiprocessing import Pool

TEST_CASES_DIR = r'/data/test_cases/'
PREVIOUS_FIM_DIR = r'/data/previous_fim'
OUTPUTS_DIR = r'/data/outputs'


def process_alpha_test(args):
    
    fim_run_dir = args[0]
    branch_name = args[1]
    test_id = args[2]
    return_interval = args[3]

    run_alpha_test(fim_run_dir, branch_name, test_id, return_interval, compare_to_previous=False, run_structure_stats=False, archive_results=True, legacy_fim_run_dir=False, waterbody_mask_technique='nwm_100')
    

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    
    compare_to_previous = False
    
    test_cases_dir_list = os.listdir(TEST_CASES_DIR)
    
    args = vars(parser.parse_args())

    fim_version = args['fim_version']
    job_number = int(args['job_number'])
    
    if fim_version != "all":
        previous_fim_list = [fim_version]
    else:
        previous_fim_list = os.listdir(PREVIOUS_FIM_DIR)    
    
    procs_list = []
    for test_id in test_cases_dir_list:
        if 'validation' not in test_id:
            print("Backfilling " + test_id + "...")
                        
            current_huc = test_id.split('_')[0]
            
            for branch_name in previous_fim_list:
                huc6 = test_id[:6]
                
                fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, branch_name, huc6)
                
                return_interval = ['100yr', '500yr']
                if job_number > 1:
                    procs_list.append([fim_run_dir, branch_name, test_id, return_interval])
                else:
                    process_alpha_test([fim_run_dir, branch_name, test_id, return_interval])


    if job_number > 1:
        pool = Pool(job_number)
        pool.map(process_alpha_test, procs_list)
    else:
        pass
