#!/usr/bin/env python3
import subprocess
import argparse
import time

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-w', '--flow_dir', help = 'Workspace where all flow files are stored.', required = True)
    parser.add_argument('-u', '--nwm_us_search',  help = 'Walk upstream on NWM network this many miles', required = True)
    parser.add_argument('-d', '--nwm_ds_search', help = 'Walk downstream on NWM network this many miles', required = True)
    parser.add_argument('-r','--fim_run_dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-o', '--output_cat_fim_dir',help='Path to directory where categorical FIM outputs will be written.',required=True, default="")
    parser.add_argument('-j','--number_of_jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    parser.add_argument('-depthtif','--write_depth_tiff',help='Using this option will write depth TIFFs.',required=False, action='store_true')

    args = vars(parser.parse_args())
    
    #Get arguments
    flow_dir = args['flow_dir']
    nwm_us_search = args['nwm_us_search']
    nwm_ds_search = args['nwm_ds_search']
    fim_run_dir = args['fim_run_dir']
    output_cat_fim_dir = args['output_cat_fim_dir']
    number_of_jobs = args['number_of_jobs']
    write_depth_tiff = args['write_depth_tiff']
    
    #Generate CatFIM flow files and then map.
    print('Creating flow files')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_flows.py', 'w' , flow_dir, 'u', nwm_us_search, 'd', nwm_ds_search])
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished creating flow files in {elapsed_time} minutes')
    
    print('Beginning mapping')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_mapping.py', 'r' , fim_run_dir, 's', flow_dir, 'o', output_cat_fim_dir, 'j', number_of_jobs, 'depthtiff', write_depth_tiff]) 
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished mapping in {elapsed_time} minutes')

   