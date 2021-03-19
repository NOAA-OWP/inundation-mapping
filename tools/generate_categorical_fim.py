#!/usr/bin/env python3
import subprocess
import argparse
import time
from pathlib import Path

if __name__ == '__main__':
    
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-f','--fim_run_dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-w', '--output_workspace', help = 'Workspace where all flow files are stored.', required = True)
    parser.add_argument('-j','--number_of_jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    args = vars(parser.parse_args())
    
    #Get arguments
    fim_run_dir = args['fim_run_dir']
    output_workspace = args['output_workspace']
    number_of_jobs = args['number_of_jobs']
    
    ####################################################################
    #Define default arguments. Modify these if necessary. 
    #Default values are: 
        #Upstream and downstream mainstem tracing of 10 miles
        #Flow files stored in subdirectory "flows"
        #Mapping files stored in subdirectory "mapping"
        #Depth grids NOT created
    nwm_us_search = 10
    nwm_ds_search = 10        
    output_flows_dir = str(Path(output_workspace) / 'flows')
    output_mapping_dir = str(Path(output_workspace) / 'mapping')    
    write_depth_tiff = False
    ####################################################################
    
    ####################################################################
    #Run CatFIM scripts in sequence
    ####################################################################
    #Generate CatFIM flow files.
    print('Creating flow files')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_flows.py', 'w' , output_flows_dir, 'u', nwm_us_search, 'd', nwm_ds_search])
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished creating flow files in {elapsed_time} minutes')
    
    #Generate CatFIM mapping.
    print('Begin mapping')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_mapping.py', 'r' , fim_run_dir, 's', output_flows_dir, 'o', output_mapping_dir, 'j', number_of_jobs, 'depthtiff', write_depth_tiff]) 
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished mapping in {elapsed_time} minutes')

   