#!/usr/bin/env python3

import numpy as np
import pandas as pd
import argparse
from foss_fim.src.acquire_and_preprocess_inputs import manage_preprocessing
import subprocess
import shutil
import os
from ast import literal_eval
import boto3
import tqdm

huc4 = 1202
#resolutions=[20,15,10,5,3,1]
resolutions=[20,15,10,5,3]
tile_sizes = {100:100000,20:10000,15:10000,10:5000,5:2000,3:1500,1:1000}
huc_sizes = {100:8,20:8, 15:8, 10:8, 5:8, 3:12, 1:12}
#stream_resolutions=['FR','MS', 'GMS']
stream_resolutions=['GMS']
acquire_data=True
use_s3_for_3dep_data_if_available = False
resolutions_to_acquire_data_for = {100,20,15,10,5,3,1}
skip_hydrofabric = False
skip_evaluate = False
manning = 12
production = True
overwrite = True
overwrite_eval = True
remove_inputs = True
remove_outputs = True
retry_3dep = False
hand_jobs_dict = { 
                'FR' : 
                  { 1 : 3, 3 : 3, 5 : 7, 10 : 7, 15 : 7 , 20 : 7 },
                'MS' :
                  { 1 : 5, 3 : 3, 5 : 7, 10 : 7, 15 : 7 , 20 : 7 },
                'GMS' :
                  { 1 : [10,5], 3 : [15,15], 5 : [7,15], 10 : [7,15], 15 : [7,15] , 20 : [7,15] }
                 }
eval_jobs_dict = { 
                'FR' :
                  { 1 : 12, 3 : 12, 5 : 4, 10 : 7, 15 : 7 , 20 : 7 },
                'MS' :
                  { 1 : 12, 3 : 12, 5 : 4, 10 : 7, 15 : 7 , 20 : 7 },
                'GMS' :
                  { 1 : [3,4], 3 : [3,4], 5 : [2,4], 10 : [3,4], 15 : [3,4] , 20 : [3,4]}
                  }


def Dem_3dep_comparison( huc4 = huc4,
                         resolutions = resolutions, 
                         tile_sizes= tile_sizes,
                         huc_sizes = huc_sizes,
                         stream_resolutions = stream_resolutions,
                         acquire_data = acquire_data,
                         use_s3_for_3dep_data_if_available = use_s3_for_3dep_data_if_available,
                         resolutions_to_acquire_data_for = resolutions_to_acquire_data_for,
                         skip_hydrofabric = skip_hydrofabric,
                         skip_evaluate = skip_evaluate,
                         manning = manning,
                         production = production,
                         overwrite = overwrite,
                         overwrite_eval = overwrite_eval,
                         remove_inputs = remove_inputs,
                         remove_outputs = remove_outputs,
                         hand_jobs_dict = hand_jobs_dict,
                         eval_jobs_dict = eval_jobs_dict,
                         retry_3dep = retry_3dep
                       ):

    resolutions_to_acquire_data_for = set(resolutions_to_acquire_data_for)

    # huc4, tile_size, resolution
    #for resolution, tile_size, huc_size in zip(resolutions,tile_sizes,huc_sizes):
    for resolution in resolutions:

        tile_size = tile_sizes[resolution]
        huc_size = huc_sizes[resolution]

        # acquire data
        if acquire_data & (resolution in resolutions_to_acquire_data_for):
            
            if use_s3_for_3dep_data_if_available:
                print(f"Acquiring data for {resolution}m from S3")

                session = boto3.Session(profile_name='default')
                s3_bucket = session.resource('s3').Bucket('fernandoa-bucket')
                
                s3_objects = [f.key for f in s3_bucket.objects.filter(Prefix=f'foss_fim/inputs/dem_3dep_rasters/{huc4}_{resolution}m')]
                for obj in tqdm.tqdm(s3_objects,desc='Downloading 3DEP from S3'):
                    prefix,file_name = os.path.split(obj)
                    
                    prefix_list = prefix.split('/')
                    prefix_list[0] = 'data'
                    prefix = os.path.join(*prefix_list)

                    download_target = os.path.join(prefix,file_name)

                    if not os.path.exists(prefix):
                        os.makedir(prefix)

                    s3_bucket.download_file(obj, download_target)

                # download vrt file
                s3_bucket.download_file(f'foss_fim/inputs/dem_3dep_rasters/dem_3dep_{huc4}_{resolution}m.vrt',
                                        f'data/inputs/dem_3dep_rasters/dem_3dep_{huc4}_{resolution}m.vrt')

            else:
                print(f"Acquiring data for {resolution}m")
                manage_preprocessing(
                                      hucs_of_interest=huc4,
                                      tile_size=tile_size,
                                      download_3dep=True,
                                      resolution=resolution,
                                      retry_3dep=retry_3dep
                                    )

        for sr in stream_resolutions:
            
            # compute HAND
            if not skip_hydrofabric:
                
                jobs = hand_jobs_dict[sr][resolution]

                if (sr == 'FR') | (sr == 'MS'):
                    
                    command_string = [
                                      '/foss_fim/fim_run.sh', 
                                      '-u', f'/data/inputs/huc_lists/dev_fim_ble_{huc4}_huc{huc_size}s.lst',
                                      '-n', f'3dep_test_{huc4}_{resolution}m_{sr}',
                                      '-c', f'/foss_fim/config/params_{resolution}m.env',
                                      '-j' , f'{jobs}', '-e', f'{sr}'
                                     ]

                    if production:
                        command_string += ['-p']
                    
                    if overwrite:
                        command_string += ['-o']
                    
                    print(f"Computing HAND for {resolution}m, {sr}")
                    result = subprocess.run(command_string, check=True)

                elif sr == 'GMS' :
                    
                    ujobs, bjobs = jobs

                    unit_command_string = [ 'gms_run_unit.sh',
                                            '-u', f'/data/inputs/huc_lists/dev_fim_ble_{huc4}_huc{huc_size}s.lst',
                                            '-n', f'3dep_test_{huc4}_{resolution}m_GMS',
                                            '-c', f'/foss_fim/config/params_{resolution}m.env',
                                            '-j', f'{ujobs}',
                                            '-d', f'/foss_fim/config/deny_gms_unit_default.lst'
                                           ]
                    
                    branch_command_string = [ 'gms_run_branch.sh',
                                              '-u', f'/data/inputs/huc_lists/dev_fim_ble_{huc4}_huc{huc_size}s.lst',
                                              '-n', f'3dep_test_{huc4}_{resolution}m_GMS',
                                              '-c', f'/foss_fim/config/params_{resolution}m.env',
                                              '-j', f'{bjobs}',
                                              '-d', f'/foss_fim/config/deny_gms_branches_default.lst',
                                            ]
                    
                    if production:
                        unit_command_string += ['-p']
                        branch_command_string += ['-p']
                    
                    if overwrite:
                        unit_command_string += ['-o']
                        branch_command_string += ['-o']
                    
                    print(f"Computing HAND for {resolution}m, {sr}")
                    unit_result = subprocess.run(unit_command_string, check=True)
                    branch_result = subprocess.run(branch_command_string, check=True)
        
            # evalute
            if not skip_evaluate:
                
                jobs = eval_jobs_dict[sr][resolution]

                if (sr == 'FR') | (sr == 'MS'):

                    command_string = [ '/foss_fim/tools/synthesize_test_cases.py',
                                       '-e', f'{sr}',
                                       '-jh', f'{jobs}',
                                       '-b', 'ble',
                                       '-v', f'3dep_test_{huc4}_{resolution}m_{sr}',
                                       '-s', f'n_{manning}',
                                       '-vr'
                                     ]
                elif sr == 'GMS':

                    hjobs,bjobs = jobs
                    
                    command_string = [ '/foss_fim/tools/synthesize_test_cases.py',
                                       '-e', f'{sr}',
                                       '-jh', f'{hjobs}',
                                       '-jb', f'{bjobs}',
                                       '-b', 'ble','-vr',
                                       '-v', f'3dep_test_{huc4}_{resolution}m_{sr}',
                                       '-s', f'n_{manning}',
                                       '-vg'
                                     ]
                
                if overwrite_eval:
                    command_string += ['-o']

                if sr == 'MS':
                    command_string += ['-d',f'3dep_test_{huc4}_{resolution}m_FR_n_{manning}']

                print(f"Evaluating HAND for {resolution}m, {sr}")
                result = subprocess.run(command_string, check=True)
                exit()
        
            # push to S3 and remove
            outputs_dir = f'/data/outputs/3dep_test_{huc4}_{resolution}m_{sr}'
            aws_string = ['aws', 's3', 'sync', outputs_dir,
                          f's3://fernandoa-bucket/foss_fim/outputs/3dep_test_{huc4}_{resolution}m_{sr}']
            
            if os.path.exists(outputs_dir):
                print(f"Pushing HAND outputs to S3 {resolution}m, {sr}")
                result_outputs = subprocess.run(aws_string, check=True)
            
            if remove_outputs:
                print(f"Removing output files {resolution}m, {sr}")
                shutil.rmtree(outputs_dir,ignore_errors=True)
        
        # push inputs to S3
        inputs_dir = f'/data/inputs/dem_3dep_rasters/{huc4}_{resolution}m'
        aws_string = ['aws', 's3', 'sync', inputs_dir,
                      f's3://fernandoa-bucket/foss_fim/inputs/dem_3dep_rasters/{huc4}_{resolution}m']
        
        if os.path.exists(inputs_dir):
            print(f"Pushing inputs to S3 {resolution}m")
            result_inputs = subprocess.run(aws_string, check=True)
        
        # push vrt file
        vrt_file = f'/data/foss_fim/inputs/dem_3dep_rasters/dem_3dep_{huc4}_{resolution}m.vrt'
        aws_string = ['aws', 's3', 'cp', vrt_file,
                      's3://fernandoa-bucket/foss_fim/inputs/dem_3dep_rasters/']
        
        if os.path.exists(vrt_file):
            print(f"Pushing vrt input to S3 {resolution}m")
            result_inputs = subprocess.run(aws_string, check=True)
        
        # remove input files
        if remove_inputs:
            print(f"Removing input files {resolution}m")
            shutil.rmtree(inputs_dir,ignore_errors=True)
            shutil.rmtree(vrt_file,ignore_errors=True)

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Master script for parents')
    parser.add_argument('-u','--huc4',help='HUC4', required=False, default=huc4, type=str)
    parser.add_argument('-r','--resolutions',help='Spatial resolutions',
                            required=False, default=resolutions, nargs='+', type=int)
    parser.add_argument('-t','--tile-sizes',help='Size of tiles for preprocessing (m)',
                            required=False, default=tile_sizes, type=dict)
    parser.add_argument('-s','--huc-sizes',help='HUC sizes',
                            required=False, default=huc_sizes, type=dict)
    parser.add_argument('-o','--stream-resolutions',help='Resolutions of Streams',
                            required=False, default=stream_resolutions, nargs='+')
    parser.add_argument('-a','--acquire-data',help='Acquire data',
                            required=False, default=acquire_data, action='store_false')
    parser.add_argument('-s3', '--use-s3-for-3dep-data-if-available',help='Use S3 3DEP data if available.',
                            required=False, default=use_s3_for_3dep_data_if_available, action='store_true')
    parser.add_argument('-sh','--skip-hydrofabric',help='Skips FIM hydrofabric computation',
                            required=False, default=skip_hydrofabric, action='store_true')
    parser.add_argument('-se','--skip-evaluate',help='Skips evaluation',
                            required=False, default=skip_evaluate, action='store_true')
    parser.add_argument('-e','--resolutions-to-acquire-data-for',help='Spatial resolutions to acquire data',
                            required=False, default=resolutions_to_acquire_data_for, nargs='+', type=int)
    parser.add_argument('-m','--manning',help='Manning\'s N value to use in integer form',
                            required=False, default=manning, type=int)
    parser.add_argument('-p','--production',help='Production data only',
                            required=False, default=production, action='store_false')
    parser.add_argument('-ov','--overwrite',help='Overwrite HAND data',
                            required=False, default=overwrite, action='store_false')
    parser.add_argument('-oe','--overwrite-eval',help='Overwrite eval data',
                            required=False, default=overwrite_eval, action='store_false')
    parser.add_argument('-n','--remove-inputs',help='Acquire data',
                            required=False, default=remove_inputs, action='store_false')
    parser.add_argument('-v','--remove-outputs',help='Acquire data',
                            required=False, default=remove_outputs, action='store_false')
    parser.add_argument('-hj','--hand-jobs-dict',help='Acquire data', required=False, default=hand_jobs_dict )
    parser.add_argument('-ej','--eval-jobs-dict',help='Acquire data', required=False, default= eval_jobs_dict)
    parser.add_argument('-rd', '--retry-3dep', help='Retry failed 3dep tiles',required=False,default=False,action='store_true')

    args = vars(parser.parse_args())
    
    Dem_3dep_comparison(**args)
