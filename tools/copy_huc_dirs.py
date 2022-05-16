"""
Created on Fri May  6 14:39:44 2022

@author: ryan.spies
"""

import shutil
import os
import argparse
import multiprocessing
from multiprocessing import Pool

########################################################################
#Function to copy huc subdirectories to new location
########################################################################
def copy_huc_dirs(huc, src, dst):
    
    # Check if file already exists
    if os.path.isdir(dst+os.sep+huc):
        print(huc, 'exists in the destination path - removing!')
        shutil.rmtree(dst+os.sep+huc)
    print('Copying huc directory: ' + str(huc))
    shutil.copytree(src+os.sep+huc, dst+os.sep+huc)

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Copy tool for moving huc directories from src to dst')
    parser.add_argument('-src','--src',help='Name of src directory with huc subdirectories (e.g. data/ouputs/dev_abc/src/)',required=True)
    parser.add_argument('-dst', '--dst',help='Name of dst directory (dir must exist) to create huc subdirectories (e.g. data/ouputs/dev_abc/dst/)',required=True,default="")
    parser.add_argument('-huc', '--huc-lst',help='Location of a line delimited text file with huc ids to copy (12345678)',required=True,default="")
    parser.add_argument('-j', '--job-number',help='The number of jobs',required=False,default=1)
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    src = args['src']
    dst = args['dst']
    huc_copy_lst = args['huc_lst']
    job_number = int(args['job_number'])

    # reading the huc list file
    huc_txt = open(huc_copy_lst, "r")
    hucs = huc_txt.read()
    huc_list = hucs.split("\n")
    huc_list = list(filter(None, huc_list))
    print(str(len(huc_list)) + ' hucs in provided text file')
    huc_txt.close()

    # append "logs" directory to list of hucs (need the logs for some stand alone processing steps)
    huc_list.append("logs") 
    
    src_huc_dirs = [dI for dI in os.listdir(src) if os.path.isdir(os.path.join(src,dI))]
    
    procs_list = []; huc_count=0
    for huc in huc_list:
        if huc in src_huc_dirs:
            procs_list.append([huc,src,dst])
            huc_count += 1
        else:
            print('!!Source directory not found: ' + str(huc))
    
    if huc_count > 0:
        if not os.path.exists(dst):
            print("destination directory does not exists - creating it: " + str(dst))
            os.mkdir(dst)

        # Multiprocess.
        print('Copying ' + str(huc_count) + ' huc dirs using '+ str(job_number) + ' jobs...')
        with Pool(processes=job_number) as pool:
            pool.starmap(copy_huc_dirs, procs_list)
        print('Completed copy process!')
    else:
        print("Could not find any HUC directories to copy - exiting!")

    