"""
Created on Fri May  6 14:39:44 2022

@author: ryan.spies
"""

import shutil
import os
import argparse

########################################################################
#Function to copy huc subdirectories to new location
########################################################################
def copy_huc_dirs(huc_copy_lst, src, dst):
    # reading the file
    huc_txt = open(huc_copy_lst, "r")
    hucs = huc_txt.read()
    huc_list = hucs.split("\n")
    huc_list = list(filter(None, huc_list))
    print(str(len(huc_list)) + ' hucs in provided text file')
    huc_txt.close()
    
    src_huc_dirs = [dI for dI in os.listdir(src) if os.path.isdir(os.path.join(src,dI))]
    
    for huc in huc_list:
        if huc in src_huc_dirs:
            # Check if file already exists
            if os.path.isdir(dst+os.sep+huc):
                print(huc, 'exists in the destination path - removing!')
                shutil.rmtree(dst+os.sep+huc)
            print('Copying huc directory: ' + str(huc))
            shutil.copytree(src+os.sep+huc, dst+os.sep+huc)
        else:
            print('!!Source directory not found: ' + str(huc))
    
    print('Completed copy process')

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Copy tool for moving huc directories from src to dst')
    parser.add_argument('-src','--src',help='Name of src directory with huc subdirectories (e.g. data/ouputs/dev_abc/src/)',required=True)
    parser.add_argument('-dst', '--dst',help='Name of dst directory to create huc subdirectories (e.g. data/ouputs/dev_abc/dst/)',required=True,default="")
    parser.add_argument('-huc', '--huc-lst',help='Location of a line delimited text file with huc ids to copy (12345678)',required=True,default="")
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    src = args['src']
    dst = args['dst']
    huc_copy_lst = args['huc_lst']
    
    copy_huc_dirs(huc_copy_lst, src, dst)

    