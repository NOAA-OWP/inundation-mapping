#!/usr/bin/env python3

import os
import argparse
import shutil
import sys

# importing python folders in other direcories
sys.path.append('/foss_fim/src/gms/')
import aggregate_branch_lists as agg

def copy_folders(folder_name_list,
                source_dir,
                target_dir,
                create_gms_input_list=False,
                overwrite=False):

    '''
    Summary: Scans the source_directory looking for huc values from the huc list. Once found,
        the huc directory is copied to the target directory. All recusive files/folders wil be copied.
        
        Line items each be on a new line  (ie "\n")
        
        ***** Note: The tool will still work, if the list is not a list of hucs. 
              It can be just a list of folder names.
    Input:
        - folder_name_list: A file and path to a .txt or .lst file with a list of line delimited huc numbers.
        - source_dir: The root folder where the huc (or named) folders reside.
        - target_dir: The root folder where the huc folders will be copied to. Note. All contents of 
             each huc folder, including branch folders if applicable, will be copied, in the extact
             structure as the source directory. Note: The target folder need not pre-exist. 
        - create_gms_input_list: If this flag is set to True, after coping the folders, the 
            "aggregate_branch_lists.py" file will be called in order to make the gms_input.csv file.
            The gms_input.csv is required for futher processing such as reprocessing branchs or set up
            for test cases.
        - overwrite:  if this value is set to true, the entire target_directory will be emptied of its
             contents as this process starts if the folder exists. 
    Output:
        - A copy of huc directories (or named folders) as listed in the folder_name_list.
    '''

    if (not os.path.exists(folder_name_list)):
        raise FileNotFoundError(f"Sorry. The file {folder_name_list} does not exist")

    if (not os.path.exists(source_dir)):
        raise NotADirectoryError(f"Sorry. Source folder of {source_dir} does not exist")

    with open(folder_name_list, "r") as fp:
        raw_folder_names = fp.read().split("\n")

    # split on new line can add an extra row of a blank value if a newline char exists on the end.
    # Some lines may have extra spaces, or dups. It is ok if the value is not necessarily a huc
    folder_names = set() # this will ensure unique names
    for folder_name in raw_folder_names:
        folder_name = folder_name.strip()
        if (folder_name) != '':
            folder_names.add(folder_name)

    sorted_folder_names = sorted(folder_names)

    print(f"{str(len(sorted_folder_names))} folders to be copied")
    ctr = 0
    
    for folder_name in sorted_folder_names:
        src_folder = os.path.join(source_dir, folder_name)
        if not os.path.exists(src_folder):
            print(f"source folder not found: {src_folder}")
        else:
            target_folder = os.path.join(target_dir, folder_name)
            print(f"coping folder : {target_folder}")
            shutil.copytree(src_folder, target_folder, dirs_exist_ok=True)
            ctr+=1

    print(f"{str(ctr)} folders have been copied to {target_dir}")

    if create_gms_input_list == True:
        # call this code, which scans each huc (unit) directory looking for the branch_id.lst
        # and adds them together to create the gms_inputs.csv file
        # Note: folder_name_list needs to be a huc list to work)
        agg.aggregate_inputs_for_gms(folder_name_list, target_dir, "gms_inputs.csv")
        print("gms_inputs.csv created")
    

if __name__ == '__main__':

# Remember: This is for pulling out only folders that are related to a huc list (such as an alpha test list)
#   and it has to be run on each root folder, one at a time (for now. aka.. no wildcards)

# Sample Usage: 
#python /foss_fim/tools/copy_test_case_folders.py -f /data/inputs/huc_lists/huc_list_for_alpha_tests_22020420.lst -s /outputs/rob_gms_test_synth/ -t /data/outputs/rob_gms_test_synth_combined -a

#  NOTE the 'a' at the end meaning go ahead create the gms_input.csv. This is normally
# left for the last folder to be copied over.

    parser = argparse.ArgumentParser(description='Based on the huc list parameter, ' \
                        'find and copy the full huc (or other) directories.')

    parser.add_argument('-f','--folder_name_list',
                            help='file and path to the huc list. Note: The list does not ' \
                                'necessarily be a list of huc, just a list of unique values',
                            required=True)                            
    parser.add_argument('-s','--source_dir', 
                            help='Source folder to be scanned for unique folders',
                            required=True)
    parser.add_argument('-t','--target_dir',
                            help='Target folder where the folders will be copied to',
                            required=True)

    parser.add_argument('-a','--create_gms_input_list',
                            help='Create the gms_input.csv list after coping',
                            required=False, default=False, action='store_true')

    args = vars(parser.parse_args())

    copy_folders(**args)
