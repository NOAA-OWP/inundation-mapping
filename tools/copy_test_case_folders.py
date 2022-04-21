#!/usr/bin/env python3

import os
import argparse
import shutil

def copy_folders(folder_name_list,
                source_dir,
                target_dir,
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
        - overwrite:  if this value is set to true, the entire target_directory will be emptied of its
             contents as this process starts if the folder exists. 
    Output:
        - A copy of huc directories (or named folders) as listed in the folder_name_list.
    '''

    if (not os.path.exists(folder_name_list)):
        raise FileNotFoundError(f"Sorry. The file {folder_name_list} does not exist")

    if (not os.path.exists(source_dir)):
        raise NotADirectoryError(f"Sorry. Source folder of {source_dir} does not exist")

    if os.path.exists(target_dir) and os.path.isdir(target_dir) and (overwrite == True):
        shutil.rmtree(target_dir)
        os.mkdir(target_dir)        

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
    

if __name__ == '__main__':

# Sample Usage: python /foss_fim/tools/copy_test_case_folders.py  -f /data/inputs/huc_lists/huc_list_for_alpha_tests_22020420.lst -s  /outputs/gms_set_1/ -t /data/outputs_fim_share_maas/220421_test_case_hucs -o

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

    parser.add_argument('-o','--overwrite', 
                            help='Overwrite the target folders if already existing? (default false)',
                            action='store_true' )

    args = vars(parser.parse_args())

    copy_folders(**args)
