#!/usr/bin/env python3

import os
import argparse
from glob import glob
import utils.shared_functions as helpers

# ========================================================= 
def __read_included_files(parent_dir_path):

    filename_patterns = glob(os.path.join(parent_dir_path,'included_huc*.lst'))

    accepted_hucs_set = set()
    for filename in filename_patterns:

        f = open(filename,'r')
        fList = f.readlines()
        f.close()

        fList = [fl.rstrip() for fl in fList]

        accepted_hucs_set.update(fList)

    return(accepted_hucs_set)

# ========================================================= 
def __read_input_hucs(huc_args):
    """
    Desc:
        Takes in the command line huc argument which comes in as a list
        and splits it to a list, based on a comma in the arg.
        
        If it has no comma, then we check to see it is a valid file path.
        
        If it only had one element, then the value should either be a number
        or a file path.

        If it is a valid file path, we load the file, and bring in each line
        to create a list of huc codes.In a later method, we will validate if
        the huc codes are valid.

    Returns:
        A list of huc codes (unvalidated).
    
    """
    huc_codes = []

    str_huc_args = huc_args[0]

    # check to see if it a set of huc codes ie) 11090105,11130102,07090002
    if "," in str_huc_args:
        huc_codes_split = str_huc_args.split(",")
        
        # strip spaces from each code.  Note: May not be valid, check later.
        for index, code in enumerate(huc_codes_split):
            huc_codes.append(code.strip())
     
    else:
        # Only one value. it might be a number or a string ie) a file path
        # see if it is a valid file path, and if so, we load the list of huc codes in it.
        # Actual codes will be validated later.
        if helpers.validate_arg(str_huc_args, "integer"):
            huc_codes.append(str_huc_args);
        elif helpers.validate_arg(str_huc_args, "file_path"):
            with open(str_huc_args,'r') as hucs_file: 
                huc_codes = hucs_file.read().splitlines()
        else:
            raise KeyError("File not found for HUC input parameter of " + str_huc_args)
    
    return(huc_codes)


# ========================================================= 
def __check_for_membership(hucs, accepted_hucs_set):

    for huc in hucs:
        if huc not in accepted_hucs_set:
            raise KeyError("HUC {} not found in available inputs. Edit HUC inputs or " \
                "acquire datasets (must match codes in the included_huc*.lst files) and try again".format(hucs))
            


# ========================================================= 
def check_hucs(huc_args):
   
    input_hucs = __read_input_hucs(huc_args)
    accepted_hucs = __read_included_files(os.path.join(os.environ['inputDataDir'],'huc_lists'))    
    __check_for_membership(input_hucs, accepted_hucs)


# ========================================================= 
if __name__ == '__main__':

    try:
        # parse arguments
        parser = argparse.ArgumentParser(description='Checks input hucs for availability within inputs')
        parser.add_argument('-u','--huc_args',help='Line-delimited file or list of HUCs to check availibility for',required=True,nargs='+')

        # extract to dictionary
        args = vars(parser.parse_args())

        # call function
        check_hucs(**args)
    except Exception as e:
    
         # Oct 2021: Rob H: Not pretty, but print is std out and will carry over to bash. It should probably be another throw or force StdErr value or something.
         print(e)
         #exit(1)


    