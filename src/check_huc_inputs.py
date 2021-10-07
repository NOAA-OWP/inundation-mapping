#!/usr/bin/env python3

import os
import argparse
from glob import glob
import utils.shared_functions as helpers

# ========================================================= 
def __read_included_files(parent_dir_path):

    '''
    This loads up a number of files on the file server, then loads up a large list 
       of individual hucs.
    '''
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
    '''
    Desc:
        Takes in the command line huc argument which comes in as a string
        and splits it to a list, based on a comma in the arg.
        
        If it has no comma, then we check to see it is a valid file path.
        
        If it only had one element, then the value should either be a number
        or a file path.

        If it is a valid file path, we load the file, and bring in each line
        to create a list of huc codes.In a later method, we will validate if
        the huc codes are valid.

    Returns:
        A list of huc codes (unvalidated).
    
    '''
    # huc_codes = []

    # # check to see if it a set of huc codes ie) 11090105,11130102,07090002
    # if "," in huc_args:
        # huc_codes_split = huc_args.split(",")
        
        # # strip spaces from each code.  Note: May not be valid, check later.
        # for index, code in enumerate(huc_codes_split):
            # huc_codes.append(code.strip())
     
    # else:
        # # Only one value. it might be a number or a string ie) a file path
        # # see if it is a valid file path, and if so, we load the list of huc codes in it.
        # # Actual codes will be validated later.
        # if helpers.validate_arg(huc_args, "integer"):
            # huc_codes.append(huc_args);
        # elif helpers.validate_arg(huc_args, "file_path"):
            # with open(huc_args,'r') as hucs_file: 
                # huc_codes = hucs_file.read().splitlines()
        # else:
            # raise KeyError("File not found for HUC input parameter of " + huc_args)
            
    if(not (huc_args and huc_args.strip())):
        raise Exception('missing -u/--hucList argument')            
        
    huc_codes = helpers.string_to_list_with_strip(huc_args, ",")
    
    if (helpers.validate_arg(huc_codes[0], "file_path")):  # load the file and its hucs
        with open(huc_args,'r') as hucs_file: 
            huc_codes = hucs_file.read().splitlines() # might not be valid hucs, but validated later
        
    elif (not helpers.validate_arg(huc_codes[0], "integer")):
        raise KeyError("File not found for HUC input parameter of " + huc_args)
    
    return(huc_codes)


# ========================================================= 
def __check_for_membership(hucs, accepted_hucs_set):

    '''
    Compares the lists of hucs that were submitted from command line
        to the list of accepted hucs that were loaded. If an input huc
        does not exist in the accepted_huc list, it errors out.
    '''
    for huc in hucs:
        if huc not in accepted_hucs_set:
            raise Exception("HUC {} code not found in available inputs. Edit HUC inputs or " \
                "acquire datasets (must match codes in the included_huc*.lst files) and try again".format(huc))
            
            


# ========================================================= 
def check_hucs(huc_args):
   
    input_hucs = __read_input_hucs(huc_args)
    accepted_hucs = __read_included_files(os.path.join(os.environ['inputDataDir'],'huc_lists'))    
    __check_for_membership(input_hucs, accepted_hucs)



    