#!/usr/bin/env python3

import os
import argparse
from glob import glob
import utils.shared_functions as helpers

# ========================================================= 
# We will be cleaning up the huc list or file name. But to do this
# we will need it as a list which we use to compare to the list of huc codes
# in the include_huc*.lst files.
# But... want to also send back a single string to Bash as a comma delimited
# version of the list. Bash can parse a comma delimited string.

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
        and splits it to a list, based on a spaces in the arg.
               
        If it only had one element, then the value should either be a number
        or a file path.

        If it is a valid file path, we load the file, and bring in each line
        to create a list of huc codes.In a later method, we will validate if
        the huc codes are valid.

    Returns:
        A list of huc codes (unvalidated).
    
    '''
    
    huc_codes = []
    
    if (len(huc_args) == 1):
   
        # Could be an invalid file path, a single HUC, a string of multiple space delimited hucs
        huc_args[0] = huc_args[0].strip()
       
        if (helpers.validate_arg(huc_args[0], "file_path")):  # load the file and its HUC
            with open(huc_args[0],'r') as hucs_file: 
                huc_codes = hucs_file.read().splitlines() # might not be valid HUCs, but validated later
                
        elif (not helpers.validate_arg(huc_args[0], "integer")):
                                 
            # If the user accidently adds two spaces between hucs, strip them down to just one space
            # Previously trimmed.
            huc_args[0] = huc_args[0].replace("  ", " ") # strip two spaces down to one if applicable
            
            if "  " in huc_args[0]:  # two spaces or more remaining
                # then initially had at least 3 spaces and lets error it.
                raise KeyError("When submitting multiple HUCs, please ensure there are one and only one space between values.")
            
            if " " in huc_args[0]: # sometimes a single string with multiple hucs with spaces can come in
                huc_codes = helpers.string_to_list_with_strip(huc_args[0], ' ')
                
            else:  # must be an invalid file path
                raise KeyError("File not found for HUC input parameter of " + str(huc_args[0]))
                
        else:  # it is a single huc and we can assign it right over.
            huc_codes = huc_args
            
    else:  
        raise KeyError("-u/--hucLis must be either a file name, a single HUC or a set of HUCS space delimited inside quotes.")


    # make sure each code is an valid int
    for huc_code in huc_codes:
        if (not helpers.validate_arg(huc_code, "integer")):
            raise KeyError("There appears to be an invalid huc (not a number or hidden chars). HUC value of " + huc_code)

     
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
            raise KeyError("HUC {} code not found in available inputs. Edit HUC inputs or " \
                "acquire datasets (must match codes in the included_huc*.lst files) and try again".format(huc))
            
            
# ========================================================= 
def check_hucs(huc_args):

    accepted_hucs = __read_included_files(os.path.join(os.environ['inputDataDir'],'huc_lists'))       
    input_hucs = __read_input_hucs(huc_args)
    __check_for_membership(input_hucs, accepted_hucs)
    
    return input_hucs

# ========================================================= 
# This will print out the response with the value of HUCS: on the front of it.
# Yes.. this ugly but it will work for now. Print to Bash is just StdOut, and with
# the pre-pending of the key word, Bash can take the comma delimted string 
# and change it to a Bash Array for further processing. Remember, the 
# huc codes being sent back have been trimmed and validated for existance.
def __create_string_of_huc_codes(input_hucs_codes_list):

    huc_list = "HUCS:"
    
    number_of_items = len(input_hucs_codes_list)
    
    for index, huc_code in enumerate(input_hucs_codes_list):
        huc_list += huc_code
        if index < (number_of_items - 1):
            huc_list += ","
        
    return huc_list


# ========================================================= 
if __name__ == '__main__':

    try:

        # parse arguments
        parser = argparse.ArgumentParser(description='Checks input hucs for availability within inputs')
        parser.add_argument('-u','--huc_args' \
                            , help='Line-delimited file or list of HUCs to check availibility for' \
                            , required=True \
                            , nargs='+')

        # extract to dictionary
        args = vars(parser.parse_args())

        # call function
        # we will return the list of cleaned up huc_code, but change it to a string with a comma delimted format.
        # Bash can receive it via print (which is StdOut) and parse it based on the comma.
        input_hucs_codes_list = check_hucs(**args)
       
        str_huc_list = __create_string_of_huc_codes(input_hucs_codes_list)
        
        print(str_huc_list) # THIS MUST BE HERE: Its how the message gets back to Bash  (via StdOut)
        
    except KeyError as ke:
        print ("err: details: " + str(ke))
    
    except Exception as e:
    
         # Oct 2021: Rob H: Not pretty, but print is std out and will carry over to bash. It should probably 
         # be another throw or force StdErr value or something.
         print ("err: details (Internal Error): ")
         print(str(e) + ": Trace=" + traceback.format_exc())
         #exit(1)    