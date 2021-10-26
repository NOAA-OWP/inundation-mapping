#!/usr/bin/env python3

# October 2021: Note: This file is not complete. It is a working copy started as part 
# of the input validation task. This file (python) version was put on hold in favour
# of completing the task in Bash. This may be revived if/when all code is changed to 
# python.

import os
import argparse
import check_huc_inputs
import utils.shared_functions as helpers

# ========================================================= 
def __validate_manditory_args_exist(huc_args, config, extent, run_name):

    if(not (huc_args and huc_args.strip())):
        raise Exception('missing -u/--hucList argument')
        
    if(not (config and config.strip())):
        raise Exception('missing -c|--configFile argument')
        
    if(not (extent and extent.strip())):
        raise Exception('missing -e|--extent argument')

    if(not (run_name and run_name.strip())):
        raise Exception('missing -n|--runName argument')

# ========================================================= 
def __validate_arg_values(config, extent, run_name, job_limit, whitelist, step_start_number, step_end_number):

	#--------------------------------------------
	# envFile (c/--config)
	#     check to see if the path exists
    config = config.strip()
    if not helpers.validate_arg(config, "file_path"):
        raise Exception('c/--config argument: The file name does not appear to exist. Check path, spelling and path.')


	#--------------------------------------------
	# extent (-e/--extent)
	#     check to see if the value of 'MS' or 'FS' (we will correct for case)
    extent = extent.strip()
    
    if (not helpers.validate_arg(extent, "integer")):
        if (extent.upper() != "MS") and (extent.upper() != "FR"):
            raise Exception('-e/--extent must be the value of MS or FR.')
    else:
        raise Exception('-e/--extent must be the value of MS or FR.')
        
        
	#--------------------------------------------
	# -n/--runName
    #     ensure it has alphanumeric or underscore chars)
    run_name = run_name.strip()
    if (not run_name.isalnum()) and (not "_" in run_name): 
        raise Exception('-n/--runName: Please use alpha-numeric or underscores only for the run name.')
        

	#--------------------------------------------
	# -j/--jobLimit
    #    can be empty 
    job_limit = job_limit.strip().lstrip('0') # strips empty spaces on both sides, plus 0 on front   
    if (len(job_limit) > 0) :
        if (not helpers.validate_arg(job_limit, "integer")):
           raise Exception('-j/--jobLimit: (Optional) argument value may be missing or is not a number.')
    
    

# ========================================================= 
def validate_inputs(huc_args, config, extent, run_name, job_limit, whitelist, step_start_number, step_end_number):

    __validate_manditory_args_exist(huc_args, config, extent, run_name)
    
    # validate the huc list
    check_huc_inputs.check_hucs(huc_args)
    
    __validate_arg_values(config, extent, run_name, job_limit, whitelist, step_start_number, step_end_number)


# ========================================================= 


# NOTE: Oct 2021, NOT complete. We can not update values and pass then back to BASH reliably, so completed this in Bash.
#   when we change it all to python, this can be completed.
if __name__ == '__main__':


    


    try:
        # parse arguments
        # Note: We not use argparse options required=True, types, etc.
        #     We make all optional, so we control the errors and their messages.
        #     We are also using nargs='?' in case a person adds a command switch but forgot the arg. 
        #        ie... -u (but missed the Huc or list name.
        parser = argparse.ArgumentParser(description='Check command line parameters submitted to Fim_run.sh')
        parser.add_argument('-u', '--huc_args',
                            help='Line-delimited file or comma seperated list of HUCs to check availibility for.')
                           
        parser.add_argument('-c', '--config',
                            help='Configuration file (env file) with bash environment variables to export.')

        parser.add_argument('-e', '--extent', 
                            help='Full resolution or mainstem method; options are MS or FR.')
                            
        parser.add_argument('-n', '--run_name', 
                            help='A name to tag the output directories and log files as. Can be a version tag. AlphaNumeric and underscore only.')
                            
        parser.add_argument('-j', '--job_limit', 
                            help='Max number of concurrent jobs to run. Default one job at time. \
                            One outputs stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest.')
                            
        parser.add_argument('-w', '--whitelist', 
                            help='List of files to save in a production run in addition to final inundation outputs. \
                            ex: file1.tif,file2.json,file3.csv')
        
        parser.add_argument('-s', '--step_start_number', 
                            help='Step number to start at (defaulted to 1).')
        
        parser.add_argument('-d', '--step_end_number', 
                            help='Step number to end after (defaulted to 99).' )
        

        # extract to dictionary
        args = vars(parser.parse_args())

        # call function
        # Note: order is important (same as parser.add_argument)
        validate_inputs(**args)
        
    except Exception as e:
    
         # Oct 2021: Rob H: Not pretty, but print is std out and will carry over to bash. It should probably be another throw or force StdErr value or something.
         print ("oh no...")
         print(e)
         #exit(1)
         