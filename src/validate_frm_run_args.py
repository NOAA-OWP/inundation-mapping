#!/usr/bin/env python3

import argparse
import check_huc_inputs
import utils.shared_functions as helpers

# ========================================================= 
def __validate_manditory_args_exist(huc_args, config, extent, runName):

    if(not (huc_args and huc_args.strip())):
        raise Exception('missing -u/--hucList argument')
        
    print(config)
    if(not (config and config.strip())):
        raise Exception('missing -c|--configFile argument')
        
    if(not (extent and extent.strip())):
        raise Exception('missing -e|--extent argument')

    if(not (runName and runName.strip())):
        raise Exception('missing -n|--runName argument')

# ========================================================= 
def __validate_arg_values(config, extent, runName, job_limit, whitelist, step_start_number, step_end_number):

	#--------------------------------------------
	# envFile (c/--config)
	#     check to see if the path exists
    if not helpers.validate_arg(config, "file_path"):
        raise Exception('c/--config argument: The file name does not appear to exist. Check path, spelling and path.')


	#--------------------------------------------
	# extent (-e/--extent)
	#     check to see if the value of 'MS' or 'FS' (we will correct for case)
    if not helpers.validate_arg(extent, "integer"):
        if (extent.upper() != "MS") and (extent.upper() != "FR"):
            raise Exception('-e/--extent must be the value of MS or FR.')
    else:
        raise Exception('-e/--extent must be the value of MS or FR.')
        
        
	#--------------------------------------------
	# -n/--runName
    #     ensure it has alphanumeric or underscore chars)
    if (not runName.isalnum()) and (not "_" in runName): 
        raise Exception('-n/--runName: Please use alpha-numeric or underscores only for the run name.')
    

	#--------------------------------------------
	# -j/--jobLimit
    #    can be empty
    
    
    #response = helpers.validate_arg(job_limit, "nope")
    #print ("..." + str(response))

    
#    if(not (job_limit and job_limit.strip())):
#        if (not helpers.validate_is_integer(job_limit):
#            raise Exception('-j/--jobLimit: (Optional) argument value may be missing or is not a number.')
    

# ========================================================= 
def validate_inputs(huc_args, config, extent, runName, jobLimit, whitelist, step_start_number, step_end_number):

    __validate_manditory_args_exist(huc_args, config, extent, runName)
    
    # validate the huc list
    check_huc_inputs.check_hucs(huc_args)
    
    __validate_arg_values(config, extent, runName, jobLimit, whitelist, step_start_number, step_end_number)


# ========================================================= 
if __name__ == '__main__':

    try:
        # parse arguments
        # Note: We not use argparse options required=True, types, etc.
        #     We make all optional, so we control the errors and their messages.
        #     We are also using nargs='?' in case a person adds a command switch but forgot the arg. 
        #        ie... -u (but missed the Huc or list name.
        parser = argparse.ArgumentParser(description='Check command line parameters submitted to Fim_run.sh')
        parser.add_argument('-u', '--huc_args', default='', nargs='?',
                            help='Line-delimited file or comma seperated list of HUCs to check availibility for.')
                           
        parser.add_argument('-c', '--config', default='', nargs='?',
                            help='Configuration file (env file) with bash environment variables to export.')

        parser.add_argument('-e', '--extent', default='', nargs='?',
                            help='Full resolution or mainstem method; options are MS or FR.')
                            
        parser.add_argument('-n', '--runName', default='',  nargs='?',
                            help='A name to tag the output directories and log files as. \
                            Can be a version tag. AlphaNumeric and underscore only.')
                            
        parser.add_argument('-j', '--jobLimit', default='', nargs='?',
                            help='Max number of concurrent jobs to run. Default one job at time. \
                            One outputs stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest.')
                            
        parser.add_argument('-w', '--whitelist', default='', nargs='?', 
                            help='List of files to save in a production run in addition to final inundation outputs. \
                            ex: file1.tif,file2.json,file3.csv')
        
        parser.add_argument('-ssn', '--step_start_number', default=0, nargs='?',
                            help='Step number to start at (defaulted to 1).')
        
        parser.add_argument('-sen', '--step_end_number', default=99, nargs='?',
                            help='Step number to end after (defaulted to 99).' )
        

        # extract to dictionary
        #print ("here?")
        args = vars(parser.parse_args())
        #args, unknown = parser.parse_known_args()
        #print (args)

        # call function
        # Note: order is important (same as parser.add_argument)
        validate_inputs(**args)
        
    except Exception as e:
    
         # Oct 2021: Rob H: Not pretty, but print is std out and will carry over to bash. It should probably be another throw or force StdErr value or something.
         print ("oh no...")
         print(e)
         #exit(1)
         