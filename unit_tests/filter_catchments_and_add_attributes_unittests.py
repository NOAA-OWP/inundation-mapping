#!/usr/bin/env python3

import inspect
import os
import sys

import argparse
import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other directories
sys.path.append('/foss_fim/src/')   # *** update your folder path here if required ***
import filter_catchments_and_add_attributes as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)


# *************
# Important: For this to work, when you run gms_run_branch.sh, you have to 
# use deny_gms_branches_default.lst or one that does not exist. Key files need to exist
# for this unit test to work.
class test_filter_catchments_and_add_attributes(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        try:
            params_file_path = ut_helpers.get_params_filename(__file__)
            #print(params_file_path)
        except FileNotFoundError as ex:
            print(f"params file not found. ({ex}). Check pathing and file name convention.")
            sys.exit(1)

        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    # MUST start with the name of "test_"
    # This is the (or one of the) valid test expected to pass
    def test_filter_catchments_and_add_attributes_success(self):

        '''
        The gw_catchments_reaches_filtered_addedAttributes_<branchID>.gpkg and
        demDerived_reaches_split_filtered_<branchid>.gpkg should not exit prior to this test.
        If the test is successful, these file will be created.
        '''

        params = self.params["valid_data"].copy() 

        # to setup the test, lets start by deleted the two expected output files to ensure
        # that they are regenerated
        if os.path.exists(params["output_catchments_filename"]):
            os.remove(params["output_catchments_filename"])
        if os.path.exists(params["output_flows_filename"]):
            os.remove(params["output_flows_filename"])
       
        src.filter_catchments_and_add_attributes(input_catchments_filename = params["input_catchments_filename"],
                                                input_flows_filename = params["input_flows_filename"],
                                                output_catchments_filename = params["output_catchments_filename"],
                                                output_flows_filename = params["output_flows_filename"],
                                                wbd_filename = params["wbd_filename"],
                                                huc_code = params["huc_code"],
                                                drop_stream_orders = params["drop_stream_orders"])
       
        if os.path.exists(params["output_catchments_filename"]) == False:
            raise Exception(params["output_catchments_filename"] + " does not exist")
        if os.path.exists(params["output_flows_filename"]) == False:
            raise Exception(params["output_flows_filename"] + " does not exist")

        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


if __name__ == '__main__':

    script_file_name = os.path.basename(__file__)

    print("*****************************")
    print(f"Start of {script_file_name} tests")
    print()
   
    unittest.main()
    
    print()    
    print(f"End of {script_file_name} tests")
    
