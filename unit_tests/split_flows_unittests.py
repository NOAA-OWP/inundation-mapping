#!/usr/bin/env python3

import inspect
import os
import sys

import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other directories
sys.path.append('/foss_fim/src/')   # *** update your folder path here if required ***
import split_flows as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_split_flows(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    # MUST start with the name of "test_"
    # This is the (or one of the) valid test expected to pass
    def test_split_flows_success(self):

        '''
        The /data/outputs/gms_example_unit_tests/<huc>/branches/<branchid>/demDerived_reaches_split_<branchid>.gpkg and
        /data/outputs/gms_example_unit_tests/<huc>/branches/<branchid>/demDerived_reaches_split_points_<branchid>.gpkg should not exit prior to this test.
        If the test is successful, these file will be created.
        '''

        params = self.params["valid_data"].copy() 

        # to setup the test, lets start by deleted the two expected output files to ensure
        # that they are regenerated
        if os.path.exists(params["split_flows_filename"]):
            os.remove(params["split_flows_filename"])
        if os.path.exists(params["split_points_filename"]):
            os.remove(params["split_points_filename"])
       
        src.split_flows(max_length = params["max_length"],
                        slope_min = params["slope_min"],
                        lakes_buffer_input = params["lakes_buffer_input"],
                        flows_filename = params["flows_filename"],
                        dem_filename = params["dem_filename"],
                        split_flows_filename = params["split_flows_filename"],
                        split_points_filename = params["split_points_filename"],
                        wbd8_clp_filename = params["wbd8_clp_filename"],
                        lakes_filename = params["lakes_filename"],
                        drop_stream_orders = params["drop_stream_orders"])
       
        if os.path.exists(params["split_flows_filename"]) == False:
            raise Exception(params["split_flows_filename"] + " does not exist")
        if os.path.exists(params["split_points_filename"]) == False:
            raise Exception(params["split_points_filename"] + " does not exist")

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
    
