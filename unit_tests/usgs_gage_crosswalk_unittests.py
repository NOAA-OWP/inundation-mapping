#!/usr/bin/env python3

import inspect
import os
import sys

import json
import unittest
import warnings

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other directories
sys.path.append('/foss_fim/src/')   # *** update your folder path here if required ***
from usgs_gage_crosswalk import GageCrosswalk

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_usgs_gage_crosswalk(unittest.TestCase):

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
    def test_GageCrosswalk_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        
        Dev Notes: (which can be removed after you make this file)
            Remember... You need to validate the method output if there is any. However, if you have time, it
            is also recommended that you validate other outputs such as writing or updating file on the file 
            system, aka: Does the expected file exist. Don't worry about its contents.

        '''
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
       
        # Delete the usgs_elev_table.csv if it exists
        if os.path.isfile(params["output_table_filename"]):
            os.remove(params["output_table_filename"])

        # Instantiate and run GageCrosswalk
        gage_crosswalk = GageCrosswalk(params["usgs_gages_filename"], params["branch_id"])

        # Run crosswalk
        gage_crosswalk.run_crosswalk(params["input_catchment_filename"], params["input_flows_filename"], 
                            params["dem_filename"], params["dem_adj_filename"], params["output_table_filename"])
        print(gage_crosswalk.gages)

        # Make sure that the usgs_elev_table.csv was written
        assert os.path.isfile(params["output_table_filename"]), f'{params["output_table_filename"]} does not exist'

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
    
