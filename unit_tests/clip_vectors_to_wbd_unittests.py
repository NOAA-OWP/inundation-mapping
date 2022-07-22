#!/usr/bin/env python3

import inspect
import os
import sys

import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other direcories
sys.path.append('/foss_fim/src/')
import clip_vectors_to_wbd as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_clip_vectors_to_wbd(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        try:        
            params_file_path = ut_helpers.get_params_filename(__file__)
            print(params_file_path)
        except FileNotFoundError as ex:
            print(f"params file not found. ({ex}). Check pathing and file name convention.")
            sys.exit(1)

        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    # MUST start with the name of "test_"
    def test_subset_vector_layers_success(self):

        '''
        This NEEDS be upgraded to check the output, as well as the fact that all of the output files exist as expected.
        Most of the output test and internal tests with this function will test a wide variety of conditions.
        For subcalls to other py classes will not exist in this file, but the unittest file for the other python file.
        Only the basic return output value shoudl be tested to ensure it is as expected.
        For now, we are adding the very basic "happy path" test.
        '''

        params = self.params["valid_data"].copy()
       
        # There is no default return value.
        # Later we need to check in this function that files were being created on the file system
        
        # for now we are happy if no exceptions are thrown.
        src.subset_vector_layers(hucCode = params["hucCode"],
                                nwm_streams_filename = params["nwm_streams"],
                                nhd_streams_filename = params["nhd_streams"],
                                nwm_lakes_filename = params["nwm_lakes"],
                                nld_lines_filename = params["nld_lines"],
                                nwm_catchments_filename = params["nwm_catchments"],
                                nhd_headwaters_filename = params["nhd_headwaters"],
                                landsea_filename = params["landsea"],
                                wbd_filename = params["wbd"],
                                wbd_buffer_filename = params["wbd_buffer"],
                                subset_nhd_streams_filename = params["subset_nhd_streams"],
                                subset_nld_lines_filename = params["subset_nld_lines"],
                                subset_nwm_lakes_filename = params["subset_lakes"],
                                subset_nwm_catchments_filename = params["subset_catchments"],
                                subset_nhd_headwaters_filename = params["subset_nhd_headwaters"],
                                subset_nwm_streams_filename = params["subset_nwm_streams"],
                                subset_landsea_filename = params["subset_landsea"],
                                extent = params["extent"],
                                great_lakes_filename = params["great_lakes_filename"],
                                wbd_buffer_distance = params["wbd_buffer_distance"],
                                lake_buffer_distance = params["lake_buffer_distance"])
       

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
    
