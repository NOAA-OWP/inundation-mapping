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
        params_file_path = ut_helpers.get_params_filename(__file__)
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
        src.subset_vector_layers(subset_nwm_lakes = params["subset_nwm_lakes"],
                                subset_nwm_streams = params["subset_nwm_streams"],
                                subset_nhd_streams = params["subset_nhd_streams"],
                                hucCode = params["hucCode"],
                                subset_nhd_headwaters = params["subset_nhd_headwaters"],
                                wbd_buffer_filename = params["wbd_buffer_filename"],
                                wbd_filename = params["wbd_filename"],
                                dem_filename = params["dem_filename"],
                                nwm_lakes = params["nwm_lakes"],
                                nwm_catchments = params["nwm_catchments"],
                                subset_nwm_catchments = params["subset_nwm_catchments"],
                                nld_lines = params["nld_lines"],
                                nhd_streams = params["nhd_streams"],
                                landsea = params["landsea"],
                                nwm_streams = params["nwm_streams"],
                                subset_landsea = params["subset_landsea"],
                                nhd_headwaters = params["nhd_headwaters"],
                                subset_nld_lines = params["subset_nld_lines"],
                                great_lakes = params["great_lakes"],                                
                                lake_buffer_distance = params["lake_buffer_distance"],
                                wbd_buffer_distance = params["wbd_buffer_distance"],
                                levee_protected_areas = params["levee_protected_areas"],
                                subset_levee_protected_areas = params["subset_levee_protected_areas"])
       

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
    
