#!/usr/bin/env python3

import inspect
import os
import sys

import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

sys.path.append('/foss_fim/tools/gms_tools')
import inundate_gms as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_inundate_gms(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    def test_Inundate_gms_create_inundation_raster_directory_single_huc_success(self):

        '''
        Test for creating a gms inundation raster, not a depth raster and no
        inundation_polygons.
        This test is based on creating a raster based on a single huc and its branches
        within a gms output folder.
        '''

        params = self.params["valid_data_inudation_raster_single_huc"].copy()         
        
        output_fileNames_df = src.Inundate_gms(hydrofabric_dir = params["hydrofabric_dir"],
                                               forecast = params["forecast"],
                                               num_workers = params["num_workers"],
                                               hucs = params["hucs"],
                                               inundation_raster = params["inundation_raster"],
                                               inundation_polygon = params["inundation_polygon"],
                                               depths_raster = params["depths_raster"],
                                               verbose = params["verbose"], 
                                               log_file = params["log_file"],
                                               output_fileNames = params["output_fileNames"])

        # check if output files df has records.
        assert len(output_fileNames_df) > 0, "Expected as least one dataframe record"

        # also check output log file and output raster. Can't... there will be multiple outputs
        #assert os.path.exists(params["inundation_raster"]), "Inundation Raster does not exist"

        assert os.path.exists(params["log_file"]), "Log file expected and does not exist"
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")

        
    # ***********************


if __name__ == '__main__':

    script_file_name = os.path.basename(__file__)

    print("*****************************")
    print(f"Start of {script_file_name} tests")
    print()
   
    unittest.main()
    
    print()    
    print(f"End of {script_file_name} tests")
    
