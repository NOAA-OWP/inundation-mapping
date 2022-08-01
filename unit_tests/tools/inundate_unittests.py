#!/usr/bin/env python3

import inspect
import os
import sys

import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

sys.path.append('/foss_fim/tools/')
import inundation as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_inundate(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    def test_inundate_create_inundation_raster_single_branch_success(self):

        '''
        Test for creating a inundation branch raster, no depth raster and no
        inundation_polygons, no subsets, no mask
        '''

        params = self.params["valid_data_inundate_branch"].copy()         
        
        # returns list of rasters and polys
        inundation_rasters, depth_rasters, inundation_polys = src.inundate(
                                            rem = params["rem"],
                                            catchments = params["catchments"],
                                            catchment_poly = params["catchment_poly"],
                                            hydro_table = params["hydro_table"],
                                            forecast = params["forecast"],
                                            mask_type = params["mask_type"],
                                            hucs = params["hucs"],
                                            hucs_layerName = params["hucs_layerName"],
                                            subset_hucs = params["subset_hucs"],
                                            num_workers = params["num_workers"],
                                            aggregate = params["aggregate"], 
                                            inundation_raster = params["inundation_raster"],
                                            inundation_polygon = params["inundation_polygon"],
                                            depths = params["depths"],
                                            out_raster_profile = params["out_raster_profile"],
                                            out_vector_profile = params["out_vector_profile"],
                                            src_table = params["src_table"],
                                            quiet = params["quiet"]                                            
                                            )

        print(inundation_rasters)
        assert len(inundation_rasters) == 1, "Expected exactly one inundation raster path records"
        assert depth_rasters[0] == None, "Expected no depth raster path records"
        assert inundation_polys[0] == None, "Expected no inundation_polys records"
        
        assert os.path.exists(params["expected_inundation_raster"]), \
            f"Expected file {params['expected_inundation_raster']} but it does not exist."

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
    
