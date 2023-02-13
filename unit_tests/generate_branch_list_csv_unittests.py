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
import generate_branch_list_csv as src

class test_generate_branch_list_csv(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

        # for these tests to work, we have to check if the .csv exists and remove it 
        # prior to exections of the tests.
        
        params = self.params["valid_data_add_branch_zero"].copy()
        if (os.path.exists(params["output_branch_csv"])):
            os.remove(params["output_branch_csv"])
        

    def test_generate_branch_list_csv_valid_data_add_branch_zero_success(self):

        # yes.. we know that we can not control the order
        
        #global params_file
        params = self.params["valid_data_add_branch_zero"].copy()

        src.generate_branch_list_csv(huc_id = params["huc_id"],
                                     branch_id = params["branch_id"],
                                     output_branch_csv = params["output_branch_csv"])
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_generate_branch_list_csv_valid_data_add_branch_success(self):
    
        # yes.. we know that we can not control the order
        
        #global params_file
        params = self.params["valid_data_add_branch"].copy()

        src.generate_branch_list_csv(huc_id = params["huc_id"],
                                     branch_id = params["branch_id"],
                                     output_branch_csv = params["output_branch_csv"])
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")

    def test_generate_branch_list_csv_invalid_bad_file_extension(self):
        
        #global params_file
        params = self.params["invalid_bad_file_extension"].copy()

        # we expect this to fail. If it does fail with an exception, then this test
        # is sucessful.
        try:
            src.generate_branch_list_csv(huc_id = params["huc_id"],
                                        branch_id = params["branch_id"],
                                        output_branch_csv = params["output_branch_csv"])
       
            raise AssertionError("Fail = excepted a thrown exception but did not get it but was received. Unit Test has 'failed'")
            
        except Exception:
            print()
            print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
       
        finally:
            print("*************************************************************")             


if __name__ == '__main__':

    script_file_name = os.path.basename(__file__)

    print("*****************************")
    print(f"Start of {script_file_name} tests")
    print()
   
    unittest.main()
    
    print()    
    print(f"End of {script_file_name} tests")
    
