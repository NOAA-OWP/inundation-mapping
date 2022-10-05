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
sys.path.append('/foss_fim/src/') 
import gms.outputs_cleanup as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_outputs_cleanup(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    def test_remove_deny_list_files_specific_branch_success(self):

        '''
        This validates removal of files for a directory already pointing to a 
        specific branch in a HUC
        '''

        params = self.params["valid_specific_branch_data"].copy()
        
        src.remove_deny_list_files(src_dir = params["src_dir"],
                                   deny_list = params["deny_list"],
                                   branch_id = params["branch_id"],
                                   verbose = params["verbose"])
                                   
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_remove_deny_list_files_huc_level_success(self):

        '''
        This validates removal of files for all files and subdirectory files.
        Normally used for covering all hucs and their branch zeros but 
        can be anything
        '''

        params = self.params["valid_directory_data"].copy()
        
        src.remove_deny_list_files(src_dir = params["src_dir"],
                                   deny_list = params["deny_list"],
                                   branch_id = params["branch_id"],
                                   verbose = params["verbose"])
                                   
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_remove_deny_list_skip_cleaning_success(self):

        '''
        This validates removal of files for all files and subdirectory files.
        Normally used for covering all hucs and their branch zeros but 
        can be anything
        '''

        params = self.params["skip_clean"].copy()
        
        src.remove_deny_list_files(src_dir = params["src_dir"],
                                   deny_list = params["deny_list"],
                                   branch_id = params["branch_id"],
                                   verbose = params["verbose"])
                                   
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_remove_deny_list_files_invalid_src_directory(self):

        '''
        Double check the src directory exists
        '''

        # NOTE: As we are expecting an exception, this MUST have a try catch.
        #   for "success" tests (as above functions), they CAN NOT have a try/catch
        
        try:

            params = self.params["valid_specific_branch_data"].copy()    
            params["src_dir"] = "/data/does_no_exist"
            
            src.remove_deny_list_files(src_dir = params["src_dir"],
                                    deny_list = params["deny_list"],
                                    branch_id = params["branch_id"],
                                    verbose = params["verbose"])
            
            raise AssertionError("Fail = excepted a thrown exception but did not get it but was received. Unit Test has 'failed'")
            
        except Exception:
            print()
            print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
            
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_remove_deny_list_files_invalid_deny_list_does_not_exist(self):

        '''
        Double check the deny list exists
        '''

        # NOTE: As we are expecting an exception, this MUST have a try catch.
        #   for "success" tests (as above functions), they CAN NOT have a try/catch
        try:

            params = self.params["valid_specific_branch_data"].copy()    
            params["deny_list"] = "invalid_file_name.txt"
            
            src.remove_deny_list_files(src_dir = params["src_dir"],
                                    deny_list = params["deny_list"],
                                    branch_id = params["branch_id"],
                                    verbose = params["verbose"])
            
            raise AssertionError("Fail = excepted a thrown exception but did not get it but was received. Unit Test has 'failed'")
            
        except Exception:
            print()
            print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
            
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
    
