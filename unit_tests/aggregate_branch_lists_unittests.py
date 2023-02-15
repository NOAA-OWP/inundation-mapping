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
import aggregate_branch_lists as src

class test_aggregate_branch_lists(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    def test_aggregate_branch_lists_success(self):

        #global params_file
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)

        src.aggregate_branch_lists(output_dir = params["output_dir"],
                                   file_name = params["file_name"],
                                   output_file_name = params["output_file_name"])
       
       
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
    
