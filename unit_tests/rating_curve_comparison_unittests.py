#!/usr/bin/env python3

import inspect
import os
import sys
import warnings

import argparse
import json
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other directories
sys.path.append('/foss_fim/tools/')   # *** update your folder path here if required ***
from rating_curve_comparison import generate_rating_curve_metrics

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_rating_curve_comparison(unittest.TestCase):

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
    def test_generate_rating_curve_metrics_01010004_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''

        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["01010004"])
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")

    def test_generate_rating_curve_metrics_02020005_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["02020005"])
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")

    def test_generate_rating_curve_metrics_02030103_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["02030103"])
       
       
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
    
