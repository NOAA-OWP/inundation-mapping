#!/usr/bin/env python3

import os
import sys
import json
import unittest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers
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
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


# Test Cases
    def test_generate_rating_curve_metrics_01010004_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''

        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["01010004"])


    def test_generate_rating_curve_metrics_02020005_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["02020005"])


    def test_generate_rating_curve_metrics_02030103_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        '''
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
                
        generate_rating_curve_metrics(params["02030103"])

