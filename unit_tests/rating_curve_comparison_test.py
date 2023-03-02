#!/usr/bin/env python3

import os
import json
import unittest
import pytest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers
from rating_curve_comparison import generate_rating_curve_metrics


class test_rating_curve_comparison(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

# Test Cases
     
    @pytest.mark.skip(reason="rating_curve has invalid pathing based on files "\
        "that are normally not in a runtime folder. This test will disabled for now.")
    def test_generate_rating_curve_metrics_02020005_success(self):

        '''
        We are testing whether a .png file was created for a FIM-USGS rating curve comparison, 
        for HUC 02020005 using the `generate_rating_curve_metrics` function.
        The 5th index (parameter) for each HUC in `rating_curve_comparison_params.json` specifies
        the FIM-USGS rating curve comparison .png filepath.
        '''
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
        
        _indiv_huc_params = params["02020005"] 

        # To setup the test, lets start by deleted the expected output file to ensure
        # that it is regenerated.
        if os.path.exists(_indiv_huc_params[5]):
            os.remove(_indiv_huc_params[5])
        
        # Test that the file was deleted
        assert os.path.exists(_indiv_huc_params[5]) == False

        generate_rating_curve_metrics(_indiv_huc_params)

        # Test that the file was created by generate_rating_curve_metrics       
        assert os.path.exists(_indiv_huc_params[5]) == True

