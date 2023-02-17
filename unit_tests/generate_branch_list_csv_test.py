#!/usr/bin/env python3

import os
import json
import unittest
import pytest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import generate_branch_list_csv as src

class test_generate_branch_list_csv(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

        # for these tests to work, we have to check if the .csv exists and remove it 
        # prior to exections of the tests.
        
        params = self.params["valid_data_add_branch_zero"].copy()
        if (os.path.exists(params["output_branch_csv"])):
            os.remove(params["output_branch_csv"])

# Test Cases:      

    def test_generate_branch_list_csv_valid_data_add_branch_zero_success(self):

        # yes.. we know that we can not control the order
        
        #global params_file
        params = self.params["valid_data_add_branch_zero"].copy()

        src.generate_branch_list_csv(huc_id = params["huc_id"],
                                     branch_id = params["branch_id"],
                                     output_branch_csv = params["output_branch_csv"])
       


    def test_generate_branch_list_csv_valid_data_add_branch_success(self):
    
        # yes.. we know that we can not control the order
        
        #global params_file
        params = self.params["valid_data_add_branch"].copy()

        src.generate_branch_list_csv(huc_id = params["huc_id"],
                                     branch_id = params["branch_id"],
                                     output_branch_csv = params["output_branch_csv"])
       

    def test_generate_branch_list_csv_invalid_bad_file_extension(self):
        
        #global params_file
        params = self.params["invalid_bad_file_extension"].copy()

        # we expect this to fail. If it does fail with an exception, then this test is sucessful.
        with pytest.raises(Exception) as e_info:
            src.generate_branch_list_csv(huc_id = params["huc_id"],
                                        branch_id = params["branch_id"],
                                        output_branch_csv = params["output_branch_csv"])

