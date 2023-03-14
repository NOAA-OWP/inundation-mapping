#!/usr/bin/env python3

import json
import unittest
import pytest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers
from utils.shared_functions import FIM_Helpers as src

class test_shared_functions(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)
    
# Test Cases:

    def test_append_id_to_file_name_single_identifier_success(self):

        '''
        Pass in a file name with the single identifier and get the single adjusted file name back
        '''

        params = self.params["append_append_id_to_file_name_single_identifier_valid"].copy() 

        actual_output = src.append_id_to_file_name(file_name = ut_helpers.json_concat(params, "outputDestDir" , "file_name"),
                                                   identifier = params["identifier"])

        err_msg = "actual output does not match expected output"

        expected_output = ut_helpers.json_concat(params, "outputDestDir" , "expected_output")
        
        assert  expected_output == actual_output, err_msg


    def test_append_id_to_file_name_indentifer_list_success(self):

        '''
        Pass in a file name with the list of identifiers and get a file name back with multiple identifers added
        '''
        
        params = self.params["append_append_id_to_file_name_identifier_list_valid"].copy()        

        actual_output = src.append_id_to_file_name(file_name = ut_helpers.json_concat(params, "outputDestDir" , "file_name"),
                                                   identifier = params["identifier"])

        err_msg = "actual output does not match expected output"
        
        expected_output = ut_helpers.json_concat(params, "outputDestDir" , "expected_output")
        
        assert expected_output == actual_output, err_msg


    def test_append_id_to_file_name_no_file_name_success(self):

        '''
        Pass in an non existant file name and get None back
        '''
        
        params = self.params["append_append_id_to_file_name_single_identifier_valid"].copy()        
        
        actual_output = src.append_id_to_file_name(None, identifier = params["identifier"])

        error_msg = "actual output should not have a value"

        assert actual_output == None, error_msg

