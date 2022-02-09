#!/usr/bin/env python3

import inspect
import os
import sys

import argparse
import json
import unittest

import unit_tests_utils as helpers

# importing python folders in other direcories
sys.path.append('/foss_fim/src/')
import nws_data

class test_get_huc8_by_nws_lid(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        params_file_path = '/foss_fim/unit_tests/nws_data_params.json'
    
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    def test_get_huc8_by_nws_lid_success(self):

        '''
            Tests that the correct huc8 is returned with a valid nws-lid
        '''

        #global params_file
        helpers.print_unit_test_function_header()
        
        params = self.params["valid_data"].copy()

        print(params["nws_lid"])
    
        # for now we are happy if no exceptions are thrown.
        actual_response = nws_data.get_huc8_by_nws_lid(nws_lid = params["nws_lid"])
        expected_response = '12090301'

        self.assertEqual(actual_response, expected_response, "HUC8 does not match expected result")
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


    def test_get_huc8_by_nws_lid_fail_invalid_nws_lid(self):

        '''
            Tests that the nws_lid does not exist in the nws_lid.gpkg
            Expecting an exception to be thrown
        '''

        #global params_file
        helpers.print_unit_test_function_header()
        
        #params = self.params["valid_data"].copy()

        try:

            nws_invalid_value = "abc"
    
            # for now we are happy if no exceptions are thrown.
            actual_response = nws_data.get_huc8_by_nws_lid(nws_lid = nws_invalid_value)

            raise AssertionError("Fail = excepted a thrown exception but did not get it but was received. Unit Test has 'failed'")

        except Exception:
            print()
            print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
            
        finally:
            print("*************************************************************")


    def test_get_huc8_by_nws_lid_fail_empty_nws_lid(self):

        '''
            Tests that the nws_lid is empty
            Expecting an exception to be thrown
        '''

        #global params_file
        helpers.print_unit_test_function_header()
        
        #params = self.params["valid_data"].copy()

        try:

            nws_invalid_value = " " # add a blank space to further test
    
            # for now we are happy if no exceptions are thrown.
            actual_response = nws_data.get_huc8_by_nws_lid(nws_lid = nws_invalid_value)

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
    