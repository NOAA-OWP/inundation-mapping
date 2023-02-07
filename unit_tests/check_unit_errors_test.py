#!/usr/bin/env python3

import math
import os
import shutil
import json
import unittest
import pytest

from utils.fim_enums import FIM_exit_codes
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers
from utils.shared_variables import (UNIT_ERRORS_MIN_NUMBER_THRESHOLD,
                                    UNIT_ERRORS_MIN_PERCENT_THRESHOLD)
import check_unit_errors as src


class test_check_unit_errors(unittest.TestCase):
    '''
    Allows the params to be loaded one and used for all test methods
    '''
    
    @classmethod
    def setUpClass(self):
        
        # get_params_filename function in ./unit_test_utils handles errors
        params_file_path = ut_helpers.get_params_filename(__file__)

        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    '''
    To make most of this unit test, we have to do the following:
    - rename the current unit_errors directory (if it exists)
    - empty the unit_errors directory
    - create a bunch of dummy files in it
    - perform the unit test
    - delete the unit_errors directory
    - rename the original unit_test folder back to unit_tests
    '''

# Test Cases:

    def test_check_unit_errors_success_below_min_errors(self):

        # Expecting no errors.
        # Test to ensure the number of dummy files is less than the overall min number of error files.

        params = self.params["valid_data"].copy()
        
        num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD - 1
        ue_folder_existed = self.__create_temp_unit_errors_folder_files(params["fim_dir"], 
                                                                        num_dummy_files_reqd)
        expected_output = 0
        actual_output = src.check_unit_errors(params["fim_dir"], num_dummy_files_reqd)
        
        err_msg = "Number of dummy files IS NOT less than the overall min number of error files."
        assert expected_output == actual_output, err_msg
        
        if (ue_folder_existed):
            self.__remove_temp_unit_errors_folder(params["fim_dir"])
        
            
    def test_check_unit_errors_fail_above_min_errors(self):

        # Test to ensure the number of dummy files is more than the overall min number of error files.
        # Expecting sys.exit of 62
        # We do expect this to fail and if it fails, it is successful.
        # Here we expect an exception, and are capturing it using pytest.raises(Exception)
        # To query the exception, or validate that it is the correct one, it is captured in the `e_info` object

        params = self.params["valid_data"].copy()
        
        num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD + 1
        
        self.__create_temp_unit_errors_folder_files(params["fim_dir"], num_dummy_files_reqd)
        
        with pytest.raises(Exception) as e_info:
            src.check_unit_errors(params["fim_dir"], num_dummy_files_reqd)
          
        # We have to put the unit_errors folders back to the way it was
        self.__remove_temp_unit_errors_folder(params["fim_dir"])


    def test_check_unit_errors_success_above_percent_errors(self):

        # Expecting no errors.
        # Test to ensure the number of dummy files is more than the overall min number of error files.
        # We do expect this not to to fail as it is greater than 10 errors but below the percent threshhold.

        params = self.params["valid_data"].copy()
        
        num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD * 2
            
        ue_folder_existed = self.__create_temp_unit_errors_folder_files(params["fim_dir"],
                                                                            num_dummy_files_reqd)
            
        num_total_units = math.trunc(num_dummy_files_reqd * (100 / UNIT_ERRORS_MIN_PERCENT_THRESHOLD)) + 1
        expected_output = 0
        actual_output = src.check_unit_errors(params["fim_dir"], num_total_units)
            
        err_msg = "Number of dummy files IS NOT more than the overall min number of error files"
        assert expected_output == actual_output, err_msg
            
        if (ue_folder_existed):
            self.__remove_temp_unit_errors_folder(params["fim_dir"]) 
            
        # We have to put the unit_errors folders back to the way is was.
        self.__remove_temp_unit_errors_folder(params["fim_dir"])


    def test_check_unit_errors_fail_below_percent_errors(self):

        # Expecting sys.exit of 62
        # Test to ensure the number of dummy files is more than the overall min number of error files.
        
        # We do expect this to fail as it is greater than 10 errors
        # AND below the percent threshhold (more percent errors than the threshold)
        # Here we expect an exception, and are capturing it using pytest.raises(Exception)
        # To query the exception, or validate that it is the correct one, it is captured in the `e_info` object

        params = self.params["valid_data"].copy()
        
        num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD * 2
            
        self.__create_temp_unit_errors_folder_files(params["fim_dir"], num_dummy_files_reqd)
            
        num_total_units = math.trunc(num_dummy_files_reqd * (100 / UNIT_ERRORS_MIN_PERCENT_THRESHOLD)) - 10

        with pytest.raises(Exception) as e_info:
            src.check_unit_errors(params["fim_dir"], num_total_units)
          
        # We have to put the unit_errors folders back to the way it was.
        self.__remove_temp_unit_errors_folder(params["fim_dir"])
    

# Helper functions:

    def __create_temp_unit_errors_folder_files(self, output_folder, number_of_files):
        '''
        Process:
            We want to preserve the original unit_errors folder if it exists,
            so we wil rename it.
            Then we will make a new unit_errors folder and fill it with a bunch of 
            dummy files.
            A dummy file for non_zero_exit_codes.log will also be created.
        Input:
            output_folder: the root output folder (ie. /outputs/gms_example_unit_tests/)
            number_of_files: how many dummy files to create
        Returns:
            True if the 'unit_errors' folder did original exist and needs to be renamed back.
            False if the 'unit_errors' folder never existed in the first place.
        '''
        
        ue_folder_preexists = False
        
        if (not os.path.isdir(output_folder)):
            raise Exception(f"unit test root folder of {output_folder} does not exist")
        
        ue_folder = os.path.join(output_folder, "unit_errors")
        temp_ue_folder = ue_folder + "_temp"
        if (os.path.isdir(ue_folder)):
            ue_folder_preexists = True
            os.rename(ue_folder, temp_ue_folder)
        
        os.mkdir(ue_folder)
        
        for i in range(0, number_of_files):
            file_name = "sample_" + str(i) + ".txt"
            file_path = os.path.join(ue_folder, file_name)
            with open(file_path, 'w') as fp:
                pass
        
        return ue_folder_preexists
        
        
    def __remove_temp_unit_errors_folder(self, output_folder):
        '''
        Process:
            We want to preserve the original unit_errors folder if it exists,
            so we will delete our temp unit test version of 'unit_errors', and rename
            the original back to 'unit_errors'
            Note.. it is possible the temp folder does not exist,
            but we don't need to error out on it. Sometimes we got here by a try/catch cleanup
        Input:
            output_folder: the root output folder (ie. /outputs/gms_example_unit_tests/)
        Returns:
            nothing
        '''

        ue_folder = os.path.join(output_folder, "unit_errors")        
        if (os.path.isdir(ue_folder)):
            shutil.rmtree(ue_folder)       
        
        temp_ue_folder = ue_folder + "_temp"        
        if (os.path.isdir(temp_ue_folder)):
            os.rename(temp_ue_folder, ue_folder)
    

# Test Cases for Helper funcitons:

    def test_create_temp_unit_errors_folder_files(self):

        # Here we are testing our helper function to see if it raise exceptions appropriately with a bad path. 
        # In this case, we want the exception to be raised if there is an invalid path. 

        params = self.params["invalid_path"]
        invalid_folder = params["fim_dir"]

        with pytest.raises(Exception) as e_info:
            self.__create_temp_unit_errors_folder_files(invalid_folder, 4)
    
    
    def test_remove_temp_unit_errors_folder(self):
        
        # Test of out helper function to see if the temp folder was removed. 

        params = self.params["valid_data"].copy()

        self.__create_temp_unit_errors_folder_files(params["fim_dir"], 2)

        self.__remove_temp_unit_errors_folder(params["fim_dir"])

        temp_folder_created = os.path.join(params["fim_dir"], "unit_errors") + "_temp"

        assert os.path.exists(temp_folder_created) == False

