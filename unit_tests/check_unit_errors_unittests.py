#!/usr/bin/env python3

import inspect
import math
import os
import shutil
import sys

import json
import warnings
import unittest

sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other directories
sys.path.append('/foss_fim/src/')   # *** update your folder path here if required ***
import check_unit_errors as src
from utils.fim_enums import FIM_exit_codes

from utils.shared_variables import (UNIT_ERRORS_MIN_NUMBER_THRESHOLD,
                                    UNIT_ERRORS_MIN_PERCENT_THRESHOLD)


# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_check_unit_errors(unittest.TestCase):


    # CURRENT 
    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        warnings.simplefilter('ignore')
        try:        
            params_file_path = ut_helpers.get_params_filename(__file__)
        except FileNotFoundError as ex:
            print(f"params file not found. ({ex}). Check pathing and file name convention.")
            sys.exit(1)

        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    '''
    To make most of these unit test work, we have to do the following:
    - rename the current unit_errors directory (if it exists)
    - empty the unit_errors directory
    - create a bunch of dummy files in it
    - perform the unit test
    - delete the unit_errors directory
    - rename the original unit_test folder back to unit_tests
    '''

    def test_check_unit_errors_success_below_min_errors(self):

        # Expecting no errors
        # test to ensure the number of dummy files is less than the
        # overall min number of error files.

        params = self.params["valid_data"].copy()
        
        try:
            num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD - 1
        
            ue_folder_existed = self.__create_temp_unit_errors_folder_files(params["fim_dir"],
                                                                            num_dummy_files_reqd)
            expected_output = 0
            actual_output = src.check_unit_errors(params["fim_dir"], num_dummy_files_reqd)
            
            err_msg = "actual output does not match expected output"
            self.assertEqual(expected_output, actual_output, err_msg)
            
            if (ue_folder_existed):
                self.__remove_temp_unit_errors_folder(params["fim_dir"])
            
            print(f"Test Success: {inspect.currentframe().f_code.co_name}")
            print("*************************************************************")
            
        except BaseException as ex:
            
            print(f"Test Failed: {inspect.currentframe().f_code.co_name}")
            
            # no matter what, we have to put the unit_errors folders back to the way
            # is was, so try again if we have too
            self.__remove_temp_unit_errors_folder(params["fim_dir"])

            # re throw for the unit test engine            
            raise ex


    def test_check_unit_errors_fail_above_min_errors(self):

        # test to ensure the number of dummy files is more than the
        # overall min number of error files.

        # Expecting sys.exit of 62
        # we do expect this to fail and if it fails, it is successful
        params = self.params["valid_data"].copy()
        
        try:
            num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD + 1
        
            self.__create_temp_unit_errors_folder_files(params["fim_dir"], num_dummy_files_reqd)
            src.check_unit_errors(params["fim_dir"], num_dummy_files_reqd)
            
            raise Exception("Test Failed: expected sys.exit error but did not receive it")
            
        except BaseException as ex:
           
            if (str(FIM_exit_codes.EXCESS_UNIT_ERRORS.value) in str(ex)):
                print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
            else:
                print("ex value is")
                print(ex)
                raise Exception("Expected a system exit code but did not receive it")
          
        finally:
            # no matter what, we have to put the unit_errors folders back to the way
            # is was, so try again if we have too
            self.__remove_temp_unit_errors_folder(params["fim_dir"])


    def test_check_unit_errors_success_above_percent_errors(self):

        # Expecting no errors
        # test to ensure the number of dummy files is more than the
        # overall min number of error files.
        
        # we do expect this not to to fail as it is greater than 10 errors
        # but below the percent threshhold
        params = self.params["valid_data"].copy()
        
        try:
            num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD * 2
            
            print(f"num_dummy_files_reqd is {num_dummy_files_reqd}")
            ue_folder_existed = self.__create_temp_unit_errors_folder_files(params["fim_dir"],
                                                                            num_dummy_files_reqd)
            
            num_total_units = math.trunc(num_dummy_files_reqd * (100 / UNIT_ERRORS_MIN_PERCENT_THRESHOLD)) + 1
            expected_output = 0
            actual_output = src.check_unit_errors(params["fim_dir"], num_total_units)
            
            err_msg = "actual output does not match expected output"
            self.assertEqual(expected_output, actual_output, err_msg)
            
            if (ue_folder_existed):
                self.__remove_temp_unit_errors_folder(params["fim_dir"])
            
            print(f"Test Success: {inspect.currentframe().f_code.co_name}")
            print("*************************************************************")
            
        except BaseException as ex:
            
            print(f"Test Failed: {inspect.currentframe().f_code.co_name}")
            
            # no matter what, we have to put the unit_errors folders back to the way
            # is was, so try again if we have too
            self.__remove_temp_unit_errors_folder(params["fim_dir"])

            # re throw for the unit test engine            
            raise ex


    def test_check_unit_errors_fail_below_percent_errors(self):

        # Expecting sys.exit of 62
        # test to ensure the number of dummy files is more than the
        # overall min number of error files.
        
        # we do expect this to fail as it is greater than 10 errors
        # AND below the percent threshhold (more percent errors than the threshold)
        params = self.params["valid_data"].copy()
        
        try:
            num_dummy_files_reqd = UNIT_ERRORS_MIN_NUMBER_THRESHOLD * 2
            
            self.__create_temp_unit_errors_folder_files(params["fim_dir"], num_dummy_files_reqd)
            
            num_total_units = math.trunc(num_dummy_files_reqd * (100 / UNIT_ERRORS_MIN_PERCENT_THRESHOLD)) - 10
            print(f" is {num_total_units}")

            src.check_unit_errors(params["fim_dir"], num_total_units)
            raise Exception("Test Failed: expected sys.exit error but did not receive it")
                    
        except BaseException as ex:
           
            if (str(FIM_exit_codes.EXCESS_UNIT_ERRORS.value) in str(ex)):
                print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
                print("*************************************************************")                             
            else:
                print("ex value is")
                print(ex)
                raise Exception("Expected a system exit code but did not receive it")
          
        finally:
            # no matter what, we have to put the unit_errors folders back to the way
            # is was, so try again if we have too
            self.__remove_temp_unit_errors_folder(params["fim_dir"])
            

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
      

    # ***** REMOVE THIS BLOCK IF YOU ARE NOT USING IT ***
    #       EXAMPLE SUCCESSFUL FAIL    
    # 
    #def test_subset_vector_layers_fail_invalid_stream_path(self):
    #    '''
    #    Notes about what the test is and the expected results (or expected exception if applicable)
    #    '''
    #   #global params_file
    #   helpers.print_unit_test_function_header()
        
    #   params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)

    #   params["nwm_streams"] = "some bad path"

    #   clip_vectors_to_wbd.subset_vector_layers(hucCode = params["hucCode"],
    #                                            nwm_streams_filename = params["nwm_streams"],
    #                                            nhd_streams_filename = params["nhd_streams"],
    #                                            etc, etc for each param)
    #   
       
    #    print(f"Test Success: {inspect.currentframe().f_code.co_name}")
    #    print("*************************************************************")
        
    # ***********************


if __name__ == '__main__':

    script_file_name = os.path.basename(__file__)

    print("*****************************")
    print(f"Start of {script_file_name} tests")
    print()
   
    unittest.main()
    
    print()    
    print(f"End of {script_file_name} tests")
    
