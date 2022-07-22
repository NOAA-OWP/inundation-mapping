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
sys.path.append('/foss_fim/src/')   # *** update your folder path here if required ***
import <Your original source python file name> as src

# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_<Your original source python file name>(unittest.TestCase):

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
    def test_<method name you are about to test>_success(self):

        '''
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>
        
        Dev Notes: (which can be removed after you make this file)
            Remember... You need to validate the method output if there is any. However, if you have time, it
            is also recommended that you validate other outputs such as writing or updating file on the file 
            system, aka: Does the expected file exist. Don't worry about its contents.

        '''

        #global params_file
        
        params = self.params["valid_data"].copy()  #update "valid_data" value if you need to (aka.. more than one node)
        
       
        # for now we are happy if no exceptions are thrown.
        
        # < See the readme.md, clip_vectors_to_wbd_unittests.py or gms/derive_level_paths_unittests.py 
        # for examples.>
        # Replace this stub example with your own.
        # Try to use the same order to make it easier.
        # Remember, if the method accepts **params, then you can sent that in here as well.
        #   ie: my_py_class.my_method(** params)
        
        src.subset_vector_layers(hucCode = params["hucCode"],
                                    nwm_streams_filename = params["nwm_streams"],
                                    nhd_streams_filename = params["nhd_streams"],
                                    etc, etc for each param)
       
       
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")


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
    
