#!/usr/bin/env python3

import inspect
import os
import sys

import argparse
import json
import warnings
import unittest

# importing python folders in other direcories
sys.path.append('/foss_fim/unit_tests/')
import unit_tests_utils as helpers

# importing python folders in other direcories
sys.path.append('/foss_fim/src/gms/')
import derive_level_paths


# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_Derive_level_paths(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):
    
        warnings.simplefilter('ignore')
        
        params_file_path = '/foss_fim/unit_tests/gms/derive_level_paths_params.json'    
    
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)


    # MUST start with the name of "test_"
    def test_Derive_level_paths_success(self):

        '''
        This NEEDS be upgraded to check the output, as well as the fact that all of the output files exist as expected.
        Most of the output test and internal tests with this function will test a wide variety of conditions.
        For subcalls to other py classes will not exist in this file, but the unittest file for the other python file.
        Only the basic return output value shoudl be tested to ensure it is as expected.
        For now, we are adding the very basic "happy path" test.
        '''
       
        # makes output readability easier and consistant with other unit tests       
        helpers.print_unit_test_function_header()
        
        params = self.params["valid_data"].copy()

        # Function Notes:
        # huc_ids no longer used, so it is not submitted
        # other params such as toNode_attribute and fromNode_attribute are defaulted and not passed into __main__, so I will
        # skip them here.
        actual = derive_level_paths.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                                       out_stream_network = params["out_stream_network"],
                                                       branch_id_attribute = params["branch_id_attribute"],
                                                       out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                                       headwaters_outfile = params["headwaters_outfile"],
                                                       catchments = params["catchments"],
                                                       catchments_outfile = params["catchments_outfile"],
                                                       branch_inlets_outfile = params["branch_inlets_outfile"],
                                                       reach_id_attribute = params["reach_id_attribute"],
                                                       verbose = params["verbose"] )
        
        #print(actual)
        
        # Later, we should check here that the files outputed to the file system, exist and are valid and those do not have to be new "test_" methods.
        # We can start using unit test "asserts" as we go as well.
        
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")        

    
    # Invalid Input stream
    # MUST start with the name of "test_"    
    def test_Derive_level_paths_invalid_input_stream_network(self):

        # makes output readability easier and consistant with other unit tests 
        helpers.print_unit_test_function_header()
        
        try:
        
            params = self.params["valid_data"].copy()
            params["in_stream_network"] = "some bad path"
          
            actual = derive_level_paths.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                                           out_stream_network = params["out_stream_network"],
                                                           branch_id_attribute = params["branch_id_attribute"],
                                                           out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                                           huc_id = params["huc_id"],
                                                           headwaters_outfile = params["headwaters_outfile"],
                                                           catchments = params["catchments"],
                                                           catchments_outfile = params["catchments_outfile"],
                                                           branch_inlets_outfile = params["branch_inlets_outfile"],
                                                           reach_id_attribute = params["reach_id_attribute"],
                                                           verbose = params["verbose"] )
                                                           
            raise AssertionError("Fail = excepted a thrown exception but did not get it but was received. Unit Test has 'failed'")
            
        except Exception:
            print()
            print(f"Test Success (failed as expected): {inspect.currentframe().f_code.co_name}")
            
        finally:
            print("*************************************************************")             
       
       
       
if __name__ == '__main__':

    print("*****************************")
    print(f"Start of {os.path.basename(__file__)} tests")
    print()
            
    unittest.main()
    
    print()    
    print(f"End of {os.path.basename(__file__)} tests")
    
