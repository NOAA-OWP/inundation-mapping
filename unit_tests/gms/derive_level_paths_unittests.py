#!/usr/bin/env python3

import inspect
import os
import sys

import json
import warnings
import unittest

# importing python folders in other direcories
sys.path.append('/foss_fim/unit_tests/')
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# importing python folders in other direcories
sys.path.append('/foss_fim/src/gms/')
import derive_level_paths as src
import stream_branches
from utils.fim_enums import FIM_exit_codes as fec


# NOTE: This goes directly to the function.
# Ultimately, it should emulate going through command line (not import -> direct function call)
class test_Derive_level_paths(unittest.TestCase):

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

    def test_Derive_level_paths_success_all_params(self):
    
        '''
        This test includes all params with many optional parms being set to the default value of the function
        '''
      
        params = self.params["valid_data"].copy()

        # Function Notes:
        # huc_ids no longer used, so it is not submitted
        # other params such as toNode_attribute and fromNode_attribute are defaulted and not passed into __main__, so I will
        # skip them here.
        # returns GeoDataframe (the nwm_subset_streams_levelPaths_dissolved.gpkg)
        actual_df = src.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                            out_stream_network = params["out_stream_network"],
                                            branch_id_attribute = params["branch_id_attribute"],
                                            out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                            headwaters_outfile = params["headwaters_outfile"],
                                            catchments = params["catchments"],
                                            catchments_outfile = params["catchments_outfile"],
                                            branch_inlets_outfile = params["branch_inlets_outfile"],
                                            reach_id_attribute = params["reach_id_attribute"],
                                            verbose = params["verbose"],
                                            drop_low_stream_orders=params["drop_low_stream_orders"])

        # -----------
        # test data type being return is as expected. Downstream code might to know that type
        self.assertIsInstance(actual_df, stream_branches.StreamNetwork)
        
        # -----------
        #**** NOTE: Based on 05030104         
        # Test row count for dissolved level path GeoDataframe which is returned.
        actual_row_count = len(actual_df) 
        expected_row_count = 58
        self.assertEqual(actual_row_count, expected_row_count)
        
        # -----------
        # Test that output files exist as expected
        if os.path.exists(params["out_stream_network"]) == False:
            raise Exception(params["out_stream_network"] + " does not exist")
            
        if os.path.exists(params["out_stream_network_dissolved"]) == False:
            raise Exception(params["out_stream_network_dissolved"] + " does not exist")

        if os.path.exists(params["headwaters_outfile"]) == False:
            raise Exception(params["headwaters_outfile"] + " does not exist")

        #if os.path.exists(params["catchments_outfile"]) == False:
        #    raise Exception(params["catchments_outfile"] + " does not exist")

        if os.path.exists(params["catchments_outfile"]) == False:
            raise Exception(params["catchments_outfile"] + " does not exist")

        if os.path.exists(params["branch_inlets_outfile"]) == False:
            raise Exception(params["branch_inlets_outfile"] + " does not exist")
       
        
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")        


    def test_Derive_level_paths_success_drop_low_stream_orders_not_submitted(self):
    
        '''
        This test includes most params but does not submit a drop_low_stream_orders param
        and it should default to "false", meaning no filtering out of stream orders 1 and 2.
        Note: Most path tests done in test_Derive_level_paths_success_all_params 
        and are not repeated here.
        '''
       
        params = self.params["valid_data"].copy()

        # Function Notes:
        # huc_ids no longer used, so it is not submitted
        # other params such as toNode_attribute and fromNode_attribute are defaulted and not passed into __main__, so I will
        # skip them here.
        # returns GeoDataframe (the nwm_subset_streams_levelPaths_dissolved.gpkg)
        actual_df = src.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                            out_stream_network = params["out_stream_network"],
                                            branch_id_attribute = params["branch_id_attribute"],
                                            out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                            headwaters_outfile = params["headwaters_outfile"],
                                            catchments = params["catchments"],
                                            catchments_outfile = params["catchments_outfile"],
                                            branch_inlets_outfile = params["branch_inlets_outfile"],
                                            reach_id_attribute = params["reach_id_attribute"],
                                            verbose = params["verbose"])

        # -----------
        # test data type being return is as expected. Downstream code might to know that type
        self.assertIsInstance(actual_df, stream_branches.StreamNetwork)
        
        # -----------
        #**** NOTE: Based on 05030104         
        # Test row count for dissolved level path GeoDataframe which is returned.
        actual_row_count = len(actual_df) 
        expected_row_count = 58    # should still be 58 with no filtering
        self.assertEqual(actual_row_count, expected_row_count)
        
        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")        
                

    def test_Derive_level_paths_success_drop_low_stream_orders_is_true(self):
    
        '''
        This test includes most params but does not submit a drop_low_stream_orders param
        and it should default to "false", meaning no filtering out of stream orders 1 and 2.
        Note: Most path tests done in test_Derive_level_paths_success_all_params 
        and are not repeated here.
        '''
        
        params = self.params["valid_data"].copy()

        params["drop_low_stream_orders"] = True

        # Function Notes:
        # huc_ids no longer used, so it is not submitted
        # other params such as toNode_attribute and fromNode_attribute are defaulted and not passed into __main__, so I will
        # skip them here.
        # returns GeoDataframe (the nwm_subset_streams_levelPaths_dissolved.gpkg)
        actual_df = src.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                            out_stream_network = params["out_stream_network"],
                                            branch_id_attribute = params["branch_id_attribute"],
                                            out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                            headwaters_outfile = params["headwaters_outfile"],
                                            catchments = params["catchments"],
                                            catchments_outfile = params["catchments_outfile"],
                                            branch_inlets_outfile = params["branch_inlets_outfile"],
                                            reach_id_attribute = params["reach_id_attribute"],
                                            verbose = params["verbose"],
                                            drop_low_stream_orders=params["drop_low_stream_orders"])

        # -----------
        # test data type being return is as expected. Downstream code might to know that type
        self.assertIsInstance(actual_df, stream_branches.StreamNetwork)
        
        # -----------
        #**** NOTE: Based on 05030104         
        # Test row count for dissolved level path GeoDataframe which is returned.
        actual_row_count = len(actual_df) 
        expected_row_count = 4
        self.assertEqual(actual_row_count, expected_row_count)

        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")        


    def test_Derive_level_paths_success_drop_low_stream_orders_no_branches_left(self):
    
        '''
        This test includes a huc that after stream orders 1 and 2 are dropped, no 
        branches are remaining (the original huc only had 1 and 2 reaches.

        # expecting an exception with a specific note
        # NOTE: this only works for a few hucs, one is 02030201

        '''
        
        params = self.params["dropped_stream_orders_no_branches_remaining"].copy()

        with self.assertRaises(SystemExit) as se:
            src.Derive_level_paths(in_stream_network = params["in_stream_network"],
                                    out_stream_network = params["out_stream_network"],
                                    branch_id_attribute = params["branch_id_attribute"],
                                    out_stream_network_dissolved = params["out_stream_network_dissolved"],
                                    headwaters_outfile = params["headwaters_outfile"],
                                    catchments = params["catchments"],
                                    catchments_outfile = params["catchments_outfile"],
                                    branch_inlets_outfile = params["branch_inlets_outfile"],
                                    reach_id_attribute = params["reach_id_attribute"],
                                    verbose = params["verbose"],
                                    drop_low_stream_orders=params["drop_low_stream_orders"])

        self.assertEqual(se.exception.code, fec.GMS_UNIT_NO_BRANCHES.value)

        print(f"Test Success: {inspect.currentframe().f_code.co_name}")
        print("*************************************************************")        

    
    # Invalid Input stream for demo purposes. Normally, you would not have this basic of a test (input validation).
    def test_Derive_level_paths_invalid_input_stream_network(self):

        # NOTE: As we are expecting an exception, this MUST have a try catch.
        #   for "success" tests (as above functions), they CAN NOT have a try/catch
        try:
        
            params = self.params["valid_data"].copy()
            params["in_stream_network"] = "some bad path"
          
            actual = src.Derive_level_paths(in_stream_network = params["in_stream_network"],
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
    
