#!/usr/bin/env python3
import os
import json
import unittest
import pytest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import split_flows as src


class test_split_flows(unittest.TestCase):

    '''
    Allows the params to be loaded one and used for all test methods
    '''
    @classmethod
    def setUpClass(self):

        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

# Test Cases:

    # Ensure split_flows_filename & split_points_filename are created by the split_flows function 
    def test_split_flows_success(self):

        '''
        The /data/outputs/gms_example_unit_tests/<huc>/branches/<branchid>/demDerived_reaches_split_<branchid>.gpkg and
        /data/outputs/gms_example_unit_tests/<huc>/branches/<branchid>/demDerived_reaches_split_points_<branchid>.gpkg should not exit prior to this test.
        If the test is successful, these files will be created.
        '''

        params = self.params["valid_data"].copy() 

        # to setup the test, lets start by deleted the two expected output files to ensure
        # that they are regenerated
        if os.path.exists(params["split_flows_filename"]):
            os.remove(params["split_flows_filename"])
        if os.path.exists(params["split_points_filename"]):
            os.remove(params["split_points_filename"])

        error_msg = params["split_flows_filename"] + " does exist, when it should not (post os.remove call)"
        assert os.path.exists(params["split_flows_filename"]) == False, error_msg 
        
        error_msg = params["split_points_filename"] + " does exist, when it should not (post os.remove call)"
        assert os.path.exists(params["split_points_filename"]) == False, error_msg
       
        src.split_flows(max_length = params["max_length"],
                        slope_min = params["slope_min"],
                        lakes_buffer_input = params["lakes_buffer_input"],
                        flows_filename = params["flows_filename"],
                        dem_filename = params["dem_filename"],
                        split_flows_filename = params["split_flows_filename"],
                        split_points_filename = params["split_points_filename"],
                        wbd8_clp_filename = params["wbd8_clp_filename"],
                        lakes_filename = params["lakes_filename"],
                        nwm_streams_filename = params["nwm_streams_filename"])
       
        error_msg = params["split_flows_filename"] + " does not exist"
        assert os.path.exists(params["split_flows_filename"]) == True, error_msg 
        
        error_msg = params["split_points_filename"] + " does not exist"
        assert os.path.exists(params["split_points_filename"]) == True, error_msg

