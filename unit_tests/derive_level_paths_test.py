#!/usr/bin/env python3

import os
import sys
import json
import unittest
import pytest

from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

# # importing python folders in other direcories
sys.path.append("/foss_fim/src/")
import derive_level_paths as src
import stream_branches
from utils.fim_enums import FIM_exit_codes as fec


class test_Derive_level_paths(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    def test_Derive_level_paths_success_all_params(self):
        """
        This test includes all params with many optional parms being set to the default value of the function
        """

        params = self.params["valid_data"].copy()

        # Notes:
        # Other params such as toNode_attribute and fromNode_attribute are defaulted and not passed into __main__ ,
        # so we skip them here.
        # Returns GeoDataframe (the nwm_subset_streams_levelPaths_dissolved.gpkg)
        actual_df = src.Derive_level_paths(
            in_stream_network=params["in_stream_network"],
            buffer_wbd_streams=params["buffer_wbd_streams"],
            wbd=params["wbd"],
            out_stream_network=params["out_stream_network"],
            branch_id_attribute=params["branch_id_attribute"],
            out_stream_network_dissolved=params["out_stream_network_dissolved"],
            headwaters_outfile=params["headwaters_outfile"],
            catchments=params["catchments"],
            catchments_outfile=params["catchments_outfile"],
            branch_inlets_outfile=params["branch_inlets_outfile"],
            reach_id_attribute=params["reach_id_attribute"],
            verbose=params["verbose"],
        )

        # test data type being return is as expected. Downstream code might to know that type
        assert isinstance(actual_df, stream_branches.StreamNetwork)

        # **** NOTE: Based on 05030104
        # Test row count for dissolved level path GeoDataframe which is returned.
        actual_row_count = len(actual_df)
        expected_row_count = 4
        assert actual_row_count == expected_row_count

        # Test that output files exist as expected
        assert (
            os.path.exists(params["out_stream_network"]) == True
        ), f"Expected file {params['out_stream_network']} but it does not exist."
        assert (
            os.path.exists(params["out_stream_network_dissolved"]) == True
        ), f"Expected file {params['out_stream_network_dissolved']} but it does not exist."
        assert (
            os.path.exists(params["headwaters_outfile"]) == True
        ), f"Expected file {params['headwaters_outfile']} but it does not exist."
        assert (
            os.path.exists(params["catchments_outfile"]) == True
        ), f"Expected file {params['catchments_outfile']} but it does not exist."
        assert (
            os.path.exists(params["catchments_outfile"]) == True
        ), f"Expected file {params['catchments_outfile']} but it does not exist."
        assert (
            os.path.exists(params["branch_inlets_outfile"]) == True
        ), f"Expected file {params['branch_inlets_outfile']} but it does not exist."

    # Invalid Input stream for demo purposes. Normally, you would not have this basic of a test (input validation).
    def test_Derive_level_paths_invalid_input_stream_network(self):
        # NOTE: As we are expecting an exception, we use pytest.raises(Exception).

        params = self.params["valid_data"].copy()
        params["in_stream_network"] = "some bad path"

        with pytest.raises(Exception) as e_info:
            actual = src.Derive_level_paths(
                in_stream_network=ut_helpers.json_concat(
                    params, "outputDestDir", "in_stream_network"
                ),
                out_stream_network=params["out_stream_network"],
                branch_id_attribute=params["branch_id_attribute"],
                out_stream_network_dissolved=params["out_stream_network_dissolved"],
                huc_id=params["huc_id"],
                headwaters_outfile=params["headwaters_outfile"],
                catchments=params["catchments"],
                catchments_outfile=params["catchments_outfile"],
                branch_inlets_outfile=params["branch_inlets_outfile"],
                reach_id_attribute=params["reach_id_attribute"],
                verbose=params["verbose"],
            )
