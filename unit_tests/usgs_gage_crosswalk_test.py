#!/usr/bin/env python3

import json
import os
import sys
import unittest

import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

from usgs_gage_crosswalk import GageCrosswalk


class test_usgs_gage_crosswalk(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    def test_GageCrosswalk_success(self):
        """
        Test whether the GageCrosswalk object can be instantiated, and test that the run_crosswalk method can
        successfully create the output table (usgs_elev_table.csv).
        """

        params = self.params[
            "valid_data"
        ].copy()  # update "valid_data" value if you need to (aka.. more than one node)

        # Delete the usgs_elev_table.csv if it exists
        if os.path.exists(params["output_table_filename"]):
            os.remove(params["output_table_filename"])

        # Verify the usgs_elev_table.csv was deleted
        msg = f'{params["output_table_filename"]} does exist, when it should have been deleted'
        assert os.path.exists(params["output_table_filename"]) is False, msg

        # Instantiate and run GageCrosswalk
        gage_crosswalk = GageCrosswalk(params["usgs_gages_filename"], params["branch_id"])

        # Run crosswalk
        gage_crosswalk.run_crosswalk(
            params["input_catchment_filename"],
            params["input_flows_filename"],
            params["dem_filename"],
            params["dem_adj_filename"],
            params["output_directory"],
        )

        # Make sure that the usgs_elev_table.csv was written
        msg = f'{params["output_table_filename"]} does not exist'
        assert os.path.exists(params["output_table_filename"]) is True, msg
