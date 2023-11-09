#!/usr/bin/env python3

import json
import os
import unittest

import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import data.wbd.clip_vectors_to_wbd as src


class test_clip_vectors_to_wbd(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    # TODO New Test Case to check that files were being created on the file system
    @pytest.mark.skip(
        reason="pre clipped vector files are now generated using generate_pre_clip_fim_huc8.py and "
        "sent to the /inputs directory. Testing the clip_vectors_to_wbd.py is no longer necessary, "
        "as this script is not run during fim_pipeline.sh execution."
    )
    def test_subset_vector_layers_success(self):
        """
        This NEEDS be upgraded to check the output, as well as the fact that all of the output files exist.
        Most of the output test and internal tests with this function will test a wide variety of conditions.
        Only the basic return output value should be tested to ensure it is as expected.
        For now, we are adding the very basic "happy path" test.
        """

        params = self.params["valid_data"].copy()

        # There is no default return value.
        # For now we are happy if no exceptions are thrown.
        try:
            src.subset_vector_layers(
                subset_nwm_lakes=params["subset_nwm_lakes"],
                subset_nwm_streams=params["subset_nwm_streams"],
                hucCode=params["hucCode"],
                subset_nwm_headwaters=params["subset_nwm_headwaters"],
                wbd_buffer_filename=params["wbd_buffer_filename"],
                wbd_streams_buffer_filename=params["wbd_streams_buffer_filename"],
                wbd_filename=params["wbd_filename"],
                dem_filename=params["dem_filename"],
                dem_domain=params["dem_domain"],
                nwm_lakes=params["nwm_lakes"],
                nwm_catchments=params["nwm_catchments"],
                subset_nwm_catchments=params["subset_nwm_catchments"],
                nld_lines=params["nld_lines"],
                nld_lines_preprocessed=params["nld_lines_preprocessed"],
                landsea=params["landsea"],
                nwm_streams=params["nwm_streams"],
                subset_landsea=params["subset_landsea"],
                nwm_headwaters=params["nwm_headwaters"],
                subset_nld_lines=params["subset_nld_lines"],
                subset_nld_lines_preprocessed=params["subset_nld_lines_preprocessed"],
                wbd_buffer_distance=params["wbd_buffer_distance"],
                levee_protected_areas=params["levee_protected_areas"],
                subset_levee_protected_areas=params["subset_levee_protected_areas"],
            )

        except (RuntimeError, TypeError, NameError) as e_info:
            pytest.fail("Error in subset_vector_layers function", e_info)
