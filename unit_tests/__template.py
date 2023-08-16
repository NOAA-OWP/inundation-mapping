#!/usr/bin/env python3

import json
import os
import unittest

import pytest
import Your_original_source_python_file_name as src
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers


class test_Your_original_source_python_file_name(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    # MUST start with the name of "test_"
    # This is the (or one of the) valid test expected to pass
    def test_method_name_you_will_test(self):
        """
        < UPDATE THESE NOTES: to say what you are testing and what you are expecting.
          If there is no return value from the method, please say so.>

        Dev Notes: (which can be removed after you make this file)
            Remember... You need to validate the method output if there is any. However, if you have time, it
            is also recommended that you validate other outputs such as writing or updating file on the file
            system, aka: Does the expected file exist. Don't worry about its contents.
        """

        # global params_file
        params = self.params[
            "valid_data"
        ].copy()  # update "valid_data" value if you need to (aka.. more than one node)

        # for now we are happy if no exceptions are thrown.

        # See the readme.md, clip_vectors_to_wbd_test.py or gms/derive_level_paths_test.py for examples.
        # Replace this stub example with your own.
        # Try to use the same order to make it easier.
        # Remember, if the method accepts **params, then you can sent that in here as well.
        # ie: my_py_class.my_method(** params)

        src.subset_vector_layers(
            hucCode=params["hucCode"],
            nwm_streams_filename=params["nwm_streams"],
            etc=params["a_number"],
            etc2=params["a_list"],
        )

        # This is what we are actually testing-
        # An assert that evaluates as True passes, one that evaluates to False fails.
        # A message (string) can be added after the assert statement to provide detail on the case being tested, and
        # why it failed.
        assert os.path.exists(params["nwm_streams"]) == True, "The nwm_streams file does not exist"

    # EXAMPLE SUCCESSFUL TEST CASE WHICH CAPTURES AN EXCEPTION (FAILURE)

    def test_subset_vector_layers_fail_invalid_stream_path(self):
        """
        Notes about what the test is and the expected results (or expected exception if applicable)
        """

        params = self.params[
            "valid_data"  # update "valid_data" value if you need to (aka.. more than one node)
        ].copy()

        params["nwm_streams"] = "/some/bad/path/"

        with pytest.raises(Exception) as e_info:
            clip_vectors_to_wbd.subset_vector_layers(
                hucCode=params["hucCode"],
                nwm_streams_filename=params["nwm_streams"],
                etc=params["a_number"],
                etc2=params["a_list"],
            )
