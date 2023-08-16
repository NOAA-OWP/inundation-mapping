#!/usr/bin/env python3

import json
import os
import unittest

import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import generate_branch_list as src


class test_Generate_branch_list(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    def test_Generate_branch_list_success(self):
        params = self.params["valid_data"].copy()

        src.generate_branch_list(
            stream_network_dissolved=params["stream_network_dissolved"],
            branch_id_attribute=params["branch_id_attribute"],
            output_branch_list_file=params["output_branch_list_file"],
        )
