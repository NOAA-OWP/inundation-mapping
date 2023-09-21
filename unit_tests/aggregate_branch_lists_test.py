#!/usr/bin/env python3

import json
import unittest

import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import aggregate_branch_lists as src


class test_aggregate_branch_lists(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    def test_aggregate_branch_lists_success(self):
        # global params_file

        params = self.params["valid_data"].copy()

        src.aggregate_branch_lists(
            output_dir=params["output_dir"],
            file_name=params["file_name"],
            output_file_name=params["output_file_name"],
        )
