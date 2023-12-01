#!/usr/bin/env python3

import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers

import outputs_cleanup as src
from utils.shared_functions import FIM_Helpers as fh


class test_outputs_cleanup(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:

    def test_remove_deny_list_files_specific_branch_success(self):
        """
        This validates removal of files for a directory already pointing to a
        specific branch in a HUC
        """

        params = self.params["valid_specific_branch_data"].copy()

        # Gather all of the file names into an array from the deny_list
        deny_files = self.__get_deny_list_filenames(
            params["src_dir"], params["deny_list"], params["branch_id"]
        )

        # Test whether we have a list of files to check
        assert len(deny_files) > 0

        src.remove_deny_list_files(
            src_dir=params["src_dir"],
            deny_list=params["deny_list"],
            branch_id=params["branch_id"],
            verbose=params["verbose"],
        )

        assert self.__check_no_deny_list_files_exist(params["src_dir"], deny_files) is True

    def test_remove_deny_list_files_huc_level_success(self):
        """
        This validates removal of files for all files and subdirectory files.
        Normally used for covering all hucs and their branch zeros but
        can be anything
        """

        params = self.params["valid_directory_data"].copy()

        # Gather all of the file names into an array from the deny_list
        deny_files = self.__get_deny_list_filenames(
            params["src_dir"], params["deny_list"], params["branch_id"]
        )

        # Test whether we have a list of files to check
        assert len(deny_files) > 0

        src.remove_deny_list_files(
            src_dir=params["src_dir"],
            deny_list=params["deny_list"],
            branch_id=params["branch_id"],
            verbose=params["verbose"],
        )

        assert self.__check_no_deny_list_files_exist(params["src_dir"], deny_files) is True

    def test_remove_deny_list_skip_cleaning_success(self):
        """
        This validates removal of files for all files and subdirectory files.
        Normally used for covering all hucs and their branch zeros but
        can be anything
        """

        params = self.params["skip_clean"].copy()

        deny_files = self.__get_deny_list_filenames(
            params["src_dir"], params["deny_list"], params["branch_id"]
        )

        # Ensure we have a value of "None" for a deny_list value,
        # __get_deny_list_filenames returns an empty array if "None" is provided
        assert len(deny_files) == 0

        # This is tricky, as we're capturing the stdout (return statement) from remove_deny_list_files,
        # to verify the function is returning at the correct place, and not removing files
        # when we do not provide a deny list file. We set f to the io stream, and redirect it using
        # redirect_stdout.
        f = io.StringIO()
        with redirect_stdout(f):
            src.remove_deny_list_files(
                src_dir=params["src_dir"],
                deny_list=params["deny_list"],
                branch_id=params["branch_id"],
                verbose=params["verbose"],
            )

        # Get the stdout value of remove_deny_list_files and set it to skip_clean_out
        skip_clean_out = f.getvalue()

        # This string must match the print statement in /src/gms/outputs_cleanup.py, including the \n newline,
        # which occurs "behind the scenes" with every call to print() in Python
        assert skip_clean_out == "file clean via the deny list skipped\n"

    def test_remove_deny_list_files_invalid_src_directory(self):
        """
        Double check the src directory exists
        """

        params = self.params["valid_specific_branch_data"].copy()
        params["src_dir"] = "/data/does_no_exist"

        # We want an exception to be thrown here, if so, the test passes.
        with pytest.raises(Exception):
            src.remove_deny_list_files(
                src_dir=params["src_dir"],
                deny_list=params["deny_list"],
                branch_id=params["branch_id"],
                verbose=params["verbose"],
            )

    def test_remove_deny_list_files_invalid_deny_list_does_not_exist(self):
        """
        Double check the deny list exists
        """

        params = self.params["valid_specific_branch_data"].copy()
        params["deny_list"] = "invalid_file_name.txt"

        # We want an exception to be thrown here, if so, the test passes.
        with pytest.raises(Exception):
            src.remove_deny_list_files(
                src_dir=params["src_dir"],
                deny_list=params["deny_list"],
                branch_id=params["branch_id"],
                verbose=params["verbose"],
            )

    # Helper Functions:

    def __get_deny_list_filenames(self, src_dir, deny_list, branch_id):
        deny_list_files = []

        if deny_list == "None":
            return deny_list_files

        # Note: some of the deny_file_names might be a comment line
        # this will validate file exists
        deny_file_names = fh.load_list_file(deny_list.strip())

        for deny_file_name in deny_file_names:
            # Only add files to the list that do not start with a #
            deny_file_name = deny_file_name.strip()
            if deny_file_name.startswith("#"):
                continue

            deny_file_name = deny_file_name.replace("{}", branch_id)

            deny_list_files.append(deny_file_name)

        return deny_list_files

    def __check_no_deny_list_files_exist(self, src_dir, deny_array):
        found_files = []

        for file_name in deny_array:
            found_files.append(os.path.join(src_dir, file_name))

        for found_file in found_files:
            if os.path.exists(found_file):
                return False

        return True
