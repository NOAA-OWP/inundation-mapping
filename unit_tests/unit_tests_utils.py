#!/usr/bin/env python3

import errno
import os
import sys


class FIM_unit_test_helpers(object):
    @staticmethod
    def get_params_filename(unit_test_file_name):
        unittest_file_name = os.path.basename(unit_test_file_name)
        params_file_name = unittest_file_name.replace("_test.py", "_params.json")
        params_file_path = os.path.join(
            os.path.dirname(unit_test_file_name), params_file_name
        )

        if not os.path.exists(params_file_path):
            raise FileNotFoundError(f"{params_file_path} does not exist")

        return params_file_path

    @staticmethod
    def json_concat(obj, keyone, keytwo):
        concat_string = obj[keyone] + obj[keytwo]

        return concat_string
