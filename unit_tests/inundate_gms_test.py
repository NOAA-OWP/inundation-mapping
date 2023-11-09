#!/usr/bin/env python3

import json
import os
import sys
import unittest

import inundate_gms as src
import pandas as pd
import pytest
from unit_tests_utils import FIM_unit_test_helpers as ut_helpers


class test_inundate_gms(unittest.TestCase):

    """
    Allows the params to be loaded one and used for all test methods
    """

    @classmethod
    def setUpClass(self):
        params_file_path = ut_helpers.get_params_filename(__file__)
        with open(params_file_path) as params_file:
            self.params = json.load(params_file)

    # Test Cases:
    def test_Inundate_gms_create_inundation_raster_directory_single_huc_success(self):
        """
        Test for creating a gms inundation rasters, not a depth raster and no
        inundation_polygons.

        This test is essentially testing the Inundate_gms function, and the creation of a
        the "output_fileNames" .csv file.
        """

        params = self.params["valid_data_inudation_raster_single_huc"].copy()

        # Clear previous outputs, if they exist.
        for file in os.listdir(params["hydrofabric_dir"]):
            if file.endswith(".tif"):
                os.remove(os.path.join(params["hydrofabric_dir"], file))

        if os.path.isfile(params["output_fileNames"]):
            os.remove(params["output_fileNames"])

        # Test the Inundate_gms function
        output_fileNames_df = src.Inundate_gms(
            hydrofabric_dir=params["hydrofabric_dir"],
            forecast=params["forecast"],
            num_workers=params["num_workers"],
            hucs=params["hucs"],
            inundation_raster=params["inundation_raster"],
            inundation_polygon=params["inundation_polygon"],
            depths_raster=params["depths_raster"],
            verbose=params["verbose"],
            log_file=None,
            output_fileNames=params["output_fileNames"],
        )

        # Check if output files df has records.
        assert len(output_fileNames_df) > 0, "Expected as least one dataframe record"

    def test_Inundate_gms_create_inundation_rasters(self):
        """
        Test for creating a gms inundation rasters, not a depth raster and no
        inundation_polygons.
        This test is based on creating a raster based on a single huc and its branches
        within the output folder.
        """

        params = self.params["valid_data_inudation_raster_single_huc"].copy()

        # Check all output rasters exist.
        csv_out = pd.read_csv(params["output_fileNames"], skipinitialspace=True)

        csv_rasters = csv_out.inundation_rasters

        actual_rasters = []
        for file in os.listdir(params["hydrofabric_dir"]):
            if file.endswith(".tif"):
                actual_rasters.append(file)

        for csv_raster, actual_raster in zip(csv_rasters, actual_rasters):
            assert os.path.exists(
                os.path.join(params["hydrofabric_dir"], actual_raster)
            ), f"Inundation Raster {csv_raster} does not exist"

    # Logging removed from tools/inundate_gms.py as of 8/1/23
    # assert os.path.exists(params["log_file"]), "Log file expected and does not exist"
