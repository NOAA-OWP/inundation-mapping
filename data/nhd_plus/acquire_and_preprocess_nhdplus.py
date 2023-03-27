#!/usr/bin/env python3

import sys
sys.path.append('/foss_fim/data')
import argparse
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
from esri import ESRI_REST

def download_sinks(url:str, out_file:str):
    """
    Downloads sink data from REST service

    Parameters
    ----------
    url: str
        URL to REST server Query
    out_file: str
        Path to save output file
    """

    sinks = ESRI_REST.query(url, f="pjson", returnGeometry="true", outFields="*", outSR="5070")

    sinks.to_file(out_file, driver='GPKG', crs=DEFAULT_FIM_PROJECTION_CRS)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Acquire and preprocess NHDPlus data')
    parser.add_argument('-u', '--url', help='URL to REST service', type=str, required=True)
    parser.add_argument('-o', '--out-file', help='Output filename', type=str, required=True)

    args = vars(parser.parse_args())

    download_sinks(**args)

    # Example:
    # /foss_fim/data/nhd_plus/acquire_and_preprocess_nhdplus.py -u https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/NHDPlusV21/FeatureServer/0/query -o /data/inputs/nhdplus_vectors/sinks.gpkg
