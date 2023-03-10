#!/usr/bin/env python3
import sys
import geopandas as gpd

sys.path.append('/foss_fim/data')
from esri import ESRI_REST
sys.path.append('/foss_fim/src')
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS

def download_nld_ln():
    # Query REST service to download levee 'system routes'
    nld_url = "https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/15/query"
    levees = ESRI_REST.query(nld_url, f="json", where="1=1", returnGeometry="True", outFields="*", outSR=DEFAULT_FIM_PROJECTION_CRS)

    # TODO: Load WBD to do a spatial join

    # TODO: Save levees to inputs



if __name__ == '__main__':

    download_nld_ln()

    # TODO: Add levee protected polygons to this file??
