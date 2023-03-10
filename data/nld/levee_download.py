#!/usr/bin/env python3
import sys
import os
import re
import geopandas as gpd

sys.path += ['/foss_fim/src', '/foss_fim/data', '/foss_fim/tools']
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
from tools_shared_variables import INPUTS_DIR
from esri import ESRI_REST
epsg_code = re.search('\d+$', DEFAULT_FIM_PROJECTION_CRS).group()

def download_nld_lines():
    # Query REST service to download levee 'system routes'
    print("Downloading levee lines from the NLD...")
    nld_url = "https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/15/query"
    levees = ESRI_REST.query(nld_url, 
            f="json", where="1=1", returnGeometry="True", outFields="*", outSR=epsg_code, returnZ="True")
                   
    # Write levees to a single geopackage
    levees.to_file(os.path.join(INPUTS_DIR+'_crs5070', 'nld_vectors', f'nld_system_routes.gpkg'))



if __name__ == '__main__':

    download_nld_lines()

    # TODO: Add levee protected polygons to this file??
