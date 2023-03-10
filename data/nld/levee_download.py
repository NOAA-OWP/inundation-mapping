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
                   
    # Load WBD 2-digit HUCs to do a spatial join
    print("Spatial join to HUC2...")
#    wbdhuc2 = gpd.read_file(os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), layer='WBDHU2')
    wbdhuc2 = gpd.read_file(os.path.join(INPUTS_DIR+'_crs5070', 'wbd', 'WBD_National.gpkg'), layer='WBDHU2')   ## TODO REMOVE ME
    levees = gpd.sjoin(levees, wbdhuc2[['HUC2', 'geometry']], how='left')

    # Save levees to inputs directory
    print("Saving GPKGs to inputs directory...")
    huc2s = list(levees['HUC2'].unique())
    for huc2 in huc2s:
        if huc2 != '02': continue   ## TODO REMOVE ME
        print(huc2)
        huc2_levees = levees.loc[levees['HUC2'] == huc2]
#        huc2_levees.to_file(os.path.join(INPUTS_DIR, 'nld_vectors', 'huc2_levee_lines', f'nld_preprocessed_{huc2}.gpkg'), 
#            driver="GPKG", index=False)
        huc2_levees.to_file(os.path.join(INPUTS_DIR+'_crs5070', 'nld_vectors', 'huc2_levee_lines', f'new_nld_preprocessed_{huc2}.gpkg'), 
            driver="GPKG", index=False)   ## TODO REMOVE ME
        break



if __name__ == '__main__':

    download_nld_lines()

    # TODO: Add levee protected polygons to this file??
