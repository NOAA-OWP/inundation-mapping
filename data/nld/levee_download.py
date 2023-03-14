#!/usr/bin/env python3
import sys
import os
import re
import geopandas as gpd
from tqdm import tqdm
from datetime import datetime
from shapely.geometry import LineString, MultiLineString

sys.path += ['/foss_fim/src', '/foss_fim/data', '/foss_fim/tools']
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
from tools_shared_variables import INPUTS_DIR
from esri import ESRI_REST
epsg_code = re.search('\d+$', DEFAULT_FIM_PROJECTION_CRS).group()
today = datetime.now().strftime('%y%m%d')
nld_vector_output = os.path.join(INPUTS_DIR, 'nld_vectors', f'System_Routes_NLDFS_5070_{today}.gpkg')
processed_nld_vector = os.path.join(INPUTS_DIR, 'nld_vectors', f'3d_nld_preprocessed_{today}.gpkg')
nld_protected_areas = os.path.join(INPUTS_DIR, 'nld_vectors', f'Leveed_Areas_NLDFS_5070_{today}.gpkg')

def download_nld_lines():
    '''
    First main function call for this module. Downloads levees from the National Levee Database 
    ESRI service, save the raw output for use in the levee masking algorithm, processes
    the levees to remove lines and vertices that have no elevation data, and saves the preprocessed
    levee geopackage for use in levee burning.
    '''
    # Query REST service to download levee 'system routes'
    print("Downloading levee lines from the NLD...")
    nld_url = "https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/15/query"
    levees = ESRI_REST.query(nld_url, 
            f="json", where="1=1", returnGeometry="true", outFields="*", outSR=epsg_code, returnZ="true")
                   
    # Write levees to a single geopackage
    levees.to_file(nld_vector_output, index=False, driver='GPKG')
    print(f"Levees written to file:\n{nld_vector_output}")

    # Spatial join to huc2
    print('Spatial join levees to HUC-2')
    huc2 = gpd.read_file(os.path.join(INPUTS_DIR, 'wbd', 'WBD_National_EPSG_5070.gpkg'), layer='WBDHU2')
    levees = gpd.sjoin(levees, huc2[['HUC2', 'geometry']], how='left')

    # Preprocess levees to remove features and vertices with no elevation
    print('Preprocess levees to remove features and vertices with no elevation ')
    process_levee_lines(levees, out_levees=processed_nld_vector)


def process_levee_lines(levee_gdf:gpd.GeoDataFrame, out_levees:str):
    '''
    Function for processing levee lines prior to rasterization and burning into 
    the DEM. NOTE: Do not use the output of this function for the levee protected
    area masking. This dataset will be incomplete since it filters out some levees
    that have no z-values.

    Parameters
    ----------
    levee_lines: gpd.GeoDataFrame
        Raw NLD vectors file.
    out_levees: str
        Path to right preprocessed levees.
    '''


    # Filter vertices that have z-values less than the minimum from levee geometry
    tqdm.pandas(desc='Removing null elevations')
    levee_gdf['geometry'] = levee_gdf.progress_apply(lambda row: remove_nulls(row.geometry, row.HUC2), axis=1)
    # Remove levees that have empty geometries resulting from the previous filter
    levee_gdf = levee_gdf[~levee_gdf.is_empty]
    levee_gdf.to_file(out_levees, index=False, driver='GPKG')
    print(f"Preprocessed levees written to \n{out_levees}")

def remove_nulls(geom:LineString, huc:str):
    '''
    Removes vertices from shapely LineString `geom` if they are less than `min_z`.

    Parameters
    ----------
    geom: shapely.geometry.LineString
        Shapely geometry from which to filter vertices.
    huc: str
        HUC. Can be any digit 2 or greater.

    Returns
    -------
    out_geom: shapely.geometry.LineString
        Filtered geometry.
    '''
    # Set min z based on HUC2
    huc2 = huc[:2] # works with any HUC digit code
    if huc2 in ['01','02','03','12']: # Coastal HUCs may have values near 0
        min_z = 0.01
    elif huc2 == '08': # Louisana below sea level
        min_z = -10.0
    else:
        min_z = 1.0 # Default set to 1 ft
    # Loop through the vertices
    out_geom = []
    part_geom = []
    skipped_vert = 0
    max_skipped_vert = 5
    for coord in geom.coords:
        skip_flag = False
        if coord[2] > min_z:
            # Convert units from feet to meters
            part_geom.append(tuple([coord[0], coord[1], coord[2]*0.3048]))
        elif skipped_vert < max_skipped_vert:
            # Allows a few (5) vertices to be skipped without forcing a multipart break.
            # This enables short sections of roads that cross levees to have the levee elevations
            # burned in to account for temporary flood walls not captured in the data.
            skip_flag = True
            skipped_vert += 1
        elif (len(part_geom) > 1) and (not skip_flag):   # Create a multipart geometry when there's a break in z-values
            out_geom.append(LineString(part_geom))
            part_geom = []
            skipped_vert = 0
    # Append the last segment
    if len(part_geom) > 1:
        out_geom.append(LineString(part_geom))
    # Compile LineString geometries into one multipart geometry
    if len(out_geom) >= 2:
        return MultiLineString(out_geom)
    elif (len(out_geom) == 1) and (len(out_geom[0].coords) > 1):
        return MultiLineString(out_geom)
    else:
        return None

def download_nld_poly():
    '''
    Second main function call for this module. Downloads levee protected areas from the National Levee Database 
    ESRI service and saves the raw output for use in the levee masking algorithm.
    '''
    # Query REST service to download levee 'system routes'
    print("Downloading levee protected areas from the NLD...")
    nld_area_url = "https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/14/query"
    # FYI to whomever takes the time to read this code, the resultRecordCount had to be set on this query because
    # the service was returning an error that turned out to be caused by the size of the request. Running the 
    # default max record count of 5000 was too large for polygons, so using resultRecordCount=2000 prevents the error.
    leveed_areas = ESRI_REST.query(nld_area_url, 
            f="json", where="1=1", returnGeometry="true", outFields="*", outSR=epsg_code, resultRecordCount=2000)
                   
    # Write levees to a single geopackage
    leveed_areas.to_file(nld_protected_areas, index=False, driver='GPKG')
    print(f"Levees written to file:\n{nld_protected_areas}")


if __name__ == '__main__':

    download_nld_lines()

    download_nld_poly()
