#!/usr/bin/env python3

import sys
import os
from multiprocessing import Pool
import argparse
import traceback
import rasterio
import geopandas as gpd
import pandas as pd
import shutil
from rasterio.features import shapes
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from inundation import inundate
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION,VIZ_PROJECTION
from utils.shared_functions import getDriver

INPUTS_DIR = r'/data/inputs'
magnitude_list = ['action', 'minor', 'moderate','major', 'record']

# Define necessary variables for inundation()
hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'
mask_type, catchment_poly = 'huc', ''


def generate_categorical_fim(fim_run_dir, source_flow_dir, output_cat_fim_dir, number_of_jobs, depthtif, log_file):

    no_data_list = []
    procs_list = []

    source_flow_dir_list = os.listdir(source_flow_dir)
    output_flow_dir_list = os.listdir(fim_run_dir)

    # Log missing hucs
    missing_hucs = list(set(source_flow_dir_list) - set(output_flow_dir_list))
    missing_hucs = [huc for huc in missing_hucs if "." not in huc]
    if len(missing_hucs) > 0:
        f = open(log_file, 'a+')
        f.write(f"Missing hucs from output directory: {', '.join(missing_hucs)}\n")
        f.close()

    # Loop through matching huc directories in the source_flow directory
    matching_hucs = list(set(output_flow_dir_list) & set(source_flow_dir_list))
    for huc in matching_hucs:

        if "." not in huc:

            # Get list of AHPS site directories
            ahps_site_dir = os.path.join(source_flow_dir, huc)
            ahps_site_dir_list = os.listdir(ahps_site_dir)

            # Map paths to HAND files needed for inundation()
            fim_run_huc_dir = os.path.join(fim_run_dir, huc)
            rem = os.path.join(fim_run_huc_dir, 'rem_zeroed_masked.tif')
            catchments = os.path.join(fim_run_huc_dir, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            hydroTable =  os.path.join(fim_run_huc_dir, 'hydroTable.csv')

            exit_flag = False  # Default to False.

            # Check if necessary data exist; set exit_flag to True if they don't exist
            for f in [rem, catchments, hydroTable]:
                if not os.path.exists(f):
                    no_data_list.append(f)
                    exit_flag = True

            # Log missing data
            if exit_flag == True:
                f = open(log_file, 'a+')
                f.write(f"Missing data for: {fim_run_huc_dir}\n")
                f.close()

            # Map path to huc directory inside out output_cat_fim_dir
            cat_fim_huc_dir = os.path.join(output_cat_fim_dir, huc)
            if not os.path.exists(cat_fim_huc_dir):
                os.mkdir(cat_fim_huc_dir)

            # Loop through AHPS sites
            for ahps_site in ahps_site_dir_list:
                # map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj)
                ahps_site_parent = os.path.join(ahps_site_dir, ahps_site)
                thresholds_dir_list = os.listdir(ahps_site_parent)

                # Map parent directory for all inundation output filesoutput files.
                cat_fim_huc_ahps_dir = os.path.join(cat_fim_huc_dir, ahps_site)
                if not os.path.exists(cat_fim_huc_ahps_dir):
                    os.mkdir(cat_fim_huc_ahps_dir)

                # Loop through thresholds/magnitudes and define inundation output files paths
                for magnitude in thresholds_dir_list:

                    if "." not in magnitude:

                        magnitude_flows_csv = os.path.join(ahps_site_parent, magnitude, 'ahps_' + ahps_site + '_huc_' + huc + '_flows_' + magnitude + '.csv')

                        if os.path.exists(magnitude_flows_csv):

                            output_extent_grid = os.path.join(cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_extent.tif')

                            if depthtif:
                                output_depth_grid = os.path.join(cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_depth.tif')
                            else:
                                output_depth_grid = None

                            # Append necessary variables to list for multiprocessing.
                            procs_list.append([rem, catchments, magnitude_flows_csv, huc, hydroTable, output_extent_grid, output_depth_grid, ahps_site, magnitude, log_file])

    # Initiate multiprocessing
    print(f"Running inundation for {len(procs_list)} sites using {number_of_jobs} jobs")
    pool = Pool(number_of_jobs)
    pool.map(run_inundation, procs_list)


def run_inundation(args):

    rem                 = args[0]
    catchments          = args[1]
    magnitude_flows_csv = args[2]
    huc                 = args[3]
    hydroTable          = args[4]
    output_extent_grid  = args[5]
    output_depth_grid   = args[6]
    ahps_site           = args[7]
    magnitude           = args[8]
    log_file            = args[9]

    try:
        inundate(rem,catchments,catchment_poly,hydroTable,magnitude_flows_csv,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=output_extent_grid,inundation_polygon=None,
                 depths=output_depth_grid,out_raster_profile=None,out_vector_profile=None,quiet=True
                )
    
    except Exception:
        # Log errors and their tracebacks
        f = open(log_file, 'a+')
        f.write(f"{output_extent_grid} - inundation error: {traceback.format_exc()}\n")
        f.close()

    #Inundation.py appends the huc code to the supplied output_extent_grid.
    #Modify output_extent_grid to match inundation.py saved filename. 
    #Search for this file, if it didn't create, send message to log file.
    base_file_path,extension = os.path.splitext(output_extent_grid)
    saved_extent_grid_filename = "{}_{}{}".format(base_file_path,huc,extension)
    if not os.path.exists(saved_extent_grid_filename):
        with open(log_file, 'a+') as f:
            f.write('FAILURE_huc_{}:{}:{} map failed to create\n'.format(huc,ahps_site,magnitude))
    elif os.path.exists(saved_extent_grid_filename):
        with open(log_file, 'a+') as f:
            f.write('SUCCESS_huc_{}:{}:{} map created\n'.format(huc,ahps_site,magnitude))
        
def post_process_cat_fim_for_viz(number_of_jobs, output_cat_fim_dir, nws_lid_attributes_filename, log_file):

    # Create workspace
    gpkg_dir = os.path.join(output_cat_fim_dir, 'gpkg')
    if not os.path.exists(gpkg_dir):
        os.mkdir(gpkg_dir)

    
    #Find the FIM version
    norm_path = os.path.normpath(output_cat_fim_dir)
    cat_fim_dir_parts = norm_path.split(os.sep)
    [fim_version] = [part for part in cat_fim_dir_parts if part.startswith('fim_3')]
    merged_layer = os.path.join(output_cat_fim_dir, 'catfim_library.shp')

    if not os.path.exists(merged_layer): # prevents appending to existing output

        huc_ahps_dir_list = os.listdir(output_cat_fim_dir)
        skip_list=['errors','logs','gpkg',merged_layer]

        for magnitude in magnitude_list:

            procs_list = []

            # Loop through all categories
            for huc in huc_ahps_dir_list:

                if huc not in skip_list:

                    huc_dir = os.path.join(output_cat_fim_dir, huc)
                    ahps_dir_list = os.listdir(huc_dir)

                    # Loop through ahps sites
                    for ahps_lid in ahps_dir_list:
                        ahps_lid_dir = os.path.join(huc_dir, ahps_lid)

                        extent_grid = os.path.join(ahps_lid_dir, ahps_lid + '_' + magnitude + '_extent_' + huc + '.tif')

                        if os.path.exists(extent_grid):
                            procs_list.append([ahps_lid, extent_grid, gpkg_dir, fim_version, huc, magnitude, nws_lid_attributes_filename])

                        else:
                            try:
                                f = open(log_file, 'a+')
                                f.write(f"Missing layers: {extent_gpkg}\n")
                                f.close()
                            except:
                                pass

            # Multiprocess with instructions
            pool = Pool(number_of_jobs)
            pool.map(reformat_inundation_maps, procs_list)

        # Merge all layers
        print(f"Merging {len(os.listdir(gpkg_dir))} layers...")

        for layer in os.listdir(gpkg_dir):

            diss_extent_filename = os.path.join(gpkg_dir, layer)

            # Open diss_extent
            diss_extent = gpd.read_file(diss_extent_filename)
            diss_extent['viz'] = 'yes'

            # Write/append aggregate diss_extent
            if os.path.isfile(merged_layer):
                diss_extent.to_file(merged_layer,driver=getDriver(merged_layer),index=False, mode='a')
            else:
                diss_extent.to_file(merged_layer,driver=getDriver(merged_layer),index=False)

            del diss_extent

        shutil.rmtree(gpkg_dir)

    else:
        print(f"{merged_layer} already exists.")


def reformat_inundation_maps(args):

    try:
        lid                         = args[0]
        grid_path                   = args[1]
        gpkg_dir                    = args[2]
        fim_version                 = args[3]
        huc                         = args[4]
        magnitude                   = args[5]
        nws_lid_attributes_filename = args[6]

        # Convert raster to to shapes
        with rasterio.open(grid_path) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = ({'properties': {'extent': 1}, 'geometry': s} for i, (s, v) in enumerate(shapes(image, mask=mask,transform=src.transform)))

        # convert list of shapes to polygon
        extent_poly  = gpd.GeoDataFrame.from_features(list(results), crs=PREP_PROJECTION)

        # Dissolve polygons
        extent_poly_diss = extent_poly.dissolve(by='extent')

        # Update attributes
        extent_poly_diss = extent_poly_diss.reset_index(drop=True)
        extent_poly_diss['ahps_lid'] = lid
        extent_poly_diss['magnitude'] = magnitude
        extent_poly_diss['version'] = fim_version
        extent_poly_diss['huc'] = huc

        # Project to Web Mercator
        extent_poly = extent_poly.to_crs(VIZ_PROJECTION)

        # Join attributes
        nws_lid_attributes_table = pd.read_csv(nws_lid_attributes_filename, dtype={'huc':str})
        nws_lid_attributes_table = nws_lid_attributes_table.loc[(nws_lid_attributes_table.magnitude==magnitude) & (nws_lid_attributes_table.nws_lid==lid)]


        extent_poly_diss = extent_poly_diss.merge(nws_lid_attributes_table, left_on=['ahps_lid','magnitude','huc'], right_on=['nws_lid','magnitude','huc'])

        extent_poly_diss = extent_poly_diss.drop(columns='nws_lid')

        # Save dissolved multipolygon
        handle = os.path.split(grid_path)[1].replace('.tif', '')

        diss_extent_filename = os.path.join(gpkg_dir, handle + "_dissolved.gpkg")

        extent_poly_diss["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in extent_poly_diss["geometry"]]

        if not extent_poly_diss.empty:

            extent_poly_diss.to_file(diss_extent_filename,driver=getDriver(diss_extent_filename),index=False)

    except Exception as e:
        # Log and clean out the gdb so it's not merged in later
        try:
            f = open(log_file, 'a+')
            f.write(str(diss_extent_filename) + " - dissolve error: " + str(e))
            f.close()
        except:
            pass


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-s', '--source-flow-dir',help='Path to directory containing flow CSVs to use to generate categorical FIM.',required=True, default="")
    parser.add_argument('-o', '--output-cat-fim-dir',help='Path to directory where categorical FIM outputs will be written.',required=True, default="")
    parser.add_argument('-j','--number-of-jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    parser.add_argument('-depthtif','--write-depth-tiff',help='Using this option will write depth TIFFs.',required=False, action='store_true')

    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    source_flow_dir = args['source_flow_dir']
    output_cat_fim_dir = args['output_cat_fim_dir']
    number_of_jobs = int(args['number_of_jobs'])
    depthtif = args['write_depth_tiff']


    # Create output directory
    if not os.path.exists(output_cat_fim_dir):
        os.mkdir(output_cat_fim_dir)

    # Create log directory
    log_dir = os.path.join(output_cat_fim_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    # Create error log path
    log_file = os.path.join(log_dir, 'errors.log')

    # Map path to points with attributes
    nws_lid_attributes_filename = os.path.join(source_flow_dir, 'nws_lid_attributes.csv')

    print("Generating Categorical FIM")
    generate_categorical_fim(fim_run_dir, source_flow_dir, output_cat_fim_dir, number_of_jobs, depthtif,log_file)

    print("Aggregating Categorical FIM")
    post_process_cat_fim_for_viz(number_of_jobs, output_cat_fim_dir,nws_lid_attributes_filename,log_file)
