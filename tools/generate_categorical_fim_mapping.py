#!/usr/bin/env python3

import sys
import os
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
import argparse
import traceback
import rasterio
import geopandas as gpd
import pandas as pd
import shutil
from rasterio.features import shapes
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION,VIZ_PROJECTION
from utils.shared_functions import getDriver
from gms_tools.mosaic_inundation import Mosaic_inundation
from gms_tools.inundate_gms import Inundate_gms

INPUTS_DIR = r'/data/inputs'
magnitude_list = ['action', 'minor', 'moderate','major', 'record']

# Define necessary variables for inundation()
hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'
mask_type, catchment_poly = 'huc', ''


def generate_categorical_fim(fim_run_dir, source_flow_dir, output_catfim_dir, 
                             job_number_huc, job_number_inundate, depthtif, log_file):

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
    
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in matching_hucs:
            if "." in huc:
                continue

            # Get list of AHPS site directories
            ahps_site_dir = os.path.join(source_flow_dir, huc)
            ahps_site_dir_list = os.listdir(ahps_site_dir)

            # Map paths to HAND files needed for inundation()
            fim_run_huc_dir = os.path.join(fim_run_dir, huc)

            # Map path to huc directory inside out output_catfim_dir
            cat_fim_huc_dir = os.path.join(output_catfim_dir, huc)
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

                            try:
                                executor.submit(run_inundation, [magnitude_flows_csv,
                                                                        huc,
                                                                        output_extent_grid,
                                                                        output_depth_grid,
                                                                        ahps_site, 
                                                                        magnitude,
                                                                        log_file,
                                                                        fim_run_dir,
                                                                        job_number_inundate])
                            except Exception as ex:
#                                    print(f"*** {ex}")
                                traceback.print_exc()
                                sys.exit(1)
                                

def run_inundation(args):
    
    magnitude_flows_csv = args[0]
    huc = args[1]
    output_extent_grid = args[2]
    output_depth_grid = args[3]
    ahps_site = args[4]
    magnitude = args[5]
    log_file = args[6]
    fim_run_dir = args[7]
    number_of_jobs = args[8]

    huc_dir = os.path.join(fim_run_dir, huc)
    try:
        print("Running Inundate_gms for " + huc)
        map_file = Inundate_gms(  hydrofabric_dir = fim_run_dir, 
                                         forecast = magnitude_flows_csv, 
                                         num_workers = number_of_jobs,
                                         hucs = huc,
                                         inundation_raster = output_extent_grid,
                                         inundation_polygon = None,
                                         depths_raster = None,
                                         verbose = False,
                                         log_file = None,
                                         output_fileNames = None )
        print("Mosaicking for " + huc)
        Mosaic_inundation( map_file,
                            mosaic_attribute = 'inundation_rasters',
                            mosaic_output = output_extent_grid,
                            mask = os.path.join(huc_dir,'wbd.gpkg'),
                            unit_attribute_name = 'huc8',
                            nodata = -9999,
                            workers = 1,
                            remove_inputs = False,
                            subset = None,
                            verbose = False )

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


def post_process_cat_fim_for_viz(number_of_jobs, output_catfim_dir, nws_lid_attributes_filename="", log_file="", fim_version=""):
    print("In post processing...")
    
    print(output_catfim_dir)
    
    # Create workspace
    gpkg_dir = os.path.join(output_catfim_dir, 'gpkg')
    if not os.path.exists(gpkg_dir):
        os.mkdir(gpkg_dir)

    # Find the FIM version
    merged_layer = os.path.join(output_catfim_dir, 'catfim_library.gpkg')

    if not os.path.exists(merged_layer): # prevents appending to existing output

        with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
            huc_ahps_dir_list = os.listdir(output_catfim_dir)
            skip_list=['errors','logs','gpkg','missing_files.txt',merged_layer]

            for magnitude in magnitude_list:
                # Loop through all categories
                for huc in huc_ahps_dir_list:
    
                    if huc not in skip_list:
                        huc_dir = os.path.join(output_catfim_dir, huc)
                        try:
                            ahps_dir_list = os.listdir(huc_dir)
                        except NotADirectoryError:
                            continue
    
                        # Loop through ahps sites
                        for ahps_lid in ahps_dir_list:
                            print(ahps_lid)
                            ahps_lid_dir = os.path.join(huc_dir, ahps_lid)
    
                            extent_grid = os.path.join(ahps_lid_dir, ahps_lid + '_' + magnitude + '_extent' + '.tif')
                            # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
                            nws_lid_attributes_filename = os.path.join(ahps_lid_dir, ahps_lid + '_attributes.csv')
                            
                            # Attributes are put into 'flows' during Flow-Based
                            if not os.path.exists(nws_lid_attributes_filename):
                                nws_lid_attributes_filename = nws_lid_attributes_filename.replace('mapping', 'flows')
                            
                            if os.path.exists(extent_grid):
                                try:
#                                    reformat_inundation_maps([ahps_lid, extent_grid, gpkg_dir, fim_version, huc, magnitude, nws_lid_attributes_filename])
                                    executor.submit(reformat_inundation_maps, [ahps_lid, extent_grid, gpkg_dir, fim_version, huc, magnitude, nws_lid_attributes_filename])
                                except Exception as ex:
                                    print("EXCEPTION")
                                    print(f"*** {ex}")
                                    traceback.print_exc() 
                                    f = open(log_file, 'a+')
                                    f.write(f"Missing layers: {extent_grid}\n")
                                    f.close()

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

        #shutil.rmtree(gpkg_dir)

    else:
        print(f"{merged_layer} already exists.")


def reformat_inundation_maps(args):

    try:
        lid = args[0]
        grid_path = args[1]
        gpkg_dir = args[2]
        fim_version = args[3]
        huc = args[4]
        magnitude = args[5]
        nws_lid_attributes_filename = args[6]
        print(nws_lid_attributes_filename)

        # Convert raster to to shapes
        with rasterio.open(grid_path) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = ({'properties': {'extent': 1}, 'geometry': s} for i, (s, v) in enumerate(shapes(image, mask=mask,transform=src.transform)))

        # Convert list of shapes to polygon
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
        extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

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
            print("NOT EMPTY")
            extent_poly_diss.to_file(diss_extent_filename,driver=getDriver(diss_extent_filename),index=False)

    except Exception as e:
        # Log and clean out the gdb so it's not merged in later
        try:
            print("EXCEPTION HERE")
            print(e)
            f = open(log_file, 'a+')
            f.write(str(diss_extent_filename) + " - dissolve error: " + str(e))
            f.close()
        except:
            pass


def manage_catfim_mapping(fim_run_dir, source_flow_dir, output_catfim_dir, 
                          job_number_huc, job_number_inundate, overwrite, depthtif):
    
    # Create output directory
    if not os.path.exists(output_catfim_dir):
        os.mkdir(output_catfim_dir)

    # Create log directory
    log_dir = os.path.join(output_catfim_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    # Create error log path
    log_file = os.path.join(log_dir, 'errors.log')

    # Map path to points with attributes
    nws_lid_attributes_filename = os.path.join(source_flow_dir, 'nws_lid_attributes.csv')
    total_number_jobs = job_number_huc * job_number_inundate

    print("Generating Categorical FIM")
    generate_categorical_fim(fim_run_dir,
                             source_flow_dir, 
                             output_catfim_dir, 
                             job_number_huc,
                             job_number_inundate,
                             depthtif,
                             log_file)

    print("Aggregating Categorical FIM")
    # Get fim_version.
    fim_version = os.path.basename(os.path.normpath(fim_run_dir)).replace('fim_','').replace('_ms_c', '').replace('_', '.')
    post_process_cat_fim_for_viz(total_number_jobs, 
                                 output_catfim_dir, 
                                 nws_lid_attributes_filename, 
                                 log_file, 
                                 fim_version)


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-s', '--source-flow-dir',help='Path to directory containing flow CSVs to use to generate categorical FIM.',required=True, default="")
    parser.add_argument('-o', '--output-catfim-dir',help='Path to directory where categorical FIM outputs will be written.',required=True, default="")
    parser.add_argument('-j','--number-of-jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    parser.add_argument('-depthtif','--write-depth-tiff',help='Using this option will write depth TIFFs.',required=False, action='store_true')

    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    source_flow_dir = args['source_flow_dir']
    output_catfim_dir = args['output_catfim_dir']
    number_of_jobs = int(args['number_of_jobs'])
    depthtif = args['write_depth_tiff']
    
    manage_catfim_mapping(fim_run_dir, source_flow_dir, output_catfim_dir, number_of_jobs, depthtif)
