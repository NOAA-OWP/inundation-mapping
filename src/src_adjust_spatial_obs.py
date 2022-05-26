import argparse
import geopandas as gpd
from geopandas.tools import sjoin
import os
import rasterio
import pandas as pd
import numpy as np
import sys
import json
import datetime as dt
from collections import deque
import multiprocessing
from multiprocessing import Pool
from src_roughness_optimization import update_rating_curve

from utils.shared_variables import DOWNSTREAM_THRESHOLD, ROUGHNESS_MIN_THRESH, ROUGHNESS_MAX_THRESH
'''
The script ingests a point database (.gpkg) representing observed FIM extent and flow data. Data is attributed with the HUC id, hydroid, and HAND value.

Processing
- Define CRS to use for initial geoprocessing and read wbd_path and points_layer.
- Spatially join the point layer and WBD polygons
- Define paths to hydroTable.csv, HAND raster, catchments raster, and synthetic rating curve JSON.
- Clip the points water_edge_df to the huc cathments polygons (for faster processing?)
- Define coords variable to be used in point raster value attribution and use point geometry to determine catchment raster pixel values
- Check that there are valid obs in the water_edge_df (not empty) and convert pandas series to dataframe to pass to update_rating_curve
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- points_layer:         .gpkg layer containing observed/truth FIM extent points and associated flow value 
- fim_directory:        fim directory containing individual HUC output dirs
- wbd_path:             path the watershed boundary dataset layer (HUC polygon boundaries)
- scale:                HUC6 or HUC8
- job_number:           number of multi-processing jobs to use
- debug_outputs_option: optional flag to output intermediate files for reviewing/debugging

Outputs
- water_edge_median_df: dataframe containing "hydroid", "flow", "submitter", "coll_time", "flow_unit", "layer", and median "HAND" value
'''

def process_points(args):

    fim_directory = args[0]
    huc = args[1]
    hand_path = args[2]
    catchments_path = args[3]
    catchments_poly_path = args[4]
    water_edge_df = args[5]
    output_src_json_file = args[6]
    htable_path = args[7]
    optional_outputs = args[8]

    ## Clip the points water_edge_df to the huc cathments polygons (for faster processing?)
    catch_poly = gpd.read_file(catchments_poly_path)
    catch_poly_crs = catch_poly.crs
    water_edge_df.to_crs(catch_poly_crs, inplace=True) 
    water_edge_df = gpd.clip(water_edge_df,catch_poly)

    ## Define coords variable to be used in point raster value attribution.
    coords = [(x,y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]

    ## Use point geometry to determine HAND raster pixel values.
    hand_src = rasterio.open(hand_path)
    hand_crs = hand_src.crs
    water_edge_df.to_crs(hand_crs)  # Reproject geodataframe to match hand_src. Should be the same, but this is a double check.
    water_edge_df['hand'] = [h[0] for h in hand_src.sample(coords)]
    hand_src.close()
    del hand_src, hand_crs,

    ## Use point geometry to determine catchment raster pixel values.
    catchments_src = rasterio.open(catchments_path)
    catchments_crs = catchments_src.crs
    water_edge_df.to_crs(catchments_crs)
    water_edge_df['hydroid'] = [c[0] for c in catchments_src.sample(coords)]
    catchments_src.close()
    del catchments_src, catchments_crs
    #water_edge_df.to_file(fim_directory + 'export_water_edge_df.gpkg', driver="GPKG",index=False)

    ## Check that there are valid obs in the water_edge_df (not empty)
    if not water_edge_df.empty:
        ## Get median HAND value for appropriate groups.
        water_edge_median_ds = water_edge_df.groupby(["hydroid", "flow", "submitter", "coll_time", "flow_unit","layer"])['hand'].median()

        ## Write user_supplied_n_vals to CSV for next step.
        pt_n_values_csv = os.path.join(fim_directory, huc, 'user_supplied_n_vals_' + huc + '.csv')
        water_edge_median_ds.to_csv(pt_n_values_csv)
        ## Convert pandas series to dataframe to pass to update_rating_curve
        water_edge_median_df = water_edge_median_ds.reset_index()
        del water_edge_median_ds

        ## Additional arguments for src_roughness_optimization
        source_tag = 'point_obs' # tag to use in source attribute field
        merge_prev_adj = True # merge in previous SRC adjustment calculations

        ## Call update_rating_curve() to perform the rating curve calibration.
        ## Still testing, so I'm having the code print out any exceptions.
        #log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs,merge_prev_adj,DOWNSTREAM_THRESHOLD)
        try:
            log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs, source_tag, merge_prev_adj, DOWNSTREAM_THRESHOLD)
        except Exception as e:
            print(e)
            log_text = 'ERROR!!!: HUC ' + str(huc) + ' --> ' + str(e)
    else:
        log_text = 'WARNING: ' + str(huc) + ': no valid observation points found within the huc catchments (skipping)'
    return(log_text)


def ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number, debug_outputs_option):

    ## Define CRS to use for initial geoprocessing.
    if scale == 'HUC8':
        hand_crs_default = 'EPSG:5070'
        wbd_layer = 'WBDHU8'
    else:
        hand_crs_default = 'EPSG:3857'
        wbd_layer = 'WBDHU6'

    ## Read wbd_path and points_layer.
    print("Reading WBD...")
    wbd_huc_read = gpd.read_file(wbd_path, layer=wbd_layer)
    print("Reading points layer...")
    points_layer_read = gpd.read_file(points_layer)

    ## Update CRS of points_layer_read.
    points_layer_read = points_layer_read.to_crs(hand_crs_default)
    wbd_huc_read = wbd_huc_read.to_crs(hand_crs_default)#[['geometry',scale]]

    ## Spatially join the two layers.
    print("Joining points to WBD...")
    water_edge_df = sjoin(points_layer_read, wbd_huc_read, op='within')
    del wbd_huc_read

    ## Convert to GeoDataFrame and add two columns for X and Y.
    gdf = gpd.GeoDataFrame(water_edge_df)
    gdf['X'] = gdf['geometry'].x
    gdf['Y'] = gdf['geometry'].y

    ## Extract information into dictionary.
    huc_list = []
    for index, row in gdf.iterrows():
        huc = row[scale]

        ## zfill to the appropriate scale to ensure leading zeros are present, if necessary.
        if scale == 'HUC8':
            huc = huc.zfill(8)
        else:
            huc = huc.zfill(6)

        if huc not in huc_list:
            huc_list.append(huc)
            log_file.write(str(huc) + '\n')
    del gdf

    procs_list = []  # Initialize list for mulitprocessing.

    ## Define paths to relevant HUC HAND data.
    for huc in huc_list:
        ## Check to make sure the HUC directory exists in the current fim_directory
        if not os.path.exists(os.path.join(fim_directory, huc)):
            log_file.write("FIM Directory for huc: " + str(huc) + " does not exist --> skipping SRC adjustments for this HUC (obs points found)\n")
            continue
        ## Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
        if scale == 'HUC8':
            hand_path = os.path.join(fim_directory, huc, 'rem_zeroed_masked.tif')
            catchments_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'src.json')
            catchments_poly_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
        else:
            hand_path = os.path.join(fim_directory, huc, 'hand_grid_' + huc + '.tif')
            catchments_path = os.path.join(fim_directory, huc, 'catchments_' + huc + '.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'rating_curves_' + huc + '.json')
            catchments_poly_path = os.path.join(fim_directory, huc, 'catchments_' + huc + '.gpkg')

        ## Check to make sure the previously defined files exist. Continue to next iteration if not and warn user.
        if not os.path.exists(hand_path):
            print("HAND grid for HUC " + huc + " not found (" + str(hand_path) + ") --> skipping this HUC")
            continue
        if not os.path.exists(catchments_path):
            print("Catchments grid for HUC " + huc + " not found (" + str(catchments_path) + ") --> skipping this HUC")
            continue
        if not os.path.isfile(output_src_json_file):
            print("ALERT: Rating Curve JSON file for " + huc + " does not exist. --> Will create a new file.")

        ## Define path to hydroTable.csv.
        htable_path = os.path.join(fim_directory, huc, 'hydroTable.csv')
        if not os.path.exists(htable_path):
            print("hydroTable for " + huc + " does not exist.")
            continue

        procs_list.append([fim_directory, huc, hand_path, catchments_path, catchments_poly_path, water_edge_df, output_src_json_file, htable_path, debug_outputs_option])

    with Pool(processes=job_number) as pool:
                log_output = pool.map(process_points, procs_list)
                log_file.writelines(["%s\n" % item  for item in log_output])


if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    ## Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve given a shapefile containing points of known water boundary.')
    parser.add_argument('-db','--points-layer',help='Path to points layer containing known water boundary locations',required=True)
    parser.add_argument('-fim_dir','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-wbd','--wbd-path',help='Path to national huc layer.',required=True)
    parser.add_argument('-debug','--extra-outputs',help='Optional: "True" or "False" --> Include intermediate output files for debugging/testing',default='False',required=False)
    parser.add_argument('-scale','--scale',help='Optional: HUC6 or HUC8 -- default is HUC8', default='HUC8',required=False)
    parser.add_argument('-dthresh','--downstream-thresh',help='Optional Override: distance in km to propogate modified roughness values downstream', default=DOWNSTREAM_THRESHOLD,required=False)
    parser.add_argument('-j','--job-number',help='Optional: Number of jobs to use',required=False,default=2)

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    points_layer = args['points_layer']
    fim_directory = args['fim_directory']
    wbd_path = args['wbd_path']
    debug_outputs_option = args['extra_outputs']
    scale = args['scale']
    ds_thresh_override = args['downstream_thresh']
    job_number = int(args['job_number'])

    assert os.path.isdir(fim_directory), 'ERROR: could not find the input fim_dir location: ' + str(fim_directory)

    if job_number > 2:
        print('WARNING!! - Using more than 2 jobs may result in memory allocation errors when working with very large obs pt database (>2Gb)')

    if scale not in ['HUC6', 'HUC8']:
        print("scale (-s) must be HUC6s or HUC8")
        quit()

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    if ds_thresh_override != DOWNSTREAM_THRESHOLD:
        print('ALERT!! - Using a downstream distance threshold value (' + str(float(ds_thresh_override)) + 'km) different than the default (' + str(DOWNSTREAM_THRESHOLD) + 'km) - interpret results accordingly')
        DOWNSTREAM_THRESHOLD = float(ds_thresh_override)

    ## Create output dir for log file
    output_dir = os.path.join(fim_directory,"logs","src_optimization")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
        
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Create log file for processing records
    print('This may take a few minutes...')
    sys.__stdout__ = sys.stdout
    log_file = open(os.path.join(output_dir,'log_rating_curve_adjust.log'),"w")
    log_file.write('#########################################################\n')
    log_file.write('Parameter Values:\n' + 'DOWNSTREAM_THRESHOLD= ' + str(DOWNSTREAM_THRESHOLD) + '\n' + 'ROUGHNESS_MIN_THRESH= ' + str( ROUGHNESS_MIN_THRESH) + '\n' + 'ROUGHNESS_MAX_THRESH=' + str(ROUGHNESS_MAX_THRESH) + '\n')
    log_file.write('#########################################################\n\n')
    log_file.write('START TIME: ' + str(begin_time) + '\n')

    ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number, debug_outputs_option)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()
