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
import psycopg2 # python package for connecting to postgres
from dotenv import load_dotenv
import time

#import variables from .env file
load_dotenv()
CALIBRATION_DB_HOST = os.getenv("CALIBRATION_DB_HOST")
CALIBRATION_DB_USER_NAME = os.getenv("CALIBRATION_DB_USER_NAME")
CALIBRATION_DB_PASS = os.getenv("CALIBRATION_DB_PASS")

from utils.shared_variables import DOWNSTREAM_THRESHOLD, ROUGHNESS_MIN_THRESH, ROUGHNESS_MAX_THRESH
'''
The script imports a PostgreSQL database containing observed FIM extent points and associated flow data. This script attributes the point data with its hydroid and HAND values before passing a dataframe to the src_roughness_optimization.py workflow.

Processing
- Define CRS to use for initial geoprocessing and read wbd_path and points_layer.
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

    '''
    The funciton ingests geodataframe and attributes the point data with its hydroid and HAND values before passing a dataframe to the src_roughness_optimization.py workflow

    Processing
    - Extract x,y coordinates from geometry
    - Projects the point data to matching CRS for HAND and hydroid rasters
    - Samples the hydroid and HAND raster values for each point and stores the values in dataframe
    - Calculates the median HAND value for all points by hydroid
    '''

    fim_directory = args[0]
    huc = args[1]
    hand_path = args[2]
    catchments_path = args[3]
    catchments_poly_path = args[4]
    water_edge_df = args[5]
    output_src_json_file = args[6]
    htable_path = args[7]
    optional_outputs = args[8]

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

    water_edge_df = water_edge_df[(water_edge_df['hydroid'].notnull()) & (water_edge_df['hand'] > 0)]

    ## Check that there are valid obs in the water_edge_df (not empty)
    if water_edge_df.empty:
        print('WARNING: skipping ' + str(huc) + ': no valid observation points found within the huc catchments (PLEASE REVIEW for potential issue)')
        log_text = 'WARNING: skipping ' + str(huc) + ': no valid observation points found within the huc catchments (PLEASE REVIEW for potential issue)'
    else:
        ## Get median HAND value for appropriate groups.
        water_edge_median_ds = water_edge_df.groupby(["hydroid", "flow", "submitter", "coll_time", "flow_unit","layer"])['hand'].median()

        ## Write user_supplied_n_vals to CSV for next step.
        pt_n_values_csv = os.path.join(fim_directory, huc, 'user_supplied_n_vals_' + huc + '.csv')
        water_edge_median_ds.to_csv(pt_n_values_csv)
        ## Convert pandas series to dataframe to pass to update_rating_curve
        water_edge_median_df = water_edge_median_ds.reset_index()
        water_edge_median_df['coll_time'] = water_edge_median_df.coll_time.astype(str)
        del water_edge_median_ds

        ## Additional arguments for src_roughness_optimization
        source_tag = 'point_obs' # tag to use in source attribute field
        merge_prev_adj = True # merge in previous SRC adjustment calculations

        ## Call update_rating_curve() to perform the rating curve calibration.
        
        log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path,optional_outputs, source_tag, merge_prev_adj, DOWNSTREAM_THRESHOLD)
        ## Still testing: use code below to print out any exceptions.
        '''
        try:
            log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs, source_tag, merge_prev_adj, DOWNSTREAM_THRESHOLD)
        except Exception as e:
            print(str(huc) + ' --> ' + str(e))
            log_text = 'ERROR!!!: HUC ' + str(huc) + ' --> ' + str(e)
        '''
    return(log_text)


def find_points_in_huc(huc_id, conn):
    # Point data in the database is already attributed with HUC8 id
    '''
    The function queries the PostgreSQL database and returns all points attributed with the input huc id.

    Processing
    - Query the PostgreSQL database for points attributed with huc id.
    - Reads the filtered database result into a pandas geodataframe

    Inputs
    - conn:         connection to PostgreSQL db
    - huc_id:       HUC id to query the db

    Outputs
    - water_edge_df: geodataframe with point data
    '''

    huc_pt_query = """SELECT ST_X(P.geom), ST_Y(P.geom), P.submitter, P.flow, P.coll_time, P.flow_unit, P.layer, P.geom 
    FROM points P 
    JOIN hucs H ON ST_Contains(H.geom, P.geom)
    WHERE H.huc8 = %s """
    
    # Need to hard code the CRS to use EPSG:5070 instead of the default ESRI:102039 (gdal pyproj throws an error with crs 102039)
    # Appears that EPSG:5070 is functionally equivalent to ESRI:102039: https://gis.stackexchange.com/questions/329123/crs-interpretation-in-qgis
    water_edge_df = gpd.GeoDataFrame.from_postgis(huc_pt_query, con=conn, params=[huc_id], crs="EPSG:5070", parse_dates=['coll_time'])
    water_edge_df = water_edge_df.drop(columns=['st_x','st_y'])
    
    return water_edge_df


def find_hucs_with_points(conn,fim_out_huc_list):
    '''
    The function queries the PostgreSQL database and returns a list of all the HUCs that contain calb point data.

    Processing
    - Query the PostgreSQL database for all unique huc ids

    Inputs
    - conn:         connection to PostgreSQL db

    Outputs
    - hucs_wpoints: list with all unique huc ids
    '''

    cursor = conn.cursor()
    '''
    cursor.execute("""
        SELECT DISTINCT H.huc8
        FROM points P JOIN hucs H ON ST_Contains(H.geom, P.geom);
    """)
    '''
    cursor.execute("SELECT DISTINCT H.huc8 FROM points P JOIN hucs H ON ST_Contains(H.geom, P.geom) WHERE H.huc8 = ANY(%s);", (fim_out_huc_list,))
    hucs_fetch = cursor.fetchall() # list with tuple with the attributes defined above (need to convert to df?)
    hucs_wpoints = []
    for huc in hucs_fetch:
        hucs_wpoints.append(huc[0])
    cursor.close()
    return hucs_wpoints

def ingest_points_layer(fim_directory, scale, job_number, debug_outputs_option):
    '''
    The function obtains all points within a given huc, locates the corresponding FIM output files for each huc (confirms all necessary files exist), and then passes a proc list of huc organized data to process_points function.

    Processing
    - Query the PostgreSQL database for all unique huc ids that have calb points
    - Loop through all HUCs with calb data and locate necessary fim output files to pass to calb workflow

    Inputs
    - fim_directory:        parent directory of fim ouputs (contains HUC directories)
    - scale:                HUC8 or HUC6 processing scale
    - job_number:           number of multiprocessing jobs to use for processing hucs
    - debug_outputs_option: optional flag to output intermediate files

    Outputs
    - procs_list:           passes multiprocessing list of input args for process_points function input
    '''
    conn = connect() # Connect to the PostgreSQL db once before looping hucs
    print("Finding all fim_output hucs that contain calibration points...")
    fim_out_huc_list  = [ item for item in os.listdir(fim_directory) if os.path.isdir(os.path.join(fim_directory, item)) ]
    fim_out_huc_list.remove('logs')
    ## Record run time and close log file
    run_time_start = dt.datetime.now()
    log_file.write('Finding all hucs that contain calibration points...' + '\n')
    huc_list_db = find_hucs_with_points(conn,fim_out_huc_list)
    #huc_list_db = ['07080206']
    run_time_end = dt.datetime.now()
    task_run_time = run_time_end - run_time_start
    log_file.write('HUC SEARCH TASK RUN TIME: ' + str(task_run_time) + '\n')
    print(f"{len(huc_list_db)} hucs found in point database" + '\n')
    log_file.write(f"{len(huc_list_db)} hucs found in point database" + '\n')
    log_file.write('#########################################################\n')

    ## Ensure HUC id is either HUC8 or HUC6
    huc_list = []
    for huc in huc_list_db:
        ## zfill to the appropriate scale to ensure leading zeros are present, if necessary.
        if scale == 'HUC8':
            huc = huc.zfill(8)
        else:
            huc = huc.zfill(6)
        if huc not in huc_list:
            huc_list.append(huc)
            log_file.write(str(huc) + '\n')

    procs_list = []  # Initialize proc list for mulitprocessing.

    ## Define paths to relevant HUC HAND data.
    for huc in huc_list:
        water_edge_df = find_points_in_huc(huc, conn).reset_index()
        print(f"{len(water_edge_df)} points found in " + str(huc))
        log_file.write(f"{len(water_edge_df)} points found in " + str(huc) + '\n')

        ## Create X and Y location columns by extracting from geometry.
        water_edge_df['X'] = water_edge_df['geom'].x
        water_edge_df['Y'] = water_edge_df['geom'].y

        ## Intermediate output for debugging
        if debug_outputs_option:
            huc_debug_pts_out = os.path.join(fim_directory, huc, 'debug_test_' + huc + '.csv')
            water_edge_df.to_csv(huc_debug_pts_out)
            huc_debug_pts_out_gpkg = os.path.join(fim_directory, huc, 'export_water_edge_df_' + huc + '.gpkg')
            water_edge_df.to_file(huc_debug_pts_out_gpkg, driver='GPKG', index=False)

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

    disconnect(conn) # move this to happen at the end of the huc looping

def connect():
    """ Connect to the PostgreSQL database server """

    print('Connecting to the PostgreSQL database...')
    conn = None
    not_connected = True
    while not_connected:
        try:

            # connect to the PostgreSQL server
            conn = psycopg2.connect(
                host=CALIBRATION_DB_HOST,
                database="calibration",
                user=CALIBRATION_DB_USER_NAME,
                password=CALIBRATION_DB_PASS)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('Host name: ' + CALIBRATION_DB_HOST)
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

               # close the communication with the PostgreSQL
            cur.close()
            not_connected = False
            print("Connected to database\n\n")
            log_file.write('Connected to database via host: ' + str(CALIBRATION_DB_HOST) + '\n')
        except (Exception, psycopg2.DatabaseError) as error:
            print("Waiting for database to come online")
            time.sleep(5)

    return conn

def disconnect(conn):
    """ Disconnect from the PostgreSQL database server """

    if conn is not None:
        conn.close()
        print('Database connection closed.')

if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    ## Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve given a shapefile containing points of known water boundary.')
    #parser.add_argument('-db','--points-layer',help='Path to points layer containing known water boundary locations',required=True)
    parser.add_argument('-fim_dir','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-debug','--extra-outputs',help='Optional flag: Use this to keep intermediate output files for debugging/testing',default=False,required=False, action='store_true')
    parser.add_argument('-scale','--scale',help='Optional: HUC6 or HUC8 -- default is HUC8', default='HUC8',required=False)
    parser.add_argument('-dthresh','--downstream-thresh',help='Optional Override: distance in km to propogate modified roughness values downstream', default=DOWNSTREAM_THRESHOLD,required=False)
    parser.add_argument('-j','--job-number',help='Optional: Number of jobs to use',required=False,default=2)

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    #points_layer = args['points_layer']
    fim_directory = args['fim_directory']
    debug_outputs_option = args['extra_outputs']
    scale = args['scale']
    ds_thresh_override = args['downstream_thresh']
    job_number = int(args['job_number'])

    print(debug_outputs_option)

    assert os.path.isdir(fim_directory), 'ERROR: could not find the input fim_dir location: ' + str(fim_directory)

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

    ingest_points_layer(fim_directory, scale, job_number, debug_outputs_option)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()
