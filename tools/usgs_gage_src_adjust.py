import argparse
import os
import pandas as pd
import sys
import json
import datetime as dt
from pathlib import Path
from collections import deque
import multiprocessing
from multiprocessing import Pool
from tools_shared_functions import check_file_age, concat_huc_csv
from adjust_rc_with_feedback import update_rating_curve


def create_usgs_rating_database(usgs_rc_filepath, agg_crosswalk_df, nwm_recurr_filepath, output_dir):
    start_time = dt.datetime.now()
    print('Reading USGS rating curve from csv...')
    log_text = 'Processing database for USGS flow/WSE at NWM flow recur intervals...\n'
    col_usgs = ["location_id", "flow", "stage", "elevation_navd88"]
    usgs_rc_df = pd.read_csv(usgs_rc_filepath, dtype={'location_id': object}, usecols=col_usgs)#, nrows=30000)
    print('Duration (read usgs_rc_csv): {}'.format(dt.datetime.now() - start_time))
    
    # convert WSE navd88 values to meters
    usgs_rc_df['elevation_navd88_m'] = usgs_rc_df['elevation_navd88'] / 3.28084
    
    # read in the aggregate USGS elev table csv
    start_time = dt.datetime.now()
    cross_df = agg_crosswalk_df[["location_id", "HydroID", "feature_id", "huc", "dem_adj_elevation"]].copy()
    cross_df.rename(columns={'dem_adj_elevation':'hand_datum', 'HydroID':'hydroid'}, inplace=True)
    
    # filter null location_id rows from cross_df (removes ahps lide entries that aren't associated with USGS gage)
    cross_df = cross_df[cross_df.location_id.notnull()]
    
    # convert usgs flow from cfs to cms
    usgs_rc_df['discharge_cms'] = usgs_rc_df.flow / 35.3147
    usgs_rc_df = usgs_rc_df.drop(columns=["flow"])
    
    # merge usgs ratings with crosswalk attributes
    usgs_rc_df = usgs_rc_df.merge(cross_df, how='left', on='location_id')
    usgs_rc_df = usgs_rc_df[usgs_rc_df['hydroid'].notna()]
    
    # calculate hand elevation
    usgs_rc_df['hand'] = usgs_rc_df['elevation_navd88_m'] - usgs_rc_df['hand_datum']
    usgs_rc_df = usgs_rc_df[['location_id','feature_id','hydroid','huc','hand','discharge_cms']]
    usgs_rc_df['feature_id'] = usgs_rc_df['feature_id'].astype(int)
    
    # read in the NWM recurr csv file
    nwm_recur_df = pd.read_csv(nwm_recurr_filepath, dtype={'feature_id': int})
    nwm_recur_df = nwm_recur_df.drop(columns=["Unnamed: 0"])
    nwm_recur_df.rename(columns={'2_0_year_recurrence_flow_17C':'2_0_year','5_0_year_recurrence_flow_17C':'5_0_year','10_0_year_recurrence_flow_17C':'10_0_year','25_0_year_recurrence_flow_17C':'25_0_year','50_0_year_recurrence_flow_17C':'50_0_year','100_0_year_recurrence_flow_17C':'100_0_year'}, inplace=True)
    
    #convert cfs to cms (x 0.028317)
    nwm_recur_df.loc[:, ['2_0_year','5_0_year','10_0_year','25_0_year','50_0_year','100_0_year']] *= 0.028317
    
    # merge nwm recurr with usgs_rc
    merge_df = usgs_rc_df.merge(nwm_recur_df, how='left', on='feature_id')
    
    # NWM recurr intervals
    recurr_intervals = ("2","5","10","25","50","100")
    final_df = pd.DataFrame() # create empty dataframe to append flow interval dataframes
    for interval in recurr_intervals:
        log_text += ('\n\nProcessing: ' + str(interval) + '-year NWM recurr intervals\n')
        print('Processing: ' + str(interval) + '-year NWM recurr intervals')
        ## Calculate the closest SRC discharge value to the NWM 1.5yr flow
        merge_df['Q_find'] = (merge_df['discharge_cms'] - merge_df[interval+"_0_year"]).abs()
        
        ## Check for any missing/null entries in the input SRC
        if merge_df['Q_find'].isnull().values.any(): # there may be null values for lake or coastal flow lines (need to set a value to do groupby idxmin below)
            log_text += 'HUC: ' + str(merge_df['huc']) + ' : feature_id' + str(merge_df['feature_id']) + ' --> Null values found in "Q_find" calc. These will be filled with 999999 () \n'
            ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            merge_df['Q_find'] = merge_df['Q_find'].fillna(999999)
        if merge_df['hydroid'].isnull().values.any():
            log_text += 'HUC: ' + str(merge_df['huc']) + ' --> Null values found in "hydroid"... \n'
        
        # Create dataframe with crosswalked USGS flow and NWM recurr flow
        calc_df = merge_df.loc[merge_df.groupby('location_id')['Q_find'].idxmin()].reset_index(drop=True) # find the index of the Q_1_5_find (closest matching flow)
        # Calculate flow difference (variance) to check for large discrepancies btw NWM flow and USGS closest flow
        calc_df['check_variance'] = ((calc_df['discharge_cms'] - calc_df[interval+"_0_year"])/calc_df['discharge_cms']).abs()
        # Assign new metadata attributes
        calc_df['nwm_recur'] = interval+"_0_year"
        calc_df['layer'] = '_usgs-gage____' + interval+"-year"
        calc_df.rename(columns={interval+"_0_year":'nwm_recur_flow_cms'}, inplace=True)
        # Subset calc_df for final output
        calc_df = calc_df[['location_id','hydroid','feature_id','huc','hand','discharge_cms','check_variance','nwm_recur_flow_cms','nwm_recur','layer']]
        final_df = final_df.append(calc_df, ignore_index=True)
        # Log any negative HAND elev values and remove from database
        log_text += ('Warning: Negative HAND stage values -->\n')
        log_text += (calc_df[calc_df['hand']<0].to_string() +'\n')
        final_df = final_df[final_df['hand']>0]
        # Log any signifant differences btw the NWM flow value and closest USGS rating flow (this ensures that we consistently sample the USGS rating curves at known intervals - NWM recur flow)
        log_text += ('Warning: Large variance (>10%) between NWM flow and closest USGS flow -->\n')
        log_text += (calc_df[calc_df['check_variance']>0.1].to_string() +'\n')
        final_df = final_df[final_df['check_variance']<0.1]
        final_df['submitter'] = 'usgs_rating_wrds_api'
        # Get datestamp from usgs rating curve file to use as coll_time attribute in hydroTable.csv
        datestamp = check_file_age(usgs_rc_filepath)
        final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the USGS RC database
    final_df.rename(columns={'discharge_cms':'flow'}, inplace=True)  
    final_df.to_csv(os.path.join(output_dir,"usgs_rc_nwm_recurr.csv"),index=False)

    # Output log text to log file
    log_text += ('Total entries per USGS gage location -->\n')
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count') 
    log_text += (loc_id_df.to_string() +'\n')
    log_usgs_db = open(os.path.join(output_dir,'log_usgs_rc_database.log'),"w")
    log_usgs_db.write(log_text)
    log_usgs_db.close()
    return(final_df)

def huc_proc_list(usgs_df,fim_directory,inter_outputs):
    huc_list = usgs_df['huc'].tolist()
    huc_list = list(set(huc_list))
    procs_list = []  # Initialize list for mulitprocessing.
    # Define paths to relevant HUC HAND data.
    #huc_list = ['01010007','05030102','01010008']
    for huc in huc_list:
        print(huc)
        # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
        if scale == 'HUC8':
            hand_path = os.path.join(fim_directory, huc, 'rem_zeroed_masked.tif')
            catchments_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'src.json')
            catchments_poly_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
            water_edge_median_ds = usgs_df[usgs_df['huc']==huc]
        else:
            hand_path = os.path.join(fim_directory, huc, 'hand_grid_' + huc + '.tif')
            catchments_path = os.path.join(fim_directory, huc, 'catchments_' + huc + '.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'rating_curves_' + huc + '.json')
            catchments_poly_path = ''
            water_edge_median_ds = usgs_df[usgs_df['huc']==huc]

        # Check to make sure the previously defined files exist. Continue to next iteration if not and warn user.
        if not os.path.exists(hand_path):
            print("HAND grid for " + huc + " does not exist.")
            continue
        if not os.path.exists(catchments_path):
            print("Catchments grid for " + huc + " does not exist.")
            continue
        if not os.path.isfile(output_src_json_file):
            print("Rating Curve JSON file for " + huc + " does not exist.")
            continue

        # Define path to hydroTable.csv.
        htable_path = os.path.join(fim_directory, huc, 'hydroTable.csv')
        if not os.path.exists(htable_path):
            print("hydroTable for " + huc + " does not exist.")
            continue

        procs_list.append([fim_directory, water_edge_median_ds, htable_path, output_src_json_file, huc, catchments_poly_path, inter_outputs])

    print(f"Calculating new SRCs for {len(procs_list)} hucs using {job_number} jobs...")
    with Pool(processes=job_number) as pool:
        log_output = pool.starmap(update_rating_curve, procs_list)
        log_file.writelines(["%s\n" % item  for item in log_output])

if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve with database of USGS rating curve (calculated WSE/flow).')
    parser.add_argument('-fim_dir','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-usgs_rc','--usgs-ratings',help='Path to USGS rating curve csv file',required=True)
    parser.add_argument('-nwm_recur','--nwm_recur',help='Path to NWM recur file (multiple NWM flow intervals). NOTE: assumes flow units are cfs!!',required=True)
    parser.add_argument('-i','--extra-outputs',help='True or False: Include intermediate output files for debugging/testing',default='False',required=False)
    parser.add_argument('-s','--scale',help='HUC6 or HUC8', default='HUC8',required=False)
    parser.add_argument('-j','--job-number',help='Number of jobs to use',required=False,default=2)

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    fim_directory = args['fim_directory']
    usgs_rc_filepath = args['usgs_ratings']
    nwm_recurr_filepath = args['nwm_recur']
    inter_outputs = args['extra_outputs']
    scale = args['scale']
    job_number = int(args['job_number'])

    if scale not in ['HUC6', 'HUC8']:
        print("scale (-s) must be HUC6s or HUC8")
        quit()

    if not os.path.isdir(fim_directory):
        print('ERROR: could not find the input fim_dir location: ' + str(fim_directory))
        quit()

    ## Create an aggregate dataframe with all usgs_elev_table.csv entries for hucs in fim_dir
    print('Reading USGS gage HAND elevation from usgs_elev_table.csv files...')
    csv_name = 'usgs_elev_table.csv'
    agg_crosswalk_df = concat_huc_csv(fim_directory,csv_name)
    if agg_crosswalk_df.empty:
        print('ERROR: agg_crosswalk_df is empty - check that usgs_elev_table.csv files exist in fim_dir!')
        quit()

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    ## Create output dir for log and usgs rc database
    output_dir = os.path.join(fim_directory,"src_optimization")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # Create log file for processing records
    print('This may take a few minutes...')

    usgs_df = create_usgs_rating_database(usgs_rc_filepath, agg_crosswalk_df, nwm_recurr_filepath, output_dir)
    print("Log file output here: " + str(output_dir))

    ## Create a time var to log run time
    begin_time = dt.datetime.now()
    log_file = open(os.path.join(output_dir,'log_usgs_rc_src_adjust.log'),"w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    ## Create huc proc_list for multiprocessing and execute the update_rating_curve function
    huc_proc_list(usgs_df,fim_directory,inter_outputs)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()
