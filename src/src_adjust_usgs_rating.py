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
from utils.shared_functions import check_file_age, concat_huc_csv
from src_roughness_optimization import update_rating_curve
'''
The script ingests a USGS rating curve csv and a NWM flow recurrence interval database. The gage location will be associated to the corresponding hydroID and attributed with the HAND elevation value

Processing
- Read in USGS rating curve from csv and convert WSE navd88 values to meters
- Read in the aggregate USGS elev table csv from the HUC fim directory (output from usgs_gage_crosswalk.py)
- Filter null entries and convert usgs flow from cfs to cms
- Calculate HAND elevation value for each gage location (NAVD88 elevation - NHD DEM thalweg elevation)
- Read in the NWM recurr csv file and convert flow to cfs
- Calculate the closest SRC discharge value to the NWM flow value
- Create dataframe with crosswalked USGS flow and NWM recurr flow and assign metadata attributes
- Calculate flow difference (variance) to check for large discrepancies btw NWM flow and USGS closest flow
- Log any signifant differences (or negative HAND values) btw the NWM flow value and closest USGS rating flow
- Produce log file
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- branch_dir:        fim directory containing individual HUC output dirs
- usgs_rc_filepath:     USGS rating curve database (produced by rating_curve_get_usgs_curves.py)
- nwm_recurr_filepath:  NWM flow recurrence interval dataset
- debug_outputs_option: optional flag to output intermediate files for reviewing/debugging
- job_number:           number of multi-processing jobs to use

Outputs
- water_edge_median_ds: dataframe containing 'location_id','hydroid','feature_id','huc','hand','discharge_cms','nwm_recur_flow_cms','nwm_recur','layer'
'''

def create_usgs_rating_database(usgs_rc_filepath, usgs_elev_df, nwm_recurr_filepath, log_dir):
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
    cross_df = usgs_elev_df[["location_id", "HydroID", "feature_id", "levpa_id", "HUC8", "dem_adj_elevation"]].copy()
    cross_df.rename(columns={'dem_adj_elevation':'hand_datum', 'HydroID':'hydroid', 'HUC8':'huc'}, inplace=True)
    
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
    usgs_rc_df = usgs_rc_df[['location_id','feature_id','hydroid','levpa_id','huc','hand','discharge_cms']]
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
        ## Calculate the closest SRC discharge value to the NWM flow value
        merge_df['Q_find'] = (merge_df['discharge_cms'] - merge_df[interval+"_0_year"]).abs()
        
        ## Check for any missing/null entries in the input SRC
        if merge_df['Q_find'].isnull().values.any(): # there may be null values for lake or coastal flow lines (need to set a value to do groupby idxmin below)
            log_text += 'HUC: ' + str(merge_df['huc']) + ' : feature_id' + str(merge_df['feature_id']) + ' --> Null values found in "Q_find" calc. These will be filled with 999999 () \n'
            ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            merge_df['Q_find'] = merge_df['Q_find'].fillna(999999)
        if merge_df['hydroid'].isnull().values.any():
            log_text += 'HUC: ' + str(merge_df['huc']) + ' --> Null values found in "hydroid"... \n'
        
        # Create dataframe with crosswalked USGS flow and NWM recurr flow
        calc_df = merge_df.loc[merge_df.groupby(['location_id','levpa_id'])['Q_find'].idxmin()].reset_index(drop=True) # find the index of the Q_1_5_find (closest matching flow)
        # Calculate flow difference (variance) to check for large discrepancies btw NWM flow and USGS closest flow
        calc_df['check_variance'] = ((calc_df['discharge_cms'] - calc_df[interval+"_0_year"])/calc_df['discharge_cms']).abs()
        # Assign new metadata attributes
        calc_df['nwm_recur'] = interval+"_0_year"
        calc_df['layer'] = '_usgs-gage____' + interval+"-year"
        calc_df.rename(columns={interval+"_0_year":'nwm_recur_flow_cms'}, inplace=True)
        # Subset calc_df for final output
        calc_df = calc_df[['location_id','hydroid','feature_id','levpa_id','huc','hand','discharge_cms','check_variance','nwm_recur_flow_cms','nwm_recur','layer']]
        final_df = pd.concat([final_df, calc_df], ignore_index=True)
        # Log any negative HAND elev values and remove from database
        log_text += ('Warning: Negative HAND stage values -->\n')
        log_text += (calc_df[calc_df['hand']<0].to_string() +'\n')
        final_df = final_df[final_df['hand']>0]
        # Log any signifant differences btw the NWM flow value and closest USGS rating flow (this ensures that we consistently sample the USGS rating curves at known intervals - NWM recur flow)
        log_text += ('Warning: Large variance (>10%) between NWM flow and closest USGS flow -->\n')
        log_text += (calc_df[calc_df['check_variance']>0.1].to_string() +'\n')
        final_df = final_df[final_df['check_variance']<0.1]
        final_df['submitter'] = 'usgs_rating_wrds_api_' + final_df['location_id']
        # Get datestamp from usgs rating curve file to use as coll_time attribute in hydroTable.csv
        datestamp = check_file_age(usgs_rc_filepath)
        final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the USGS RC database
    final_df.rename(columns={'discharge_cms':'flow'}, inplace=True)  
    final_df.to_csv(os.path.join(log_dir,"usgs_rc_nwm_recurr.csv"),index=False)

    # Output log text to log file
    log_text += ('#########\nTotal entries per USGS gage location -->\n')
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count') 
    log_text += (loc_id_df.to_string() +'\n')
    log_text += ('#########\nTotal entries per NWM recur value -->\n')
    recur_count_df = final_df.groupby(['nwm_recur']).size().reset_index(name='count') 
    log_text += (recur_count_df.to_string() +'\n')
    log_usgs_db = open(os.path.join(log_dir,'log_usgs_rc_database.log'),"w")
    log_usgs_db.write(log_text)
    log_usgs_db.close()
    return(final_df)

def branch_proc_list(usgs_df,run_dir,debug_outputs_option,log_file):
    procs_list = []  # Initialize list for mulitprocessing.

    # loop through all unique level paths that have a USGS gage
    #branch_huc_dict = pd.Series(usgs_df.levpa_id.values,index=usgs_df.huc).to_dict('list')
    #branch_huc_dict = usgs_df.set_index('huc').T.to_dict('list')
    huc_branch_dict = usgs_df.groupby('huc')['levpa_id'].apply(set).to_dict()

    for huc in sorted(huc_branch_dict.keys()): # sort huc_list for helping track progress in future print statments
        branch_set = huc_branch_dict[huc]
        for branch_id in branch_set: 
            # Define paths to branch HAND data.
            # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
            # Assumes outputs are for HUC8 (not HUC6)
            branch_dir = os.path.join(run_dir,huc,'branches',branch_id)
            hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')
            catchments_path = os.path.join(branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif')
            catchments_poly_path = os.path.join(branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg')
            htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
            water_edge_median_ds = usgs_df[(usgs_df['huc']==huc) & (usgs_df['levpa_id']==branch_id)]

            # Check to make sure the fim output files exist. Continue to next iteration if not and warn user.
            if not os.path.exists(hand_path):
                print("WARNING: HAND grid does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id))
                log_file.write("WARNING: HAND grid does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id) + '\n')
            elif not os.path.exists(catchments_path):
                print("WARNING: Catchments grid does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id))
                log_file.write("WARNING: Catchments grid does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id) + '\n')        
            elif not os.path.exists(htable_path):
                print("WARNING: hydroTable does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id))
                log_file.write("WARNING: hydroTable does not exist (skipping): " + str(huc) + ' - branch-id: ' + str(branch_id) + '\n')
            else:
                ## Additional arguments for src_roughness_optimization
                source_tag = 'usgs_rating' # tag to use in source attribute field
                merge_prev_adj = False # merge in previous SRC adjustment calculations

                print('Will perform SRC adjustments for huc: ' + str(huc) + ' - branch-id: ' + str(branch_id))
                procs_list.append([branch_dir, water_edge_median_ds, htable_path, huc, branch_id, catchments_poly_path, debug_outputs_option, source_tag, merge_prev_adj])

    # multiprocess all available branches
    print(f"Calculating new SRCs for {len(procs_list)} branches using {job_number} jobs...")
    with Pool(processes=job_number) as pool:
            log_output = pool.starmap(update_rating_curve, procs_list)
            log_file.writelines(["%s\n" % item  for item in log_output])
    # try statement for debugging 
    # try:
    #     with Pool(processes=job_number) as pool:
    #         log_output = pool.starmap(update_rating_curve, procs_list)
    #         log_file.writelines(["%s\n" % item  for item in log_output])
    # except Exception as e:
    #     print(str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e))
    #     log_file.write('ERROR!!!: HUC ' + str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e) + '\n')

def run_prep(run_dir,usgs_rc_filepath,nwm_recurr_filepath,debug_outputs_option,job_number):
    ## Check input args are valid
    assert os.path.isdir(run_dir), 'ERROR: could not find the input fim_dir location: ' + str(run_dir)

    ## Create an aggregate dataframe with all usgs_elev_table.csv entries for hucs in fim_dir
    print('Reading USGS gage HAND elevation from usgs_elev_table.csv files...')
    #usgs_elev_file = os.path.join(branch_dir,'usgs_elev_table.csv')
    #usgs_elev_df = pd.read_csv(usgs_elev_file, dtype={'HUC8': object, 'location_id': object, 'feature_id': int})
    csv_name = 'usgs_elev_table.csv'
    
    available_cores = multiprocessing.cpu_count()
    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    ## Create output dir for log and usgs rc database
    log_dir = os.path.join(run_dir,"logs","src_optimization")
    print("Log file output here: " + str(log_dir))
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    ## Create a time var to log run time
    begin_time = dt.datetime.now()
    # Create log file for processing records
    log_file = open(os.path.join(log_dir,'log_usgs_rc_src_adjust.log'),"w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')
   
    usgs_elev_df = concat_huc_csv(run_dir,csv_name)

    if usgs_elev_df is None:
        warn_err = 'WARNING: usgs_elev_df not created - check that usgs_elev_table.csv files exist in fim_dir!'
        print(warn_err)
        log_file.write(warn_err)
        
    elif usgs_elev_df.empty:
        warn_err = 'WARNING: usgs_elev_df is empty - check that usgs_elev_table.csv files exist in fim_dir!'
        print(warn_err)
        log_file.write(warn_err)

    else:
        print('This may take a few minutes...')
        log_file.write("starting create usgs rating db")
        usgs_df = create_usgs_rating_database(usgs_rc_filepath, usgs_elev_df, nwm_recurr_filepath, log_dir)

        ## Create huc proc_list for multiprocessing and execute the update_rating_curve function
        branch_proc_list(usgs_df,run_dir,debug_outputs_option,log_file)

    ## Record run time and close log file
    log_file.write('#########################################################\n\n')
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()

if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve with database of USGS rating curve (calculated WSE/flow).')
    parser.add_argument('-run_dir','--run-dir',help='Parent directory of FIM run.',required=True)
    parser.add_argument('-usgs_rc','--usgs-ratings',help='Path to USGS rating curve csv file',required=True)
    parser.add_argument('-nwm_recur','--nwm_recur',help='Path to NWM recur file (multiple NWM flow intervals). NOTE: assumes flow units are cfs!!',required=True)
    parser.add_argument('-debug','--extra-outputs',help='Optional flag: Use this to keep intermediate output files for debugging/testing',default=False,required=False, action='store_true')
    parser.add_argument('-j','--job-number',help='Number of jobs to use',required=False,default=1)

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    run_dir = args['run_dir']
    usgs_rc_filepath = args['usgs_ratings']
    nwm_recurr_filepath = args['nwm_recur']
    debug_outputs_option = args['extra_outputs']
    job_number = int(args['job_number'])

    ## Prepare/check inputs, create log file, and spin up the proc list
    run_prep(run_dir,usgs_rc_filepath,nwm_recurr_filepath,debug_outputs_option,job_number)
    
