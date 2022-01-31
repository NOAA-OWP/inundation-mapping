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

from tools_shared_variables import DOWNSTREAM_THRESHOLD, ROUGHNESS_MIN_THRESH, ROUGHNESS_MAX_THRESH


def update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs):
    print("Processing huc --> " + str(huc))
    log_text = "\nProcessing huc --> " + str(huc) + '\n'
    df_nvalues = water_edge_median_df.copy()
    #df_nvalues = pd.read_csv(pt_n_values_csv) # read csv to import as a dataframe
    df_nvalues = df_nvalues[df_nvalues.hydroid.notnull()] # remove null entries that do not have a valid hydroid

    ## Read in the hydroTable.csv and check wether it has previously been updated (rename default columns if needed)
    df_htable = pd.read_csv(htable_path, dtype={'HUC': object, 'last_updated':object, 'submitter':object})
    if 'default_ManningN' in df_htable.columns:
        df_htable.drop(['ManningN','discharge_cms','submitter','last_updated','adjust_ManningN','adjust_src_on'], axis=1, inplace=True) # Delete these to prevent duplicates (if adjust_rc_with_feedback.py was previously applied)
        #df_htable = df_htable[['HydroID','feature_id','stage','orig_discharge_cms','HydraulicRadius (m)','WetArea (m2)','SLOPE','default_ManningN','HUC','LakeID']]
        df_htable.rename(columns={'default_discharge_cms':'discharge_cms','default_ManningN':'ManningN'}, inplace=True)
        log_text += str(huc) + ': found previous hydroTable calibration attributes --> removing previous calb columns...\n'

    ## loop through the user provided point data --> stage/flow dataframe row by row
    for index, row in df_nvalues.iterrows():
        df_htable_hydroid = df_htable[df_htable.HydroID == row.hydroid] # filter htable for entries with matching hydroid
        find_src_stage = df_htable_hydroid.loc[df_htable_hydroid['stage'].sub(row.hand).abs().idxmin()] # find closest matching stage to the user provided HAND value
        ## copy the corresponding htable values for the matching stage->HAND lookup
        df_nvalues.loc[index,'feature_id'] = find_src_stage.feature_id
        df_nvalues.loc[index,'NextDownID'] = find_src_stage.NextDownID
        df_nvalues.loc[index,'LENGTHKM'] = find_src_stage.LENGTHKM
        df_nvalues.loc[index,'src_stage'] = find_src_stage.stage
        df_nvalues.loc[index,'ManningN'] = find_src_stage.ManningN
        df_nvalues.loc[index,'SLOPE'] = find_src_stage.SLOPE
        df_nvalues.loc[index,'HydraulicRadius_m'] = find_src_stage['HydraulicRadius (m)']
        df_nvalues.loc[index,'WetArea_m2'] = find_src_stage['WetArea (m2)']
        df_nvalues.loc[index,'discharge_cms'] = find_src_stage.discharge_cms

    ## mask src values that crosswalk to the SRC zero point (src_stage ~ 0 or discharge <= 0)
    df_nvalues[['HydraulicRadius_m','WetArea_m2']] = df_nvalues[['HydraulicRadius_m','WetArea_m2']].mask((df_nvalues['src_stage'] <= 0.1) | (df_nvalues['discharge_cms'] <= 0.0), np.nan)

    ## Calculate roughness using Manning's equation
    df_nvalues.rename(columns={'ManningN':'default_ManningN','hydroid':'HydroID'}, inplace=True) # rename the previous ManningN column
    df_nvalues['hydroid_ManningN'] = df_nvalues['WetArea_m2']* \
    pow(df_nvalues['HydraulicRadius_m'],2.0/3)* \
    pow(df_nvalues['SLOPE'],0.5)/df_nvalues['flow']

    ## Create dataframe to check for erroneous Manning's n values (values set in tools_shared_variables.py --> >0.6 or <0.001)
    df_nvalues['Mann_flag'] = np.where((df_nvalues['hydroid_ManningN'] >= ROUGHNESS_MAX_THRESH) | (df_nvalues['hydroid_ManningN'] <= ROUGHNESS_MIN_THRESH) | (df_nvalues['hydroid_ManningN'].isnull()),'Fail','Pass')
    df_mann_flag = df_nvalues[(df_nvalues['Mann_flag'] == 'Fail')][['HydroID','hydroid_ManningN']]
    if not df_mann_flag.empty:
        log_text += '!!! Flaged Mannings Roughness values below !!!' +'\n'
        log_text += df_mann_flag.to_string() + '\n'

    ## Create magnitude and ahps column by subsetting the "layer" attribute
    df_nvalues['magnitude'] = df_nvalues['layer'].str.split("_").str[5]
    df_nvalues['ahps_lid'] = df_nvalues['layer'].str.split("_").str[1]
    df_nvalues['huc'] = str(huc)
    df_nvalues.drop(['layer'], axis=1, inplace=True)

    ## Create df grouped by hydroid with ahps_lid and huc number
    df_huc_lid = df_nvalues.groupby(["HydroID"]).first()[['ahps_lid','huc']]
    df_huc_lid.columns = pd.MultiIndex.from_product([['info'], df_huc_lid.columns])

    ## pivot the magnitude column to display n value for each magnitude at each hydroid
    df_nvalues_mag = df_nvalues.pivot_table(index='HydroID', columns='magnitude', values=['hydroid_ManningN'], aggfunc='mean') # if there are multiple entries per hydroid and magnitude - aggregate using mean
    
    ## Optional: Export csv with the newly calculated Manning's N values
    if optional_outputs == 'True':
        output_calc_n_csv = os.path.join(fim_directory, huc, 'calc_src_n_vals_' + huc + '.csv')
        df_nvalues.to_csv(output_calc_n_csv,index=False)

    ## filter the modified Manning's n dataframe for values out side allowable range
    df_nvalues = df_nvalues[df_nvalues['Mann_flag'] == 'Pass']

    ## Check that there are valid entries in the calculate roughness df after filtering
    if not df_nvalues.empty:
        ## Create df with the most recent collection time entry and submitter attribs
        df_updated = df_nvalues[['HydroID','coll_time','submitter','ahps_lid']] # subset the dataframe
        df_updated = df_updated.sort_values('coll_time').drop_duplicates(['HydroID'],keep='last') # sort by collection time and then drop duplicate HydroIDs (keep most recent coll_time per HydroID)
        df_updated.rename(columns={'coll_time':'last_updated'}, inplace=True)

        ## cacluate median ManningN to handle cases with multiple hydroid entries
        df_mann_hydroid = df_nvalues.groupby(["HydroID"])[['hydroid_ManningN']].median()

        ## Create a df with the median hydroid_ManningN value per feature_id
        #df_mann_featid = df_nvalues.groupby(["feature_id"])[['hydroid_ManningN']].mean()
        #df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)

        ## Rename the original hydrotable variables to allow new calculations to use the primary var name
        df_htable.rename(columns={'ManningN':'default_ManningN','discharge_cms':'default_discharge_cms'}, inplace=True)

        ## Check for large variabilty in the calculated Manning's N values (for cases with mutliple entries for a singel hydroid)
        df_nrange = df_nvalues.groupby('HydroID').agg({'hydroid_ManningN': ['median', 'min', 'max', 'std', 'count']})
        df_nrange['hydroid_ManningN','range'] = df_nrange['hydroid_ManningN','max'] - df_nrange['hydroid_ManningN','min']
        df_nrange = df_nrange.join(df_nvalues_mag, how='outer') # join the df_nvalues_mag containing hydroid_manningn values per flood magnitude category
        df_nrange = df_nrange.merge(df_huc_lid, how='outer', on='HydroID') # join the df_huc_lid df to add attributes for lid and huc#
        log_text += 'Statistics for Modified Roughness Calcs -->' +'\n'
        log_text += df_nrange.to_string() + '\n'
        log_text += '----------------------------------------\n\n'

        ## Optional: Output csv with SRC calc stats
        if optional_outputs == 'True':
            output_stats_n_csv = os.path.join(fim_directory, huc, 'stats_src_n_vals_' + huc + '.csv')
            df_nrange.to_csv(output_stats_n_csv,index=True)

        ## subset the original hydrotable dataframe and subset to one row per HydroID
        df_nmerge = df_htable[['HydroID','feature_id','NextDownID','LENGTHKM','LakeID','order_']].drop_duplicates(['HydroID'],keep='first') 

        ## Create attributes to traverse the flow network between HydroIDs
        df_nmerge = branch_network(df_nmerge)

        ## Merge the newly caluclated ManningN dataframes
        df_nmerge = df_nmerge.merge(df_mann_hydroid, how='left', on='HydroID')
        df_nmerge = df_nmerge.merge(df_updated, how='left', on='HydroID')

        ## Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to non-calb hydroids (constrained to first 10km of hydroids)
        #df_nmerge.sort_values(by=['NextDownID'], inplace=True)
        dist_accum = 0; hyid_count = 0; hyid_accum_count = 0; 
        run_accum_mann = 0; group_ManningN = 0; branch_start = 1                                        # initialize counter and accumulation variables
        lid_count = 0; prev_lid = 'x'
        for index, row in df_nmerge.iterrows():                                                         # loop through the df (parse by hydroid)
            if int(df_nmerge.loc[index,'branch_id']) != branch_start:                                   # check if start of new branch
                dist_accum = 0; hyid_count = 0; hyid_accum_count = 0;                                   # initialize counter vars
                run_accum_mann = 0; group_ManningN = 0                                                  # initialize counter vars
                branch_start = int(df_nmerge.loc[index,'branch_id'])                                    # reassign the branch_start var to evaluate on next iteration
            #     lid_count = 0                                                                         # use the code below to withold downstream hydroid_ManningN values (use this for downstream evaluation tests)
            # if not pd.isna(df_nmerge.loc[index,'ahps_lid']):
            #     if df_nmerge.loc[index,'ahps_lid'] == prev_lid:
            #         lid_count += 1
            #         if lid_count > 3: # only keep the first 3 HydroID n values (everything else set to null for downstream application)
            #             df_nmerge.loc[index,'hydroid_ManningN'] = np.nan
            #             df_nmerge.loc[index,'featid_ManningN'] = np.nan
            #     else:
            #         lid_count = 1
            #     prev_lid = df_nmerge.loc[index,'ahps_lid']
            if np.isnan(df_nmerge.loc[index,'hydroid_ManningN']):                                       # check if the hydroid_ManningN value is nan (indicates a non-calibrated hydroid)
                df_nmerge.loc[index,'accum_dist'] = row['LENGTHKM'] + dist_accum                        # calculate accumulated river distance
                dist_accum += row['LENGTHKM']                                                           # add hydroid length to the dist_accum var
                hyid_count = 0                                                                          # reset the hydroid counter to 0
                df_nmerge.loc[index,'hyid_accum_count'] = hyid_accum_count                              # output the hydroid accum counter
                if dist_accum < DOWNSTREAM_THRESHOLD:                                                   # check if the accum distance is less than Xkm downstream from valid hydroid_ManningN group value
                    if hyid_accum_count > 1:                                                            # only apply the group_ManningN if there are 2 or more valid hydorids that contributed to the upstream group_ManningN
                        df_nmerge.loc[index,'group_ManningN'] = group_ManningN                          # output the group_ManningN var
                else:
                    run_avg_mann = 0                                                                    # reset the running average manningn variable (greater than 10km downstream)
            else:                                                                                       # performs the following for hydroids that have a valid hydroid_ManningN value
                dist_accum = 0; hyid_count += 1                                                         # initialize vars
                df_nmerge.loc[index,'accum_dist'] = 0                                                   # output the accum_dist value (set to 0)
                if hyid_count == 1:                                                                     # checks if this the first in a series of valid hydroid_ManningN values
                    run_accum_mann = 0; hyid_accum_count = 0                                            # initialize counter and running accumulated manningN value
                group_ManningN = (row['hydroid_ManningN'] + run_accum_mann)/float(hyid_count)           # calculate the group_ManningN (NOTE: this will continue to change as more hydroid values are accumulated in the "group" moving downstream)
                df_nmerge.loc[index,'group_ManningN'] = group_ManningN                                  # output the group_ManningN var 
                df_nmerge.loc[index,'hyid_count'] = hyid_count                                          # output the hyid_count var 
                run_accum_mann += row['hydroid_ManningN']                                               # add current hydroid manningn value to the running accum mann var
                hyid_accum_count += 1                                                                   # increase the # of hydroid accum counter
                df_nmerge.loc[index,'hyid_accum_count'] = hyid_accum_count                              # output the hyid_accum_count var

        ## Delete unnecessary intermediate outputs
        if 'hyid_count' in df_nmerge.columns:
            df_nmerge.drop(['hyid_count'], axis=1, inplace=True) # drop hydroid counter if it exists
        df_nmerge.drop(['accum_dist','hyid_accum_count'], axis=1, inplace=True) # drop accum vars from group calc

        ## Create a df with the median hydroid_ManningN value per feature_id
        df_mann_featid = df_nmerge.groupby(["feature_id"])[['hydroid_ManningN']].mean()
        df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)
        df_nmerge = df_nmerge.merge(df_mann_featid, how='left', on='feature_id')

        if not df_nmerge['hydroid_ManningN'].isnull().all():
            ## Temp testing filter to only use the hydroid manning n values (drop the featureid and group ManningN variables)
            #df_nmerge = df_nmerge.assign(featid_ManningN=np.nan)
            #df_nmerge = df_nmerge.assign(group_ManningN=np.nan)

            ## Create the adjust_ManningN column by combining the hydroid_ManningN with the featid_ManningN (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
            conditions  = [ (df_nmerge['hydroid_ManningN'].isnull()) & (df_nmerge['featid_ManningN'].notnull()), (df_nmerge['hydroid_ManningN'].isnull()) & (df_nmerge['featid_ManningN'].isnull()) & (df_nmerge['group_ManningN'].notnull()) ]
            choices     = [ df_nmerge['featid_ManningN'], df_nmerge['group_ManningN'] ]
            df_nmerge['adjust_ManningN'] = np.select(conditions, choices, default=df_nmerge['hydroid_ManningN'])
            df_nmerge.drop(['feature_id','NextDownID','LENGTHKM','LakeID','order_'], axis=1, inplace=True) # drop these columns to avoid duplicates where merging with the full hydroTable df
            
            ## Update the catchments polygon .gpkg with joined attribute - "src_calibrated"
            if os.path.isfile(catchments_poly_path):
                input_catchments = gpd.read_file(catchments_poly_path)
                ## Create new "src_calibrated" column for viz query
                if 'src_calibrated' in input_catchments.columns: # check if this attribute already exists and drop if needed
                    input_catchments.drop(['src_calibrated'], axis=1, inplace=True)
                df_nmerge['src_calibrated'] = np.where(df_nmerge['adjust_ManningN'].notnull(), 'True', 'False')
                output_catchments = input_catchments.merge(df_nmerge[['HydroID','src_calibrated']], how='left', on='HydroID')
                output_catchments['src_calibrated'].fillna('False', inplace=True)
                output_catchments.to_file(catchments_poly_path,driver="GPKG",index=False) # overwrite the previous layer
                df_nmerge.drop(['src_calibrated'], axis=1, inplace=True)
            ## Optional ouputs: 1) merge_n_csv csv with all of the calculated n values and 2) a catchments .gpkg with new joined attributes
            if optional_outputs == 'True':
                output_merge_n_csv = os.path.join(fim_directory, huc, 'merge_src_n_vals_' + huc + '.csv')
                df_nmerge.to_csv(output_merge_n_csv,index=False)
                ## output new catchments polygon layer with several new attributes appended
                if os.path.isfile(catchments_poly_path):
                    input_catchments = gpd.read_file(catchments_poly_path)
                    output_catchments_fileName = os.path.join(os.path.split(catchments_poly_path)[0],"gw_catchments_src_adjust_" + str(huc) + ".gpkg")
                    output_catchments = input_catchments.merge(df_nmerge, how='left', on='HydroID')
                    output_catchments.to_file(output_catchments_fileName,driver="GPKG",index=False)

            ## Merge the final ManningN dataframe to the original hydroTable
            df_nmerge.drop(['ahps_lid','start_catch','route_count','branch_id','hydroid_ManningN','featid_ManningN','group_ManningN',], axis=1, inplace=True) # drop these columns to avoid duplicates where merging with the full hydroTable df
            df_htable = df_htable.merge(df_nmerge, how='left', on='HydroID')
            df_htable['adjust_src_on'] = np.where(df_htable['adjust_ManningN'].notnull(), 'True', 'False') # create true/false column to clearly identify where new roughness values are applied

            ## Create the ManningN column by combining the hydroid_ManningN with the default_ManningN (use modified where available)
            df_htable['ManningN'] = np.where(df_htable['adjust_ManningN'].isnull(),df_htable['default_ManningN'],df_htable['adjust_ManningN'])

            ## Calculate new discharge_cms with new adjusted ManningN
            df_htable['discharge_cms'] = df_htable['WetArea (m2)']* \
            pow(df_htable['HydraulicRadius (m)'],2.0/3)* \
            pow(df_htable['SLOPE'],0.5)/df_htable['ManningN']

            ## Replace discharge_cms with 0 or -999 if present in the original discharge (carried over from thalweg notch workaround in SRC post-processing)
            df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==0.0,0.0,inplace=True)
            df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==-999,-999,inplace=True)

            ## Export a new hydroTable.csv and overwrite the previous version
            out_htable = os.path.join(fim_directory, huc, 'hydroTable.csv')
            df_htable.to_csv(out_htable,index=False)

            ## output new src json (overwrite previous)
            output_src_json(df_htable,output_src_json_file)

        else:
            print('ALERT: ' + str(huc) + ' --> no valid hydroid roughness calculations after removing lakeid catchments from consideration')
            log_text += 'ALERT: ' + str(huc) + ' --> no valid hydroid roughness calculations after removing lakeid catchments from consideration\n'

    else:
        print('ALERT: ' + str(huc) + ' --> no valid roughness calculations - please check point data and src calculations to evaluate')
        log_text += 'ALERT: ' + str(huc) + ' --> no valid roughness calculations - please check point data and src calculations to evaluate\n'

    log_text += 'Completed: ' + str(huc)
    print("Completed huc --> " + str(huc))
    return(log_text)

def branch_network(df_input_htable):
    df_input_htable = df_input_htable.astype({'NextDownID': 'int64'}) # ensure attribute has consistent format as int
    df_input_htable = df_input_htable.loc[df_input_htable['LakeID'] == -999] # remove all hydroids associated with lake/water body (these often have disjoined artifacts in the network)
    df_input_htable["start_catch"] = ~df_input_htable['HydroID'].isin(df_input_htable['NextDownID']) # define start catchments as hydroids that are not found in the "NextDownID" attribute for all other hydroids
            
    df_input_htable.set_index('HydroID',inplace=True,drop=False) # set index to the hydroid
    branch_heads = deque(df_input_htable[df_input_htable['start_catch'] == True]['HydroID'].tolist()) # create deque of hydroids to define start points in the while loop
    visited = set() # create set to keep track of all hydroids that have been accounted for
    branch_count = 0 # start branch id 
    while branch_heads:
        hid = branch_heads.popleft() # pull off left most hydroid from deque of start hydroids
        Q = deque(df_input_htable[df_input_htable['HydroID'] == hid]['HydroID'].tolist()) # create a new deque that will be used to populate all relevant downstream hydroids
        vert_count = 0; branch_count += 1
        while Q:
            q = Q.popleft()
            if q not in visited:
                df_input_htable.loc[df_input_htable.HydroID==q,'route_count'] = vert_count # assign var with flow order ranking
                df_input_htable.loc[df_input_htable.HydroID==q,'branch_id'] = branch_count # assign var with current branch id
                vert_count += 1
                visited.add(q)
                nextid = df_input_htable.loc[q,'NextDownID'] # find the id for the next downstream hydroid
                order = df_input_htable.loc[q,'order_'] # find the streamorder for the current hydroid
            
                if nextid not in visited and nextid in df_input_htable.HydroID:
                    check_confluence = (df_input_htable.NextDownID == nextid).sum() > 1 # check if the NextDownID is referenced by more than one hydroid (>1 means this is a confluence)
                    nextorder = df_input_htable.loc[nextid,'order_'] # find the streamorder for the next downstream hydroid
                    if nextorder > order and check_confluence == True: # check if the nextdownid streamorder is greater than the current hydroid order and the nextdownid is a confluence (more than 1 upstream hydroid draining to it)
                        branch_heads.append(nextid) # found a terminal point in the network (append to branch_heads for second pass)
                        continue # if above conditions are True than stop traversing downstream and move on to next starting hydroid
                    Q.append(nextid)
    df_input_htable.reset_index(drop=True, inplace=True) # reset index (previously using hydroid as index)
    df_input_htable.sort_values(['branch_id','route_count'], inplace=True) # sort the dataframe by branch_id and then by route_count (need this ordered to ensure upstream to downstream ranking for each branch)
    return(df_input_htable)

def output_src_json(df_htable,output_src_json_file):
    output_src_json = dict()
    hydroID_list = np.unique(df_htable['HydroID'])

    for hid in hydroID_list:
        indices_of_hid = df_htable['HydroID'] == hid
        stage_list = df_htable['stage'][indices_of_hid].astype(float)
        q_list = df_htable['discharge_cms'][indices_of_hid].astype(float)
        stage_list = stage_list.tolist()
        q_list = q_list.tolist()
        output_src_json[str(hid)] = { 'q_list' : q_list , 'stage_list' : stage_list }

    with open(output_src_json_file,'w') as f:
        json.dump(output_src_json,f,sort_keys=True)

    for hid in hydroID_list:
        indices_of_hid = df_htable['HydroID'] == hid
        stage_list = df_htable['stage'][indices_of_hid].astype(float)
        q_list = df_htable['discharge_cms'][indices_of_hid].astype(float)
        stage_list = stage_list.tolist()
        q_list = q_list.tolist()
        output_src_json[str(hid)] = { 'q_list' : q_list , 'stage_list' : stage_list }

    with open(output_src_json_file,'w') as f:
        json.dump(output_src_json,f,sort_keys=True)


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

        ## Call update_rating_curve() to perform the rating curve calibration.
        ## Still testing, so I'm having the code print out any exceptions.
        try:
            log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs)
        except Exception as e:
            print(e)
            log_text = 'ERROR!!!: HUC ' + str(huc) + ' --> ' + str(e)
        #log_text = update_rating_curve(fim_directory, water_edge_median_df, htable_path, output_src_json_file, huc, catchments_poly_path, optional_outputs)
    else:
        log_text = 'WARNING: ' + str(huc) + ': no valid observation points found within the huc catchments (skipping)'
    return(log_text)


def ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number, inter_outputs):

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
    wbd_huc_read = wbd_huc_read.to_crs(hand_crs_default)

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
        print(huc)
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
            print("HAND grid for " + huc + " does not exist.")
            continue
        if not os.path.exists(catchments_path):
            print("Catchments grid for " + huc + " does not exist.")
            continue
        if not os.path.isfile(output_src_json_file):
            print("ALERT: Rating Curve JSON file for " + huc + " does not exist. --> Will create a new file.")

        ## Define path to hydroTable.csv.
        htable_path = os.path.join(fim_directory, huc, 'hydroTable.csv')
        if not os.path.exists(htable_path):
            print("hydroTable for " + huc + " does not exist.")
            continue

        procs_list.append([fim_directory, huc, hand_path, catchments_path, catchments_poly_path, water_edge_df, output_src_json_file, htable_path, inter_outputs])

    with Pool(processes=job_number) as pool:
                log_output = pool.map(process_points, procs_list)
                log_file.writelines(["%s\n" % item  for item in log_output])


if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    ## Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve given a shapefile containing points of known water boundary.')
    parser.add_argument('-p','--points-layer',help='Path to points layer containing known water boundary locations',required=True)
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-w','--wbd-path',help='Path to national huc layer.',required=True)
    parser.add_argument('-i','--extra-outputs',help='Optional: "True" or "False" --> Include intermediate output files for debugging/testing',default='False',required=False)
    parser.add_argument('-s','--scale',help='Optional: HUC6 or HUC8 -- default is HUC8', default='HUC8',required=False)
    parser.add_argument('-m','--downstream-thresh',help='Optional Override: distance in km to propogate modified roughness values downstream', default=DOWNSTREAM_THRESHOLD,required=False)
    parser.add_argument('-j','--job-number',help='Optional: Number of jobs to use',required=False,default=2)

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    points_layer = args['points_layer']
    fim_directory = args['fim_directory']
    wbd_path = args['wbd_path']
    inter_outputs = args['extra_outputs']
    scale = args['scale']
    ds_thresh_override = args['downstream_thresh']
    job_number = int(args['job_number'])

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
    output_dir = os.path.join(fim_directory,"src_optimization")
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

    ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number, inter_outputs)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()
