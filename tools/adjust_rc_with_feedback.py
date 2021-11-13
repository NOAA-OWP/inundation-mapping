import argparse
import geopandas as gpd
from geopandas.tools import sjoin
import os
import rasterio
import pandas as pd
import numpy as np
import sys
import json
import multiprocessing
from multiprocessing import Pool


def update_rating_curve(fim_directory, pt_n_values_csv, htable_path, output_src_json_file, huc, catchments_poly_path):
    print("Processing huc --> " + str(huc))
    log_text = "\nProcessing huc --> " + str(huc) + '\n'
    df_nvalues = pd.read_csv(pt_n_values_csv) # read csv to import as a dataframe
    df_nvalues = df_nvalues[df_nvalues.hydroid != 0] # remove null entries that do not have a valid hydroid

    # Read in the hydroTable.csv and check wether it has previously been updated (rename default columns if needed)
    df_htable = pd.read_csv(htable_path)
    if 'default_ManningN' in df_htable.columns:
        df_htable.drop(['ManningN','discharge_cms','hydroid_ManningN','featid_ManningN','last_updated','modify_ManningN'], axis=1, inplace=True) # Delete these to prevent duplicates (if adjust_rc_with_feedback.py was previously applied)
        #df_htable = df_htable[['HydroID','feature_id','stage','orig_discharge_cms','HydraulicRadius (m)','WetArea (m2)','SLOPE','default_ManningN','HUC','LakeID']]
        df_htable.rename(columns={'default_discharge_cms':'discharge_cms','default_ManningN':'ManningN'}, inplace=True)
        log_text += str(huc) + ': found previous hydroTable calibration attributes --> removing previous calb columns...\n'

    # loop through the user provided point data --> stage/flow dataframe row by row
    for index, row in df_nvalues.iterrows():
        df_htable_hydroid = df_htable[df_htable.HydroID == row.hydroid] # filter htable for entries with matching hydroid
        find_src_stage = df_htable_hydroid.loc[df_htable_hydroid['stage'].sub(row.hand).abs().idxmin()] # find closest matching stage to the user provided HAND value
        # copy the corresponding htable values for the matching stage->HAND lookup
        df_nvalues.loc[index,'feature_id'] = find_src_stage.feature_id
        df_nvalues.loc[index,'NextDownID'] = find_src_stage.NextDownID
        df_nvalues.loc[index,'LENGTHKM'] = find_src_stage.LENGTHKM
        df_nvalues.loc[index,'src_stage'] = find_src_stage.stage
        df_nvalues.loc[index,'ManningN'] = find_src_stage.ManningN
        df_nvalues.loc[index,'SLOPE'] = find_src_stage.SLOPE
        df_nvalues.loc[index,'HydraulicRadius_m'] = find_src_stage['HydraulicRadius (m)']
        df_nvalues.loc[index,'WetArea_m2'] = find_src_stage['WetArea (m2)']
        df_nvalues.loc[index,'discharge_cms'] = find_src_stage.discharge_cms

    # mask src values that crosswalk to the SRC zero point (src_stage ~ 0 or discharge <= 0)
    df_nvalues[['HydraulicRadius_m','WetArea_m2']] = df_nvalues[['HydraulicRadius_m','WetArea_m2']].mask((df_nvalues['src_stage'] <= 0.1) | (df_nvalues['discharge_cms'] <= 0.0), np.nan)

    ## Calculate roughness using Manning's equation
    df_nvalues.rename(columns={'ManningN':'default_ManningN','hydroid':'HydroID'}, inplace=True) # rename the previous ManningN column
    df_nvalues['hydroid_ManningN'] = df_nvalues['WetArea_m2']* \
    pow(df_nvalues['HydraulicRadius_m'],2.0/3)* \
    pow(df_nvalues['SLOPE'],0.5)/df_nvalues['flow']

    # Create dataframe to check for erroneous Manning's n values (>0.6 or <0.001)
    df_nvalues['Mann_flag'] = np.where((df_nvalues['hydroid_ManningN'] >= 0.6) | (df_nvalues['hydroid_ManningN'] <= 0.001) | (df_nvalues['hydroid_ManningN'] == np.nan),'Fail','Pass')
    df_mann_flag = df_nvalues[(df_nvalues['Mann_flag'] == 'Fail')][['HydroID','hydroid_ManningN']]
    if not df_mann_flag.empty:
        log_text += '!!! Flaged Mannings Roughness values below !!!' +'\n'
        log_text += df_mann_flag.to_string() + '\n'

    # Create magnitude column by subsetting the "layer" attribute
    df_nvalues['magnitude'] = df_nvalues['layer'].str.split("_").str[5]
    df_nvalues.drop(['layer'], axis=1, inplace=True)

    # pivot the magnitude column to display n value for each magnitude at each hydroid
    df_nvalues_mag = df_nvalues.pivot_table(index='HydroID', columns='magnitude', values=['hydroid_ManningN'], aggfunc='mean') # if there are multiple entries per hydroid and magnitude - aggregate using mean
    
    # Export csv with the newly calculated Manning's N values
    output_calc_n_csv = os.path.join(fim_directory, huc, 'calc_src_n_vals_' + huc + '.csv')
    df_nvalues.to_csv(output_calc_n_csv,index=False)

    # filter the modified Manning's n dataframe for values out side allowable range
    df_nvalues = df_nvalues[df_nvalues['Mann_flag'] == 'Pass']

    # Merge df with hydroid and featureid crosswalked
    #df_nvalues = df_nvalues.merge(df_hydro_feat, how='left', on='HydroID')

    # Create df with the most recent collection time entry and submitter attribs
    df_updated = df_nvalues[['HydroID','coll_time','submitter']] # subset the dataframe
    df_updated = df_updated.sort_values('coll_time').drop_duplicates(['HydroID'],keep='last') # sort by collection time and then drop duplicate HydroIDs (keep most recent coll_time per HydroID)
    df_updated.rename(columns={'coll_time':'last_updated'}, inplace=True)

    # cacluate median ManningN to handle cases with multiple hydroid entries
    df_mann_hydroid = df_nvalues.groupby(["HydroID"])[['hydroid_ManningN']].median()

    # Create a df with the median hydroid_ManningN value per feature_id
    df_mann_featid = df_nvalues.groupby(["feature_id"])[['hydroid_ManningN']].median()
    df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)

    # Rename the original hydrotable variables to allow new calculations to use the primary var name
    df_htable.rename(columns={'ManningN':'default_ManningN','discharge_cms':'default_discharge_cms'}, inplace=True)

    ## Check for large variabilty in the calculated Manning's N values (for cases with mutliple entries for a singel hydroid)
    df_nrange = df_nvalues.groupby('HydroID').agg({'hydroid_ManningN': ['median', 'min', 'max', 'std', 'count']})
    df_nrange['hydroid_ManningN','range'] = df_nrange['hydroid_ManningN','max'] - df_nrange['hydroid_ManningN','min']
    df_nrange = df_nrange.join(df_nvalues_mag, how='outer') # join the df_nvalues_mag containing hydroid_manningn values per flood magnitude category
    output_stats_n_csv = os.path.join(fim_directory, huc, 'stats_src_n_vals_' + huc + '.csv')
    df_nrange.to_csv(output_stats_n_csv,index=True)
    log_text += 'Statistics for Modified Roughness Calcs -->' +'\n'
    log_text += df_nrange.to_string() + '\n'
    log_text += '----------------------------------------\n\n'

    # Merge the newly caluclated ManningN dataframes
    df_nmerge = df_htable[['HydroID','feature_id','NextDownID','LENGTHKM']].drop_duplicates(['HydroID'],keep='first') # subset the dataframe
    df_nmerge = df_nmerge.merge(df_mann_hydroid, how='left', on='HydroID')
    df_nmerge = df_nmerge.merge(df_mann_featid, how='left', on='feature_id')
    df_nmerge = df_nmerge.merge(df_updated, how='left', on='HydroID')

    # Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to non-calb hydroids (constrained to first 10km of hydroids)
    df_nmerge.sort_values(by=['HydroID'], inplace=True)
    dist_accum = 0; hyid_count = 0; hyid_accum_count = 0; run_accum_mann = 0; group_ManningN = 0    # initialize counter and accumulation variables
    for index, row in df_nmerge.iterrows():                                                         # loop through the df (parse by hydroid)
        if np.isnan(row['hydroid_ManningN']):                                                       # check if the hydroid_ManningN value is nan (indicates a non-calibrated hydroid)
            df_nmerge.loc[index,'accum_dist'] = row['LENGTHKM'] + dist_accum                        # calculate accumulated river distance
            dist_accum += row['LENGTHKM']                                                           # add hydroid length to the dist_accum var
            hyid_count = 0                                                                          # reset the hydroid counter to 0
            df_nmerge.loc[index,'hyid_accum_count'] = hyid_accum_count                              # output the hydroid accum counter
            if dist_accum < 10.0:                                                                   # check if the accum distance is less than 10km downstream from valid hydroid_ManningN group value
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

    # Ouput csv with all of the calculated n values
    df_nmerge.drop(['accum_dist','hyid_count','hyid_accum_count'], axis=1, inplace=True) # drop intermediate vars from group calc
    output_merge_n_csv = os.path.join(fim_directory, huc, 'merge_src_n_vals_' + huc + '.csv')
    df_nmerge.to_csv(output_merge_n_csv,index=False)

    # Merge the final ManningN dataframe to the original hydroTable
    df_nmerge.drop(['feature_id','NextDownID','LENGTHKM'], axis=1, inplace=True)
    df_htable = df_htable.merge(df_nmerge, how='left', on='HydroID')
    #df_htable = df_htable.merge(df_mann_featid, how='left', on='feature_id')
    #df_htable = df_htable.merge(df_updated, how='left', on='HydroID')

    # Create the modify_ManningN column by combining the hydroid_ManningN with the featid_ManningN (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
    df_htable['modify_ManningN'] = np.where(df_htable['hydroid_ManningN'].isnull(),df_htable['featid_ManningN'],df_htable['hydroid_ManningN'])

    # Create the ManningN column by combining the hydroid_ManningN with the default_ManningN (use modified where available)
    df_htable['ManningN'] = np.where(df_htable['modify_ManningN'].isnull(),df_htable['default_ManningN'],df_htable['modify_ManningN'])

    # Calculate new discharge_cms with new ManningN
    df_htable['discharge_cms'] = df_htable['WetArea (m2)']* \
    pow(df_htable['HydraulicRadius (m)'],2.0/3)* \
    pow(df_htable['SLOPE'],0.5)/df_htable['ManningN']

    # Replace discharge_cms with 0 or -999 if present in the original discharge
    df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==0.0,0.0,inplace=True)
    df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==-999,-999,inplace=True)

    # Export a new hydroTable.csv and overwrite the previous version
    out_htable = os.path.join(fim_directory, huc, 'hydroTable_usgs_nws.csv')
    df_htable.to_csv(out_htable,index=False)

    # output new src json (overwrite previous)
    output_src_json(df_htable,output_src_json_file)

    # output new catchments polygon layer with the new manning's n value attributes appended
    if catchments_poly_path != '':
        input_catchments = gpd.read_file(catchments_poly_path)
        output_catchments_fileName = os.path.join(os.path.split(catchments_poly_path)[0],"gw_catchments_src_adjust.gpkg")
        output_catchments = input_catchments.merge(df_nmerge, how='left', on='HydroID')
        output_catchments.to_file(output_catchments_fileName,driver="GPKG",index=False)

    log_text += 'Completed: ' + str(huc)
    print("Completed huc --> " + str(huc))
    return(log_text)

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

    # Clip the points water_edge_df to the huc cathments polygons (for faster processing?)
    catch_poly = gpd.read_file(catchments_poly_path)
    catch_poly_crs = catch_poly.crs
    water_edge_df.to_crs(catch_poly_crs) 
    water_edge_df = gpd.clip(water_edge_df,catch_poly)

    # Define coords variable to be used in point raster value attribution.
    coords = [(x,y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]

    # Use point geometry to determine HAND raster pixel values.
    hand_src = rasterio.open(hand_path)
    hand_crs = hand_src.crs
    water_edge_df.to_crs(hand_crs)  # Reproject geodataframe to match hand_src. Should be the same, but this is a double check.
    water_edge_df['hand'] = [h[0] for h in hand_src.sample(coords)]
    hand_src.close()
    del hand_src, hand_crs,

    # Use point geometry to determine catchment raster pixel values.
    catchments_src = rasterio.open(catchments_path)
    catchments_crs = catchments_src.crs
    water_edge_df.to_crs(catchments_crs)
    water_edge_df['hydroid'] = [c[0] for c in catchments_src.sample(coords)]
    catchments_src.close()
    del catchments_src, catchments_crs

    # Get median HAND value for appropriate groups.
    water_edge_median_ds = water_edge_df.groupby(["hydroid", "flow", "submitter", "coll_time", "flow_unit","layer"])['hand'].median()

    # Write user_supplied_n_vals to CSV for next step.
    pt_n_values_csv = os.path.join(fim_directory, huc, 'user_supplied_n_vals_' + huc + '.csv')
    water_edge_median_ds.to_csv(pt_n_values_csv)
    del water_edge_median_ds

    # Call update_rating_curve() to perform the rating curve calibration.
    # Still testing, so I'm having the code print out any exceptions.
    # try:
    #     update_rating_curve(fim_directory, pt_n_values_csv, htable_path, output_src_json_file, huc)
    # except Exception as e:
    #     print(e)
    log_text = update_rating_curve(fim_directory, pt_n_values_csv, htable_path, output_src_json_file, huc, catchments_poly_path)
    return(log_text)


def ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number):

    # Define CRS to use for initial geoprocessing.
    if scale == 'HUC8':
        hand_crs_default = 'EPSG:5070'
        wbd_layer = 'WBDHU8'
    else:
        hand_crs_default = 'EPSG:3857'
        wbd_layer = 'WBDHU6'

    # Read wbd_path and points_layer.
    print("Reading WBD...")
    wbd_huc_read = gpd.read_file(wbd_path, layer=wbd_layer)
    print("Reading points layer...")
    points_layer_read = gpd.read_file(points_layer)

    # Update CRS of points_layer_read.
    points_layer_read = points_layer_read.to_crs(hand_crs_default)
    wbd_huc_read = wbd_huc_read.to_crs(hand_crs_default)

    # Spatially join the two layers.
    print("Joining points to WBD...")
    water_edge_df = sjoin(points_layer_read, wbd_huc_read)
    del wbd_huc_read

    # Convert to GeoDataFrame and add two columns for X and Y.
    gdf = gpd.GeoDataFrame(water_edge_df)
    gdf['X'] = gdf['geometry'].x
    gdf['Y'] = gdf['geometry'].y

    # Extract information into dictionary.
    huc_list = []
    for index, row in gdf.iterrows():
        huc = row[scale]

        # zfill to the appropriate scale to ensure leading zeros are present, if necessary.
        if scale == 'HUC8':
            huc = huc.zfill(8)
        else:
            huc = huc.zfill(6)

        if huc not in huc_list:
            huc_list.append(huc)
            log_file.write(str(huc) + '\n')
    del gdf

    procs_list = []  # Initialize list for mulitprocessing.

    # Define paths to relevant HUC HAND data.
    for huc in huc_list:
        print(huc)
        # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
        if scale == 'HUC8':
            hand_path = os.path.join(fim_directory, huc, 'rem_zeroed_masked.tif')
            catchments_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'src.json')
            catchments_poly_path = os.path.join(fim_directory, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
        else:
            hand_path = os.path.join(fim_directory, huc, 'hand_grid_' + huc + '.tif')
            catchments_path = os.path.join(fim_directory, huc, 'catchments_' + huc + '.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'rating_curves_' + huc + '.json')
            catchments_poly_path = ''

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

        procs_list.append([fim_directory, huc, hand_path, catchments_path, catchments_poly_path, water_edge_df, output_src_json_file, htable_path])

    with Pool(processes=job_number) as pool:
                log_output = pool.map(process_points, procs_list)
                log_file.writelines(["%s\n" % item  for item in log_output])


if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve given a shapefile containing points of known water boundary.')
    parser.add_argument('-p','--points-layer',help='Path to points layer containing known water boundary locations',required=True)
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-w','--wbd-path',help='Path to national huc layer.',required=True)
    parser.add_argument('-s','--scale',help='huc or HUC8', required=True)
    parser.add_argument('-j','--job-number',help='Number of jobs to use',required=False,default=available_cores - 2)

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    points_layer = args['points_layer']
    fim_directory = args['fim_directory']
    wbd_path = args['wbd_path']
    scale = args['scale']
    job_number = int(args['job_number'])

    if scale not in ['HUC6', 'HUC8']:
        print("scale (-s) must be HUC6s or HUC8")
        quit()

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    # Create log file for processing records
    print('This may take a few minutes...')
    sys.__stdout__ = sys.stdout
    log_file = open(os.path.join(fim_directory,'log_rating_curve_adjust.log'),"w")

    ingest_points_layer(points_layer, fim_directory, wbd_path, scale, job_number)

    # Close log file
    sys.stdout = sys.__stdout__
    log_file.close()
