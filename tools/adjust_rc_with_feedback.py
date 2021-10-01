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


def update_rating_curve(fim_directory, output_csv, htable_path, output_src_json_file, huc):
    print("Processing huc --> " + str(huc))
    log_file.write("\nProcessing huc --> " + str(huc) + '\n')
    df_gmed = pd.read_csv(output_csv) # read csv to import as a dataframe
    df_gmed = df_gmed[df_gmed.hydroid != 0] # remove entries that do not have a valid hydroid

    # Read in the hydroTable.csv and check wether it has previously been updated (rename orig columns if needed)
    df_htable = pd.read_csv(htable_path)
    if 'orig_discharge_cms' in df_htable.columns:
        df_htable = df_htable[['HydroID','feature_id','stage','orig_discharge_cms','HydraulicRadius (m)','WetArea (m2)','SLOPE','default_ManningN','HUC','LakeID']]
        df_htable.rename(columns={'orig_discharge_cms':'discharge_cms','default_ManningN':'ManningN'}, inplace=True)
    else:
        df_htable = df_htable[['HydroID','feature_id','stage','discharge_cms','HydraulicRadius (m)','WetArea (m2)','SLOPE','ManningN','HUC','LakeID']]

    # loop through the user provided point data --> stage/flow dataframe row by row
    for index, row in df_gmed.iterrows():
        df_htable_hydroid = df_htable[df_htable.HydroID == row.hydroid] # filter htable for entries with matching hydroid
        find_src_stage = df_htable_hydroid.loc[df_htable_hydroid['stage'].sub(row.hand).abs().idxmin()] # find closest matching stage to the user provided HAND value
        # copy the corresponding htable values for the matching stage->HAND lookup
        df_gmed.loc[index,'src_stage'] = find_src_stage.stage
        df_gmed.loc[index,'ManningN'] = find_src_stage.ManningN
        df_gmed.loc[index,'SLOPE'] = find_src_stage.SLOPE
        df_gmed.loc[index,'HydraulicRadius_m'] = find_src_stage['HydraulicRadius (m)']
        df_gmed.loc[index,'WetArea_m2'] = find_src_stage['WetArea (m2)']
        df_gmed.loc[index,'discharge_cms'] = find_src_stage.discharge_cms

    ## Create a df of hydroids and featureids
    df_hydro_feat = df_htable.groupby(["HydroID"])[["feature_id"]].median()
#    print(df_hydro_feat.to_string())

    output_calc_n_csv = os.path.join(fim_directory, huc, 'calc_src_n_vals_' + huc + '.csv')
    df_gmed.to_csv(output_calc_n_csv,index=False)

    ## Calculate roughness using Manning's equation
    df_gmed.rename(columns={'ManningN':'ManningN_default','hydroid':'HydroID'}, inplace=True) # rename the previous ManningN column
    df_gmed['hydroid_ManningN'] = df_gmed['WetArea_m2']* \
    pow(df_gmed['HydraulicRadius_m'],2.0/3)* \
    pow(df_gmed['SLOPE'],0.5)/df_gmed['flow']
#    print('Adjusted Mannings N Calculations -->')
#    print(df_gmed)

    # Create dataframe to check for erroneous Manning's n values (>0.6 or <0.001)
    df_gmed['Mann_flag'] = np.where((df_gmed['hydroid_ManningN'] >= 0.6) | (df_gmed['hydroid_ManningN'] <= 0.001),'Fail','Pass')
    df_mann_flag = df_gmed[(df_gmed['hydroid_ManningN'] >= 0.6) | (df_gmed['hydroid_ManningN'] <= 0.001)][['HydroID','hydroid_ManningN']]
#    print('Here is the df with mann_flag filter:')
#    print(df_mann_flag.to_string())
    if not df_mann_flag.empty:
        log_file.write('!!! Flaged Mannings Roughness values below !!!' +'\n')
        log_file.write(df_mann_flag.to_string() + '\n')

    # Export csv with the newly calculated Manning's N values
    output_calc_n_csv = os.path.join(fim_directory, huc, 'calc_src_n_vals_' + huc + '.csv')
    df_gmed.to_csv(output_calc_n_csv,index=False)

    # filter the modified Manning's n dataframe for values out side allowable range
    df_gmed = df_gmed[df_gmed['Mann_flag'] == 'Pass']

    # Merge df with hydroid and featureid crosswalked
    df_gmed = df_gmed.merge(df_hydro_feat, how='left', on='HydroID')

    # Create df with the most recent collection time entry
    df_updated = df_gmed.groupby(["HydroID"])[['coll_time']].max()
    df_updated.rename(columns={'coll_time':'last_updated'}, inplace=True)

    # cacluate median ManningN to handle cases with multiple hydroid entries
    df_mann = df_gmed.groupby(["HydroID"])[['hydroid_ManningN']].median()
#    print('df_mann:')
#    print(df_mann)

    # Create a df with the median hydroid_ManningN value per feature_id
    df_mann_featid = df_gmed.groupby(["feature_id"])[['hydroid_ManningN']].median()
    df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)

    # Rename the original hydrotable variables to allow new calculations to use the primary var name
    df_htable.rename(columns={'ManningN':'default_ManningN','discharge_cms':'orig_discharge_cms'}, inplace=True)

    ## Check for large variabilty in the calculated Manning's N values (for cases with mutliple entries for a singel hydroid)
    df_nrange = df_gmed.groupby('HydroID').agg({'hydroid_ManningN': ['median', 'min', 'max','count']})
    log_file.write('Statistics for Modified Roughness Calcs -->' +'\n')
    log_file.write(df_nrange.to_string() + '\n')
    log_file.write('----------------------------------------\n\n')

    # Merge the newly caluclated ManningN dataframe with the original hydroTable
    df_htable = df_htable.merge(df_mann, how='left', on='HydroID')
    df_htable = df_htable.merge(df_mann_featid, how='left', on='feature_id')
    df_htable = df_htable.merge(df_updated, how='left', on='HydroID')

    # Create the modify_ManningN column by combining the hydroid_ManningN with the featid_ManningN (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
    df_htable['modify_ManningN'] = np.where(df_htable['hydroid_ManningN'].isnull(),df_htable['featid_ManningN'],df_htable['hydroid_ManningN'])

    # Create the ManningN column by combining the hydroid_ManningN with the default_ManningN (use modified where available)
    df_htable['ManningN'] = np.where(df_htable['modify_ManningN'].isnull(),df_htable['default_ManningN'],df_htable['modify_ManningN'])

    # Calculate new discharge_cms with new ManningN
    df_htable['discharge_cms'] = df_htable['WetArea (m2)']* \
    pow(df_htable['HydraulicRadius (m)'],2.0/3)* \
    pow(df_htable['SLOPE'],0.5)/df_htable['ManningN']

    # Replace discharge_cms with 0 or -999 if present in the original discharge
    df_htable['discharge_cms'].mask(df_htable['orig_discharge_cms']==0.0,0.0,inplace=True)
    df_htable['discharge_cms'].mask(df_htable['orig_discharge_cms']==-999,-999,inplace=True)

    # Export a new hydroTable.csv and overwrite the previous version
    out_htable = os.path.join(fim_directory, huc, 'hydroTable.csv')
    df_htable.to_csv(out_htable,index=False)

    # output new src json (overwrite previous)
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


def process_points(args):
        
    fim_directory = args[0]
    huc = args[1]
    hand_path = args[2]
    catchments_path = args[3]
    water_edge_df = args[4]
    output_src_json_file = args[5]
    htable_path = args[6]
    
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
    water_edge_median_ds = water_edge_df.groupby(["hydroid", "flow", "submitter", "coll_time", "flow_unit"])['hand'].median()
    
    # Write user_supplied_n_vals to CSV for next step.
    output_csv = os.path.join(fim_directory, huc, 'user_supplied_n_vals_' + huc + '.csv')
    water_edge_median_ds.to_csv(output_csv)
    del water_edge_median_ds

    # Call update_rating_curve() to perform the rating curve calibration.
    # Still testing, so I'm having the code print out any exceptions.
    try:
        update_rating_curve(fim_directory, output_csv, htable_path, output_src_json_file, huc)
    except Exception as e:
        print(e)


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
    print("Joining points to HUC8...")
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
        else:
            hand_path = os.path.join(fim_directory, huc, 'hand_grid_' + huc + '.tif')
            catchments_path = os.path.join(fim_directory, huc, 'catchments_' + huc + '.tif')
            output_src_json_file = os.path.join(fim_directory, huc, 'rating_curves_' + huc + '.json')
        
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

        procs_list.append([fim_directory, huc, hand_path, catchments_path, water_edge_df, output_src_json_file, htable_path])
            
    with Pool(processes=job_number) as pool:
                pool.map(process_points, procs_list)


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
