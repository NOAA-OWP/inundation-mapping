#!/usr/bin/env python3

import datetime as dt
import os
import re
import sys
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from os.path import join
import shutil

import geopandas as gpd
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# ############################################from synthesize_test_cases import progress_bar_handler

path_aib_bathy_parquet = "/efs-drives/fim-dev-efs/fim-home/heidi.safa/aib_bathy_adjustment/data/ml_outputs_v1.01.parquet"
fim_dir = "/efs-drives/fim-dev-efs/fim-home/heidi.safa/aib_bathy_adjustment/data/fim_4_4_15_0/"
huc = "07130003"
bathy_file = "/efs-drives/fim-dev-efs/fim-home/bathymetry_processing/bathymetry_illinois.gpkg"

# -------------------------------------------------------
def correct_rating_for_ai_based_bathymetry(path_aib_bathy_parquet, fim_dir, huc):

    # Load AI-based bathymetry data
    ml_bathy_data = pd.read_parquet(path_aib_bathy_parquet, engine='pyarrow') #
    ml_bathy_data_df = ml_bathy_data[[
        'COMID',
        'owp_tw_inchan', # topwidth at in channel
        'owp_roughness',
        'owp_inchan_channel_area',
        'owp_inchan_channel_perimeter',
        'owp_inchan_channel_volume',
        'owp_inchan_channel_bed_area',
        'owp_y_inchan', # in channel depth
        ]]
    
    fim_huc_dir = join(fim_dir, huc)

    path_nwm_streams = join(fim_huc_dir, "nwm_subset_streams.gpkg")
    nwm_stream = gpd.read_file(path_nwm_streams)

    wbd8 = gpd.read_file(join(fim_huc_dir, 'wbd.gpkg'), engine="pyogrio", use_arrow=True)
    nwm_stream_clp = nwm_stream.clip(wbd8)

    # Create a dictionary mapping ID to order
    id_to_order = dict(zip(nwm_stream_clp['ID'], nwm_stream_clp['order_']))
    # Create a dictionary mapping ID to geometry
    id_to_geometry = dict(zip(nwm_stream_clp['ID'], nwm_stream_clp['geometry']))

    # Use map to add the order column to aib_bathy_data_df
    ml_bathy_data_df['order_'] = ml_bathy_data_df['COMID'].map(id_to_order)

    # Mask aib_bathy_data for huc of interest
    aib_bathy_data_df = ml_bathy_data_df.dropna(subset=['order_'])

    # Use map to add the geometry column to aib_bathy_data_df
    aib_bathy_data_df['geometry'] = aib_bathy_data_df['COMID'].map(id_to_geometry)

    # Convert to geodataframe
    aib_bathy_data_gdf = gpd.GeoDataFrame(aib_bathy_data_df, geometry = aib_bathy_data_df['geometry'])
    aib_bathy_data_gdf.crs = nwm_stream.crs
    aib_bathy_data_gdf = aib_bathy_data_gdf.rename(columns={'COMID': 'feature_id'})
    aib_bathy_data_gdf = aib_bathy_data_gdf.rename(columns={'owp_inchan_channel_area': 'missing_xs_area_m2'})

    feature_id_aib = aib_bathy_data_gdf["feature_id"].drop_duplicates(keep="first")
    feature_id_aib.index = range(len(feature_id_aib))

    # Calculating missing_wet_perimeter_m and adding it to aib_bathy_data_gdf
    missing_wet_perimeter_m = aib_bathy_data_gdf['owp_inchan_channel_perimeter'] - aib_bathy_data_gdf['owp_tw_inchan']
    aib_bathy_data_gdf['missing_wet_perimeter_m'] = missing_wet_perimeter_m
    aib_bathy_data_gdf['Bathymetry_source'] = "AI_Based"

    log_text = f'Calculating bathymetry adjustment: {huc}\n'

    # Get src_full from each branch
    processing_files = join(fim_huc_dir, "src_processing", "original_srcs")
    src_all_branches_path = glob.glob(join(processing_files, "*.csv"))

    # Make a directory for ai-based bathy adjusted SRCs
    path_aib_src = join(join(fim_huc_dir, "src_processing"), "aib_srcs")
    os.mkdir(path_aib_src)
    # Update src parameters with bathymetric data
    for src in src_all_branches_path:
        src_df = pd.read_csv(src)
        if 'Bathymetry_source' in src_df.columns:
            src_df = src_df.drop(columns='Bathymetry_source')
        
        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'  Branch: {branch}\n'

        # Merge in missing bathy data and fill Nans
        try:
            src_df = src_df.merge(
                aib_bathy_data_gdf[
                    ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
                ],
                on='feature_id',
                how='left',
                validate='many_to_one',
            )
        # If there's more than one feature_id in the bathy data, just take the mean
        except pd.errors.MergeError:
            reconciled_bathy_data = aib_bathy_data_gdf.groupby('feature_id')[
                ['missing_xs_area_m2', 'missing_wet_perimeter_m']
            ].mean()
            reconciled_bathy_data['Bathymetry_source'] = aib_bathy_data_gdf.groupby('feature_id')[
                'Bathymetry_source'
            ].first()
            src_df = src_df.merge(reconciled_bathy_data, on='feature_id', how='left', validate='many_to_one')

        # Exit if there are no recalculations to be made
        if ~src_df['Bathymetry_source'].any(axis=None):
            log_text += '    No matching feature_ids in this branch\n'
            continue

        src_df['missing_xs_area_m2'] = src_df['missing_xs_area_m2'].fillna(0.0)
        src_df['missing_wet_perimeter_m'] = src_df['missing_wet_perimeter_m'].fillna(0.0)

        # Add missing hydraulic geometry into base parameters
        src_df['Volume (m3)'] = src_df['Volume (m3)'] + (
            src_df['missing_xs_area_m2'] * (src_df['LENGTHKM'] * 1000)
        )
        src_df['BedArea (m2)'] = src_df['BedArea (m2)'] + (
            src_df['missing_wet_perimeter_m'] * (src_df['LENGTHKM'] * 1000)
        )
        # Recalc discharge with adjusted geometries
        src_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter_m']
        src_df['WetArea (m2)'] = src_df['WetArea (m2)'] + src_df['missing_xs_area_m2']
        src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
        src_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)'].fillna(0)
        src_df['Discharge (m3s-1)'] = (
            src_df['WetArea (m2)']
            * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
            * pow(src_df['SLOPE'], 0.5)
            / src_df['ManningN']
        )
        # Force zero stage to have zero discharge
        src_df.loc[src_df['Stage'] == 0, ['Discharge (m3s-1)']] = 0
        # Calculate number of adjusted HydroIDs

        # Write src in ai-based file        
        path2save = join(path_aib_src, src_name)
        src_df.to_csv(path2save, index=False)
        
    return feature_id_aib


def plot_aib_original_src(fim_dir, huc):
        
    fim_huc_dir = join(fim_dir, huc)
    path_aib = join(fim_huc_dir, "src_processing", "aib_srcs")
    path_original = join(fim_huc_dir, "src_processing", "original_srcs")
    csv_files_aib = glob.glob(join(path_aib,'*.csv'))
    
    # Plot original srcs and ai-based srcs
    for src_aib in csv_files_aib:
        src_name = os.path.basename(src_aib)
        branch = src_name.split(".")[0].split("_")[-1]
        path2orig_src = join(path_original, src_name)

        src_df_aib = pd.read_csv(src_aib)
        src_df = pd.read_csv(path2orig_src)

        src_df_aib_target = src_df_aib[["HydroID", "feature_id", "order_", "Stage","Discharge (m3s-1)", "Bathymetry_source"]]
        src_df_target = src_df[["HydroID", "feature_id", "order_", "Stage", "Discharge (m3s-1)", "Bathymetry_source"]]

        hydro_ids = src_df_aib_target["HydroID"].drop_duplicates(keep = 'first')

        path_fig = join(path_aib, branch)
        os.mkdir(path_fig)

        for hid in hydro_ids:

            discharge_org_df = src_df_target[src_df_target['HydroID'] == hid]['Discharge (m3s-1)']
            discharge_aib_df = src_df_aib_target[src_df_aib_target['HydroID'] == hid]['Discharge (m3s-1)']

            stage_df = src_df_aib_target[src_df_aib_target['HydroID'] == hid]['Stage']
            feature_id = src_df_aib_target[src_df_aib_target['HydroID'] == hid]['feature_id'].drop_duplicates(keep = "first").iloc[0]

            fig, ax = plt.subplots()

            colors = ['darkmagenta', 'teal', 'darkorange']

            # Define a list of line styles to use for the plots
            line_styles = ['--', 'None', ':']

            plt.plot(discharge_org_df, stage_df, label='Original', color=colors[1])
            plt.plot(discharge_aib_df, stage_df, label='AI-Based Bathymetry', color=colors[0], linestyle=line_styles[0])

            plt.xlabel('Discharge (m3s-1)')
            plt.ylabel('Stage (m)')
            plt.title(f"HUC {huc}, FID = {feature_id}, HydroID = {hid}")
            plt.legend()

            fig_name = f"{feature_id}_{hid}.png"
            path_savefig = join(path_fig, fig_name)
            plt.savefig(path_savefig)

            plt.close(fig)

# Plot original srcs and ehydro and ai-based srcs
def plot_ehydro_aib_original_srcs(fim_dir, huc):
        
    fim_huc_dir = join(fim_dir, huc)
    path_aib = join(fim_huc_dir, "src_processing", "aib_srcs")
    path_ehydro = join(fim_huc_dir, "src_processing", "ehydro_srcs")
    path_original = join(fim_huc_dir, "src_processing", "original_srcs")
    csv_files_ehydro = glob.glob(join(path_ehydro,'*.csv'))
    
    # Plot original srcs and ai-based and ehydro srcs
    for src_eh in csv_files_ehydro:

        src_name = os.path.basename(src_eh)    
        path2aib_src = join(path_aib, src_name)

        branch = src_name.split(".")[0].split("_")[-1]
        path2orig_src = join(path_original, src_name)

        src_df_ehydro = pd.read_csv(src_eh)
        src_df_aib = pd.read_csv(path2aib_src)
        src_df = pd.read_csv(path2orig_src)

        src_df_ehydro_target = src_df_ehydro[["HydroID", "feature_id", "order_", "Stage", "Discharge (m3s-1)", "Bathymetry_source"]]
        src_df_aib_target = src_df_aib[["HydroID", "feature_id", "order_", "Stage", "Discharge (m3s-1)", "Bathymetry_source"]]
        src_df_target = src_df[["HydroID", "feature_id", "order_", "Stage", "Discharge (m3s-1)", "Bathymetry_source"]]

        hydro_ids = src_df_ehydro_target["HydroID"].drop_duplicates(keep = 'first')

        path_fig = join(path_ehydro, branch)
        os.mkdir(path_fig)

        for hid in hydro_ids:

            discharge_org_df = src_df_target[src_df_target['HydroID'] == hid]['Discharge (m3s-1)']
            discharge_aib_df = src_df_aib_target[src_df_aib_target['HydroID'] == hid]['Discharge (m3s-1)']
            discharge_ehydro_df = src_df_ehydro_target[src_df_ehydro_target['HydroID'] == hid]['Discharge (m3s-1)']

            stage_df = src_df_ehydro_target[src_df_ehydro_target['HydroID'] == hid]['Stage']
            feature_id = src_df_ehydro_target[src_df_ehydro_target['HydroID'] == hid]['feature_id'].drop_duplicates(keep = "first").iloc[0]

            fig, ax = plt.subplots()

            colors = ['darkmagenta', 'teal', 'darkorange']

            # Define a list of line styles to use for the plots
            line_styles = ['--', 'None', ':']

            plt.plot(discharge_org_df, stage_df, label='Original', color=colors[1])
            plt.plot(discharge_aib_df, stage_df, label='AI-Based Bathymetry', color=colors[0], linestyle=line_styles[0])
            plt.plot(discharge_ehydro_df, stage_df, label='eHydro Bathymetry', color=colors[2], linestyle=line_styles[2])

            plt.xlabel('Discharge (m3s-1)')
            plt.ylabel('Stage (m)')
            plt.title(f"HUC {huc}, FID = {feature_id}, HydroID = {hid}")
            plt.legend()

            fig_name = f"{feature_id}_{hid}.png"
            path_savefig = join(path_fig, fig_name)
            plt.savefig(path_savefig)

            plt.close(fig)

# -------------------------------------------------------
# Adjusting synthetic rating curves using 'USACE eHydro' bathymetry data
def correct_rating_for_ehydro_bathymetry(fim_dir, huc, bathy_file, verbose):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input bathy_file.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.
        bathy_file : str
            Path to bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        Returns
        ----------
        log_text : str

    """

    log_text = f'Calculating bathymetry adjustment: {huc}\n'

    # Load wbd and use it as a mask to pull the bathymetry data
    fim_huc_dir = join(fim_dir, huc)
    wbd8_clp = gpd.read_file(join(fim_huc_dir, 'wbd8_clp.gpkg'), engine="pyogrio", use_arrow=True)
    bathy_data = gpd.read_file(bathy_file, mask=wbd8_clp, engine="fiona")
    bathy_data = bathy_data.rename(columns={'ID': 'feature_id'})
    feature_id_ehydro = bathy_data["feature_id"].drop_duplicates(keep="first")
    feature_id_ehydro.index = range(len(feature_id_ehydro))

    # Get src_full from each branch
    src_all_branches = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches.append(src_full)

    path_ehydro_src = join(join(fim_huc_dir, "src_processing"), "ehydro_srcs")
    os.mkdir(path_ehydro_src)
    # Update src parameters with bathymetric data
    for src in src_all_branches:
        src_df = pd.read_csv(src)
        if 'Bathymetry_source' in src_df.columns:
            src_df = src_df.drop(columns='Bathymetry_source')
        branch = re.search(r'branches/(\d{10}|0)/', src).group()[9:-1]
        log_text += f'  Branch: {branch}\n'

        if bathy_data.empty:
            log_text += '  There were no eHydro bathymetry feature_ids for this branch'
            src_df['Bathymetry_source'] = [""] * len(src_df)
            src_df.to_csv(src, index=False)
            return log_text

        # Merge in missing bathy data and fill Nans
        try:
            src_df = src_df.merge(
                bathy_data[
                    ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
                ],
                on='feature_id',
                how='left',
                validate='many_to_one',
            )
        # If there's more than one feature_id in the bathy data, just take the mean
        except pd.errors.MergeError:
            reconciled_bathy_data = bathy_data.groupby('feature_id')[
                ['missing_xs_area_m2', 'missing_wet_perimeter_m']
            ].mean()
            reconciled_bathy_data['Bathymetry_source'] = bathy_data.groupby('feature_id')[
                'Bathymetry_source'
            ].first()
            src_df = src_df.merge(reconciled_bathy_data, on='feature_id', how='left', validate='many_to_one')
        
        # Exit if there are no recalculations to be made
        if ~src_df['Bathymetry_source'].any(axis=None):
            log_text += '    No matching feature_ids in this branch\n'
            continue

        src_df['missing_xs_area_m2'] = src_df['missing_xs_area_m2'].fillna(0.0)
        src_df['missing_wet_perimeter_m'] = src_df['missing_wet_perimeter_m'].fillna(0.0)
        # Add missing hydraulic geometry into base parameters
        src_df['Volume (m3)'] = src_df['Volume (m3)'] + (
            src_df['missing_xs_area_m2'] * (src_df['LENGTHKM'] * 1000)
        )
        src_df['BedArea (m2)'] = src_df['BedArea (m2)'] + (
            src_df['missing_wet_perimeter_m'] * (src_df['LENGTHKM'] * 1000)
        )
        # Recalc discharge with adjusted geometries
        src_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter_m']
        src_df['WetArea (m2)'] = src_df['WetArea (m2)'] + src_df['missing_xs_area_m2']
        src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
        src_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)'].fillna(0)
        src_df['Discharge (m3s-1)'] = (
            src_df['WetArea (m2)']
            * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
            * pow(src_df['SLOPE'], 0.5)
            / src_df['ManningN']
        )
        # Force zero stage to have zero discharge
        src_df.loc[src_df['Stage'] == 0, ['Discharge (m3s-1)']] = 0
        # Calculate number of adjusted HydroIDs
        count = len(src_df.loc[(src_df['Stage'] == 0) & (src_df['Bathymetry_source'] == 'USACE eHydro')])

        # Write src in ai-based file
        src_name = os.path.basename(src)
        path2save = join(path_ehydro_src, src_name)
        src_df.to_csv(path2save, index=False)

        # Write src back to file
        src_df.to_csv(src, index=False)

        log_text += f'    Successfully recalculated {count} HydroIDs\n'

    return [feature_id_ehydro, log_text]


def multi_process_hucs(fim_dir, bathy_file, wbd_buffer, wbd, output_suffix, number_of_jobs, verbose):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input bathy_file.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        bathy_file : str
            Path to bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        wbd_buffer : int
            Distance in meters to buffer wbd dataset when searching for relevant HUCs.
        wbd : str
            Path to wbd input data, e.g.
            "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
        output_suffix : str
            Output filename suffix.
        number_of_jobs : int
            Number of CPU cores to parallelize HUC processing.
        verbose : bool
            Verbose printing.

    """

    # Set up log file
    print(
        'Writing progress to log file here: '
        + str(join(fim_dir, 'logs', 'bathymetric_adjustment' + output_suffix + '.log'))
    )
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Initiate log file
    log_file = open(join(fim_dir, 'logs', 'bathymetric_adjustment' + output_suffix + '.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    # Exit program if the bathymetric data doesn't exist
    if not os.path.exists(bathy_file):
        statement = f'The input bathymetry file {bathy_file} does not exist. Exiting...'
        log_file.write(statement)
        print(statement)
        sys.exit(0)

    # Find applicable HUCs to apply bathymetric adjustment
    # NOTE: This block can be removed if we have estimated bathymetry data for
    # the whole domain later.
    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]
    bathy_gdf = gpd.read_file(bathy_file, engine="pyogrio", use_arrow=True)
    buffered_bathy = bathy_gdf.geometry.buffer(wbd_buffer)  # We buffer the bathymetric data to get adjacent
    wbd = gpd.read_file(
        wbd, mask=buffered_bathy, engine="fiona"
    )  # HUCs that could also have bathymetric reaches included
    hucs_with_bathy = wbd.HUC8.to_list()
    hucs = [h for h in fim_hucs if h in hucs_with_bathy]
    log_file.write(f"Identified {len(hucs)} HUCs that have bathymetric data: {hucs}\n")
    print(f"Identified {len(hucs)} HUCs that have bathymetric data\n")

    # Set up multiprocessor
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
        executor_dict = {}
        for huc in hucs:
            arg_keeper = {'fim_dir': fim_dir, 'huc': huc, 'bathy_file': bathy_file, 'verbose': verbose}
            future = executor.submit(correct_rating_for_bathymetry, **arg_keeper)
            executor_dict[future] = huc

        # Send the executor to the progress bar and wait for all tasks to finish
        progress_bar_handler(executor_dict, True, f"Running BARC on {len(hucs)} HUCs")
        # Get the returned logs and write to the log file
        for future in executor_dict.keys():
            try:
                log_file.write(future.result())
            except Exception as ex:
                print(f"WARNING: {executor_dict[future]} BARC failed for some reason")
                log_file.write(f"ERROR --> {executor_dict[future]} BARC failed (details: *** {ex} )\n")
                traceback.print_exc(file=log_file)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    log_file.close()

verbose = False
# correct_rating_for_bathymetry(fim_dir, huc, bathy_file, verbose)
bathymetry_source = "USACE eHydro"

def apply_bathy_data_to_srcs(
    fim_dir, 
    huc,
    path_aib_bathy_parquet,
    bathy_file,
    verbose,
    bathymetry_source,
    ploting_flag
    ):

    fim_huc_dir = join(fim_dir, huc)
    path_src_processing = join(fim_huc_dir, "src_processing")
    os.mkdir(path_src_processing)

    # Get src_full from each branch
    src_all_branches = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches.append(src_full)
    
    # Make a copy of original srcs
    destination_dir = join(path_src_processing, "original_srcs")  
    os.mkdir(destination_dir)
    for file_path in src_all_branches:
        shutil.copy(file_path, destination_dir)

    if bathymetry_source == "USACE eHydro":
        output_ehydro = correct_rating_for_ehydro_bathymetry(fim_dir, huc, bathy_file, verbose)
        feature_id_ehydro = output_ehydro[0]
        feature_id_aib = correct_rating_for_ai_based_bathymetry(path_aib_bathy_parquet, fim_dir, huc)
    else: 
        feature_id_aib = correct_rating_for_ai_based_bathymetry(path_aib_bathy_parquet, fim_dir, huc)

    # feature_id_target = feature_id_aib[~feature_id_aib.isin(feature_id_ehydro)]
    path_aib_src = join(path_src_processing, "aib_srcs")
    src_all_branches_path_aib = glob.glob(join(path_aib_src, "*.csv"))

    src_all_branches_path_aib.sort()
    src_all_branches.sort()

    for src in range(len(src_all_branches)):
        src_df_ehy = pd.read_csv(src_all_branches[src])
        src_df_ehy = src_df_ehy.sort_index(axis=1)
        src_df_aib = pd.read_csv(src_all_branches_path_aib[src])
        src_df_aib = src_df_aib.sort_index(axis=1)

        # src_df_ehy["Bathymetry_source"][3:5] = 'n'
        # src_df_ehy["BedArea (m2)"][3:5] = 0
        mask = src_df_ehy['Bathymetry_source'] != 'USACE eHydro'
        # Use np.where to replace rows in df1 with rows from df2 where mask is True
        src_ar_ehy = np.where(mask[:, None], src_df_aib, src_df_ehy)
        src_df_ehy = pd.DataFrame(src_ar_ehy, columns = src_df_ehy.columns)

        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'  Branch: {branch}\n'





if __name__ == '__main__':
    """
    Parameters
    ----------
    fim_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/bathymetric_adjustment.log.
    bathy_file : str
        Path to bathymetric adjustment geopackage, e.g.
        "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
    wbd_buffer : int
        Distance in meters to buffer wbd dataset when searching for relevant HUCs.
    wbd : str
        Path to wbd input data, e.g.
        "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
    output_suffix : str
        Optional. Output filename suffix. Defaults to no suffix.
    number_of_jobs : int
        Optional. Number of CPU cores to parallelize HUC processing. Defaults to 8.
    verbose : bool
        Optional flag for enabling verbose printing.

    Sample Usage
    ----------
    python3 /foss_fim/src/bathymetric_adjustment.py -fim_dir /outputs/fim_run_dir -bathy /data/inputs/bathymetry/bathymetry_adjustment_data.gpkg
        -buffer 5000 -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg -j $jobLimit

    """

    parser = ArgumentParser(description="Bathymetric Adjustment")
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-bathy',
        '--bathy_file',
        help="Path to geopackage with preprocessed bathymetic data",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-buffer',
        '--wbd-buffer',
        help="Buffer to apply to bathymetry data to find applicable HUCs",
        required=True,
        type=int,
    )
    parser.add_argument(
        '-wbd',
        '--wbd',
        help="Buffer to apply to bathymetry data to find applicable HUCs",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-suff',
        '--output-suffix',
        help="Suffix to append to the output log file (e.g. '_global_06_011')",
        default="",
        required=False,
        type=str,
    )
    parser.add_argument(
        '-j',
        '--number-of-jobs',
        help='OPTIONAL: number of workers (default=8)',
        required=False,
        default=8,
        type=int,
    )
    parser.add_argument(
        '-vb',
        '--verbose',
        help='OPTIONAL: verbose progress bar',
        required=False,
        default=None,
        action='store_true',
    )

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bathy_file = args['bathy_file']
    wbd_buffer = int(args['wbd_buffer'])
    wbd = args['wbd']
    output_suffix = args['output_suffix']
    number_of_jobs = args['number_of_jobs']
    verbose = bool(args['verbose'])

    multi_process_hucs(fim_dir, bathy_file, wbd_buffer, wbd, output_suffix, number_of_jobs, verbose)
