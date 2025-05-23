#!/usr/bin/env python3

import datetime as dt
import os
import re
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy
from scipy.ndimage import generic_filter


# -------------------------------------------------------
def extract_longitudinal_variables(src_df, hydroid, stage):
    """
    Function for extracting hydraulic variable to longitudinal smooth
    along a stream in synthetic rating curves.
    Candidate_variables_to_smooth_longitudinally = [
        'BedArea (m2)',
        'Volume (m3)',
        'SurfaceArea (m2)',
        'WetArea (m2)',
        'HydraulicRadius (m)',
        'Discharge (m3s-1)'
    ]
        Parameters
        ----------
        src_df : dataframe
            Synthetic rating curve dataframe.
        hydroid : str
            Fim hydroid string.
        stage : float
            Fim stage.

        Returns
        ----------
        voi_hid_stage : list

    """

    src = src_df.loc[src_df.HydroID == hydroid]

    if src.LakeID.iloc[0] > 0:
        return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    else:
        bedArea = round(np.interp(stage, src.Stage, src['BedArea (m2)']), 2)
        volume = round(np.interp(stage, src.Stage, src['Volume (m3)']), 2)
        surfacearea = round(np.interp(stage, src.Stage, src['SurfaceArea (m2)']), 2)
        wetarea = round(np.interp(stage, src.Stage, src['WetArea (m2)']), 2)
        hydraulicRadius = round(np.interp(stage, src.Stage, src['HydraulicRadius (m)']), 3)
        flow = round(np.interp(stage, src.Stage, src['Discharge (m3s-1)']), 2)

    voi_hid_stage = [flow, bedArea, volume, surfacearea, wetarea, hydraulicRadius]

    return voi_hid_stage


# -------------------------------------------------------
def min_ignore_zeros(lst):
    """
    Function for calculation non-zero minimumns.

        Parameters
        ----------
        lst : list

        Returns
        ----------
        minimum : float

    """
    nonzero = lst[lst > 0]
    if nonzero.size > 0:
        return np.min(nonzero)
    else:
        return 0


# -------------------------------------------------------
def filter_voi(voi_array):
    """
    Function for a gaussian and minimum filtering on an array.

        Parameters
        ----------
        voi_array : array

        Returns
        ----------
        gfilter

    """
    minfilter = generic_filter(voi_array, min_ignore_zeros, size=4)
    gfilter = scipy.ndimage.gaussian_filter1d(minfilter, sigma=2, radius=2)
    return gfilter


# -------------------------------------------------------
def filter_longitudinal_discharge_jitters(fim_dir, huc):
    """
    Function for smoothing longitudinal jitters in any variables
    of interest along a stream in synthetic rating curves.
    This will only correct GMS branch's SRCs based on the hydro_ids.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.

        Returns
        ----------
        log_text : str

    """
    log_text = f'Filtering Longitudinal Flow Fluctuation for HUC8: {huc}\n'
    fim_huc_dir = join(fim_dir, huc)

    # if int(branch) == 0:
    src_full_0 = join(fim_huc_dir, 'branches', str(0), 'src_full_crosswalked_0.csv')
    ht_0_path = join(fim_huc_dir, 'branches', str(0), 'hydroTable_0.csv')

    if os.path.isfile(src_full_0) and os.path.isfile(ht_0_path):
        src_0_df = pd.read_csv(src_full_0, low_memory=False)
        ht_0_df = pd.read_csv(ht_0_path, low_memory=False)

        src_0_df.loc[src_0_df['Bathymetry_source'] == str(0), 'Bathymetry_source'] = 'No Bathymetry Applied'
        src_0_df.loc[src_0_df['Bathymetry_source'] == 0, 'Bathymetry_source'] = 'No Bathymetry Applied'
        src_0_df['Bathymetry_source'] = src_0_df['Bathymetry_source'].fillna('No Bathymetry Applied')
        ht_0_df['Bathymetry_source'] = src_0_df['Bathymetry_source']

        # Save updated branch 0 ht and src tables
        src_0_df = src_0_df.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)
        src_0_df.to_csv(src_full_0, index=False)
        ht_0_df = ht_0_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
        ht_0_df.to_csv(ht_0_path, index=False)
    else:
        print("Files do not exist: src_full_crosswalked_0.csv and hydroTable_0.csv")

    # Get src_full, hydrotable and catchment from each branch
    src_all_branches_path = []
    cathment_gpkg_path = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:  # Just for GMS branches
            src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
            cathment_gpkg = join(
                fim_huc_dir,
                'branches',
                str(branch),
                f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.gpkg',
            )
            if os.path.isfile(src_full):
                src_all_branches_path.append(src_full)
            if os.path.isfile(src_full):
                cathment_gpkg_path.append(cathment_gpkg)

    # Longitudinally adjust srcs for WSE
    for isrc in range(len(src_all_branches_path)):  # 5

        branch = re.search(r'branches/(\d{10}|0)/', src_all_branches_path[isrc]).group()[9:-1]
        print(f'Processing Longitudinal flow adjustment for HUC {huc} Branch: {branch}')
        log_text += f'Processing Longitudinal flow adjustment for HUC {huc} Branch: {branch}\n'

        catchment_gdf0 = gpd.read_file(cathment_gpkg_path[isrc])
        catchment_gdf = catchment_gdf0.drop_duplicates(subset=['HydroID'], keep='first')
        lakeID_df = catchment_gdf[['HydroID', 'LakeID']].drop_duplicates(subset=['HydroID'])
        src_df = pd.read_csv(src_all_branches_path[isrc], low_memory=False)
        src_df = src_df.merge(lakeID_df, on='HydroID', how='inner')  # validate='many_to_one'
        stages = [round(num, 4) for num in src_df['Stage'][0:84]]

        # Defining stages with discharge = 0 and Number of Cells = 0 for later masking
        Q0_mask = src_df['Discharge (m3s-1)'] == 0
        nocell0_mask = src_df['Number of Cells'] == 0

        # num_headwaters = len(
        #     catchment_gdf.loc[~catchment_gdf.HydroID.isin(catchment_gdf.NextDownID.astype(int)), "HydroID"]
        # )
        headwaters_rows = catchment_gdf.loc[
            ~catchment_gdf.HydroID.isin(catchment_gdf.NextDownID.astype(int)),
        ]
        # Remove headwaters with lakeID
        headwaters = list(headwaters_rows[headwaters_rows['LakeID'] < 0]['HydroID'])

        # Build hydroid chain first
        hydroid_chain_mhws = []
        for headwater in headwaters:
            hydroid_chain = [headwater]
            nexthydroid = headwater
            # While loop to create the list of hydroids
            while catchment_gdf.HydroID.isin([nexthydroid]).any():
                # print(nexthydroid)
                nexthydroid = int(
                    catchment_gdf.loc[catchment_gdf.HydroID == nexthydroid, "NextDownID"].item()
                )
                hydroid_chain.append(nexthydroid)

            if len(hydroid_chain[:-1]) > 2:  # Excluding headwaters with len 2 or smaller
                hydroid_chain_mhws.append(hydroid_chain)

        # print(f'Hydroids_chain was created for branch: HUC {huc} Branch: {branch}')

        # Makes a logitudinal dataframes of variables of interests
        keys = [
            'Discharge (m3s-1)',
            'BedArea (m2)',
            'Volume (m3)',
            'SurfaceArea (m2)',
            'WetArea (m2)',
            'HydraulicRadius (m)',
        ]
        original_all_voi = {}
        filtered_all_voi = {}
        if len(hydroid_chain_mhws) > 0:
            for ikey in range(len(keys[0:1])):  # Just apply to discharge
                voi2smooth_mhws = []
                filtered_voi_mhws = []
                for hydroid_chain in hydroid_chain_mhws:
                    voi2smooth_df = dict()
                    filtered_voi_df = dict()
                    long_index = 0
                    for nexthydroid in hydroid_chain[:-1]:  # Excluding the last HydroID
                        voi2smooth_list = []
                        for stage in stages:
                            voi2smooth_list.append(
                                extract_longitudinal_variables(src_df, nexthydroid, stage)[ikey]
                            )
                        voi2smooth_df[nexthydroid] = voi2smooth_list + [long_index]
                        long_index += 1

                    stages_cols = [str(istg) for istg in stages]
                    voi2smooth_df = pd.DataFrame.from_dict(
                        voi2smooth_df, orient="index", columns=stages_cols + ['long_position']
                    )
                    # Applies 2 filters of minimum and gaussian
                    # on the logitudinal surface area, volume and bedArea
                    for stage in stages_cols:
                        filtered_voi_array = filter_voi(voi2smooth_df[stage])
                        filtered_voi_df[stage] = list(filtered_voi_array)

                    # Convert filtered_voi_df to a DataFrame
                    filtered_voi_df = pd.DataFrame.from_dict(filtered_voi_df, orient="columns")
                    # Align indices and add "long_position"
                    filtered_voi_df.index = voi2smooth_df.index  # Ensure indices match
                    filtered_voi_df["long_position"] = voi2smooth_df["long_position"]

                    voi2smooth_mhws.append(voi2smooth_df)
                    filtered_voi_mhws.append(filtered_voi_df)

                voi2smooth_mhws_df = pd.concat(voi2smooth_mhws)
                filtered_voi_mhws_df = pd.concat(filtered_voi_mhws)

                # Add the dataframe to the dictionary
                # print(f'{keys[ikey]} variable were filtered for HUC {huc} Branch: {branch}')
                original_all_voi[keys[ikey]] = voi2smooth_mhws_df
                filtered_all_voi[keys[ikey]] = filtered_voi_mhws_df

            # Defining a lake_discharge dataframe
            Q_lake_hydroID = src_df[['HydroID', 'LakeID', 'Stage', 'Discharge (m3s-1)']]
            # mask_src = (src_df['LakeID'] < 0)
            for jkey in range(len(keys[0:1])):  # Just apply to discharge
                # Reshaping variables of interest (voi) to be included in src
                filtered_voi = filtered_all_voi[keys[jkey]].drop('long_position', axis=1)
                reshaped_filtered_voi = filtered_voi.reset_index().melt(
                    id_vars='index', var_name='Stage', value_name=f'Filtered_{keys[jkey]}'
                )
                # print(reshaped_filtered_voi)
                reshaped_filtered_voi.rename(columns={'index': 'HydroID'}, inplace=True)
                reshaped_filtered_voi['Stage'] = reshaped_filtered_voi['Stage'].astype(float)

                # Adding filtered SurfaceArea, volume and bedarea to src
                src_df = src_df.merge(
                    reshaped_filtered_voi[['HydroID', 'Stage', f'Filtered_{keys[jkey]}']],
                    on=['HydroID', 'Stage'],
                    how='left',
                )
                # Update voi including SurfaceArea (m2), 'BedArea (m2)' and 'Volume (m3)' in src
                # Update src_df where LakeID > 0 and Stage matches
                mask_src = (src_df[f'Filtered_{keys[jkey]}'].notna()) & (src_df['LakeID'] < 0)
                src_df.loc[mask_src, keys[jkey]] = src_df.loc[mask_src, f'Filtered_{keys[jkey]}']

            # # Recalculating discharge variables
            # src_df['WettedPerimeter (m)'] = src_df['BedArea (m2)'] / src_df['LENGTHKM'] / 1000
            # src_df['WetArea (m2)'] = src_df['Volume (m3)'] / src_df['LENGTHKM'] / 1000
            # src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
            # src_df['HydraulicRadius (m)'].fillna(0, inplace=True)

            # # Recalculating the discharge
            # src_df['Discharge (m3s-1)'] = (
            #     src_df['WetArea (m2)']
            #     * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
            #     * pow(src_df['SLOPE'], 0.5)
            #     / src_df['ManningN']
            # )
            # Refining Discharge for lake hydroIDs with the original Q
            # Merge with src_df
            src_df_merged = src_df.merge(
                Q_lake_hydroID, on=['HydroID', 'LakeID', 'Stage'], how='left', suffixes=('', '_lake')
            )
            # Update src_df where LakeID > 0 and Stage matches
            mask = (src_df_merged['LakeID'] > 0) & (src_df_merged['Discharge (m3s-1)_lake'].notnull())
            src_df.loc[mask, 'Discharge (m3s-1)'] = src_df_merged.loc[mask, 'Discharge (m3s-1)_lake']
            src_df = src_df.round(5)

            # Set Hydraulic properties of original stages with discharge = 0 back to 0
            src_df.loc[Q0_mask, 'Discharge (m3s-1)'] = 0
            # src_df.loc[Q0_mask, 'Volume (m3)'] = 0
            # src_df.loc[Q0_mask, 'WettedPerimeter (m)'] = 0
            # src_df.loc[Q0_mask, 'WetArea (m2)'] = 0
            # src_df.loc[Q0_mask, 'HydraulicRadius (m)'] = 0
            # src_df.loc[Q0_mask, 'BedArea (m2)'] = 0

            # Set cahnnel properties of original stages with Number of Cells = 0 back to 0
            src_df.loc[nocell0_mask, 'Number of Cells'] = 0
            src_df.loc[nocell0_mask, 'SurfaceArea (m2)'] = 0
            src_df.loc[nocell0_mask, 'TopWidth (m)'] = 0

            # Set nans to 0
            src_df.loc[src_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

            # Write src back to file
            src_df.to_csv(src_all_branches_path[isrc], index=False)
            # log_text += f'Successfully recalculated discharge for HUC {huc} branch {branch}'

        log_text += f'Adjusting hydroTable for longitudinal filter for HUC {huc} Branch {branch}'
        ht_branch_path = join(fim_huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        ht_df = pd.read_csv(ht_branch_path, low_memory=False)

        # updating discharge_cms column
        ht_df['discharge_cms'] = src_df['Discharge (m3s-1)']

        # Write ht back to file
        ht_df.to_csv(ht_branch_path, index=False)

    log_text += f'Successfully recalculated discharge for HUC {huc}\n'
    print(f'Successfully recalculated discharges for HUC {huc}\n')

    return log_text


# --------------------------------------------------------
# Apply longitudinal dischage adjustment
def apply_longitudinal_dischage_adjustment(fim_dir, huc, log_file_path):  # bankfull_flows_file,
    """
    Function for applying longitudinal dischage adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_src_thalweg_notches and
        process longitudinal dischage functions parameters.

        Returns
        ----------
        log_text : str
    """
    log_text = ""
    try:
        msg = f"Correcting rating curve for longitudinal discharge ajustment SRC for HUC : {huc}\n"
        log_text += msg
        print(msg)
        log_text += filter_longitudinal_discharge_jitters(fim_dir, huc)  # bankfull_flows_file

    except Exception:
        log_text += f"An error has occurred while processing longitudinal adjustment for huc {huc}\n"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -------------------------------------------------------
def process_longitudinal_flow_adjustment(fim_dir, number_of_jobs):
    """
    Function for correcting synthetic rating curves using Multi-Proc approach.
    It will correct each branch's SRCs in serial based on the HydroIDs.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        number_of_jobs : int
            Number of CPU cores to parallelize HUC processing.

    """
    # Set up log file
    log_file_path = os.path.join(fim_dir, 'logs', 'longitudinal_filter' + '.log')
    print(f'Writing progress to log file here: {log_file_path}')
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    # Find applicable HUCs to apply longitudinal filter
    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]

    msg = f"Applying longitudinal discharge adjustment on {len(fim_hucs)} HUCs: {fim_hucs}\n"
    log_text += msg

    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all hucs, build the arguments, and submit them to the process pool
        futures = {}
        for huc in fim_hucs:
            args = {'fim_dir': fim_dir, 'huc': huc, 'log_file_path': log_file_path}
            future = executor.submit(apply_longitudinal_dischage_adjustment, **args)
            futures[future] = future

        for future in as_completed(futures):
            if future is not None:
                if future.exception():
                    raise future.exception()

    ## Record run time and close log file
    end_time = dt.datetime.now(dt.timezone.utc)
    log_text += 'END TIME: ' + str(end_time) + '\n'
    tot_run_time = end_time - begin_time
    log_text += 'TOTAL RUN TIME: ' + str(tot_run_time).split('.')[0]
    log_file.close()


if __name__ == '__main__':

    """
    Parameters
    ----------
    fim_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/longitudinal_filter.log.
    number_of_jobs : int
        Optional. Number of CPU cores to parallelize HUC processing. Defaults to 1.

    Sample Usage
    ----------
    python3 /foss_fim/src/filter_longitudinal_flow.py
        -fim_dir /outputs/fim_run_dir
        -j $jobLimit
    """
    parser = ArgumentParser(description="Longitudinal depth/flow filter")
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-j',
        '--number-of-jobs',
        help='OPTIONAL: number of workers (default=1)',
        required=False,
        default=1,
        type=int,
    )

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    number_of_jobs = args['number_of_jobs']

    process_longitudinal_flow_adjustment(fim_dir, number_of_jobs)
