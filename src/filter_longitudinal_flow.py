#!/usr/bin/env python3

import datetime as dt
import os
import re
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
from scipy.ndimage import generic_filter

# -------------------------------------------------------
def extract_longitudinal_variables(src_df, hydroid, stage):
    """Candidate_variables_to_smooth_longitudinally = [
    'BedArea (m2)',
    'Volume (m3)',
    'SurfaceArea (m2)',
    'WetArea (m2)',
    'HydraulicRadius (m)',
    'Discharge (m3s-1)'
    ]"""

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

    voi_hid_stage = [bedArea, volume, surfacearea, wetarea, hydraulicRadius, flow]

    return voi_hid_stage

def min_ignore_zeros(window):
    nonzero = window[window > 0]
    if nonzero.size > 0:
        return np.min(nonzero)
    else:
        return 0 

# -------------------------------------------------------
def filter_voi(voi_array):
    minfilter = generic_filter(voi_array, min_ignore_zeros, size=4) #scipy.ndimage.minimum_filter1d(voi_array, 4)
    gfilter = scipy.ndimage.gaussian_filter1d(minfilter, sigma=2, radius=2)
    return gfilter


# -------------------------------------------------------
def filter_longitudinal_jitters_src(fim_dir, huc):
    """Function for smoothing longitudinal jitters in any variables
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

    # Get src_full, hydrotable and catchment from each branch
    src_all_branches_path = []
    cathment_gpkg_path = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:
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

    # Longitudinally filter srcs for WSE
    for isrc in range(len(src_all_branches_path)):  # 1

        branch = re.search(r'branches/(\d{10}|0)/', src_all_branches_path[isrc]).group()[9:-1]
        print(f'Processing branch {branch}\n')
        log_text += f'  Branch: {branch}\n'

        catchment_gdf0 = gpd.read_file(cathment_gpkg_path[isrc])
        catchment_gdf = catchment_gdf0.drop_duplicates(subset=['HydroID'], keep='first')
        lakeID_df = catchment_gdf[['HydroID', 'LakeID']].drop_duplicates(subset=['HydroID'])
        src_df = pd.read_csv(src_all_branches_path[isrc], low_memory=False)
        src_df = src_df.merge(lakeID_df, on='HydroID', how='inner')  # validate='many_to_one'
        stages = [round(num, 4) for num in src_df['Stage'][0:84]]

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

        print(f'Chain_hydroids was created for branch: {branch}')

        # Makes a logitudinal dataframes of variables of interests
        keys = [
            'BedArea (m2)',
            'Volume (m3)',
            'SurfaceArea (m2)',
            'WetArea (m2)',
            'HydraulicRadius (m)',
            'Discharge (m3s-1)',
        ]
        original_all_voi = {}
        filtered_all_voi = {}
        if len(hydroid_chain_mhws) > 0:
            for ikey in range(len(keys[0:3])):  # 3
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
                # voi2smooth_mhws_df.to_csv(join(fim_huc_dir,f'voi2smooth_mhws_df_{ikey}.csv'))
                filtered_voi_mhws_df = pd.concat(filtered_voi_mhws)
                # filtered_voi_mhws_df.to_csv(join(fim_huc_dir,f'filtered_voi_mhws_df_{ikey}.csv'))

                # Add the dataframe to the dictionary
                print(f'{keys[ikey]} variable were filtered for branch {branch}')
                original_all_voi[keys[ikey]] = voi2smooth_mhws_df
                filtered_all_voi[keys[ikey]] = filtered_voi_mhws_df

            # Defining a lake_discharge dataframe
            Q_lake_hydroID = src_df[['HydroID', 'LakeID', 'Stage', 'Discharge (m3s-1)']]
            # mask_src = (src_df['LakeID'] < 0)
            for jkey in range(len(keys[0:3])):
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

            # Recalculating discharge parameteres
            src_df['WettedPerimeter (m)'] = src_df['BedArea (m2)'] / src_df['LENGTHKM'] / 1000
            src_df['WetArea (m2)'] = src_df['Volume (m3)'] / src_df['LENGTHKM'] / 1000
            src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
            src_df['HydraulicRadius (m)'].fillna(0, inplace=True)

            # Recalculating the discharge
            src_df['Discharge (m3s-1)'] = (
                src_df['WetArea (m2)']
                * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
                * pow(src_df['SLOPE'], 0.5)
                / src_df['ManningN']
            )
            # Refining Discharge for lake hydroIDs with the original Q
            # Merge with src_df
            src_df_merged = src_df.merge(
                Q_lake_hydroID, on=['HydroID', 'LakeID', 'Stage'], how='left', suffixes=('', '_lake')
            )
            # Update src_df where LakeID > 0 and Stage matches
            mask = (src_df_merged['LakeID'] > 0) & (src_df_merged['Discharge (m3s-1)_lake'].notnull())
            src_df.loc[mask, 'Discharge (m3s-1)'] = src_df_merged.loc[mask, 'Discharge (m3s-1)_lake']

            # set nans to 0
            src_df.loc[src_df['Stage'] == 0, ['Discharge (m3s-1)']] = 0
            # src_df.to_csv(join(fim_huc_dir,f'src_full_{branch}_test_new_min_filter.csv'))

            # Write src back to file
            src_df.to_csv(src_all_branches_path[isrc], index=False)
            log_text += f'Successfully recalculated discharge for branch {branch}'
            print(f'Successfully recalculated discharge for branch {branch}')

    log_text += f'Successfully recalculated discharge for HUC {huc}\n'
    print(f'Successfully recalculated discharges for HUC {huc}\n')

    return log_text


def analyse_filtered_voi(original_all_voi, filtered_all_voi, fim_huc_dir):

    # Get one hydroid
    print(filtered_all_voi['BedArea (m2)'].index)
    bedarea = filtered_all_voi['BedArea (m2)'][filtered_all_voi['BedArea (m2)'].index == 23160060].melt()
    volume = filtered_all_voi['Volume (m3)'][filtered_all_voi['Volume (m3)'].index == 23160060].melt()

    bedarea_org = original_all_voi['BedArea (m2)'][original_all_voi['BedArea (m2)'].index == 23160060].melt()
    volume_org = original_all_voi['Volume (m3)'][original_all_voi['Volume (m3)'].index == 23160060].melt()

    bedarea = bedarea.drop(84)  # drop the `position` row
    volume = volume.drop(84)  # drop the `position` row
    bedarea_org = bedarea_org.drop(84)  # drop the `position` row
    volume_org = volume_org.drop(84)  # drop the `position` row

    bedarea['value'] = pd.to_numeric(bedarea['value'], errors='coerce')
    bedarea['variable'] = pd.to_numeric(bedarea['variable'], errors='coerce')
    volume['value'] = pd.to_numeric(volume['value'], errors='coerce')
    volume['variable'] = pd.to_numeric(volume['variable'], errors='coerce')
    bedarea_org['value'] = pd.to_numeric(bedarea_org['value'], errors='coerce')
    bedarea_org['variable'] = pd.to_numeric(bedarea_org['variable'], errors='coerce')
    volume_org['value'] = pd.to_numeric(volume_org['value'], errors='coerce')
    volume_org['variable'] = pd.to_numeric(volume_org['variable'], errors='coerce')

    # ax = bedarea.plot(x='value', y='variable', label="adjusted bedarea", logy=True, logx=True)
    fig, ax = plt.subplots(figsize=(10, 6))
    bedarea.plot(x='value', y='variable', label="Adjusted bed area", ax=ax, logx=True)  # , logy=True)
    bedarea_org.plot(x='value', y='variable', label="Bed area", ax=ax, logx=True)  # , logy=True)
    # Customize the plot (optional)
    ax.set_xlabel('Bed Area')
    ax.set_ylabel('Stage')
    ax.set_title('Reach 23160060')
    ax.legend()
    # Display the plot
    plt.savefig(f'{fim_huc_dir}/adjusted_bedArea_plot.png', dpi=300, bbox_inches='tight')

    fig, ax = plt.subplots(figsize=(10, 6))
    volume.plot(x='value', y='variable', label="Adjusted Volume (m3)", ax=ax, logx=True)  # , logy=True)
    volume_org.plot(x='value', y='variable', label="Volume (m3)", ax=ax, logx=True)  # , logy=True)
    # Customize the plot (optional)
    ax.set_xlabel('Volume (m3)')
    ax.set_ylabel('Stage')
    ax.set_title('Reach 23160060')
    ax.legend()
    # Display the plot
    plt.savefig(f'{fim_huc_dir}/adjusted_Volume_plot.png', dpi=300, bbox_inches='tight')

    # ******************* Longitudinal Plotting *************************
    bedarea_filtered = filtered_all_voi['BedArea (m2)']
    volume_filtered = filtered_all_voi['Volume (m3)']
    # bedarea_original = original_all_voi['BedArea (m2)']
    # volume_original = original_all_voi['Volume (m3)']
    'viridis'
    fig, ax = plt.subplots(figsize=(10, 6))
    bedarea_filtered.plot(
        x='long_position', label="filtered bed area", logy=True, colormap='inferno', ax=ax, legend=False
    )
    ax.set_xlim(-1, 101)
    ax.set_xlabel('Reaches')
    ax.set_ylabel('Bed Area')
    ax.set_title('Longitudinal Changes in bed area for different stages')
    plt.savefig(f'{fim_huc_dir}/longitudinal_bedarea_filtered.png', dpi=300, bbox_inches='tight')

    fig, ax = plt.subplots(figsize=(10, 6))
    volume_filtered.plot(
        x='long_position', label="filtered Volume", logy=True, colormap='inferno', ax=ax, legend=False
    )
    ax.set_xlim(-1, 101)
    ax.set_xlabel('Reaches')
    ax.set_ylabel('Volume (m3)')
    ax.set_title('Longitudinal Changes in volume for different stages')
    plt.savefig(f'{fim_huc_dir}/longitudinal_volume_filtered.png', dpi=300, bbox_inches='tight')


# -------------------------------------------------------
def process_filtering_src(fim_dir, number_of_jobs):
    """Function for correcting synthetic rating curves. It will correct
    each branch's SRCs in serial based on the HydroIDs.

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

    msg = f"Applying longitudinal filter on {len(fim_hucs)} HUCs: {fim_hucs}\n"
    log_text += msg

    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all hucs, build the arguments, and submit them to the process pool
        futures = {}
        for huc in fim_hucs:
            args = {'fim_dir': fim_dir, 'huc': huc}
            future = executor.submit(filter_longitudinal_jitters_src, **args)
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
    python3 /foss_fim/src/longitudinal_filter.py
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

    process_filtering_src(fim_dir, number_of_jobs)
