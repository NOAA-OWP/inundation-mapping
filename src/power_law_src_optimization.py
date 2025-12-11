import argparse
import datetime as dt
import json
import multiprocessing
import os
import sys
from collections import deque
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
from geopandas.tools import sjoin

from utils.shared_variables import DOWNSTREAM_THRESHOLD


gpd.options.io_engine = "pyogrio"

from sklearn.linear_model import LinearRegression

def fit_power_law(discharge_cms, stage):
    """
    Fit a weighted log-log power law relationship between discharge (Q) and stage (S):
        stage = a * Q^b
    Processing steps:
    - Remove non-positive discharge and stage values.
    - Log-transform Q and S.
    - Apply weighted linear regression in log space, using discharge as weights.
    - Convert regression intercept and slope into power-law parameters a and b.
    - Compute model R² in log spcae.

    Returns:
        a (float): power-law coefficient
        b (float): power-law exponent
        R² (float): coefficient of determination in log space.
        If insufficient valid points (<5), returns (None, None, None)

    """

    valid_data = (discharge_cms > 0) & (stage > 0)

    q_data = discharge_cms[valid_data]
    s_data = stage[valid_data]
    if len(q_data) < 5:
        return None, None, None # Not enough points

    # log transform
    log_q = np.log(q_data).reshape(-1, 1)
    log_s = np.log(s_data)
    # Add weights for higher flow values
    weights = q_data

    model = LinearRegression()
    model.fit(log_q, log_s, sample_weight=weights)

    # log(s) = log(a) + b * log(q)
    log_a = model.intercept_
    b = model.coef_[0]
    
    a = np.exp(log_a)
    model_log_s = model.predict(log_q)
    r2 = 1 - np.sum((log_s - model_log_s) **2) / np.sum((log_s - np.mean(log_s)) **2)
    return a, b, r2


def update_rating_curve(
    fim_directory,
    water_edge_median_df,
    htable_path,
    huc,
    branch_id,
    catchments_poly_path,
    debug_outputs_option,
    source_tag,
    merge_prev_adj=False,
    down_dist_thresh=DOWNSTREAM_THRESHOLD,
):
    '''
    This script ingests a dataframe containing observed data (HAND elevation and flow) and
    updates SRC discharge values by fitting and propagating power-law rating curve parameters (a and b) derived from 
    observed stage-discharge data.

    -------------------------
    Overview of new workflow
    -------------------------

    - Read in the hydroTable.csv and insure required columns exist.
    - For each gage-linked HydroID:
        - extract observed stage (HAND) and discharge (cms).
        - append the highest stage from the hydroTable to give the model a hint about where the curve should reach in the FIM domain.
          USGS observations rarely include the heighest stages that FIM cares about.
        - fit a weighted power-law
        - store a, b , and R².
    - Merge fitted coefficients back to all rows belonging to each HydroID.
    - Apply weighted averaging for HydroIds that have multiple observations,
      using accumulated length as weights.
    - Propagate a/b values downstream within each branch for a limited distance.
    - Compute feature-level average a/b and merge into hydroTable.
    - Recalculate discharge per row using Q = (S / a)^ (1/b) fallig back to feature-level a/b if HydroID-level values are missing,
      otherwise reverting to pre-calibration discharge.
    - Write: updated hydroTable_branch.csv, updated catchments GPKG, and debug CSVs if requested

    Inputs:
    - fim_directory:        fim directory containing individual HUC output dirs
    - water_edge_median_df: dataframe containing observation data (attributes: "hydroid", "flow", "submitter",
                                "coll_time", "flow_unit", "layer", "HAND")
    - htable_path:          path to the current HUC hydroTable.csv
    - huc:                  string variable for the HUC id # (huc8 or huc6)
    - branch_id:            string variable for the branch id
    - catchments_poly_path: path to the current HUC catchments polygon layer .gpkg
    - debug_outputs_option: optional input argument to output additional intermediate data files
                                (csv files with SRC calculations)
    - source_tag:           input text tag used to specify the type/source of the input obs data used for the
                                SRC adjustments (e.g. usgs_rating or point_obs)
    - merge_prev_adj:       boolean argument to specify when to merge previous SRC adjustments vs. overwrite
                                (default=False)
    - down_dist_thresh:     optional input argument to override the env variable that controls the downstream
                                distance new roughness values are applied downstream of locations with valid
                                obs data

    Ouputs:
    - output_catchments:    same input "catchments_poly_path" .gpkg with appened attributes for SRC
                                adjustments fields
    - df_htable:            same input "htable_path" --> updated hydroTable.csv with new/modified attributes
    - output_src_json:      src.json file with new SRC discharge values

    '''
    print(
        "Processing "
        + str(source_tag)
        + " calibration for huc --> "
        + str(huc)
        + '  branch id: '
        + str(branch_id)
    )
    log_text = (
        "\nProcessing "
        + str(source_tag)
        + " calibration for huc --> "
        + str(huc)
        + '  branch id: '
        + str(branch_id)
        + '\n'
    )
    log_text += "DOWNSTREAM_THRESHOLD: " + str(down_dist_thresh) + 'km\n'
    log_text += "Merge Previous Adj Values: " + str(merge_prev_adj) + '\n'
    df_nvalues = water_edge_median_df.copy()
    df_nvalues.reset_index(inplace=True)
    df_nvalues = df_nvalues[
        (df_nvalues.hydroid.notnull()) & (df_nvalues.hydroid > 0)
    ]  # remove null entries that do not have a valid hydroid

    ## Determine calibration data type for naming calb dataframe column
    if source_tag == 'usgs_rating':
        calb_type = 'calb_coef_usgs'
    else:
        log_text += "WARNING - unknown calibration data source type: " + str(source_tag) + '\n'

    ## Read in the hydroTable.csv and check wether it has previously been updated
    # (rename default columns if needed)
    df_htable = pd.read_csv(
        htable_path, dtype={'HUC': object, 'last_updated': object, 'submitter': object, 'obs_source': object}
    )

    df_prev_adj = pd.DataFrame()  # initialize empty df for populating/checking later
    if 'precalb_discharge_cms' not in df_htable.columns:  # need this column to exist before continuing
        df_htable['calb_applied'] = False
        df_htable['last_updated'] = pd.NA
        df_htable['submitter'] = pd.NA
        df_htable['obs_source'] = pd.NA
        df_htable['precalb_discharge_cms'] = pd.NA
        # df_htable['calb_coef_usgs'] = pd.NA
        df_htable['calb_coef_final'] = pd.NA
    if (
        df_htable['precalb_discharge_cms'].isnull().values.any()
    ):  # check if there are not valid values in the column (True = no previous calibration outputs)
        df_htable['precalb_discharge_cms'] = df_htable['discharge_cms'].values

    ## The section below allows for previous calibration modifications (i.e. usgs rating calbs) to be
    #  available in the final calibration outputs
    # Check if the merge_prev_adj setting is True and there are valid 'calb_coef_final' values from previous
    # calibration outputs
    if merge_prev_adj and not df_htable['calb_coef_final'].isnull().all():
        # Create a subset of hydrotable with previous adjusted SRC attributes
        df_prev_adj_htable = df_htable.copy()[
            ['HydroID', 'submitter', 'last_updated', 'obs_source', 'calb_coef_final']
        ]
        df_prev_adj_htable = df_prev_adj_htable.rename(
            columns={
                'submitter': 'submitter_prev',
                'last_updated': 'last_updated_prev',
                'calb_coef_final': 'calb_coef_final_prev',
                'obs_source': 'obs_source_prev',
            }
        )
        df_prev_adj_htable = df_prev_adj_htable.groupby(["HydroID"]).first()
        # Only keep previous USGS rating curve adjustments (previous spatial obs adjustments are not retained)
        df_prev_adj = df_prev_adj_htable[
            df_prev_adj_htable['obs_source_prev'].str.contains("usgs_rating|ras2fim_rating", na=False)
        ]
        log_text += (
            'HUC: '
            + str(huc)
            + '  Branch: '
            + str(branch_id)
            + ': found previous hydroTable calibration attributes --> '
            + 'retaining previous calb attributes for blending...\n'
        )

    # Delete previous adj columns to prevent duplicate variable issues
    # (if src_roughness_optimization.py was previously applied)
    df_htable = df_htable.drop(
        [
            'discharge_cms',
            'submitter',
            'last_updated',
            calb_type,
            'calb_coef_final',
            'calb_applied',
            'obs_source',
            'a',
            'b',
            'a_featid',
            'b_featid',
        ],
        axis=1,
        errors='ignore',
    )
    df_htable = df_htable.rename(columns={'precalb_discharge_cms': 'discharge_cms'})
    df_nvalues['a'] = np.nan
    df_nvalues['b'] = np.nan
    df_nvalues['r2'] = np.nan

    ## loop through the user provided point data --> stage/flow dataframe only for hydroid_gage
    for hydroid_g in df_nvalues['hydroid_gauge'].unique():
        df_hydro = df_htable[(df_htable.HydroID == hydroid_g) & (df_htable.stage > 0)]
        if df_hydro.empty:
            log_text += f"Warning: No valid hydroTable entries for HydroID {hydroid_g} in HUC {huc} branch {branch_id}\n"
            continue
        df_obs = df_nvalues[df_nvalues.hydroid_gauge == hydroid_g]
        if df_obs.empty or len(df_obs) < 5:
            log_text += f"Warning: insufficent points"
            continue
        stages_obs = df_obs['hand'].values
        flow_obs = df_obs['discharge_cms'].values # CMS

        # Find the highest stage (last point)
        max_row = df_hydro.loc[df_hydro['stage'].idxmax()]
        stages_fit = np.append(stages_obs, max_row['stage'])
        flow_fit = np.append(flow_obs, max_row['discharge_cms'])

        a, b, r2 = fit_power_law(flow_fit, stages_fit)

        df_nvalues.loc[df_nvalues.hydroid_gauge == hydroid_g, ['a', 'b', 'r2']] = a, b, r2
        df_nvalues.loc[df_nvalues.hydroid_gauge == hydroid_g, ['feature_id', 'LakeID', 'NextDownID', 'LENGTHKM']] = \
            max_row[['feature_id', 'LakeID', 'NextDownID', 'LENGTHKM']].values
    if df_nvalues.empty:
        log_text += f'no valid power law fits for Huc {huc}'
        return log_text
    df_nvalues.to_csv(os.path.join(fim_directory, f"calb_coef_usgs_powe_law_fit22222_{branch_id}.csv"), index=False)

    # Take only rows were a and b are known
    df_vals = df_nvalues.groupby('hydroid_gauge')[['a', 'b']].mean().reset_index()
    # Merge back on hydroid_gauge
    df_nvalues = (df_nvalues.set_index('hydroid_gauge').combine_first(df_vals.set_index('hydroid_gauge')).reset_index())
 

    for hydroid in df_nvalues['hydroid'].unique():
        df_hydro = df_htable[(df_htable.HydroID == hydroid) & (df_htable.stage > 0)]
        max_row = df_hydro.loc[df_hydro['stage'].idxmax()]
        df_nvalues.loc[df_nvalues.hydroid == hydroid, ['feature_id', 'LakeID', 'NextDownID', 'LENGTHKM']] = \
            max_row[['feature_id', 'LakeID', 'NextDownID', 'LENGTHKM']].values
    
    if debug_outputs_option:
        df_nvalues.to_csv(os.path.join(fim_directory, f"calb_coef_usgs_powe_law_fit_{branch_id}.csv"), index=False)
    
    df_updated = df_nvalues[['hydroid', 'coll_time', 'submitter']] 
    df_updated = df_updated.sort_values('coll_time').drop_duplicates(
        ['hydroid'], keep='last'
    )  # sort by collection time and then drop duplicate HydroIDs (keep most recent coll_time per HydroID)

    df_updated = df_updated.rename(columns={'coll_time': 'last_updated'})

    ## subset the original hydrotable dataframe and subset to one row per HydroID
    df_nmerge = df_htable[
        ['HydroID', 'feature_id', 'NextDownID', 'LENGTHKM', 'LakeID', 'order_']
    ].drop_duplicates(['HydroID'])

    df_nmerge = branch_network_tracer(df_nmerge)
    ## Merge the newly caluclated power law coefficients
    def weighted_avg(group):
        weights = 1 / group['accum_length']
        a_avg = (group['a'] * weights).sum() / weights.sum()
        b_avg = (group['b'] * weights).sum() / weights.sum()
        return pd.Series({'a': a_avg, 'b': b_avg})
    

    df_nvalues = df_nvalues.groupby('hydroid').apply(weighted_avg).reset_index()
    df_nvalues.to_csv(os.path.join(fim_directory, f"weighted_{branch_id}.csv"), index=False)
    df_nmerge = df_nmerge.merge(df_nvalues, how='left', left_on='HydroID', right_on='hydroid').drop('hydroid', axis=1)
    df_nmerge = df_nmerge.merge(df_updated, how='left', left_on='HydroID', right_on='hydroid').drop('hydroid', axis=1)

    df_nmerge = group_power_law_calc(df_nmerge, down_dist_thresh)
    df_featid_ab = df_nmerge.groupby('feature_id')[['a', 'b']].mean().reset_index()
    df_nmerge = df_nmerge.merge(df_featid_ab, how='left', on='feature_id', suffixes=('', '_featid'))
    df_htable = df_htable.rename(columns={'discharge_cms': 'precalb_discharge_cms'})

    df_htable = df_htable.merge(df_nmerge[['HydroID', 'a', 'b', 'a_featid', 'b_featid', 'last_updated', 'submitter']], how='left', on='HydroID')
    for col in ['a', 'b', 'a_featid', 'b_featid']:
        if col not in df_htable.columns:
            df_htable[col] = pd.NA
    df_htable['calb_applied'] = df_htable['a'].notnull() | df_htable['a_featid'].notnull()
    df_htable['discharge_cms'] = np.where(
        df_htable['a'].notnull(),
        (df_htable['stage'] / df_htable['a']) ** (1 / df_htable['b']),
        np.where(
            df_htable['a_featid'].notnull(),
            (df_htable['stage'] / df_htable['a_featid']) ** (1 / df_htable['b_featid']),
            df_htable['precalb_discharge_cms']
        )
    )
    df_htable['discharge_cms'].mask(df_htable['precalb_discharge_cms'] == 0.0, 0.0, inplace=True)
    df_htable['discharge_cms'].mask(
        df_htable['precalb_discharge_cms'] == -999, -999, inplace=True
    )
    out_htable = os.path.join(fim_directory, 'hydroTable_' + branch_id + '.csv')
    df_htable.to_csv(out_htable, index=False)
    if os.path.isfile(catchments_poly_path):
        try:
            input_catchments = gpd.read_file(catchments_poly_path)
            if 'src_calibrated' in input_catchments.columns:
                input_catchments = input_catchments.drop(
                    ['src_calibrated', 'obs_source', 'calb_coef_final'], axis=1, errors='ignore'
                )
            df_nmerge['src_calibrated'] = np.where(
                df_nmerge['a'].notnull() | df_nmerge['a_featid'].notnull(), 'True', 'False'
            )
            output_catchments = input_catchments.merge(
                df_nmerge[['HydroID', 'src_calibrated']],
                how='left',
                on='HydroID',
            )
            output_catchments['src_calibrated'].fillna('False', inplace=True)
            output_catchments.to_file(
                catchments_poly_path,
                driver="GPKG",
                index=False,
                engine='fiona',
            )
        except Exception as e:
            log_text += f"Error writing GeoPackage: {e}\n"
    log_text += '\n Completed: ' + str(huc) + ' --> branch: ' + str(branch_id) + '\n'
    log_text += '#########################################################\n'
    print("Completed huc: " + str(huc) + ' --> branch: ' + str(branch_id))
    return log_text

def branch_network_tracer(df_input_htable):
    df_input_htable = df_input_htable.astype(
        {'NextDownID': 'int64'}
    ) # ensure attribute has consistent format as int
    # remove all hydroids associated with lake/water body
    # (these often have disjoined artifacts in the network)
    df_input_htable = df_input_htable.loc[df_input_htable['LakeID'] == -999]
    # define start catchments as hydroids that are not found in the "NextDownID" attribute for all
    # other hydroids
    df_input_htable["start_catch"] = ~df_input_htable['HydroID'].isin(df_input_htable['NextDownID'])
    df_input_htable = df_input_htable.set_index('HydroID', drop=False) # set index to the hydroid
    branch_heads = deque(
        df_input_htable[df_input_htable['start_catch'] == True]['HydroID'].tolist()
    ) # create deque of hydroids to define start points in the while loop
    visited = set() # create set to keep track of all hydroids that have been accounted for
    branch_count = 0 # start branch id
    while branch_heads:
        hid = branch_heads.popleft() # pull off left most hydroid from deque of start hydroids
        Q = deque(
            df_input_htable[df_input_htable['HydroID'] == hid]['HydroID'].tolist()
        ) # create a new deque that will be used to populate all relevant downstream hydroids
        vert_count = 0
        branch_count += 1
        while Q:
            q = Q.popleft()
            if q not in visited:
                df_input_htable.loc[df_input_htable.HydroID == q, 'route_count'] = (
                    vert_count # assign var with flow order ranking
                )
                df_input_htable.loc[df_input_htable.HydroID == q, 'branch_id'] = (
                    branch_count # assign var with current branch id
                )
                vert_count += 1
                visited.add(q)
                # find the id for the next downstream hydroid
                nextid = df_input_htable.loc[q, 'NextDownID']
                order = df_input_htable.loc[q, 'order_'] # find the streamorder for the current hydroid
                if nextid not in visited and nextid in df_input_htable.HydroID:
                    # check if the NextDownID is referenced by more than one hydroid
                    # (>1 means this is a confluence)
                    check_confluence = (df_input_htable.NextDownID == nextid).sum() > 1
                    nextorder = df_input_htable.loc[
                        nextid, 'order_'
                    ] # find the streamorder for the next downstream hydroid
                    # check if the nextdownid streamorder is greater than the current hydroid order and the
                    # nextdownid is a confluence (more than 1 upstream hydroid draining to it)
                    if nextorder > order and check_confluence == True:
                        branch_heads.append(
                            nextid
                        ) # found a terminal point in the network (append to branch_heads for second pass)
                        # if above conditions are True than stop traversing downstream and move on to next
                        # starting hydroid
                        continue
                    Q.append(nextid)
    df_input_htable = df_input_htable.reset_index(
        drop=True
    ) # reset index (previously using hydroid as index)
    # sort the dataframe by branch_id and then by route_count
    # (need this ordered to ensure upstream to downstream ranking for each branch)
    df_input_htable = df_input_htable.sort_values(['branch_id', 'route_count'])
    return df_input_htable

def group_power_law_calc(df_nmerge, down_dist_thresh):
    """
    Propagate power-law coefficients (a, b) downstream within each branch.

    Logic:
        - Walk through each branch in order
        - For rows lacking a/b:
            - accumulate downstream distance
            - if accumulated distance < down_dist_tresh,
              fill missing a/b using the mean of all upstream hydroids in the same banch.
        - Reset distance accumulation whenever a hydroid already has a valid a/b.
    """
    dist_accum = 0
    branch_start = 1
    for index, row in df_nmerge.iterrows():
        if int(row['branch_id']) != branch_start:
            dist_accum = 0
            branch_start = int(row['branch_id'])
        if pd.isna(row['a']):
            dist_accum += row['LENGTHKM']
            df_nmerge.loc[index, 'accum_dist'] = dist_accum
            if dist_accum < down_dist_thresh:
                upstream_rows = df_nmerge[(df_nmerge['branch_id'] == row['branch_id']) & (df_nmerge['route_count'] < row['route_count']) & df_nmerge['a'].notna()]
                if not upstream_rows.empty:
                    df_nmerge.loc[index, ['a', 'b']] = upstream_rows[['a', 'b']].mean()
        else:
            dist_accum = 0
            df_nmerge.loc[index, 'accum_dist'] = 0
    df_nmerge = df_nmerge.drop(['accum_dist'], axis=1, errors='ignore')
    return df_nmerge

