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
import rasterio
from geopandas.tools import sjoin

from utils.shared_variables import DOWNSTREAM_THRESHOLD, ROUGHNESS_MAX_THRESH, ROUGHNESS_MIN_THRESH


gpd.options.io_engine = "pyogrio"


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
    calculates new SRC roughness values via Manning's equation.
    The new roughness values are averaged for each HydroID and then progated downstream and
    a new discharge value is calculated where applicable.

    Processing Steps:
    - Read in the hydroTable.csv and check whether it has previously been updated
        (rename default columns if needed)
    - Loop through the user provided point data --> stage/flow dataframe row by row and copy the corresponding
        htable values for the matching stage->HAND lookup
    - Calculate new HydroID roughness values for input obs data using Manning's equation
    - Create dataframe to check for erroneous Manning's n values
        (values set in tools_shared_variables.py: >0.6 or <0.001 --> see input args)
    - Create magnitude and ahps column by subsetting the "layer" attribute
    - Create df grouped by hydroid with ahps_lid and huc number and then pivot the magnitude column to display
        n value for each magnitude at each hydroid
    - Create df with the most recent collection time entry and submitter attribs
    - Cacluate median ManningN to handle cases with multiple hydroid entries and create a df with the median
        hydroid_ManningN value per feature_id
    - Rename the original hydrotable variables to allow new calculations to use the primary var name
    - Check for large variabilty in the calculated Manning's N values
        (for cases with mutliple entries for a single hydroid)
    - Create attributes to traverse the flow network between HydroIDs
    - Calculate group_calb_coef (mean calb n for consective hydroids) and apply values downsteam to
        non-calb hydroids (constrained to first Xkm of hydroids -
        set downstream diststance var as input arg)
    - Create the adjust_ManningN column by combining the hydroid_ManningN with the featid_ManningN
        (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
    - Merge in previous SRC adjustments (where available) for hydroIDs that do not have a new adjusted
        roughness value
    - Update the catchments polygon .gpkg with joined attribute - "src_calibrated"
    - Merge the final ManningN dataframe to the original hydroTable
    - Create the ManningN column by combining the hydroid_ManningN with the default_ManningN
        (use modified where available)
    - Calculate new discharge_cms with new adjusted ManningN
    - Export a new hydroTable.csv and overwrite the previous version and output new src json
        (overwrite previous)

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
    if source_tag == 'point_obs':
        calb_type = 'calb_coef_spatial'
    elif source_tag == 'usgs_rating':
        calb_type = 'calb_coef_usgs'
    elif source_tag == 'ras2fim_rating':
        calb_type = 'calb_coef_ras2fim'
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
        df_htable['calb_coef_usgs'] = pd.NA
        df_htable['calb_coef_spatial'] = pd.NA
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
        ],
        axis=1,
        errors='ignore',
    )
    df_htable = df_htable.rename(columns={'precalb_discharge_cms': 'discharge_cms'})

    ## loop through the user provided point data --> stage/flow dataframe row by row
    for index, row in df_nvalues.iterrows():
        if row.hydroid not in df_htable['HydroID'].values:
            print(
                'WARNING: HydroID for calb point was not found in the hydrotable (check hydrotable) for HUC: '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' hydroid: '
                + str(row.hydroid)
            )
            log_text += (
                'WARNING: HydroID for calb point was not found in the hydrotable (check hydrotable) for HUC: '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' hydroid: '
                + str(row.hydroid)
                + '\n'
            )
        else:
            # filter htable for entries with matching hydroid and ignore stage 0
            # (first possible stage match at 1ft)
            df_htable_hydroid = df_htable[(df_htable.HydroID == row.hydroid) & (df_htable.stage > 0)]
            if df_htable_hydroid.empty:
                print(
                    'WARNING: df_htable_hydroid is empty but expected data: '
                    + str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' hydroid: '
                    + str(row.hydroid)
                )
                log_text += (
                    'WARNING: df_htable_hydroid is empty but expected data: '
                    + str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' hydroid: '
                    + str(row.hydroid)
                    + '\n'
                )

            find_src_stage = df_htable_hydroid.loc[
                df_htable_hydroid['stage'].sub(row.hand).abs().idxmin()
            ]  # find closest matching stage to the user provided HAND value

            ## copy the corresponding htable values for the matching stage->HAND lookup
            df_nvalues.loc[index, 'feature_id'] = find_src_stage.feature_id
            df_nvalues.loc[index, 'LakeID'] = find_src_stage.LakeID
            df_nvalues.loc[index, 'NextDownID'] = find_src_stage.NextDownID
            df_nvalues.loc[index, 'LENGTHKM'] = find_src_stage.LENGTHKM
            df_nvalues.loc[index, 'src_stage'] = find_src_stage.stage
            df_nvalues.loc[index, 'channel_n'] = find_src_stage.channel_n
            df_nvalues.loc[index, 'overbank_n'] = find_src_stage.overbank_n
            df_nvalues.loc[index, 'discharge_cms'] = find_src_stage.discharge_cms

    if 'discharge_cms' not in df_nvalues:
        print(
            'WARNING: "discharge_cms" column does not exist in df_nvalues df: '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
        )
        log_text += (
            'WARNING: "discharge_cms" column does not exist in df_nvalues df: '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + '\n'
        )
        return log_text

    ## Calculate calibration coefficient
    df_nvalues = df_nvalues.rename(columns={'hydroid': 'HydroID'})  # rename the previous ManningN column
    df_nvalues['hydroid_calb_coef'] = df_nvalues['discharge_cms'] / df_nvalues['flow']  # Qobs / Qsrc

    ## Calcuate a "calibration adjusted" n value using channel and overbank n-values multiplied by calb_coef
    df_nvalues['channel_n_calb'] = df_nvalues['hydroid_calb_coef'] * df_nvalues['channel_n']
    df_nvalues['overbank_n_calb'] = df_nvalues['hydroid_calb_coef'] * df_nvalues['overbank_n']

    ## Create dataframe to check for unrealistic/egregious calibration adjustments by applying the calibration
    # coefficient to the Manning's n values and setting an acceptable range
    # (values set in tools_shared_variables.py --> >0.8 or <0.001)
    df_nvalues['Mann_flag'] = np.where(
        (df_nvalues['channel_n_calb'] >= ROUGHNESS_MAX_THRESH)
        | (df_nvalues['overbank_n_calb'] >= ROUGHNESS_MAX_THRESH)
        | (df_nvalues['channel_n_calb'] <= ROUGHNESS_MIN_THRESH)
        | (df_nvalues['overbank_n_calb'] <= ROUGHNESS_MIN_THRESH)
        | (df_nvalues['hydroid_calb_coef'].isnull()),
        'Fail',
        'Pass',
    )
    df_mann_flag = df_nvalues[(df_nvalues['Mann_flag'] == 'Fail')][
        ['HydroID', 'hydroid_calb_coef', 'channel_n_calb', 'overbank_n_calb']
    ]
    if not df_mann_flag.empty:
        log_text += '!!! Flaged Mannings Roughness values below !!!' + '\n'
        log_text += df_mann_flag.to_string() + '\n'

    ## Create magnitude and ahps column by subsetting the "layer" attribute
    df_nvalues['magnitude'] = df_nvalues['layer'].str.split("_").str[5]
    df_nvalues['ahps_lid'] = df_nvalues['layer'].str.split("_").str[1]
    df_nvalues['huc'] = str(huc)
    df_nvalues = df_nvalues.drop(['layer'], axis=1)

    ## Create df grouped by hydroid with ahps_lid and huc number
    df_huc_lid = df_nvalues.groupby(["HydroID"]).first()[['ahps_lid', 'huc']]
    df_huc_lid.columns = pd.MultiIndex.from_product([['info'], df_huc_lid.columns])

    ## pivot the magnitude column to display n value for each magnitude at each hydroid
    df_nvalues_mag = df_nvalues.pivot_table(
        index='HydroID', columns='magnitude', values=['hydroid_calb_coef'], aggfunc='mean'
    )  # if there are multiple entries per hydroid and magnitude - aggregate using mean

    ## Optional: Export csv with the newly calculated Manning's N values
    if debug_outputs_option:
        output_calc_n_csv = os.path.join(fim_directory, calb_type + '_src_calcs_' + branch_id + '.csv')
        df_nvalues.to_csv(output_calc_n_csv, index=False)

    ## filter the modified Manning's n dataframe for values out side allowable range
    df_nvalues = df_nvalues[df_nvalues['Mann_flag'] == 'Pass']

    ## Check that there are valid entries in the calculate roughness df after filtering
    if not df_nvalues.empty:
        ## Create df with the most recent collection time entry and submitter attribs
        df_updated = df_nvalues[['HydroID', 'coll_time', 'submitter', 'ahps_lid']]  # subset the dataframe
        df_updated = df_updated.sort_values('coll_time').drop_duplicates(
            ['HydroID'], keep='last'
        )  # sort by collection time and then drop duplicate HydroIDs (keep most recent coll_time per HydroID)
        df_updated = df_updated.rename(columns={'coll_time': 'last_updated'})

        ## cacluate median ManningN to handle cases with multiple hydroid entries
        df_mann_hydroid = df_nvalues.groupby(["HydroID"])[['hydroid_calb_coef']].median()

        ## Create a df with the median hydroid_ManningN value per feature_id
        # df_mann_featid = df_nvalues.groupby(["feature_id"])[['hydroid_ManningN']].mean()
        # df_mann_featid = df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'})

        ## Rename the original hydrotable variables to allow new calculations to use the primary var name
        df_htable = df_htable.rename(columns={'discharge_cms': 'precalb_discharge_cms'})

        ## Check for large variabilty in the calculated Manning's N values
        # (for cases with mutliple entries for a singel hydroid)
        df_nrange = df_nvalues.groupby('HydroID').agg(
            {'hydroid_calb_coef': ['median', 'min', 'max', 'std', 'count']}
        )
        df_nrange['hydroid_calb_coef', 'range'] = (
            df_nrange['hydroid_calb_coef', 'max'] - df_nrange['hydroid_calb_coef', 'min']
        )
        df_nrange = df_nrange.join(
            df_nvalues_mag, how='outer'
        )  # join the df_nvalues_mag containing hydroid_manningn values per flood magnitude category
        df_nrange = df_nrange.merge(
            df_huc_lid, how='outer', on='HydroID'
        )  # join the df_huc_lid df to add attributes for lid and huc#
        log_text += 'Statistics for Modified Roughness Calcs -->' + '\n'
        log_text += df_nrange.to_string() + '\n'
        log_text += '----------------------------------------\n'

        ## Optional: Output csv with SRC calc stats
        if debug_outputs_option:
            output_stats_n_csv = os.path.join(
                fim_directory, calb_type + '_src_coef_vals_stats_' + branch_id + '.csv'
            )
            df_nrange.to_csv(output_stats_n_csv, index=True)

        ## subset the original hydrotable dataframe and subset to one row per HydroID
        df_nmerge = df_htable[
            ['HydroID', 'feature_id', 'NextDownID', 'LENGTHKM', 'LakeID', 'order_']
        ].drop_duplicates(['HydroID'], keep='first')

        ## Need to check that there are non-lake hydroids in the branch hydrotable (prevents downstream error)
        df_htable_check_lakes = df_nmerge.loc[df_nmerge['LakeID'] == -999]
        if not df_htable_check_lakes.empty:
            ## Create attributes to traverse the flow network between HydroIDs
            df_nmerge = branch_network_tracer(df_nmerge)

            ## Merge the newly caluclated ManningN dataframes
            df_nmerge = df_nmerge.merge(df_mann_hydroid, how='left', on='HydroID')
            df_nmerge = df_nmerge.merge(df_updated, how='left', on='HydroID')

            ## Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to
            # non-calb hydroids (constrained to first Xkm of hydroids- set downstream diststance var as input)
            df_nmerge = group_manningn_calc(df_nmerge, down_dist_thresh)

            ## Create a df with the median hydroid_calb_coef value per feature_id
            df_mann_featid = df_nmerge.groupby(["feature_id"])[['hydroid_calb_coef']].mean()
            df_mann_featid = df_mann_featid.rename(columns={'hydroid_calb_coef': 'featid_calb_coef'})
            # create a seperate df with attributes to apply to other hydroids that share a featureid
            df_mann_featid_attrib = df_nmerge.groupby('feature_id').first()
            df_mann_featid_attrib = df_mann_featid_attrib[df_mann_featid_attrib['submitter'].notna()][
                ['last_updated', 'submitter']
            ]
            df_nmerge = df_nmerge.merge(df_mann_featid, how='left', on='feature_id').set_index('feature_id')
            df_nmerge = df_nmerge.combine_first(df_mann_featid_attrib).reset_index()

            if not df_nmerge['hydroid_calb_coef'].isnull().all():
                ## Create the calibration coefficient column by combining the hydroid_calb_coef with the
                # featid_calb_coef (use feature_id value if the hydroid is in a feature_id that contains
                # valid hydroid_calb_coef value(s))
                conditions = [
                    (df_nmerge['hydroid_calb_coef'].isnull()) & (df_nmerge['featid_calb_coef'].notnull()),
                    (df_nmerge['hydroid_calb_coef'].isnull())
                    & (df_nmerge['featid_calb_coef'].isnull())
                    & (df_nmerge['group_calb_coef'].notnull()),
                ]
                choices = [df_nmerge['featid_calb_coef'], df_nmerge['group_calb_coef']]
                df_nmerge[calb_type] = np.select(conditions, choices, default=df_nmerge['hydroid_calb_coef'])
                df_nmerge['obs_source'] = np.where(df_nmerge[calb_type].notnull(), source_tag, pd.NA)
                df_nmerge = df_nmerge.drop(
                    ['feature_id', 'NextDownID', 'LENGTHKM', 'LakeID', 'order_'], axis=1, errors='ignore'
                )  # drop these columns to avoid duplicates where merging with the full hydroTable df

                ## Merge in previous SRC adjustments (where available) for hydroIDs that do not have a new
                # adjusted roughness value
                if not df_prev_adj.empty:
                    df_nmerge = pd.merge(df_nmerge, df_prev_adj, on='HydroID', how='outer')
                    df_nmerge['submitter'] = np.where(
                        (df_nmerge[calb_type].isnull() & df_nmerge['calb_coef_final_prev'].notnull()),
                        df_nmerge['submitter_prev'],
                        df_nmerge['submitter'],
                    )
                    df_nmerge['last_updated'] = np.where(
                        (df_nmerge[calb_type].isnull() & df_nmerge['calb_coef_final_prev'].notnull()),
                        df_nmerge['last_updated_prev'],
                        df_nmerge['last_updated'],
                    )
                    df_nmerge['obs_source'] = np.where(
                        (df_nmerge[calb_type].isnull() & df_nmerge['calb_coef_final_prev'].notnull()),
                        df_nmerge['obs_source_prev'],
                        df_nmerge['obs_source'],
                    )
                    df_nmerge['calb_coef_final'] = np.where(
                        (df_nmerge[calb_type].isnull() & df_nmerge['calb_coef_final_prev'].notnull()),
                        df_nmerge['calb_coef_final_prev'],
                        df_nmerge[calb_type],
                    )
                    df_nmerge = df_nmerge.drop(
                        ['submitter_prev', 'last_updated_prev', 'calb_coef_final_prev', 'obs_source_prev'],
                        axis=1,
                        errors='ignore',
                    )
                else:
                    df_nmerge['calb_coef_final'] = df_nmerge[calb_type]

                ## Update the catchments polygon .gpkg with joined attribute - "src_calibrated"
                if os.path.isfile(catchments_poly_path):
                    try:
                        input_catchments = gpd.read_file(catchments_poly_path)
                        ## Create new "src_calibrated" column for viz query
                        if 'src_calibrated' in input_catchments.columns:
                            input_catchments = input_catchments.drop(
                                ['src_calibrated', 'obs_source', 'calb_coef_final'], axis=1, errors='ignore'
                            )
                        df_nmerge['src_calibrated'] = np.where(
                            df_nmerge['calb_coef_final'].notnull(), 'True', 'False'
                        )
                        output_catchments = input_catchments.merge(
                            df_nmerge[['HydroID', 'src_calibrated', 'obs_source', 'calb_coef_final']],
                            how='left',
                            on='HydroID',
                        )
                        output_catchments['src_calibrated'].fillna('False', inplace=True)

                        try:
                            output_catchments.to_file(
                                catchments_poly_path,
                                driver="GPKG",
                                index=False,
                                overwrite=True,
                                engine='fiona',
                            )  # overwrite the previous layer

                        except Exception as e:
                            error_message = (
                                "ERROR occurred while writing to catchments gpkg "
                                f"for huc: {huc} & branch id: {branch_id}"
                            )
                            print(error_message)
                            log_text += f"{error_message}\n"
                            log_text += f"Error details: {e}\n"

                            # Delete the original GeoPackage file
                            if os.path.exists(catchments_poly_path):
                                os.remove(catchments_poly_path)
                            try:
                                # Attempt to write to the file again
                                output_catchments.to_file(
                                    catchments_poly_path,
                                    driver="GPKG",
                                    index=False,
                                    overwrite=True,
                                    engine='fiona',
                                )
                                log_text += 'Successful second attempt to write output_catchments gpkg' + '\n'
                            except Exception as e:
                                second_attempt_error_message = "ERROR: Failed to write to catchments gpkg file even after deleting the original"
                                print(second_attempt_error_message)
                                log_text += f"{second_attempt_error_message}\n"
                                log_text += f"Second attempt error details: {e}\n"

                    except Exception as e:
                        print(f"Error reading GeoPackage file: {e}")
                        log_text += f"Error reading GeoPackage file: {e}\n"
                        output_catchments = None

                df_nmerge = df_nmerge.drop(['src_calibrated'], axis=1, errors='ignore')

                ## Optional ouputs:
                #   1) merge_n_csv csv with all of the calculated n values
                #   2) a catchments .gpkg with new joined attributes
                if debug_outputs_option:
                    output_merge_n_csv = os.path.join(
                        fim_directory, calb_type + '_merge_vals_' + branch_id + '.csv'
                    )
                    df_nmerge.to_csv(output_merge_n_csv, index=False)
                    ## output new catchments polygon layer with several new attributes appended
                    if os.path.isfile(catchments_poly_path):
                        input_catchments = gpd.read_file(catchments_poly_path)
                        output_catchments_fileName = os.path.join(
                            os.path.split(catchments_poly_path)[0],
                            "gw_catchments_src_adjust_" + str(branch_id) + ".gpkg",
                        )
                        output_catchments = input_catchments.merge(df_nmerge, how='left', on='HydroID')
                        output_catchments.to_file(
                            output_catchments_fileName, driver="GPKG", index=False, engine='fiona'
                        )
                        output_catchments = None

                ## Merge the final ManningN dataframe to the original hydroTable
                df_nmerge = df_nmerge.drop(
                    [
                        'ahps_lid',
                        'start_catch',
                        'route_count',
                        'branch_id',
                        'hydroid_calb_coef',
                        'featid_calb_coef',
                        'group_calb_coef',
                    ],
                    axis=1,
                    errors='ignore',
                )  # drop these columns to avoid duplicates where merging with the full hydroTable df
                df_htable = df_htable.merge(df_nmerge, how='left', on='HydroID')
                df_htable['calb_applied'] = np.where(
                    df_htable['calb_coef_final'].notnull(), 'True', 'False'
                )  # create true/false column to clearly identify where new roughness values are applied

                ## Calculate new discharge_cms with new adjusted ManningN
                df_htable['discharge_cms'] = np.where(
                    df_htable['calb_coef_final'].isnull(),
                    df_htable['precalb_discharge_cms'],
                    df_htable['precalb_discharge_cms'] / df_htable['calb_coef_final'],
                )

                ## Replace discharge_cms with 0 or -999 if present in the original discharge
                # (carried over from thalweg notch workaround in SRC post-processing)
                df_htable['discharge_cms'].mask(df_htable['precalb_discharge_cms'] == 0.0, 0.0, inplace=True)
                df_htable['discharge_cms'].mask(
                    df_htable['precalb_discharge_cms'] == -999, -999, inplace=True
                )

                ## Export a new hydroTable.csv and overwrite the previous version
                out_htable = os.path.join(fim_directory, 'hydroTable_' + branch_id + '.csv')
                df_htable.to_csv(out_htable, index=False)

            else:
                print(
                    'ALERT!! HUC: '
                    + str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' --> no valid hydroid roughness calculations after removing lakeid catchments from '
                    + ' consideration \n'
                )
                log_text += (
                    'ALERT!! HUC: '
                    + str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' --> no valid hydroid roughness calculations after removing lakeid catchments from '
                    + ' consideration \n'
                )
        else:
            print(
                'WARNING!! HUC: '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' --> hydrotable is empty after removing lake catchments (will ignore branch)'
            )
            log_text += (
                'ALERT!! HUC: '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' --> hydrotable is empty after removing lake catchments (will ignore branch)\n'
            )
    else:
        print(
            'ALERT!! HUC: '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + ' --> no valid roughness calculations- please check point data and src calculations to evaluate'
        )
        log_text += (
            'ALERT!! HUC: '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + ' --> no valid roughness calculations- please check point data and src calculations to evaluate'
        )

    log_text += '\n Completed: ' + str(huc) + ' --> branch: ' + str(branch_id) + '\n'
    log_text += '#########################################################\n'
    print("Completed huc: " + str(huc) + ' --> branch: ' + str(branch_id))
    return log_text


def branch_network_tracer(df_input_htable):
    df_input_htable = df_input_htable.astype(
        {'NextDownID': 'int64'}
    )  # ensure attribute has consistent format as int
    # remove all hydroids associated with lake/water body
    # (these often have disjoined artifacts in the network)
    df_input_htable = df_input_htable.loc[df_input_htable['LakeID'] == -999]
    # define start catchments as hydroids that are not found in the "NextDownID" attribute for all
    # other hydroids
    df_input_htable["start_catch"] = ~df_input_htable['HydroID'].isin(df_input_htable['NextDownID'])

    df_input_htable = df_input_htable.set_index('HydroID', drop=False)  # set index to the hydroid
    branch_heads = deque(
        df_input_htable[df_input_htable['start_catch'] == True]['HydroID'].tolist()
    )  # create deque of hydroids to define start points in the while loop
    visited = set()  # create set to keep track of all hydroids that have been accounted for
    branch_count = 0  # start branch id
    while branch_heads:
        hid = branch_heads.popleft()  # pull off left most hydroid from deque of start hydroids
        Q = deque(
            df_input_htable[df_input_htable['HydroID'] == hid]['HydroID'].tolist()
        )  # create a new deque that will be used to populate all relevant downstream hydroids
        vert_count = 0
        branch_count += 1
        while Q:
            q = Q.popleft()
            if q not in visited:
                df_input_htable.loc[df_input_htable.HydroID == q, 'route_count'] = (
                    vert_count  # assign var with flow order ranking
                )
                df_input_htable.loc[df_input_htable.HydroID == q, 'branch_id'] = (
                    branch_count  # assign var with current branch id
                )
                vert_count += 1
                visited.add(q)
                # find the id for the next downstream hydroid
                nextid = df_input_htable.loc[q, 'NextDownID']
                order = df_input_htable.loc[q, 'order_']  # find the streamorder for the current hydroid

                if nextid not in visited and nextid in df_input_htable.HydroID:
                    # check if the NextDownID is referenced by more than one hydroid
                    # (>1 means this is a confluence)
                    check_confluence = (df_input_htable.NextDownID == nextid).sum() > 1
                    nextorder = df_input_htable.loc[
                        nextid, 'order_'
                    ]  # find the streamorder for the next downstream hydroid
                    # check if the nextdownid streamorder is greater than the current hydroid order and the
                    # nextdownid is a confluence (more than 1 upstream hydroid draining to it)
                    if nextorder > order and check_confluence == True:
                        branch_heads.append(
                            nextid
                        )  # found a terminal point in the network (append to branch_heads for second pass)
                        # if above conditions are True than stop traversing downstream and move on to next
                        # starting hydroid
                        continue
                    Q.append(nextid)
    df_input_htable = df_input_htable.reset_index(
        drop=True
    )  # reset index (previously using hydroid as index)
    # sort the dataframe by branch_id and then by route_count
    # (need this ordered to ensure upstream to downstream ranking for each branch)
    df_input_htable = df_input_htable.sort_values(['branch_id', 'route_count'])
    return df_input_htable


def group_manningn_calc(df_nmerge, down_dist_thresh):
    ## Calculate group_calb_coef (mean calb n for consective hydroids) and apply values downsteam to
    # non-calb hydroids (constrained to first Xkm of hydroids - set downstream diststance var as input arg
    # df_nmerge = df_nmerge.sort_values(by=['NextDownID'])
    dist_accum = 0
    hyid_count = 0
    hyid_accum_count = 0
    run_accum_mann = 0
    group_calb_coef = 0
    branch_start = 1  # initialize counter and accumulation variables
    lid_count = 0
    prev_lid = 'x'
    for index, row in df_nmerge.iterrows():  # loop through the df (parse by hydroid)
        if int(df_nmerge.loc[index, 'branch_id']) != branch_start:  # check if start of new branch
            dist_accum = 0
            hyid_count = 0
            hyid_accum_count = 0
            # initialize counter vars
            run_accum_mann = 0
            group_calb_coef = 0  # initialize counter vars
            branch_start = int(
                df_nmerge.loc[index, 'branch_id']
            )  # reassign the branch_start var to evaluate on next iteration
            # use the code below to withold downstream hydroid_calb_coef values
            # (use this for downstream evaluation tests)
            '''
            lid_count = 0
        if not pd.isna(df_nmerge.loc[index,'ahps_lid']):
            if df_nmerge.loc[index,'ahps_lid'] == prev_lid:
                lid_count += 1
                # only keep the first 3 HydroID n values
                # (everything else set to null for downstream application)
                if lid_count > 3:
                    df_nmerge.loc[index,'hydroid_ManningN'] = np.nan
                    df_nmerge.loc[index,'featid_ManningN'] = np.nan
            else:
                lid_count = 1
            prev_lid = df_nmerge.loc[index,'ahps_lid']
            '''
        if np.isnan(
            df_nmerge.loc[index, 'hydroid_calb_coef']
        ):  # check if the hydroid_calb_coef value is nan (indicates a non-calibrated hydroid)
            df_nmerge.loc[index, 'accum_dist'] = (
                row['LENGTHKM'] + dist_accum
            )  # calculate accumulated river distance
            dist_accum += row['LENGTHKM']  # add hydroid length to the dist_accum var
            hyid_count = 0  # reset the hydroid counter to 0
            df_nmerge.loc[index, 'hyid_accum_count'] = hyid_accum_count  # output the hydroid accum counter
            # check if the accum distance is less than Xkm downstream from valid hydroid_calb_coef group value
            if dist_accum < down_dist_thresh:
                # only apply the group_calb_coef if there are 2 or more valid hydorids that contributed to the
                # upstream group_calb_coef
                if hyid_accum_count > 1:
                    df_nmerge.loc[index, 'group_calb_coef'] = (
                        group_calb_coef  # output the group_calb_coef var
                    )
            else:
                # reset the running average manningn variable (greater than 10km downstream)
                run_avg_mann = 0
        # performs the following for hydroids that have a valid hydroid_calb_coef value
        else:
            dist_accum = 0
            hyid_count += 1  # initialize vars
            df_nmerge.loc[index, 'accum_dist'] = 0  # output the accum_dist value (set to 0)
            if hyid_count == 1:  # checks if this the first in a series of valid hydroid_calb_coef values
                run_accum_mann = 0
                hyid_accum_count = 0  # initialize counter and running accumulated manningN value
            # calculate the group_calb_coef (NOTE: this will continue to change as more hydroid values are
            # accumulated in the "group" moving downstream)
            group_calb_coef = (row['hydroid_calb_coef'] + run_accum_mann) / float(hyid_count)
            df_nmerge.loc[index, 'group_calb_coef'] = group_calb_coef  # output the group_calb_coef var
            df_nmerge.loc[index, 'hyid_count'] = hyid_count  # output the hyid_count var
            run_accum_mann += row[
                'hydroid_calb_coef'
            ]  # add current hydroid manningn value to the running accum mann var
            hyid_accum_count += 1  # increase the # of hydroid accum counter
            df_nmerge.loc[index, 'hyid_accum_count'] = hyid_accum_count  # output the hyid_accum_count var

    ## Delete unnecessary intermediate outputs
    if 'hyid_count' in df_nmerge.columns:
        df_nmerge = df_nmerge.drop(
            ['hyid_count', 'accum_dist', 'hyid_accum_count'], axis=1, errors='ignore'
        )  # drop hydroid counter if it exists
    ## drop accum vars from group calc
    # df_nmerge = df_nmerge.drop(['accum_dist','hyid_accum_count'], axis=1)
    return df_nmerge
