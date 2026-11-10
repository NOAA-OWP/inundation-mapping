#!/usr/bin/env python3

import datetime as dt
import logging
import os
import re
import sys
import traceback
from argparse import ArgumentParser

from os.path import join

import geopandas as gpd
import pandas as pd

from src.utils.shared_functions import setup_file_logger

# -------------------------------------------------------
# Function to use RFC Bathymetry where available over eHydro Data
def ohrfc_bathy_precedence(group):
    # Re-inject the grouping column
    group['feature_id'] = group.name

    ohrfc_source = 'OHRFC provided bathymetry, compiled from USACE data'
    ohrfc_data = group['Bathymetry_source'] == ohrfc_source
    if ohrfc_data.any():
        return group[ohrfc_data].iloc[0]
    else:
        id_group = group.iloc[0].copy()
        id_group[['missing_xs_area_m2', 'missing_wet_perimeter_m']] = group[
            ['missing_xs_area_m2', 'missing_wet_perimeter_m']
        ].mean()
        return id_group


# -------------------------------------------------------
# Adjusting synthetic rating curves using 'USACE eHydro' bathymetry data
def correct_rating_for_ehydro_bathymetry(huc_dir, huc, bathy_file_ehydro):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input eHydro bathy_file.

        Parameters
        ----------
        huc_dir : str
            Directory path for a HUC run.
        huc : str
            HUC-8 string.
        bathy_file_ehydro : str
            Path to eHydro bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".

    """

    logging.debug(f'Calculating eHydro bathymetry rating curve adjustment: {huc}')

    # Load wbd and use it as a mask to pull the bathymetry data
    # fim_huc_dir = join(fim_dir, huc)
    wbd8_clp = gpd.read_file(join(huc_dir, 'wbd8_clp.gpkg'), engine="pyogrio", use_arrow=True)
    bathy_data = gpd.read_file(bathy_file_ehydro, mask=wbd8_clp, engine="fiona")
    bathy_data = bathy_data.rename(columns={'ID': 'feature_id'})

    # Get src_full from each branch
    src_all_branches = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches.append(src_full)

    # Update src parameters with bathymetric data
    logging.debug("About to process branch crosswalks for ehydro")
    for src_branch_crosswalk_path in src_all_branches:
        branch = re.search(r'branches/(\d{10}|0)/', src_branch_crosswalk_path).group()[9:-1]
        logging.debug(f'  Branch: {branch} - Update src parameters with bathymetric data')

        src_df = pd.read_csv(src_branch_crosswalk_path, low_memory=False)
        if src_df.empty:
            logging.debug(f'  Branch: {branch} - There were no eHydro bathymetry feature_ids for this branch')
            src_df['Bathymetry_source'] = [""] * len(src_df)
            src_df.to_csv(src_branch_crosswalk_path, index=False)
            logging.debug(f"  Branch: {branch} - {src_branch_crosswalk_path} saved")
            continue

        if 'Bathymetry_source' in src_df.columns:
            src_df = src_df.drop(columns='Bathymetry_source')

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
        # If there's more than one survey for a single feature_id in the bathy data:
        # take the ohrfc value first, then the mean of all availabe ehydro values.
        except pd.errors.MergeError:
            reconciled_bathy_data = (
                bathy_data[
                    ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
                ]
                .groupby('feature_id')
                .apply(ohrfc_bathy_precedence, include_groups=False)
                .reset_index(drop=True)
            )
            # TODO: Nov 2026: Added a card for this fix  (1972)
            logging.warning(f"  Branch: {branch} - pd.errors.MergeError exception found."
                            " PS.. do we need to check something to avoid an exception?")

            # reconciled_bathy_data = bathy_data.groupby('feature_id')[
            #     ['missing_xs_area_m2', 'missing_wet_perimeter_m']
            # ].mean()
            # reconciled_bathy_data['Bathymetry_source'] = bathy_data.groupby('feature_id')[
            #     'Bathymetry_source'
            # ].first()
            src_df = src_df.merge(reconciled_bathy_data, on='feature_id', how='left', validate='many_to_one')

        # # Exit if there are no recalculations to be made
        # if ~src_df['Bathymetry_source'].any(axis=None):
        #     log_text += '    No matching feature_ids in this branch\n'
        #     continue

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
            * src_df['HydraulicRadius (m)'] ** (2.0 / 3)
            * src_df['SLOPE'] ** 0.5
            / src_df['ManningN']
        )
        # Force zero stage to have zero discharge
        src_df.loc[src_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0
        cond_q = src_df['Number of Cells'] == 0
        src_df.loc[cond_q, 'Discharge (m3s-1)'] = 0
        src_df.loc[cond_q, 'BedArea (m2)'] = 0
        src_df.loc[cond_q, 'Volume (m3)'] = 0
        src_df.loc[cond_q, 'WettedPerimeter (m)'] = 0
        src_df.loc[cond_q, 'WetArea (m2)'] = 0
        src_df.loc[cond_q, 'HydraulicRadius (m)'] = 0

        src_df2 = src_df.copy()
        discharge_bathymetry = src_df2['Discharge (m3s-1)']
        src_df['Discharge (m3s-1)_bathymetryAdjusted'] = discharge_bathymetry

        # Write src back to file
        # src_df = src_df.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)
        src_df.to_csv(src_branch_crosswalk_path, index=False)
        logging.debug(f"  Branch: {branch} - {src_branch_crosswalk_path} saved after ehydro bathy adjustments")


# -------------------------------------------------------
# Adjusting synthetic rating curves using 'AI-based' bathymetry data
def correct_rating_for_ai_bathymetry(huc_dir, huc, strm_order, bathy_file_aibased):
    """
    Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input AI-based bathy_file.

        Parameters
        ----------
        huc_dir : str
            Directory path for a HUC run.
        huc : str
            HUC-8 string.
        strm_order : int
            stream order on or higher for which you want to apply AI-based bathymetry data.
            default = 4
        bathy_file_aibased : str
            Path to AI-based bathymetric adjustment file, e.g.
            "/data/inputs/bathymetry/ml_outputs_v1.01.parquet".

    """
    logging.debug(f'Calculating AI-based bathymetry rating curve adjustment: {huc}')

    # Load AI-based bathymetry data
    ml_bathy_data = pd.read_parquet(bathy_file_aibased, engine='pyarrow')
    ml_bathy_data_df = ml_bathy_data[
        ['hf_id', 'owp_tw_inchan', 'owp_inchan_channel_area', 'owp_inchan_channel_perimeter']
    ]
    # ml_bathy_data_df = ml_bathy_data[
    #     ['COMID', 'owp_tw_inchan', 'owp_inchan_channel_area', 'owp_inchan_channel_perimeter']
    # ] # 'owp_inchan_channel_volume', 'owp_inchan_channel_bed_area', 'owp_y_inchan'

    # fim_huc_dir = join(fim_dir, huc)

    path_nwm_streams = join(huc_dir, "nwm_subset_streams.gpkg")
    nwm_stream = gpd.read_file(path_nwm_streams)

    wbd8 = gpd.read_file(join(huc_dir, 'wbd.gpkg'), engine="pyogrio", use_arrow=True)
    nwm_stream_clp = nwm_stream.clip(wbd8)

    ml_bathy_data_df = ml_bathy_data_df.merge(
        nwm_stream_clp[['ID', 'order_']], left_on='hf_id', right_on='ID'
    )
    aib_bathy_data_df = ml_bathy_data_df.drop(columns=['ID'])

    aib_bathy_data_df = aib_bathy_data_df.rename(columns={'hf_id': 'feature_id'})
    aib_bathy_data_df = aib_bathy_data_df.rename(columns={'owp_inchan_channel_area': 'missing_xs_area_m2'})

    # Calculating missing_wet_perimeter_m and adding it to aib_bathy_data_gdf
    missing_wet_perimeter_m = (
        aib_bathy_data_df['owp_inchan_channel_perimeter'] - aib_bathy_data_df['owp_tw_inchan']
    )
    aib_bathy_data_df['missing_wet_perimeter_m'] = missing_wet_perimeter_m
    aib_bathy_data_df['Bathymetry_source'] = "AI_Based"

    # Excluding streams with order lower than desired order (default = 4)
    # Assign 0 to numeric columns
    aib_bathy_data_df.loc[
        aib_bathy_data_df["order_"] < strm_order, ["missing_xs_area_m2", "missing_wet_perimeter_m"]
    ] = 0

    # Assign a string to the string column
    aib_bathy_data_df.loc[aib_bathy_data_df["order_"] < strm_order, "Bathymetry_source"] = (
        "No Bathymetry Applied"
    )

    aib_df0 = aib_bathy_data_df[
        ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
    ]

    # test = aib_df[aib_df.duplicated(subset='feature_id', keep=False)]
    aib_df = aib_df0.drop_duplicates(subset=['feature_id'], keep='first')
    aib_df.index = range(len(aib_df))
    logging.debug(f'Adjusting SRCs with EHydro and OHRFC Bathymetry Data: {huc}')

    # Get src_full from each branch
    src_all_branches_path = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches_path.append(src_full)

    # Update src parameters with bathymetric data
    logging.debug("About to process branches for AI-based bathymetry")
    for src_branch_crosswwalk_path in src_all_branches_path:
        src_name = os.path.basename(src_branch_crosswwalk_path)
        branch = src_name.split(".")[0].split("_")[-1]
        logging.debug(f"  Branch: {branch} - started processing")
       # print(src_df.loc[~src_df['Bathymetry_source'].isna()]['Bathymetry_source'])        
        src_df = pd.read_csv(src_branch_crosswwalk_path, low_memory=False)

        # Merge in missing ai bathy data and fill Nans
        if 'missing_xs_area_m2' not in src_df.columns:
            logging.debug(f"  Branch: {branch} IS missing the missing_xs_area_m2 column")
            src_df = src_df.drop(columns=["Bathymetry_source"])
            src_df = src_df.merge(aib_df, on='feature_id', how='left', validate='many_to_one')

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
                * src_df['HydraulicRadius (m)'] ** (2.0 / 3)
                * src_df['SLOPE'] ** 0.5
                / src_df['ManningN']
            )
            # Force zero stage to have zero discharge
            src_df.loc[src_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

            # Force zero "Number of the cells" to have zero discharge ...
            cond_q = src_df['Number of Cells'] == 0
            src_df.loc[cond_q, 'Discharge (m3s-1)'] = 0
            src_df.loc[cond_q, 'BedArea (m2)'] = 0
            src_df.loc[cond_q, 'Volume (m3)'] = 0
            src_df.loc[cond_q, 'WettedPerimeter (m)'] = 0
            src_df.loc[cond_q, 'WetArea (m2)'] = 0
            src_df.loc[cond_q, 'HydraulicRadius (m)'] = 0

            # Write src back to file
            src_df = src_df.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)

            src_df2 = src_df.copy()
            discharge_bathymetry = src_df2['Discharge (m3s-1)']
            src_df['Discharge (m3s-1)_bathymetryAdjusted'] = discharge_bathymetry

            src_df.to_csv(src_branch_crosswwalk_path, index=False)
            logging.debug(f"  Branch: {branch} - {src_branch_crosswwalk_path} saved")

        else:
            logging.debug(f"  Branch: {branch} IS NOT missing the missing_xs_area_m2 column")
            # src_df = src_df.merge(aib_df, on='feature_id', how='left', validate='many_to_one')
            # # checked

            # src_df.loc[src_df["Bathymetry_source_x"].isna(), ["missing_xs_area_m2_x"]] = src_df[
            #     "missing_xs_area_m2_y"
            # ]
            # src_df.loc[src_df["Bathymetry_source_x"].isna(), ["missing_wet_perimeter_m_x"]] = src_df[
            #     "missing_wet_perimeter_m_y"
            # ]
            # src_df.loc[src_df["Bathymetry_source_x"].isna(), ["Bathymetry_source_x"]] = src_df[
            #     "Bathymetry_source_y"
            # ]
            # checked
            src_df = src_df.merge(aib_df, on='feature_id', how='left', validate='many_to_one')

            mask = src_df["Bathymetry_source_x"].isna()

            # Explicitly cast the Bathymetry_source_x to 'object' dtype so it can accept strings
            src_df["Bathymetry_source_x"] = src_df["Bathymetry_source_x"].astype(object)

            src_df.loc[mask, "missing_xs_area_m2_x"] = src_df.loc[mask, "missing_xs_area_m2_y"]
            src_df.loc[mask, "missing_wet_perimeter_m_x"] = src_df.loc[mask, "missing_wet_perimeter_m_y"]
            src_df.loc[mask, "Bathymetry_source_x"] = src_df.loc[mask, "Bathymetry_source_y"]

            src_df = src_df.drop(
                columns=["missing_xs_area_m2_y", "missing_wet_perimeter_m_y", "Bathymetry_source_y"]
            )
            src_df = src_df.rename(columns={'missing_xs_area_m2_x': 'missing_xs_area_m2'})
            src_df = src_df.rename(columns={'missing_wet_perimeter_m_x': 'missing_wet_perimeter_m'})
            src_df = src_df.rename(columns={'Bathymetry_source_x': 'Bathymetry_source'})

            src_df['missing_xs_area_m2'] = src_df['missing_xs_area_m2'].fillna(0.0)
            src_df['missing_wet_perimeter_m'] = src_df['missing_wet_perimeter_m'].fillna(0.0)

            # Add missing hydraulic geometry into base parameters
            Volume_m3 = src_df['Volume (m3)'] + (src_df['missing_xs_area_m2'] * (src_df['LENGTHKM'] * 1000))
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "Volume (m3)"] = Volume_m3

            BedArea_m2 = src_df['BedArea (m2)'] + (
                src_df['missing_wet_perimeter_m'] * (src_df['LENGTHKM'] * 1000)
            )
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "BedArea (m2)"] = BedArea_m2

            # Recalc discharge with adjusted geometries
            WettedPerimeter_m = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter_m']
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "WettedPerimeter (m)"] = WettedPerimeter_m
            # src_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter_m']
            Wetarea_m2 = src_df['WetArea (m2)'] + src_df['missing_xs_area_m2']
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "WetArea (m2)"] = Wetarea_m2
            # src_df['WetArea (m2)'] = src_df['WetArea (m2)'] + src_df['missing_xs_area_m2']
            HydraulicRadius_m = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "HydraulicRadius (m)"] = HydraulicRadius_m
            # src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
            src_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)'].fillna(0)

            discharge_cms = (
                src_df['WetArea (m2)']
                * src_df['HydraulicRadius (m)'] ** (2.0 / 3)
                * src_df['SLOPE'] ** 0.5
                / src_df['ManningN']
            )
            src_df.loc[src_df["Bathymetry_source"] == "AI_Based", "Discharge (m3s-1)"] = discharge_cms

            # Force zero stage to have zero discharge
            src_df.loc[src_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

            # Force zero "Number of the cells" to have zero discharge ...
            cond_q = src_df['Number of Cells'] == 0
            src_df.loc[cond_q, 'Discharge (m3s-1)'] = 0
            src_df.loc[cond_q, 'BedArea (m2)'] = 0
            src_df.loc[cond_q, 'Volume (m3)'] = 0
            src_df.loc[cond_q, 'WettedPerimeter (m)'] = 0
            src_df.loc[cond_q, 'WetArea (m2)'] = 0
            src_df.loc[cond_q, 'HydraulicRadius (m)'] = 0

            # Write src back to file
            src_df = src_df.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)

            src_df3 = src_df.copy()
            discharge_bathymetry2 = src_df3['Discharge (m3s-1)']
            src_df['Discharge (m3s-1)_bathymetryAdjusted'] = discharge_bathymetry2

            src_df.to_csv(src_branch_crosswwalk_path, index=False)
            logging.debug(f"  Branch: {branch} - {src_branch_crosswwalk_path} saved")


# --------------------------------------------------------
# Apply src_adjustment_for_bathymetry
def apply_src_adjustment_for_bathymetry(huc_dir, huc, strm_order, bathy_file_ehydro, bathy_file_aibased, ai_toggle):
    """
    Function for applying both eHydro & AI-based bathymetry adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_rating_for_ehydro_bathymetry and
        correct_rating_for_ai_based_bathymetry functions parameters.
    """
    try:
        if os.path.exists(bathy_file_ehydro):
            correct_rating_for_ehydro_bathymetry(huc_dir, huc, bathy_file_ehydro)
        else:
            logging.info(f'Bathymetry file does not exist for huc: {huc} - {bathy_file_ehydro}')

    except Exception:
        logging.error(f"An error has occurred while applying src bathy adj for huc {huc}")
        logging.error(traceback.format_exc())
        # raise  # Yes.. re-raise to stop downstream processing
        sys.exit(1)

    if ai_toggle == 1:
        try:
            if os.path.exists(bathy_file_aibased):
                correct_rating_for_ai_bathymetry(huc_dir, huc, strm_order, bathy_file_aibased)  # , ai_toggle
            else:
                logging.info(f'AI-based bathymetry file does not exist for huc : {huc} - {bathy_file_aibased}')

        # except Exception as ex:
        except Exception:
            # this goes back to calibrate_rating_curve.sh which rolls up to its parent "tee"
            # Then it can be scanned in the error system based on solely the "tee" file
            logging.error(f"An error has occurred while applying AI-based bathy rating curve adj for huc {huc}")
            logging.error(traceback.format_exc())
            # raise  # Yes.. re-raise to stop downstream processing
            sys.exit(1)

# -------------------------------------------------------
def process_bathy_adjustment(
    huc_dir, strm_order, bathy_file_ehydro, bathy_file_aibased, output_suffix, ai_toggle
):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input bathy_file.

        Parameters
        ----------
        huc_dir : str
            Directory path for a HUC run output.
        strm_order : int
            stream order on or higher for which you want to apply AI-based bathymetry data.
            default = 4
        bathy_file_eHydro : str
            Path to eHydro bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        bathy_file_aibased : str
            Path to AI-based bathymetric adjustment file, e.g.
            "/data/inputs/bathymetry/ml_outputs_v1.01.parquet".
        wbd_buffer : int
            Distance in meters to buffer wbd dataset when searching for relevant HUCs.
        wbd : str
            Path to wbd input data, e.g.
            "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
        output_suffix : str
            Output filename suffix.

    """
    # Set up log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    log_file_path = setup_file_logger(log_dir, f"bathymetric_adjustment_{output_suffix}")

    print(f'Writing progress to log file here: {log_file_path}')
    logging.debug("Start bathmetric adjustments")
    logging.debug("")
    begin_time = dt.datetime.now(dt.timezone.utc)    

    # Exit program if the bathymetric data doesn't exist
    if ai_toggle == 1:
        if not all([os.path.exists(bathy_file_ehydro), os.path.exists(bathy_file_aibased)]):
            logging.warning(f"The input bathymetry files {bathy_file_ehydro} and {bathy_file_aibased}"
                         " do not exist. Exiting...")
            sys.exit(0)
    else:
        if not os.path.exists(bathy_file_ehydro):
            logging.warning(f'The input bathymetry file {bathy_file_ehydro} does not exist. Exiting...')
            sys.exit(0)

    if ai_toggle == 1:
        logging.info(f"AI-Based bathymetry data will be applied on streams with order {strm_order} or higher")

    # get hucnumber
    huc = os.path.basename(os.path.normpath(huc_dir))

    apply_src_adjustment_for_bathymetry(huc_dir, huc, strm_order, bathy_file_ehydro, bathy_file_aibased, ai_toggle)
    
    logging.debug("End bathymetric adjustments")

    end_time = dt.datetime.now(dt.timezone.utc)
    logging.debug(f'END TIME: {end_time}')
    tot_run_time = end_time - begin_time    
    logging.debug('DURATION: ' + str(tot_run_time).split('.')[0])


if __name__ == '__main__':

    """
    Parameters
    ----------
    fim_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/bathymetric_adjustment.log.
    strm_order : int
        stream order on or higher for which you want to apply AI-based bathymetry data.
        default = 4
    bathy_file_ehydro : str
        Path to eHydro bathymetric adjustment geopackage, e.g.
        "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
    bathy_file_aibased : str
        Path to AI-based bathymetric adjustment file, e.g.
        "/data/inputs/bathymetry/ml_outputs_v1.01.parquet".
    wbd_buffer : int
        Distance in meters to buffer wbd dataset when searching for relevant HUCs.
    wbd : str
        Path to wbd input data, e.g.
        "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
    output_suffix : str
        Optional. Output filename suffix. Defaults to no suffix.
    verbose : bool
        Optional flag for enabling verbose printing.

    Sample Usage
    ----------
    python3 /foss_fim/src/bathymetric_adjustment.py -fim-dir /outputs/fim_run_dir
        -bath-_eHydro /data/inputs/bathymetry/bathymetric_adjustment_data.gpkg
        -bathy-aibased /data/inputs/bathymetry/ml_outputs_v1.01.parquet
        -buffer 5000 -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg
    """
    parser = ArgumentParser(description="Bathymetric Adjustment")
    parser.add_argument('-huc-dir', '--huc-dir', help='HUC Directory', required=True, type=str)
    parser.add_argument(
        '-sor',
        '--strm-order',
        help="stream order on or higher for which AI-based bathymetry data is applied",
        default=4,
        required=False,
        type=int,
    )
    parser.add_argument(
        '-bathy-ehydro',
        '--bathy-file-ehydro',
        help="Path to geopackage with preprocessed eHydro bathymetic data",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-bathy-aibased',
        '--bathy-file-aibased',
        help="Path to parquet file with preprocessed AI-based bathymetic data",
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
        '-ait',
        '--ai-toggle',
        help='Toggle to apply ai_based bathymetry, ait = 1',
        required=False,
        default=1,
        type=int,
    )

    args = vars(parser.parse_args())
    process_bathy_adjustment(**args)
