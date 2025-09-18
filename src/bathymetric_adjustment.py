#!/usr/bin/env python3

import datetime as dt
import os
import re
import sys
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import geopandas as gpd
import pandas as pd


# -------------------------------------------------------
# Function to use RFC Bathymetry where available over eHydro Data
def ohrfc_bathy_precedence(group):
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
        verbose : bool
            Verbose printing.

        Returns
        ----------
        log_text : str

    """

    log_text = f'Calculating eHydro bathymetry adjustment: {huc}\n'

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
    for src in src_all_branches:
        src_df = pd.read_csv(src, low_memory=False)
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
        # If there's more than one survey for a single feature_id in the bathy data:
        # take the ohrfc value first, then the mean of all availabe ehydro values.
        except pd.errors.MergeError:
            reconciled_bathy_data = (
                bathy_data[
                    ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
                ]
                .groupby('feature_id')
                .apply(ohrfc_bathy_precedence)  # , include_groups=False)
                .reset_index(drop=True)
            )

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

        # Write src back to file
        # src_df = src_df.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)
        src_df.to_csv(src, index=False)

    return log_text


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
        verbose : bool
            Verbose printing.

        Returns
        ----------
        log_text : str

    """
    log_text = f'Calculating AI-based bathymetry adjustment: {huc}\n'
    print(f'Calculating AI-based bathymetry adjustment: {huc}\n')

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
    print(f'Adjusting SRCs with EHydro and OHRFC Bathymetry Data: {huc}\n')

    # Get src_full from each branch
    src_all_branches_path = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches_path.append(src_full)

    # Update src parameters with bathymetric data
    for src in src_all_branches_path:
        src_df = pd.read_csv(src, low_memory=False)
        # print(src_df.loc[~src_df['Bathymetry_source'].isna()]['Bathymetry_source'])

        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'  Branch: {branch}\n'

        # Merge in missing ai bathy data and fill Nans
        if 'missing_xs_area_m2' not in src_df.columns:
            src_df.drop(columns=["Bathymetry_source"], inplace=True)
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
            src_df.to_csv(src, index=False)

        else:
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

            src_df.loc[mask, "missing_xs_area_m2_x"] = src_df.loc[mask, "missing_xs_area_m2_y"]
            src_df.loc[mask, "missing_wet_perimeter_m_x"] = src_df.loc[mask, "missing_wet_perimeter_m_y"]
            src_df.loc[mask, "Bathymetry_source_x"] = src_df.loc[mask, "Bathymetry_source_y"]

            src_df.drop(
                columns=["missing_xs_area_m2_y", "missing_wet_perimeter_m_y", "Bathymetry_source_y"],
                inplace=True,
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
            src_df.to_csv(src, index=False)

    return log_text


# --------------------------------------------------------
# Apply src_adjustment_for_bathymetry
def apply_src_adjustment_for_bathymetry(
    huc_dir, huc, strm_order, bathy_file_ehydro, bathy_file_aibased, ai_toggle, verbose, log_file_path
):
    """
    Function for applying both eHydro & AI-based bathymetry adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_rating_for_ehydro_bathymetry and
        correct_rating_for_ai_based_bathymetry functions parameters.

        Returns
        ----------
        log_text : str
    """
    log_text = ""
    try:
        if os.path.exists(bathy_file_ehydro):
            msg = f"Correcting rating curve for ehydro bathy for huc : {huc}"
            log_text += msg + '\n'
            print(msg)
            log_text += correct_rating_for_ehydro_bathymetry(huc_dir, huc, bathy_file_ehydro)
        else:
            print(f'Bathymetry file does not exist for huc: {huc}')

    except Exception:
        log_text += f"An error has occurred while processing ehydro bathy for huc {huc}"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}")

    if ai_toggle == 1:
        try:
            if os.path.exists(bathy_file_aibased):
                msg = f"Correcting rating curve for AI-based bathy for huc : {huc}"
                log_text += msg + '\n'
                print(msg + '\n')

                log_text += correct_rating_for_ai_bathymetry(
                    huc_dir, huc, strm_order, bathy_file_aibased
                )  # , ai_toggle
            else:
                print(f'AI-based bathymetry file does not exist for huc : {huc}')

        except Exception:
            log_text += f"An error has occurred while processing AI-based bathy for huc {huc}"
            log_text += traceback.format_exc()

    with open(log_file_path, "a") as log_file:
        log_file.write(log_text + '\n')


# -------------------------------------------------------
def process_bathy_adjustment(
    huc_dir, strm_order, bathy_file_ehydro, bathy_file_aibased, output_suffix, ai_toggle, verbose
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
        verbose : bool
            Verbose printing.

    """
    # Set up log file
    log_dir = os.path.join(huc_dir, "logs", "src_optimization")
    print("Log file output here: " + str(log_dir))
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, 'bathymetric_adjustment' + output_suffix + '.log')

    print(f'Writing progress to log file here: {log_file_path}')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""
    # Exit program if the bathymetric data doesn't exist
    if ai_toggle == 1:
        if not all([os.path.exists(bathy_file_ehydro), os.path.exists(bathy_file_aibased)]):
            statement = f'The input bathymetry files {bathy_file_ehydro} and {bathy_file_aibased} do not exist. Exiting...'
            with open(log_file_path, "w") as log_file:
                log_file.write(statement)
            print(statement)
            sys.exit(0)
    else:
        if not os.path.exists(bathy_file_ehydro):
            statement = f'The input bathymetry file {bathy_file_ehydro} does not exist. Exiting...'
            with open(log_file_path, "w") as log_file:
                log_file.write(statement)
            print(statement)
            sys.exit(0)

    if ai_toggle == 1:
        msg = f"AI-Based bathymetry data is applied on streams with order {strm_order} or higher\n"
        log_text += msg
        print(msg)

    # get hucnumber
    huc = os.path.basename(os.path.normpath(huc_dir))

    apply_src_adjustment_for_bathymetry(
        huc_dir, huc, strm_order, bathy_file_ehydro, bathy_file_aibased, ai_toggle, verbose, log_file_path
    )

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
    python3 /foss_fim/src/bathymetric_adjustment.py -fim_dir /outputs/fim_run_dir
        -bathy_eHydro /data/inputs/bathymetry/bathymetric_adjustment_data.gpkg
        -bathy_aibased /data/inputs/bathymetry/ml_outputs_v1.01.parquet
        -buffer 5000 -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg
    """
    parser = ArgumentParser(description="Bathymetric Adjustment")
    parser.add_argument('-huc_dir', '--huc_dir', help='HUC Directory', required=True, type=str)
    parser.add_argument(
        '-sor',
        '--strm_order',
        help="stream order on or higher for which AI-based bathymetry data is applied",
        default=4,
        required=False,
        type=int,
    )
    parser.add_argument(
        '-bathy_ehydro',
        '--bathy_file_ehydro',
        help="Path to geopackage with preprocessed eHydro bathymetic data",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-bathy_aibased',
        '--bathy_file_aibased',
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
        '--ai_toggle',
        help='Toggle to apply ai_based bathymetry, ait = 1',
        required=False,
        default=0,
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

    huc_dir = args['huc_dir']
    strm_order = args['strm_order']
    bathy_file_ehydro = args['bathy_file_ehydro']
    bathy_file_aibased = args['bathy_file_aibased']
    output_suffix = args['output_suffix']
    ai_toggle = args['ai_toggle']
    verbose = bool(args['verbose'])

    process_bathy_adjustment(
        huc_dir, strm_order, bathy_file_ehydro, bathy_file_aibased, output_suffix, ai_toggle, verbose
    )
