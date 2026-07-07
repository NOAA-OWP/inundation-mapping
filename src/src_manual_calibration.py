#!/usr/bin/env python3

import argparse
import glob
import os
import shutil

import numpy as np
import pandas as pd

#################################
# TODO: July 4, 2026:  In the event of an exception, the log file will not exist
# and its details as well. 
# This needs a try/except with printing to log and at least a one liner
# saying including the word "exception or error", which can be picked up automatically
# by the rollup to fim_process_huc.sh or process_rerun_calibration_huc.sh
################################

htable_dtypes = {
    'HydroID': int,
    'branch_id': int,
    'feature_id': int,
    'NextDownID': int,
    'order_': int,
    'Number of Cells': int,
    'SurfaceArea (m2)': float,
    'BedArea (m2)': float,
    'TopWidth (m)': float,
    'LENGTHKM': float,
    'AREASQKM': float,
    'WettedPerimeter (m)': float,
    'HydraulicRadius (m)': float,
    'WetArea (m2)': float,
    'Volume (m3)': float,
    'SLOPE': float,
    'ManningN': float,
    'stage': float,
    'default_discharge_cms': float,
    'default_Volume (m3)': float,
    'default_WetArea (m2)': float,
    'default_HydraulicRadius (m)': float,
    'default_ManningN': float,
    'calb_applied': bool,
    'last_updated': str,
    'submitter': str,
    'obs_source': str,
    'precalb_discharge_cms': float,
    'calb_coef_usgs': float,
    'calb_coef_spatial': float,
    'calb_coef_final': float,
    'HUC': str,
    'LakeID': int,
    'subdiv_applied': bool,
    'channel_n': float,
    'overbank_n': float,
    'subdiv_discharge_cms': float,
    'discharge_cms': float,
}


def manual_calibration(huc_dir: str, calibration_file: str):
    """
    Perform manual calibration.

    Use a CSV of coefficients to output a new rating curve.
    Coefficient values between 0 and 1 increase the discharge value
    (and decrease inundation) for each stage in the rating curve while
    values greater than 1 decrease the discharge value (and increase
    inundation).

    The original rating curve is saved with a suffix of '_pre_manual_calib'
    before the new rating curve is written.

    Parameters
    ----------
    huc_dir : str
        Path to a huc output directory.
    calibration_file : str
        Path to the manual calibration file. This file should be a CSV
        with the following columns:
            HUC8: str
                HUC8 code
            feature_id: int
                NWM feature_id
            calb_coef_manual: float
                Manual calibration coefficient for each HUC8 and
                feature_id combination.
    """

    # fim_inputs_hucs = fim_inputs['HUC'].unique()
    hucNumber = os.path.basename(os.path.normpath(huc_dir))

    # Read manual calibration table

    if os.path.exists(calibration_file):
        manual_calib_df = pd.read_csv(
            calibration_file,
            dtype={'HUC8': str, 'feature_id': int, 'calb_coef_manual': float},
            index_col=False,
        )

        if "Unnamed: 0" in manual_calib_df:
            manual_calib_df = manual_calib_df.drop("Unnamed: 0", axis=1)

        if manual_calib_df['calb_coef_manual'].min() >= 0:
            # Find HUCs with manual calibration coefficients
            calib_hucs = manual_calib_df['HUC8'].unique()
            hucs = np.intersect1d(np.array([hucNumber]), calib_hucs)

            for huc in hucs:
                if huc in calib_hucs:
                    print(f'Updating hydrotable for {huc}')

                    huc_branches_dir = os.path.join(huc_dir, 'branches')

                    # Yes... Upper case T
                    ht_branch_files = list(
                        glob.glob(os.path.join(huc_branches_dir, '**/hydroTable_*.csv'), recursive=True)
                    )
                    ht_branch_files.sort()

                    for ht_file in ht_branch_files:

                        # Note: Saving a copy messes with other code which just looks for hydrotable_*.csv
                        pre_calib_ht_file = ht_file.replace("hydroTable_", "htable_pre_manual_calib_")
                        shutil.copyfile(ht_file, pre_calib_ht_file)

                        # htable_file_split = os.path.splitext(ht_file)
                        # htable_file_original = htable_file_split[0] + 'htable_pre_manual_calib' + htable_file_split[1]
                        # # Save a copy of the original hydrotable
                        # os.rename(ht_file, htable_file_original)

                        # Read the branch hydrotable
                        df_htable = pd.read_csv(ht_file, dtype=htable_dtypes, index_col=False)

                        # covers a legacy column, now renamed
                        if "postcalb_discharge_cms" in df_htable:
                            df_htable = df_htable.drop("postcalb_discharge_cms", axis=1)

                        # Needed in case this is run a second time.
                        if "calb_coef_manual" in df_htable:
                            df_htable = df_htable.drop("calb_coef_manual", axis=1)

                        # TODO: Jun 2025: By product of a bug in add_crosswalk.ph
                        if "HydroID Int16" in df_htable:
                            df_htable = df_htable.drop("HydroID Int16", axis=1)

                        # make a backup of the discharge_cms column
                        df_htable['pre_manual_calb_discharge_cms'] = df_htable['discharge_cms']
                        df_htable = df_htable.merge(manual_calib_df, how='left', on='feature_id')
                        df_htable.drop(columns=['HUC8'], inplace=True)

                        # Calculate new discharge_cms with manual calibration coefficient
                        df_htable['discharge_cms'] = np.where(
                            df_htable['calb_coef_manual'].isnull(),
                            df_htable['pre_manual_calb_discharge_cms'],
                            df_htable['pre_manual_calb_discharge_cms'] / df_htable['calb_coef_manual'],
                        )

                        # Write new hydroTable.csv rating curve (overwrites the previous file)
                        df_htable.to_csv(ht_file, index=False)

        else:
            raise ValueError(
                'Manual calibration coefficients must be greater than 0. Minimum value found: '
                f'{manual_calib_df["calb_coef_manual"].min()} for {huc_dir}'
            )

    else:
        raise FileNotFoundError(
            f'No calibration file found at {calibration_file}. Skipping manual calibration for {huc_dir}.'
        )


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(description='Manually calibrate rating curve')
    parser.add_argument(
        '-huc_dir', '--huc_dir', help='Parent directory of FIM-required datasets.', required=True
    )
    parser.add_argument(
        '-calb_file', '--calibration-file', help='Path to manual calibration file', required=True
    )

    # args = parser.parse_args()
    args = vars(parser.parse_args())
    huc_dir = args['huc_dir']
    calibration_file = args['calibration_file']

    manual_calibration(huc_dir, calibration_file)

    # Usage example:
    # python /foss_fim/src/src_manual_calibration.py -huc_dir /outputs/dev-manual-calibration \
    # -calb_file /data/inputs/rating_curve/manual_calibration_coefficients.csv
