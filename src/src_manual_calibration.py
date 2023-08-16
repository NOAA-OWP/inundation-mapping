#!/usr/bin/env python3

import argparse
import os

import numpy as np
import pandas as pd


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


def manual_calibration(fim_directory: str, calibration_file: str):
    """
    Perform manual calibration.

    Use a CSV of coefficients to output a new rating curve.
    Coefficient values between 0 and 1 increase the discharge value
    (and decrease inundation) for each stage in the rating curve while
    values greater than 1 decrease the discharge value (and increase
    inundation).

    The original rating curve is saved with a suffix of '_pre-manual'
    before the new rating curve is written.

    Parameters
    ----------
    fim_directory : str
        Path to the parent directory of FIM-required datasets.
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

    # Get list of HUCs
    fim_inputs = pd.read_csv(
        os.path.join(fim_directory, 'fim_inputs.csv'),
        header=None,
        names=['HUC', 'branch_id'],
        dtype={'HUC': str, 'branch_id': int},
    )

    fim_inputs_hucs = fim_inputs['HUC'].unique()

    # Read manual calibration table
    if os.path.exists(calibration_file):
        manual_calib_df = pd.read_csv(
            calibration_file, dtype={'HUC8': str, 'feature_id': int, 'calb_coef_manual': float}
        )

        if manual_calib_df['calb_coef_manual'].min() >= 0:
            # Find HUCs with manual calibration coefficients
            calib_hucs = manual_calib_df['HUC8'].unique()
            hucs = np.intersect1d(fim_inputs_hucs, calib_hucs)

            for huc in hucs:
                print(f'Updating hydrotable for {huc}')

                # Read hydrotable
                htable_file = os.path.join(fim_directory, huc, 'hydrotable.csv')
                htable_file_split = os.path.splitext(htable_file)
                htable_file_original = htable_file_split[0] + '_pre-manual' + htable_file_split[1]

                if not os.path.exists(htable_file_original):
                    # Save a copy of the original hydrotable
                    os.rename(htable_file, htable_file_original)

                df_htable = pd.read_csv(htable_file_original, dtype=htable_dtypes)

                df_htable = df_htable.rename(columns={'discharge_cms': 'postcalb_discharge_cms'})

                df_htable = df_htable.merge(manual_calib_df, how='left', on='feature_id')
                df_htable.drop(columns=['HUC8'], inplace=True)

                # Calculate new discharge_cms with manual calibration coefficient
                df_htable['discharge_cms'] = np.where(
                    df_htable['calb_coef_manual'].isnull(),
                    df_htable['postcalb_discharge_cms'],
                    df_htable['postcalb_discharge_cms'] / df_htable['calb_coef_manual'],
                )

                # Write new hydroTable.csv rating curve (overwrites the previous file)
                df_htable.to_csv(htable_file, index=False)

        else:
            raise ValueError(
                f'Manual calibration coefficients must be greater than 0. Minimum value found: {manual_calib_df["calb_coef_manual"].min()}'
            )

    else:
        raise FileNotFoundError(
            f'No calibration file found at {calibration_file}. Skipping manual calibration.'
        )


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(description=f'Manually calibrate rating curve')
    parser.add_argument(
        '-fim_dir',
        '--fim-directory',
        help='Parent directory of FIM-required datasets.',
        required=True,
    )
    parser.add_argument(
        '-calb_file', '--calibration-file', help='Path to manual calibration file', required=True
    )

    # args = parser.parse_args()
    args = vars(parser.parse_args())
    fim_directory = args['fim_directory']
    calibration_file = args['calibration_file']

    manual_calibration(fim_directory, calibration_file)

    # Usage example:
    # python /foss_fim/src/src_manual_calibration.py -fim_dir /outputs/dev-manual-calibration \
    # -calb_file /data/inputs/rating_curve/manual_calibration_coefficients.csv
