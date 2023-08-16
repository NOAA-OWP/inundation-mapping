#!/usr/bin/env python3

import argparse
import json

import geopandas as gpd
import pandas as pd
from numpy import unique

from utils.shared_functions import getDriver


def finalize_srcs(srcbase, srcfull, hydrotable, output_srcfull=None, output_hydrotable=None):
    # calculate src_full
    srcbase = pd.read_csv(srcbase, dtype={'CatchId': int})
    srcbase.rename(columns={'CatchId': 'HydroID'}, inplace=True)
    srcbase = srcbase.rename(columns=lambda x: x.strip(" "))

    # read and merge in attributes from base hydrofabric src full
    srcfull = pd.read_csv(srcfull, dtype={'CatchId': int})
    srcfull.rename(columns={'CatchId': 'HydroID'}, inplace=True)
    srcfull = srcfull.loc[:, ["ManningN", "HydroID", "feature_id"]].drop_duplicates(
        subset='HydroID'
    )

    srcbase = srcbase.merge(srcfull, how='inner', left_on='HydroID', right_on='HydroID')

    srcbase = srcbase.apply(pd.to_numeric, **{'errors': 'coerce'})
    srcbase['TopWidth (m)'] = srcbase['SurfaceArea (m2)'] / srcbase['LENGTHKM'] / 1000
    srcbase['WettedPerimeter (m)'] = srcbase['BedArea (m2)'] / srcbase['LENGTHKM'] / 1000
    srcbase['WetArea (m2)'] = srcbase['Volume (m3)'] / srcbase['LENGTHKM'] / 1000
    srcbase['HydraulicRadius (m)'] = srcbase['WetArea (m2)'] / srcbase['WettedPerimeter (m)']
    srcbase['HydraulicRadius (m)'].fillna(0, inplace=True)
    srcbase['Discharge (m3s-1)'] = (
        srcbase['WetArea (m2)']
        * pow(srcbase['HydraulicRadius (m)'], 2.0 / 3)
        * pow(srcbase['SLOPE'], 0.5)
        / srcbase['ManningN']
    )

    # set nans to 0
    srcbase.loc[srcbase['Stage'] == 0, ['Discharge (m3s-1)']] = 0

    if output_srcfull is not None:
        srcbase.to_csv(output_srcfull, index=False)

    hydrotable = pd.read_csv(hydrotable)
    hydrotable.drop(columns=['stage', 'discharge_cms'], inplace=True)

    hydrotable.drop_duplicates(subset='HydroID', inplace=True)
    # srcfull = srcfull.loc[:,["ManningN","HydroID","feature_id"]].drop_duplicates(subset='HydroID')
    hydrotable = hydrotable.merge(
        srcbase.loc[:, ['HydroID', 'Stage', 'Discharge (m3s-1)']],
        how='right',
        left_on='HydroID',
        right_on='HydroID',
    )
    hydrotable.rename(
        columns={'Stage': 'stage', 'Discharge (m3s-1)': 'discharge_cms'}, inplace=True
    )
    # hydrotable.drop_duplicates(subset='stage',inplace=True)

    if output_hydrotable is not None:
        hydrotable.to_csv(output_hydrotable, index=False)

    return (srcbase, hydrotable)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-b', '--srcbase', help='Base synthetic rating curve table', required=True)
    parser.add_argument('-f', '--srcfull', help='Base synthetic rating curve table', required=True)
    parser.add_argument('-w', '--hydrotable', help='Input Hydro-Table', required=False)
    parser.add_argument(
        '-r',
        '--output-srcfull',
        help='Output crosswalked synthetic rating curve table',
        required=False,
        default=None,
    )
    parser.add_argument(
        '-t', '--output-hydrotable', help='Hydrotable', required=False, default=None
    )

    args = vars(parser.parse_args())

    finalize_srcs(**args)
