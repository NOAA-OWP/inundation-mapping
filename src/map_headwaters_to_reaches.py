#!/usr/bin/env python3

import os

import geopandas as gpd


fim_dir = '/outputs/dev-4.4.2.3/18100201'

headwaters = gpd.read_file(os.path.join(fim_dir, 'nwm_headwater_points_subset.gpkg'))
reaches = gpd.read_file(os.path.join(fim_dir, 'branches', '0', 'demDerived_reaches_0.shp'))

# Snap nearest reaches where magnitude is 1 to headwaters
reaches_mag_1 = reaches[reaches['Magnitude'] == 1]
snapped_reaches = gpd.sjoin_nearest(reaches_mag_1, headwaters, how='left', distance_col='distance')
reaches_snapped = reaches.merge(snapped_reaches[['LINKNO', 'ID', 'distance']], on='LINKNO', how='left')


def sum_upstream(row):
    """Recursive function to sum up all upstream reaches."""

    sum = 0
    if row['USLINKNO1'] != -1:
        sum += sum_upstream(row['USLINKNO1'])
    else:
        sum += row['ID']

    if row['USLINKNO2'] != -1:
        sum += sum_upstream(row['USLINKNO2'])
    else:
        sum += row['ID']


# Apply stream algorithm with headwaters IDs
for i, row in reaches_snapped[reaches_snapped['DSLINKNO'] == -1].iterrows():
    sum = sum_upstream(row)
