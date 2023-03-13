#!/usr/bin/env python3

import os
import argparse
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkb

def associate_levelpaths_with_levees(levees_filename, leveed_areas_filename, levelpaths_filename, levee_buffer, out_filename):
    """
    Finds the level path associated with each levee
    """

    if os.path.exists(levees_filename):
        # Read in geodataframes
        levees = gpd.read_file(levees_filename)
        leveed_areas = gpd.read_file(leveed_areas_filename)
        levelpaths = gpd.read_file(levelpaths_filename)

        levees['SYSTEM_ID'] = levees['SYSTEM_ID'].astype(int)
        leveed_areas['SYSTEM_ID'] = leveed_areas['SYSTEM_ID'].astype(int)
        levelpaths['levpa_id'] = levelpaths['levpa_id'].astype(int)

        # Buffer each side of levee line
        levees_buffered_left = levees.copy()
        levees_buffered_right = levees.copy()
        levees_buffered_left.geometry = levees.buffer(levee_buffer, single_sided=True)
        levees_buffered_right.geometry = levees.buffer(-levee_buffer, single_sided=True)

        # Intersect leveed areas with single-sided levee buffers
        leveed_left = gpd.overlay(levees_buffered_left, leveed_areas, how='intersection')
        leveed_right = gpd.overlay(levees_buffered_right, leveed_areas, how='intersection')

        # Associate levees and leveed areas
        matches_left = np.where(leveed_left['SYSTEM_ID_1']==leveed_left['SYSTEM_ID_2'])[0]
        matches_right = np.where(leveed_right['SYSTEM_ID_1']==leveed_right['SYSTEM_ID_2'])[0]

        leveed_left = leveed_left.loc[matches_left]
        leveed_right = leveed_right.loc[matches_right]

        # Get area of associated leveed areas
        leveed_left['leveed_area'] = leveed_left.area
        leveed_right['leveed_area'] = leveed_right.area

        leveed_left = leveed_left[['SYSTEM_ID_1', 'leveed_area', 'geometry']]
        leveed_right = leveed_right[['SYSTEM_ID_1', 'leveed_area', 'geometry']]

        # Merge left and right levee protected areas
        leveed = leveed_left.merge(leveed_right, on='SYSTEM_ID_1', how='outer', suffixes=['_left', '_right'])

        # Set unmatched areas to zero
        leveed.loc[np.isnan(leveed['leveed_area_left']), 'leveed_area_left'] = 0
        leveed.loc[np.isnan(leveed['leveed_area_right']), 'leveed_area_right'] = 0

        # Determine which side the levee is protecting (opposite of levee protected area)
        leveed['levee_side'] = np.where(leveed['leveed_area_left'] < leveed['leveed_area_right'], 'left', 'right')

        # Split into sides
        left_ids = leveed.loc[leveed['levee_side']=='left', 'SYSTEM_ID_1']
        right_ids = leveed.loc[leveed['levee_side']=='right', 'SYSTEM_ID_1']

        # Associate level paths with levee buffers
        levee_levelpaths_left = gpd.sjoin(levees_buffered_left, levelpaths)
        levee_levelpaths_right = gpd.sjoin(levees_buffered_right, levelpaths)

        levee_levelpaths_left = levee_levelpaths_left[['SYSTEM_ID', 'levpa_id']]
        levee_levelpaths_right = levee_levelpaths_right[['SYSTEM_ID', 'levpa_id']]

        # Select streams on the correct side of levee
        levee_levelpaths_left = levee_levelpaths_left[levee_levelpaths_left['SYSTEM_ID'].isin(left_ids)]
        levee_levelpaths_right = levee_levelpaths_right[levee_levelpaths_right['SYSTEM_ID'].isin(right_ids)]

        out_df =  pd.concat([levee_levelpaths_right[['SYSTEM_ID', 'levpa_id']], levee_levelpaths_left[['SYSTEM_ID', 'levpa_id']]]).drop_duplicates()

        out_df['SYSTEM_ID'] = out_df['SYSTEM_ID'].astype(int)
        out_df['levpa_id'] = out_df['levpa_id'].astype(int)

        out_df = out_df.reset_index(drop=True)

        # Remove levelpaths that cross the levee exactly once
        for j, row in out_df.iterrows():
            # Intersect levees and levelpaths
            row_intersections = gpd.overlay(levees[levees['SYSTEM_ID'] == row['SYSTEM_ID']], levelpaths[levelpaths['levpa_id'] == row['levpa_id']], how='intersection', keep_geom_type=False)
            row_intersection_points = row_intersections[row_intersections.geom_type.isin(['Point', 'MultiPoint'])]

            if len(row_intersection_points) == 1:
                out_df = out_df.drop(j)
                
        out_df.to_csv(out_filename, columns=['SYSTEM_ID', 'levpa_id'], index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Associate level paths with levees')
    parser.add_argument('-nld','--levees-filename', help='NLD levees filename', required=True, type=str)
    parser.add_argument('-out','--out-filename', help='out CSV filename', required=True, type=str)
    parser.add_argument('-s', '--levelpaths-filename', help='Level path layer filename', required=True, type=str)
    parser.add_argument('-lpa', '--leveed-areas-filename', help='NLD levee-protected areas filename', required=True, type=str)
    parser.add_argument('-w', '--levee-buffer', help='Buffer width (in meters)', required=False, type=float, default=1000.)

    args = vars(parser.parse_args())

    associate_levelpaths_with_levees(**args)
