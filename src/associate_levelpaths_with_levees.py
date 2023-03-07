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
        levees = gpd.read_file(levees_filename)

        leveed_areas = gpd.read_file(leveed_areas_filename)
        levelpaths = gpd.read_file(levelpaths_filename)

        # Buffer each side of levee line
        levees_buffered_left = levees.copy()
        levees_buffered_right = levees.copy()

        levees_buffered_left.geometry = levees.buffer(levee_buffer, single_sided=True)
        levees_buffered_right.geometry = levees.buffer(levee_buffer, single_sided=True)

        # Intersect leveed areas with single-sided levee buffers
        leveed_left = gpd.overlay(levees_buffered_left, leveed_areas, how='intersection', keep_geom_type=False)
        leveed_right = gpd.overlay(levees_buffered_right, leveed_areas, how='intersection', keep_geom_type=False)

        # Associate levees and leveed areas
        matches_left = np.where(leveed_left['SYSTEM_ID_1']==leveed_left['SYSTEM_ID_2'])[0]
        matches_right = np.where(leveed_right['SYSTEM_ID_1']==leveed_right['SYSTEM_ID_2'])[0]

        leveed_left = leveed_left.loc[matches_left]
        leveed_right = leveed_right.loc[matches_right]

        # Get area of associated leveed areas
        leveed_left['leveed_area'] = leveed_left.area
        leveed_right['leveed_area'] = leveed_right.area

        levee_left = leveed_left[['SYSTEM_ID_1', 'leveed_area', 'geometry']]
        levee_right = leveed_right[['SYSTEM_ID_1', 'leveed_area', 'geometry']]

        # Merge left and right levee protected areas
        leveed = levee_left.merge(levee_right, on='SYSTEM_ID_1', how='outer', suffixes=['_left', '_right'])

        # Convert NaNs to zero
        leveed.loc[np.isnan(leveed['leveed_area_left']), 'leveed_area_left'] = 0
        leveed.loc[np.isnan(leveed['leveed_area_right']), 'leveed_area_right'] = 0

        # Determine which side the levee is protecting (opposite of levee protected area)
        leveed['levee_side'] = np.where(leveed['leveed_area_left'] < leveed['leveed_area_right'], 'left', 'right')

        leveed_right = leveed[leveed['levee_side']=='right']
        leveed_left = leveed[leveed['levee_side']=='left']

        leveed_right = gpd.GeoDataFrame(leveed_right, geometry='geometry_right', crs='epsg:5070')
        leveed_left = gpd.GeoDataFrame(leveed_left, geometry='geometry_left', crs='epsg:5070')

        levee_streams_right = gpd.sjoin(leveed_right, levelpaths)
        levee_streams_left = gpd.sjoin(leveed_left, levelpaths)

        # Remove levelpaths crossing the levee exactly once
        remove_levelpaths = []
        for i, levelpath in levelpaths.iterrows():
            # Get GeoSeries of intersections of row with all rows
            row_intersections = levees.intersection(levelpath['geometry'])
            # Exclude any rows that aren't a MultiPoint geometry
            row_intersection_points = row_intersections[row_intersections.geom_type == 'MultiPoint']
            # Create a DataFrame of the the row intersection points
            row_intersections_df = pd.DataFrame([[k, point.x, point.y] for k, v in row_intersection_points.items() for point in wkb.loads(v.wkb)], columns=['ID', 'X', 'Y'])

            if len(row_intersections_df) == 1:
                remove_levelpaths.append(levelpath['levpa_id'])

        levee_streams_right = levee_streams_right[~levee_streams_right['levpa_id'].isin(remove_levelpaths)]
        levee_streams_left = levee_streams_left[~levee_streams_left['levpa_id'].isin(remove_levelpaths)]        

        out_df =  pd.concat([levee_streams_right, levee_streams_left])[['SYSTEM_ID_1', 'levpa_id']].drop_duplicates().reset_index()

        out_df.to_csv(out_filename, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Associate level paths with levees')
    parser.add_argument('-nld','--levees-filename', help='NLD levees filename', required=True, type=str)
    parser.add_argument('-out','--out-filename', help='out CSV filename', required=True, type=str)
    parser.add_argument('-s', '--levelpaths-filename', help='Level path layer filename', required=True, type=str)
    parser.add_argument('-lpa', '--leveed-areas-filename', help='NLD levee-protected areas filename', required=True, type=str)
    parser.add_argument('-w', '--levee-buffer', help='Buffer width (in meters)', required=False, type=float, default=1000.)

    args = vars(parser.parse_args())

    associate_levelpaths_with_levees(**args)
