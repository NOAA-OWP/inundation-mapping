#!/usr/bin/env python3

import argparse
import os
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


def associate_levelpaths_with_levees(
    levees_filename: str,
    levee_id_attribute: str,
    leveed_areas_filename: str,
    levelpaths_filename: str,
    branch_id_attribute: str,
    levee_buffer: float,
    out_filename: str,
):
    """
    Finds the levelpath(s) associated with each levee. Ignores levelpaths that cross a levee exactly once.

    Parameters
    ----------
    levees_filename: str
        Path to levees file.
    levee_id_attribute: str
        Name of levee ID attribute.
    leveed_areas_filename: str
        Path to levee-protected areas file.
    levelpaths_filename: str
        Path to level paths file.
    branch_id_attribute: str
        Name of branch ID attribute.
    levee_buffer: float
        Distance to buffer from levee.
    out_filename: str
        Path to write output CSV file.
    """

    if (
        os.path.exists(levees_filename)
        and os.path.exists(leveed_areas_filename)
        and os.path.exists(levelpaths_filename)
    ):
        # Read in geodataframes
        levees = gpd.read_file(levees_filename)
        leveed_areas = gpd.read_file(leveed_areas_filename)
        levelpaths = gpd.read_file(levelpaths_filename)

        levees[levee_id_attribute] = levees[levee_id_attribute].astype(int)
        leveed_areas[levee_id_attribute] = leveed_areas[levee_id_attribute].astype(int)
        levelpaths[branch_id_attribute] = levelpaths[branch_id_attribute].astype(int)

        # Buffer each side of levee line
        levees_buffered_left = levees.copy()
        levees_buffered_right = levees.copy()
        levees_buffered_left.geometry = levees.buffer(levee_buffer, single_sided=True)
        levees_buffered_right.geometry = levees.buffer(-levee_buffer, single_sided=True)

        # Intersect leveed areas with single-sided levee buffers
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            leveed_left = gpd.overlay(levees_buffered_left, leveed_areas, how='intersection')
            leveed_right = gpd.overlay(levees_buffered_right, leveed_areas, how='intersection')

        # Find leveed areas not intersected by either buffer
        leveed_intersected = []
        if not leveed_left.empty:
            [leveed_intersected.append(x) for x in leveed_left[f'{levee_id_attribute}_1'].values]

            # Associate levees and leveed areas
            matches_left = np.where(
                leveed_left[f'{levee_id_attribute}_1'] == leveed_left[f'{levee_id_attribute}_2']
            )[0]

            leveed_left = leveed_left.loc[matches_left]

            # Get area of associated leveed areas
            leveed_left['leveed_area'] = leveed_left.area

            leveed_left = leveed_left[[f'{levee_id_attribute}_1', 'leveed_area', 'geometry']]

        if not leveed_right.empty:
            [leveed_intersected.append(x) for x in leveed_right[f'{levee_id_attribute}_1'].values]

            # Associate levees and leveed areas
            matches_right = np.where(
                leveed_right[f'{levee_id_attribute}_1'] == leveed_right[f'{levee_id_attribute}_2']
            )[0]

            leveed_right = leveed_right.loc[matches_right]

            # Get area of associated leveed areas
            leveed_right['leveed_area'] = leveed_right.area

            leveed_right = leveed_right[[f'{levee_id_attribute}_1', 'leveed_area', 'geometry']]

        if len(leveed_intersected) > 0:
            levees_not_found = leveed_areas[~leveed_areas[levee_id_attribute].isin(leveed_intersected)]

        # Merge left and right levee protected areas
        if leveed_left.empty and leveed_right.empty:
            return

        elif not leveed_left.empty and not leveed_right.empty:
            leveed = leveed_left.merge(
                leveed_right, on=f'{levee_id_attribute}_1', how='outer', suffixes=['_left', '_right']
            )

            # Set unmatched areas to zero
            leveed.loc[np.isnan(leveed['leveed_area_left']), 'leveed_area_left'] = 0.0
            leveed.loc[np.isnan(leveed['leveed_area_right']), 'leveed_area_right'] = 0.0

        elif leveed_left.empty:
            leveed = leveed_right.rename(columns={'leveed_area': 'leveed_area_right'})
            leveed['leveed_area_left'] = 0.0

        elif leveed_right.empty:
            leveed = leveed_left.rename(columns={'leveed_area': 'leveed_area_left'})
            leveed['leveed_area_right'] = 0.0

        # Determine which side the levee is protecting (opposite of levee protected area)
        leveed['levee_side'] = np.where(
            leveed['leveed_area_left'] < leveed['leveed_area_right'], 'left', 'right'
        )

        # Split into sides
        left_ids = leveed.loc[leveed['levee_side'] == 'left', f'{levee_id_attribute}_1']
        right_ids = leveed.loc[leveed['levee_side'] == 'right', f'{levee_id_attribute}_1']

        # Associate level paths with levee buffers
        levee_levelpaths_left = gpd.sjoin(levees_buffered_left, levelpaths)
        levee_levelpaths_right = gpd.sjoin(levees_buffered_right, levelpaths)

        levee_levelpaths_left = levee_levelpaths_left[[levee_id_attribute, branch_id_attribute]]
        levee_levelpaths_right = levee_levelpaths_right[[levee_id_attribute, branch_id_attribute]]

        # Select streams on the correct side of levee
        levee_levelpaths_left = levee_levelpaths_left[
            levee_levelpaths_left[levee_id_attribute].isin(left_ids)
        ]
        levee_levelpaths_right = levee_levelpaths_right[
            levee_levelpaths_right[levee_id_attribute].isin(right_ids)
        ]

        # Join left and right
        out_df = (
            pd.concat(
                [
                    levee_levelpaths_right[[levee_id_attribute, branch_id_attribute]],
                    levee_levelpaths_left[[levee_id_attribute, branch_id_attribute]],
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Add level paths to levees not found
        if len(levees_not_found) > 0:
            levees_not_found.geometry = levees_not_found.buffer(2 * levee_buffer)
            levees_not_found = gpd.sjoin(levees_not_found, levelpaths)

            # Add to out_df
            out_df = (
                pd.concat(
                    [
                        out_df[[levee_id_attribute, branch_id_attribute]],
                        levees_not_found[[levee_id_attribute, branch_id_attribute]],
                    ]
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )

        for j, row in out_df.iterrows():
            # Intersect levees and levelpaths
            row_intersections = gpd.overlay(
                levees[levees[levee_id_attribute] == row[levee_id_attribute]],
                levelpaths[levelpaths[branch_id_attribute] == row[branch_id_attribute]],
                how='intersection',
                keep_geom_type=False,
            )

            # Convert MultiPoint to Point
            row_intersections = row_intersections.explode(index_parts=True)

            # Select Point geometry type
            row_intersections = row_intersections[row_intersections.geom_type == 'Point']

            # Remove levelpaths that cross the levee exactly once
            if len(row_intersections) == 1:
                out_df = out_df.drop(j)

            # Find associated levelpaths that don't intersect levees
            elif row_intersections.empty:
                # Get levelpaths that intersect leveed areas
                leveed_area_levelpaths = gpd.overlay(
                    levelpaths[levelpaths[branch_id_attribute] == row[branch_id_attribute]],
                    leveed_areas[leveed_areas[levee_id_attribute] == row[levee_id_attribute]],
                    how='intersection',
                    keep_geom_type=False,
                )

                if not leveed_area_levelpaths.empty:
                    out_df = out_df.drop(j)

        out_df.to_csv(out_filename, columns=[levee_id_attribute, branch_id_attribute], index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Associate level paths with levees')
    parser.add_argument('-nld', '--levees-filename', help='NLD levees filename', required=True, type=str)
    parser.add_argument('-l', '--levee-id-attribute', help='Levee ID attribute name', required=True, type=str)
    parser.add_argument('-out', '--out-filename', help='out CSV filename', required=True, type=str)
    parser.add_argument(
        '-s', '--levelpaths-filename', help='Level path layer filename', required=True, type=str
    )
    parser.add_argument(
        '-b', '--branch-id-attribute', help='Level path ID attribute name', required=True, type=str
    )
    parser.add_argument(
        '-lpa', '--leveed-areas-filename', help='NLD levee-protected areas filename', required=True, type=str
    )
    parser.add_argument('-w', '--levee-buffer', help='Buffer width (in meters)', required=True, type=float)

    args = vars(parser.parse_args())

    associate_levelpaths_with_levees(**args)
