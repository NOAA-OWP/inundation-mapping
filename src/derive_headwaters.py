#!/usr/bin/env python3

import argparse

import geopandas as gpd
from shapely.geometry import Point

from utils.shared_functions import getDriver


gpd.options.io_engine = "pyogrio"


def findHeadWaterPoints(flows):
    flows = flows.explode(index_parts=True)
    headwater_points = []
    starting_points = set()
    end_points = set()
    for i, g in enumerate(flows.geometry):
        g_points = [(x, y) for x, y in zip(*g.coords.xy)]

        starting_point = g_points[0]
        end_point = g_points[-1]

        starting_points.add(starting_point)
        end_points.add(end_point)

        # line_points = np.append(line_points,g_points)

    for i, sp in enumerate(starting_points):
        # print(sp)
        if sp not in end_points:
            headwater_points += [sp]

    # print(headwater_points)
    headwater_points_geometries = [Point(*hwp) for hwp in headwater_points]
    hw_gdf = gpd.GeoDataFrame({'geometry': headwater_points_geometries}, crs=flows.crs, geometry='geometry')

    return hw_gdf


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Derive headwater points from flowlines. Linestrings must flow downstream'
    )
    parser.add_argument(
        '-f',
        '--input-flows',
        help='Input flowlines. Linestrings must flow downstream',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-l', '--input-flows-layer', help='Input layer name', required=False, type=str, default=None
    )
    parser.add_argument(
        '-o', '--output-headwaters', help='Output headwaters points', required=False, type=str, default=None
    )

    args = vars(parser.parse_args())

    flows = gpd.read_file(args['input_flows'], layer=args['input_flows_layer'])

    hw_gdf = findHeadWaterPoints(flows)

    output_headwaters = args['output_headwaters']

    if output_headwaters is not None:
        hw_gdf.to_file(args['output_headwaters'], driver=getDriver(args['output_headwaters']), engine='fiona')
