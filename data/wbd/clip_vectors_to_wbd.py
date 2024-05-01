#!/usr/bin/env python3

import argparse
import sys

import geopandas as gpd
import pandas as pd
import rasterio as rio
from shapely.geometry import MultiPolygon, Polygon

from utils.shared_functions import getDriver


# from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS


def subset_vector_layers(
    subset_nwm_lakes,
    subset_nwm_streams,
    hucCode,
    subset_nwm_headwaters,
    wbd_buffer_filename,
    wbd_streams_buffer_filename,
    wbd_filename,
    dem_filename,
    dem_domain,
    nwm_lakes,
    nwm_catchments,
    subset_nwm_catchments,
    nld_lines,
    nld_lines_preprocessed,
    landsea,
    nwm_streams,
    subset_landsea,
    nwm_headwaters,
    subset_nld_lines,
    subset_nld_lines_preprocessed,
    wbd_buffer_distance,
    levee_protected_areas,
    subset_levee_protected_areas,
    osm_bridges,
    subset_osm_bridges,
    huc_CRS,
):
    print(f"Getting Cell Size for {hucCode}", flush=True)
    with rio.open(dem_filename) as dem_raster:
        dem_cellsize = max(dem_raster.res)

    wbd = gpd.read_file(wbd_filename, engine="pyogrio", use_arrow=True)
    dem_domain = gpd.read_file(dem_domain, engine="pyogrio", use_arrow=True)

    # Get wbd buffer
    print(f"Create wbd buffer for {hucCode}", flush=True)
    wbd_buffer = wbd.copy()
    wbd_buffer.geometry = wbd_buffer.geometry.buffer(wbd_buffer_distance, resolution=32)
    wbd_buffer = gpd.clip(wbd_buffer, dem_domain)

    # Make the streams buffer smaller than the wbd_buffer so streams don't reach the edge of the DEM
    wbd_streams_buffer = wbd_buffer.copy()
    wbd_streams_buffer.geometry = wbd_streams_buffer.geometry.buffer(-8 * dem_cellsize, resolution=32)

    wbd_buffer = wbd_buffer[['geometry']]
    wbd_streams_buffer = wbd_streams_buffer[['geometry']]
    wbd_buffer.to_file(wbd_buffer_filename, driver=getDriver(wbd_buffer_filename), index=False, crs=huc_CRS)
    wbd_streams_buffer.to_file(
        wbd_streams_buffer_filename, driver=getDriver(wbd_streams_buffer_filename), index=False, crs=huc_CRS
    )

    # Clip ocean water polygon for future masking ocean areas (where applicable)
    landsea = gpd.read_file(landsea, mask=wbd_buffer)
    if not landsea.empty:
        print(f"Create landsea gpkg for {hucCode}", flush=True)
        landsea.to_file(subset_landsea, driver=getDriver(subset_landsea), index=False, crs=huc_CRS)
    del landsea

    # Clip levee-protected areas polygons for future masking ocean areas (where applicable)
    print(f"Subsetting Levee Protected Areas for {hucCode}", flush=True)
    levee_protected_areas = gpd.read_file(levee_protected_areas, mask=wbd_buffer)
    if not levee_protected_areas.empty:
        levee_protected_areas.to_file(
            subset_levee_protected_areas,
            driver=getDriver(subset_levee_protected_areas),
            index=False,
            crs=huc_CRS,
        )
    del levee_protected_areas

    # Find intersecting lakes and writeout
    print(f"Subsetting NWM Lakes for {hucCode}", flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes, mask=wbd_buffer)
    nwm_lakes = nwm_lakes.loc[nwm_lakes.Shape_Area < 18990454000.0]

    if not nwm_lakes.empty:
        # Perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode(index_parts=True)
        nwm_lakes_fill_holes = MultiPolygon(
            Polygon(p.exterior) for p in nwm_lakes['geometry']
        )  # remove donut hole geometries
        # Loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes.geoms)):
            nwm_lakes.loc[i, 'geometry'] = nwm_lakes_fill_holes.geoms[i]
        nwm_lakes.to_file(subset_nwm_lakes, driver=getDriver(subset_nwm_lakes), index=False, crs=huc_CRS)
    del nwm_lakes

    # Find intersecting levee lines
    print(f"Subsetting NLD levee lines for {hucCode}", flush=True)
    nld_lines = gpd.read_file(nld_lines, mask=wbd_buffer)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines, driver=getDriver(subset_nld_lines), index=False, crs=huc_CRS)
    del nld_lines

    # Preprocessed levee lines for burning
    nld_lines_preprocessed = gpd.read_file(nld_lines_preprocessed, mask=wbd_buffer)
    if not nld_lines_preprocessed.empty:
        nld_lines_preprocessed.to_file(
            subset_nld_lines_preprocessed,
            driver=getDriver(subset_nld_lines_preprocessed),
            index=False,
            crs=huc_CRS,
        )
    del nld_lines_preprocessed

    # Subset NWM headwaters
    print(f"Subsetting NWM Headwater Points for {hucCode}", flush=True)
    nwm_headwaters = gpd.read_file(nwm_headwaters, mask=wbd_streams_buffer)

    if len(nwm_headwaters) > 0:
        nwm_headwaters.to_file(
            subset_nwm_headwaters, driver=getDriver(subset_nwm_headwaters), index=False, crs=huc_CRS
        )
    else:
        print("No headwater point(s) within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    del nwm_headwaters

    # Find intersecting nwm_catchments
    print(f"Subsetting NWM Catchments for {hucCode}", flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments, mask=wbd_buffer)

    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(
            subset_nwm_catchments, driver=getDriver(subset_nwm_catchments), index=False, crs=huc_CRS
        )
    else:
        print("No NWM catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    del nwm_catchments

    # Subset OSM (Open Street Map) bridges
    # TODO: Skipping Alaska for now. TBD later.
    # TODO: for now it comes in a 5070, but we may have to check if the mask auto reproject to match.
    if osm_bridges != "":
        print(f"Subsetting OSM Bridges for {hucCode}", flush=True)

        subset_osm_bridges_gdb = gpd.read_file(osm_bridges, mask=wbd_buffer)
        if not subset_osm_bridges_gdb.empty:
            print(f"Create subset of osm bridges gpkg for {hucCode}", flush=True)
            subset_osm_bridges_gdb.to_file(
                subset_osm_bridges, driver=getDriver(subset_osm_bridges_gdb), index=False, crs=huc_CRS
            )
        else:
            print("-- No applicable bridges for this HUC", flush=True)
        del subset_osm_bridges_gdb

    # Subset nwm streams
    print(f"Subsetting NWM Streams for {hucCode}", flush=True)

    nwm_streams = gpd.read_file(nwm_streams, mask=wbd_buffer)

    # NWM can have duplicate records, but appear to always be identical duplicates
    nwm_streams.drop_duplicates(subset="ID", keep="first", inplace=True)

    nwm_streams_outlets = nwm_streams[~nwm_streams['to'].isin(nwm_streams['ID'])]
    nwm_streams_nonoutlets = nwm_streams[nwm_streams['to'].isin(nwm_streams['ID'])]

    if len(nwm_streams) > 0:
        # Address issue where NWM streams exit the HUC boundary and then re-enter, creating a MultiLineString
        nwm_streams_nonoutlets = (
            gpd.clip(nwm_streams_nonoutlets, wbd_streams_buffer).explode(index_parts=True).reset_index()
        )

        # Find and keep the downstream segment of the NWM stream
        max_parts = nwm_streams_nonoutlets[['level_0', 'level_1']].groupby('level_0').max()

        nwm_streams_nonoutlets = nwm_streams_nonoutlets.merge(max_parts, on='level_0', suffixes=('', '_max'))

        nwm_streams_nonoutlets = nwm_streams_nonoutlets[
            nwm_streams_nonoutlets['level_1'] == nwm_streams_nonoutlets['level_1_max']
        ]

        nwm_streams_nonoutlets = nwm_streams_nonoutlets.drop(columns=['level_1_max'])

        nwm_streams = pd.concat([nwm_streams_nonoutlets, nwm_streams_outlets])

        nwm_streams.to_file(
            subset_nwm_streams, driver=getDriver(subset_nwm_streams), index=False, crs=huc_CRS
        )
    else:
        print("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_streams


if __name__ == '__main__':
    # print(sys.argv)

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-a', '--subset-nwm-lakes', help='NWM lake subset', required=True)
    parser.add_argument('-b', '--subset-nwm-streams', help='NWM streams subset', required=True)
    parser.add_argument('-d', '--hucCode', help='HUC boundary ID', required=True, type=str)
    parser.add_argument(
        '-e', '--subset-nwm-headwaters', help='NWM headwaters subset', required=True, default=None
    )
    parser.add_argument('-f', '--wbd_buffer_filename', help='Buffered HUC boundary', required=True)
    parser.add_argument(
        '-s', '--wbd_streams_buffer_filename', help='Buffered HUC boundary (streams)', required=True
    )
    parser.add_argument('-g', '--wbd-filename', help='HUC boundary', required=True)
    parser.add_argument('-i', '--dem-filename', help='DEM filename', required=True)
    parser.add_argument('-j', '--dem-domain', help='DEM domain polygon', required=True)
    parser.add_argument('-l', '--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-m', '--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-n', '--subset-nwm-catchments', help='NWM catchments subset', required=True)
    parser.add_argument('-r', '--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument(
        '-rp', '--nld-lines-preprocessed', help='Levee vectors to use for DEM burning', required=True
    )
    parser.add_argument('-v', '--landsea', help='LandSea - land boundary', required=True)
    parser.add_argument('-w', '--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-x', '--subset-landsea', help='LandSea subset', required=True)
    parser.add_argument('-y', '--nwm-headwaters', help='NWM headwaters', required=True)
    parser.add_argument('-z', '--subset-nld-lines', help='Subset of NLD levee vectors for HUC', required=True)
    parser.add_argument(
        '-zp',
        '--subset-nld-lines-preprocessed',
        help='Subset of NLD levee vectors for burning elevations into DEMs',
        required=True,
    )
    parser.add_argument(
        '-wb', '--wbd-buffer-distance', help='WBD Mask buffer distance', required=True, type=int
    )
    parser.add_argument(
        '-lpf', '--levee-protected-areas', help='Levee-protected areas filename', required=True
    )
    parser.add_argument(
        '-lps', '--subset-levee-protected-areas', help='Levee-protected areas subset', required=True
    )

    parser.add_argument('-osm', '--osm-bridges', help='Open Street Maps gkpg', required=True)

    parser.add_argument('-crs', '--huc-crs', help='HUC crs', required=True)

    args = vars(parser.parse_args())

    subset_vector_layers(**args)
