# This routine is used to conflate the DEM-derived reaches to the
# National Water Model (NWM) streams (feature_id)

import argparse
import datetime
import multiprocessing as mp
import os
import time
from functools import partial
from multiprocessing import Pool
from time import sleep

import geopandas as gpd
import numpy as np
import pandas as pd
import tqdm
from geopandas.tools import sjoin
from shapely import wkt
from shapely.geometry import Point

# import errors
import utils.shared_variables as sv


# -------------------------------------------------
def fn_wkt_loads(x):
    try:
        return wkt.loads(x)
    except Exception:
        return None


# -------------------------------------------------
def fn_snap_point(shply_line, list_of_df_row):
    # int_index, int_feature_id, str_huc12, shp_point = list_of_df_row
    int_index, shp_point, int_feature_id = list_of_df_row

    point_project_wkt = shply_line.interpolate(shply_line.project(shp_point)).wkt

    list_col_names = ["feature_id", "geometry_wkt"]
    df = pd.DataFrame([[int_feature_id, point_project_wkt]], columns=list_col_names)

    sleep(0.03)  # this allows the tqdm progress bar to update

    return df


# -------------------------------------------------
def fn_create_gdf_of_points(tpl_request):
    # function to create and return a geoDataframe from a list of shapely points

    str_feature_id = tpl_request[0]
    list_of_points = tpl_request[1]

    # Create an empty dataframe
    df_points_nwm = pd.DataFrame(list_of_points, columns=["geometry"])

    # convert dataframe to geodataframe
    gdf_points_nwm = gpd.GeoDataFrame(df_points_nwm, geometry="geometry")

    gdf_points_nwm["feature_id"] = str_feature_id

    return gdf_points_nwm


# -------------------------------------------------
def fn_conflate_demDerived_to_nwm(huc8, demDerived_reaches_path, nwm_streams_path, str_gpkg_out_arg):
    # supress all warnings
    # warnings.filterwarnings("ignore", category=UserWarning)

    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # INPUT

    flt_start_conflate_demDerived_to_nwm = time.time()

    print(" ")
    print("+=========================================================================+")
    print("|        CONFLATE DEM-DERIVED REACHES TO NATIONAL WATER MODEL STREAMS         |")
    print("+-------------------------------------------------------------------------+")

    print("  ---(u) HUC-8: " + huc8)

    print("  ---(d) DEM-DERIVED REACHES INPUT GPKG: " + demDerived_reaches_path)

    print("  ---(n) NWM STREAMS INPUT GPKG: " + nwm_streams_path)

    STR_OUT_PATH = str_gpkg_out_arg
    print("  ---(o) OUTPUT DIRECTORY: " + STR_OUT_PATH)

    if not os.path.exists(STR_OUT_PATH):
        os.makedirs(STR_OUT_PATH)

    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # distance to buffer around modeled stream centerlines
    int_buffer_dist = 600
    # ~~~~~~~~~~~~~~~~~~~~~~~~

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # TODO - 2021.09.21 - this should be 50 if meters and 150 if feet
    # too small a value creates long buffering times
    int_distance_delta = 50  # distance between points in hec-ras projection units
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # Input - projection of the base level engineering models
    # get this string from the input shapefiles of the stream
    nwm_streams = gpd.read_file(nwm_streams_path)
    nwm_prj = str(nwm_streams.crs)

    # Note that this routine requires three (3) datasets.
    # (1) the NHD Watershed Boundary dataset
    # (2) the National water model flowlines geopackage
    # (3) the DEM-derived flows

    # demDerived_reaches = gpd.read_file(demDerived_reaches_path)

    # Geospatial projections
    # wgs = "epsg:4326" - not needed
    # lambert = "epsg:3857" - not needed
    # nwm_prj = "ESRI:102039"
    # nwm_prj = "epsg:5070"
    # ~~~~~~~~~~~~~~~~~~~~~~~~

    # ````````````````````````
    # option to turn off the SettingWithCopyWarning
    # pd.set_option("mode.chained_assignment", None)
    # ````````````````````````

    # Load the geopackage into geodataframe
    print("+-----------------------------------------------------------------+")
    print("Loading NWM streams")

    # Get the NWM stream centerlines from the provided geopackage

    # rename ID to feature_id
    nwm_streams = nwm_streams.rename(columns={"ID": "feature_id"})

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Create points at desired interval along each
    # national water model stream
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # Multi-Linestrings to Linestrings
    nwm_streams_nwm_explode = nwm_streams.explode(index_parts=True)

    # TODO - 2021.08.03 - Quicker to buffer the dem_streams first
    # and get the nwm streams that are inside or touch the buffer?

    list_points_aggregate = []
    print("+-----------------------------------------------------------------+")

    for index, row in nwm_streams_nwm_explode.iterrows():
        str_current_linestring = row["geometry"]
        distances = np.arange(0, str_current_linestring.length, int_distance_delta)
        inter_distances = [str_current_linestring.interpolate(distance) for distance in distances]
        boundary_point = Point(
            str_current_linestring.boundary.bounds[0], str_current_linestring.boundary.bounds[1]
        )
        inter_distances.append(boundary_point)

        tpl_request = (row["feature_id"], inter_distances)
        list_points_aggregate.append(tpl_request)

    # create a pool of processors
    num_processors = mp.cpu_count() - 2
    pool = Pool(processes=num_processors)

    len_points_agg = len(list_points_aggregate)

    list_gdf_points_all_lines = list(
        tqdm.tqdm(
            pool.imap(fn_create_gdf_of_points, list_points_aggregate),
            total=len_points_agg,
            desc="Points on lines",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
            ncols=67,
        )
    )

    pool.close()
    pool.join()

    gdf_points_nwm = gpd.GeoDataFrame(pd.concat(list_gdf_points_all_lines, ignore_index=True))
    gdf_points_nwm = gdf_points_nwm.set_crs(nwm_prj)

    # path of the shapefile to write
    str_filepath_nwm_points = os.path.join(STR_OUT_PATH, f"{huc8}_nwm_points_PT.gpkg")

    # write the shapefile
    gdf_points_nwm.to_file(str_filepath_nwm_points)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # read in the model stream shapefile
    gdf_segments = gpd.read_file(demDerived_reaches_path)

    # Simplify geom by 4.5 tolerance and rewrite the
    # geom to eliminate streams with too many verticies

    flt_tolerance = 4.5  # tolerance for simplification of DEM-DERIVED REACHES stream centerlines

    for index, row in gdf_segments.iterrows():
        shp_geom = row["geometry"]
        shp_simplified_line = shp_geom.simplify(flt_tolerance, preserve_topology=False)
        gdf_segments.at[index, "geometry"] = shp_simplified_line

    # create merged geometry of all streams
    shply_line = gdf_segments.geometry.unary_union

    # read in the national water model points
    gdf_points = gdf_points_nwm

    # reproject the points
    gdf_points = gdf_points.to_crs(gdf_segments.crs)

    print("+-----------------------------------------------------------------+")
    print("Buffering stream centerlines")
    # buffer the merged stream ceterlines - distance to find valid conflation point
    shp_buff = shply_line.buffer(int_buffer_dist)

    # convert shapely to geoDataFrame
    gdf_buff = gpd.GeoDataFrame(geometry=[shp_buff])

    # set the CRS of buff
    gdf_buff = gdf_buff.set_crs(gdf_segments.crs)

    # spatial join - points in polygon
    gdf_points_in_poly = sjoin(gdf_points, gdf_buff, how="left")

    # drop all points that are not within polygon
    gdf_points_within_buffer = gdf_points_in_poly.dropna()

    # need to reindex the returned geoDataFrame
    gdf_points_within_buffer = gdf_points_within_buffer.reset_index()

    # delete the index_right field
    del gdf_points_within_buffer["index_right"]

    total_points = len(gdf_points_within_buffer)

    df_points_within_buffer = pd.DataFrame(gdf_points_within_buffer)
    # TODO - 2021.09.21 - create a new df that has only the variables needed in the desired order
    list_dataframe_args_snap = df_points_within_buffer.values.tolist()

    print("+-----------------------------------------------------------------+")
    p = mp.Pool(processes=(mp.cpu_count() - 2))

    list_df_points_projected = list(
        tqdm.tqdm(
            p.imap(partial(fn_snap_point, shply_line), list_dataframe_args_snap),
            total=total_points,
            desc="Snap Points",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
            ncols=67,
        )
    )

    p.close()
    p.join()

    gdf_points_snap_to_dem = gpd.GeoDataFrame(pd.concat(list_df_points_projected, ignore_index=True))

    gdf_points_snap_to_dem["geometry"] = gdf_points_snap_to_dem.geometry_wkt.apply(fn_wkt_loads)
    gdf_points_snap_to_dem = gdf_points_snap_to_dem.dropna(subset=["geometry"])
    gdf_points_snap_to_dem = gdf_points_snap_to_dem.set_crs(gdf_segments.crs)

    # write the shapefile
    str_filepath_dem_points = os.path.join(STR_OUT_PATH, f"{huc8}_dem_snap_points_PT.gpkg")

    gdf_points_snap_to_dem.to_file(str_filepath_dem_points)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # Buffer the Base Level Engineering streams 0.1 feet (line to polygon)

    gdf_segments_buffer = gdf_segments
    gdf_segments["geometry"] = gdf_segments_buffer.geometry.buffer(0.1)

    # Spatial join of the points and buffered stream

    gdf_dem_points_feature_id = gpd.sjoin(
        gdf_points_snap_to_dem, gdf_segments_buffer[["geometry"]], how="left", op="intersects"
    )

    # delete the wkt_geom field
    del gdf_dem_points_feature_id["index_right"]

    # Intialize the variable
    gdf_dem_points_feature_id["count"] = 1

    df_dem_guess = pd.pivot_table(
        gdf_dem_points_feature_id, index=["feature_id"], values=["count"], aggfunc=np.sum
    )

    df_test = df_dem_guess.sort_values("count")

    str_csv_file = os.path.join(STR_OUT_PATH, f"{huc8}_interim_list_of_streams.csv")

    # Write out the table - read back in
    # this is to white wash the data type
    df_test.to_csv(str_csv_file)
    df_test = pd.read_csv(str_csv_file)

    # Remove the duplicates and determine the feature_id with the highest count
    df_test = df_test.drop_duplicates(subset="feature_id", keep="last")

    # Left join the nwm shapefile and the
    # feature_id/ras_path dataframe on the feature_id

    # When we merge df_test into nwm_streams_nwm_demprj (which does not have a ras_path colum)
    # the new gdf_nwm_stream_raspath does have the column of ras_path but it is all NaN
    # So, we flipped it for nwm_streams_nwm_demprj into df_test and it fixed it
    df_nwm_stream_raspath = df_test.merge(nwm_streams, on="feature_id", how="left")

    # We need to convert it back to a geodataframe for next steps (and exporting)
    gdf_nwm_stream_raspath = gpd.GeoDataFrame(df_nwm_stream_raspath)

    # path of the shapefile to write
    str_filepath_nwm_stream = os.path.join(STR_OUT_PATH, f"{huc8}_nwm_streams_ln.gpkg")

    # write the shapefile
    gdf_nwm_stream_raspath.to_file(str_filepath_nwm_stream)

    print()
    print("COMPLETE")

    flt_end_create_shapes_from_demDerived = time.time()
    flt_time_pass_conflate_demDerived_to_nwm = (
        flt_end_create_shapes_from_demDerived - flt_start_conflate_demDerived_to_nwm
    ) // 1
    time_pass_conflate_demDerived_to_nwm = datetime.timedelta(
        seconds=flt_time_pass_conflate_demDerived_to_nwm
    )
    print("Compute Time: " + str(time_pass_conflate_demDerived_to_nwm))

    print("+=================================================================+")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample:
    # python add_crosswalk -u 12040101
    # -d /outputs/dev-4.4.3.0/12040101/branches/0/demDerived_reaches_split_addedAttributes.gpkg
    # -n /outputs/dev-4.4.3.0/12040101/nwm_streams_subset.gpkg
    # -w /outputs/dev-4.4.3.0/12040101/wbd.gpkg
    # -o /outputs/temp/dev-conflate-carter

    parser = argparse.ArgumentParser(
        description="===== CONFLATE DEM-DERIVED REACHES TO NATIONAL WATER MODEL STREAMS ====="
    )

    parser.add_argument(
        "-u",
        dest="huc8",
        help="REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-d",
        dest="demDerived_reaches_path",
        help=r"REQUIRED: Path to demDerived reaches:  Example: D:\ras_shapes",
        required=True,
        metavar="DIR",
        type=str,
    )
    parser.add_argument(
        "-n",
        dest="nwm_streams_path",
        help=r"REQUIRED: Path to NWM streams:  Example: D:\ras_shapes",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="str_gpkg_out_arg",
        help=r"REQUIRED: path to folder to write output files: Example: D:\conflation_output",
        required=True,
        metavar="DIR",
        type=str,
    )

    args = vars(parser.parse_args())

    fn_conflate_demDerived_to_nwm(**args)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
