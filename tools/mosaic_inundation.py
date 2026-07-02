#!/usr/bin/env python
# coding: utf-8

import argparse
import logging
import os
import traceback
from typing import Optional, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

# import rioxarray as rxr
# import xarray as xr
# from geocube.api.core import make_geocube
from rasterio.features import shapes
from rasterio.merge import merge

# from shapely.geometry import box
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_variables import elev_raster_ndv


gpd.options.io_engine = "pyogrio"


# TODO: Jun 2026: We will likely want to change this as we upgrade logging everywhere (see run_test_case.py)
# The newer system has a bunch of new features, but the biggest is that if we use our python.logging
# it logs everything we might want
# Also see note at mask_mosaic. Technically we do need mask_path of workers value anymore
def Mosaic_inundation(
    map_file: Union[str, pd.DataFrame],
    mosaic_attribute: str,
    mosaic_output: Optional[str] = None,
    nodata: Optional[int] = elev_raster_ndv,
    remove_inputs: Optional[bool] = False,
    verbose: Optional[bool] = True,
    is_mosaic_for_branches: Optional[bool] = False,
    inundation_polygon: Optional[str] = None
) -> str:
    """
    Mosaic inundation extents or depths

    Parameters
    ----------
    map_file : Union[str, pd.DataFrame]
        Either the path or dataframe of the files processed previously in inundation
    mosaic_attribute: str
        Attribute to mosaic the map files
    mosaic_output: Optional[str], default = None
        Name of final mosaiced inundation file
    nodata: Optional[int], default = elev_raster_ndv
        Value to represent nodata
    Number of parallel processes to use
    remove_inputs: Optional[bool], default = False
        Whether to remove intermediate input files
    verbose: Optional[bool], default = True
        Quiet output
    is_mosaic_for_branches: Optional[bool] = False,
        Whether to append branch name after output
    inundation_polygon: Optional[str], default = None
        File path for inundation polygon

    Returns
    -------
    str
        File path of mosaiced output
    """

    msg = f"Starting mosaic for {mosaic_output}"
    if verbose:
        # print(f"Removing inputs ... (in Mosiac_inundation) .. {mosaic_output}")
        logging.info(msg)
    else:
        logging.debug(msg)

    if not os.path.isdir(os.path.dirname(mosaic_output)):
        os.makedirs(os.path.dirname(mosaic_output))

    # check input
    if mosaic_attribute not in ("inundation_rasters", "depths_rasters"):
        raise ValueError("Pass inundation_rasters or depths_raster for mosaic_attribute argument")

    ag_mosaic_output = ""
    try:
        # load file
        print(f"map_file is {map_file}")
        if isinstance(map_file, pd.DataFrame):
            inundation_maps_df = map_file
            del map_file
        elif isinstance(map_file, str):
            inundation_maps_df = pd.read_csv(map_file, dtype=str, cols=["huc8", "branchID"])
        else:
            raise TypeError("Pass Pandas Dataframe or file path string to csv for map_file argument")

        # inundation_maps_df = inundation_maps_df.dropna(axis=0, how="all")
        # remove all records where the three inudation paths are empty
        # TODO: Jun 2026: Rob.. research this. Sometimes all three are nulls. It
        # seems to be ok if I leave this test out, but fails when I put the test in.
        # inundation_maps_df = inundation_maps_df.dropna(
        #     subset=['inundation_rasters', 'depths_rasters', 'inundation_polygons']
        # )
        # if len(inundation_maps_df) == 0:
        #     raise Exception(
        #         f"inundation_maps_df has no values in the three raster/polygon columns for {mosaic_output}"
        #     )

        print(inundation_maps_df.info)

        # unique aggregation units
        unit_attribute_name = 'huc8'
        aggregation_units = inundation_maps_df.loc[:, 'huc8'].unique()

        inundation_maps_df = inundation_maps_df.set_index(unit_attribute_name, drop=True)

        remove_at_end = []
        for ag in aggregation_units:
            inundation_maps_list = inundation_maps_df.loc[ag, mosaic_attribute].tolist()

            if inundation_maps_list.empty:
                raise Exception("inunation map list is empty after applying the mosaic_attribute")

            # logging.debug(f"inundation_maps_list is {inundation_maps_list} for {mosaic_output}")

            # Some processes may have already added the ag value (if it is a huc) to
            # the file name, so don't re-add it.
            # Only add the huc into the name if branches are being processed, as
            # sometimes the mosaic is not for gms branches but maybe mosaic of an
            # fr set with a gms composite map.

            ag_mosaic_output = mosaic_output
            if (is_mosaic_for_branches) and (ag not in mosaic_output):
                ag_mosaic_output = fh.append_id_to_file_name(mosaic_output, ag)  # change it

            # logging.debug("Starting mosaic_by_unit")
            remove_list = mosaic_by_unit(
                inundation_maps_list,
                ag_mosaic_output,
                nodata,
                remove_inputs=remove_inputs,
            )

            if len(remove_list) > 0:
                remove_at_end.extend(remove_list)
                remove_at_end = list(set(remove_at_end))  # Ensures unique values

        if inundation_polygon is not None:
            mosaic_final_inundation_extent_to_poly(ag_mosaic_output, inundation_polygon)

        if remove_inputs:
            # fh.vprint("Removing inputs ...", verbose)

            for remove_file in remove_at_end:
                if os.path.exists(remove_file):
                    # In theory, the file should exist but sometimes does not (errors? Multi-threading?)
                    os.remove(remove_file)

        msg = f"Completed mosaic for {mosaic_output}"
        if verbose:
            # print(f"Removing inputs ... (in Mosiac_inundation) .. {mosaic_output}")
            logging.info(msg)
        else:
            logging.debug(msg)

    except Exception as ex:
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical(f"Critical Error while creating a mosaic for {mosaic_output}")
        logging.critical(traceback.format_exc())
        raise ex
    # Return file name and path of the final mosaic output file.
    # Might be empty.
    return ag_mosaic_output


# Jun 2026: See note in mask_mosaic function which has not worked
# for quite a while. This means both wokers, mask and verbose are not needed
# Number of workers remove as it is no longer relavent
def mosaic_by_unit(
    inundation_maps_list: list,
    mosaic_output: str,
    nodata: Optional[int] = elev_raster_ndv,
    remove_inputs: Optional[bool] = False,
    verbose: Optional[bool] = False,
) -> Union[list, None]:
    """
    Mosaic inundation extents or depths

    Parameters
    ----------
    inundation_maps_list : list
        List of inundation maps to mosaic
    mosaic_output: Optional[str], default = None
        Name of final mosaiced inundation file
    nodata: Optional[int], default = elev_raster_ndv
        Value to represent nodata
    remove_inputs: Optional[bool], default = False
        Whether to remove intermediate input files
    verbose: Optional[bool], default = True
        Quiet output

    Returns
    -------
    str
        File name of mosaiced output
    """

    if mosaic_output is not None:

        merge(inundation_maps_list, method='max', nodata=nodata, dst_path=mosaic_output)

        # see note at mask_mosaic
        # if mask:
        #     # fh.vprint("Masking ...", verbose)
        #     if verbose:
        #         logging.info(f"Masking ... ({mosaic_output})")
        # mask_mosaic(mosaic_output, mask, outfile=mosaic_output, workers=workers)

    remove_list = []
    if remove_inputs:
        for inun_map in inundation_maps_list:
            if inun_map is not None and os.path.isfile(inun_map):
                remove_list.append(inun_map)

    return remove_list


# def _vprint(message, verbose):
#     if verbose:
#         print(message)


# Jun 2026:
#     This was adjusted as part of PR 1605, 4.8.10.0 in Oct 2025, but never worked.
#     tracing it showed that it always failed and appears to not be needed.
#     Commenting it out for now.
# NOTE: if revived, it needs upgrades for all places for .open and needs logging and other perf reviews
# def mask_mosaic(mosaic, polys, polys_layer=None, outfile=None, workers=4, quiet=True):

#     if isinstance(mosaic, str):
#         logging.info(f"if isinstance(mosaic, str): {mosaic} - 1")
#         with rasterio.open(mosaic, 'r') as rst:
#             logging.info(f"if isinstance(mosaic, str): {mosaic} - 2")
#             windows = [windows for _, windows in rst.block_windows()]
#             logging.info(f"if isinstance(mosaic, str): {mosaic} - 3")
#             profile = rst.profile
#             logging.info(f"if isinstance(mosaic, str): {mosaic} - 4")
#         logging.info(f"if isinstance(mosaic, str): {mosaic} - 5")

#     elif isinstance(mosaic, rasterio.DatasetReader):
#         pass
#     else:
#         raise TypeError("Pass rasterio dataset or filepath for mosaic")

#     if isinstance(polys, str):
#         polys = gpd.read_file(polys, layer=polys_layer)
#     elif isinstance(polys, gpd.GeoDataFrame):
#         pass
#     else:
#         raise TypeError("Pass geopandas dataset or filepath for catchment polygons")

#     mosaic_read = rxr.open_rasterio(mosaic)
#     mosaic_read = mosaic_read.sel({'band': 1})
#     geom = polys['geometry'].values[0]

#     # def write_window(geom, window, wrst, lock):
#     def write_window(mosaic, geom, window, wrst, lock, outfile):
#         mosaic_slice = mosaic.isel(
#             y=slice(window.row_off, window.row_off + window.height),
#             x=slice(window.col_off, window.col_off + window.width),
#         )
#         logging.info(f"write_window - 1 {outfile}")
#         bbox = box(*mosaic_slice.rio.bounds())
#         logging.info(f"write_window - 2 {outfile}")

#         if geom.intersects(bbox):

#             logging.info(f"write_window - 3 {outfile}")
#             inter = geom.intersection(bbox)

#             if inter.area != bbox.area:
#                 logging.info(f"write_window - 4 {outfile}")

#                 gdf_temp = gpd.GeoDataFrame(geometry=[inter], crs=mosaic_slice.rio.crs)
#                 gdf_temp['arb'] = np.int8(1)
#                 temp_rast = make_geocube(vector_data=gdf_temp, measurements=['arb'], like=mosaic_slice)
#                 mosaic_slice.data = xr.where(np.isnan(temp_rast['arb']), 0, mosaic_slice.data)
#                 with lock:  # Will slow it down a little
#                     wrst.write_band(1, mosaic_slice.data.squeeze(), window=window)
#                 logging.info(f"write_window - 5 {outfile}")

#         logging.info(f"write_window - 6 {outfile}")

#     executor = ThreadPoolExecutor(max_workers=workers)

#     def __data_generator(windows, mosaic, geom, wrst, lock, outfile):
#         for window in windows:
#             yield mosaic, geom, window, wrst, lock, outfile

#     lock = Lock()

#     with rasterio.open(outfile, "r+", **profile) as wrst:
#         logging.info(f"start of opening via rasterio the outfile is {outfile}")
#         dgen = __data_generator(windows, mosaic_read, geom, wrst, lock, outfile)
#         logging.info(f"After generator {outfile}")
#         results = {executor.submit(write_window, *wg): 1 for wg in dgen}  # ThreadPoolExector

#         for future in as_completed(results):
#             try:
#                 # TODO: Jun 2026: Upgrade this to look for future.result if this gets fixed
#                 # future.exception, etc.
#                 # future.result()
#                 if future is not None:
#                     if not future.exception():
#                         if future.result() is None:
#                             logging.info(f"... data_generator complete for {outfile}")
#                         else:
#                             logging.info(f"... data_generator complete for {outfile}."
#                                          f" future result is {future.result()}")
#                     else:
#                         raise future.exception()
#                 else:
#                     logging.info(f"... complete future result is {results[future]} for {outfile}")

#             except Exception as exc:
#                 # _vprint("Exception {} for {}".format(exc, results[future]), not quiet)
#                 # logging.error instead of .critical as we leave .critical for places that the app down
#                 # logging.error("Exception {} for {}".format(exc, results[future]))
#                 logging.critical(f"Critical Error in mask_mosaic opening {outfile}")
#                 logging.critical(traceback.format_exc())
#                 # do not shut it down

#             # else:  # will always execute, like a "finally"
#             #     if results[future] is not None and not quiet:
#             #         # _vprint("... {} complete".format(results[future]), not quiet)
#             #         # if not quiet:
#             #         logging.info(f"... complete future result is {results[future]}")


def mosaic_final_inundation_extent_to_poly(
    inundation_raster: Optional[str] = None,
    inundation_polygon: Optional[str] = None,
    driver: Optional[str] = "GPKG",
):
    """
    Vectorize moasiced raster dataset

    Parameters
    ----------
    inundation_raster: Optional[str], default = None
        File path to input inundation raster
    inundation_polygon: Optional[str], default = None
        File path to output inundation polygon
    driver: Optional[str], default = "GPKG",
        File type to output inundation polygon

    """

    with rasterio.open(inundation_raster) as src:
        # Open inundation_raster using rasterio.
        image = src.read(1)

        # Use numpy.where operation to reclassify depth_array on the condition that the pixel values are > 0.
        reclass_inundation_array = np.where((image > 0) & (image != src.nodata), 1, 0).astype("uint8")

        # Aggregate shapes
        results = (
            {"properties": {"extent": 1}, "geometry": s}
            for i, (s, v) in enumerate(
                shapes(reclass_inundation_array, mask=reclass_inundation_array > 0, transform=src.transform)
            )
        )

        # Convert list of shapes to polygon, then dissolve
        extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=src.crs)
        extent_poly_diss = extent_poly.dissolve(by="extent")
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        # Write polygon
        extent_poly_diss.to_file(inundation_polygon, driver=driver, engine='fiona')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mosaic GMS Inundation Rasters")
    parser.add_argument(
        "-m",
        "--map-file",
        help="Pandas Dataframe or file path string to CSV of inundation/depth maps to mosaic.",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--mosaic-attribute",
        help="Attribute name: should be either inundation_rasters or depths_rasters.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-s",
        "--subset",
        help="Value(s) of unit attribute name used to subset (optional)",
        required=False,
        default=None,
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "-n", "--nodata", help="NODATA value for output raster", required=False, default=elev_raster_ndv
    )
    parser.add_argument(
        "-w",
        "--workers",
        help="Number of Workers (optional). Default value is 1.",
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        "-o",
        "--mosaic-output",
        help="Mosaiced inundation Maps file name",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-i",
        "--inundation-polygon",
        help="Filename of the final inundation extent polygon (optional). Default is None.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-r",
        "--remove-inputs",
        help="Remove original input inundation Maps (optional). Default is False",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Print out messages (optional). Default is False",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-g",
        "--is-mosaic-for-branches",
        help="If the mosaic is for branchs, include this arg. If is_mosaic_for_branches is true, "
        "the mosaic output name will add the HUC into the output name for overwrite reasons.",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    Mosaic_inundation(**args)
