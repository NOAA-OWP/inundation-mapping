#!/usr/bin/env python
# coding: utf-8

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Optional, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from geocube.api.core import make_geocube
from rasterio.features import shapes
from rasterio.merge import merge
from shapely.geometry import box
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from tqdm import tqdm

from utils.shared_functions import FIM_Helpers as fh
from utils.shared_variables import elev_raster_ndv


gpd.options.io_engine = "pyogrio"


# Set rasterio logger to only show errors, not warnings
logging.getLogger('rasterio').setLevel(logging.ERROR)


def Mosaic_inundation(
    map_file: Union[str, pd.DataFrame],
    mosaic_attribute: str,
    mosaic_output: Optional[str] = None,
    mask: Optional[str] = None,
    unit_attribute_name: Optional[str] = "huc8",
    nodata: Optional[int] = elev_raster_ndv,
    workers: Optional[int] = 1,
    remove_inputs: Optional[bool] = False,
    subset: Optional[str] = None,
    verbose: Optional[bool] = True,
    is_mosaic_for_branches: Optional[bool] = False,
    inundation_polygon: Optional[str] = None,
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
    mask: Optional[str], default = None
        Name of file to inclusively mask final output file
    unit_attribute_name: Optional[str], default = None
        Processing unit to mosaic inundation
    nodata: Optional[int], default = elev_raster_ndv
        Value to represent nodata
    workers: Optional[int], default = 1
        Number of parallel processes to use
    remove_inputs: Optional[bool], default = False
        Whether to remove intermediate input files
    subset: Optional[str], default = None
        Path to file for subsetting inundation files
    verbose: Optional[bool], default = True
        Quiet output
    is_mosaic_for_branches: Optional[bool] = False,
        Whether to append branch name after output
    inundation_polygon: Optional[str], default = None
        File path for inundation polygon

    Returns
    -------
    str
        File name of mosaiced output

    """
    if not os.path.isdir(os.path.dirname(mosaic_output)):
        os.makedirs(os.path.dirname(mosaic_output))

    # check input
    if mosaic_attribute not in ("inundation_rasters", "depths_rasters"):
        raise ValueError("Pass inundation_rasters or depths_raster for mosaic_attribute argument")

    # load file
    if isinstance(map_file, pd.DataFrame):
        inundation_maps_df = map_file
        del map_file
    elif isinstance(map_file, str):
        inundation_maps_df = pd.read_csv(map_file, dtype={unit_attribute_name: str, "branchID": str})
    else:
        raise TypeError("Pass Pandas Dataframe or file path string to csv for map_file argument")

    # remove NaNs
    inundation_maps_df = inundation_maps_df.dropna(axis=0, how="all")

    # subset
    if subset is not None:
        subset_mask = inundation_maps_df.loc[:, unit_attribute_name].isin(subset)
        inundation_maps_df = inundation_maps_df.loc[subset_mask, :]

    # unique aggregation units
    aggregation_units = inundation_maps_df.loc[:, unit_attribute_name].unique()

    inundation_maps_df = inundation_maps_df.set_index(unit_attribute_name, drop=True)

    # decide upon whether to display the progress bar
    if verbose & len(aggregation_units) == 1:
        tqdm_disable = False
    elif verbose:
        tqdm_disable = False
    else:
        tqdm_disable = True

    ag_mosaic_output = ""
    remove_at_end = []

    for ag in tqdm(aggregation_units, disable=tqdm_disable, desc="Mosaicing FIMs"):
        try:
            inundation_maps_list = inundation_maps_df.loc[ag, mosaic_attribute].tolist()
        except AttributeError:
            inundation_maps_list = [inundation_maps_df.loc[ag, mosaic_attribute]]

        # Some processes may have already added the ag value (if it is a huc) to
        # the file name, so don't re-add it.
        # Only add the huc into the name if branches are being processed, as
        # sometimes the mosaic is not for gms branches but maybe mosaic of an
        # fr set with a gms composite map.

        ag_mosaic_output = mosaic_output
        if (is_mosaic_for_branches) and (ag not in mosaic_output):
            ag_mosaic_output = fh.append_id_to_file_name(mosaic_output, ag)  # change it

        remove_list = mosaic_by_unit(
            inundation_maps_list,
            ag_mosaic_output,
            nodata,
            workers=workers,
            remove_inputs=remove_inputs,
            mask=mask,
            verbose=verbose,
        )

        if remove_list is not None:
            remove_at_end.extend(remove_list)
            remove_at_end = list(set(remove_at_end))  # Ensures unique values

    if inundation_polygon is not None:
        mosaic_final_inundation_extent_to_poly(ag_mosaic_output, inundation_polygon)

    if remove_inputs:
        fh.vprint("Removing inputs ...", verbose)

        for remove_file in remove_at_end:
            os.remove(remove_file)

    # Return file name and path of the final mosaic output file.
    # Might be empty.
    return ag_mosaic_output


# Note: This uses threading and not processes. If the number of workers is more than
# the number of possible threads, no results will be returned. But it is usually
# pretty fast anyways. This needs to be fixed.
def mosaic_by_unit(
    inundation_maps_list: list,
    mosaic_output: str,
    nodata: Optional[int] = elev_raster_ndv,
    workers: Optional[int] = 1,
    remove_inputs: Optional[bool] = False,
    mask: Optional[str] = None,
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
    workers: Optional[int], default = 1
        Number of parallel processes to use
    remove_inputs: Optional[bool], default = False
        Whether to remove intermediate input files
    mask: Optional[str], default = None
        Name of file to inclusively mask final output file
    verbose: Optional[bool], default = True
        Quiet output

    Returns
    -------
    str
        File name of mosaiced output
    """

    if mosaic_output is not None:

        merge(inundation_maps_list, method='max', nodata=nodata, dst_path=mosaic_output)

        if mask:
            fh.vprint("Masking ...", verbose)
            mask_mosaic(mosaic_output, mask, outfile=mosaic_output, workers=workers)

    if remove_inputs:
        fh.vprint("Removing inputs ...", verbose)

        remove_list = []
        for inun_map in inundation_maps_list:
            if inun_map is not None and os.path.isfile(inun_map):
                remove_list.append(inun_map)

        return remove_list


def _vprint(message, verbose):
    if verbose:
        print(message)


def mask_mosaic(mosaic, polys, polys_layer=None, outfile=None, workers=4, quiet=True):

    if isinstance(mosaic, str):
        with rasterio.open(mosaic, 'r') as rst:
            windows = [windows for _, windows in rst.block_windows()]
            profile = rst.profile
    elif isinstance(mosaic, rasterio.DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio dataset or filepath for mosaic")

    if isinstance(polys, str):
        if os.path.splitext(polys)[-1].lower() == '.parquet':
            polys = gpd.read_parquet(polys, layer=polys_layer)
        else:
            polys = gpd.read_file(polys, layer=polys_layer)
    elif isinstance(polys, gpd.GeoDataFrame):
        pass
    else:
        raise TypeError("Pass geopandas dataset or filepath for catchment polygons")

    mosaic_read = rxr.open_rasterio(mosaic)
    mosaic_read = mosaic_read.sel({'band': 1})
    geom = polys['geometry'].values[0]

    def write_window(geom, window, wrst, lock):
        mosaic_slice = mosaic.isel(
            y=slice(window.row_off, window.row_off + window.height),
            x=slice(window.col_off, window.col_off + window.width),
        )
        bbox = box(*mosaic_slice.rio.bounds())

        if geom.intersects(bbox):

            inter = geom.intersection(bbox)

            if inter.area != bbox.area:
                gdf_temp = gpd.GeoDataFrame(geometry=[inter], crs=mosaic_slice.rio.crs)
                gdf_temp['arb'] = np.int8(1)
                temp_rast = make_geocube(vector_data=gdf_temp, measurements=['arb'], like=mosaic_slice)
                mosaic_slice.data = xr.where(np.isnan(temp_rast['arb']), 0, mosaic_slice.data)
                # with lock:
                wrst.write_band(1, mosaic_slice.data.squeeze(), window=window)

    executor = ThreadPoolExecutor(max_workers=workers)

    def __data_generator(windows, mosaic, geom, wrst, lock):
        for window in windows:
            yield mosaic, geom, window, wrst, lock

    lock = Lock()

    with rasterio.open(outfile, "r+", **profile) as wrst:
        dgen = __data_generator(windows, mosaic_read, geom, wrst, lock)
        results = {executor.submit(write_window, *wg): 1 for wg in dgen}

        for future in as_completed(results):
            try:
                future.result()
            except Exception as exc:
                _vprint("Exception {} for {}".format(exc, results[future]), not quiet)
            else:
                if results[future] is not None:
                    _vprint("... {} complete".format(results[future]), not quiet)
                else:
                    _vprint("... complete", not quiet)


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
        extent_poly_diss.to_file(inundation_polygon)


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
        "-a",
        "--mask",
        help="File path to vector polygon mask used to clip mosaic (optional). Default is None",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-u",
        "--unit-attribute-name",
        help="Unit attribute name (optional). Default is huc8",
        required=False,
        default="huc8",
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
        help="Filename (GPKG or GeoParquet) of the final inundation extent polygon. (optional). Default is None.",
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
