#!/usr/bin/env python
# coding: utf-8

import argparse
import logging
import os
import traceback
import warnings

# from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Optional, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr

# from geocube.api.core import make_geocube
from rasterio.features import shapes
from rasterio.merge import merge
from shapely.geometry import box
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from tqdm import tqdm

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_variables import elev_raster_ndv


gpd.options.io_engine = "pyogrio"


# Set rasterio logger to only show errors, not warnings
logging.getLogger('rasterio').setLevel(logging.ERROR)
warnings.simplefilter(action='ignore', category=FutureWarning)


# NOTE: Aug 1, 2026: Changes:
# - Renamed a bunch of args to be more intitutive
# - subset removed: Nothing was using it and does not make sense to have it, what value could that have?


# Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
# Note: inundate_mosiac_wrapper might call this twice but it wwas unrealistic as the whole system
# never allowed for processing of inundation files and depth files at the same time.
def Mosaic_inundation(
    # Aug 2026: At this point... all are passing in df's, but left door open to csv paths if it matches
    # the correct schema with headers.
    raster_data: Union[str, pd.DataFrame],
    output_mosaic_path: str,  # can not be empty
    # mask_path: Optional[str] = None,
    # mosaic_attribute is the colum name in the (raster_paths_df) that has the paths to be mosiacked
    mosaic_attribute: Optional[str] = "inundation_raster_path",
    # Aug 2026: has to be huc8, unless significant inundation system upgrade, but left in for now
    unit_attribute_name: Optional[str] = "huc8",
    nodata: Optional[int] = elev_raster_ndv,
    # num_threads: Optional[int] = 1,  # dropped because of masking system drop
    remove_intermediate_files: Optional[bool] = True,
    # subset: Optional[str] = None,  # has no validity consider inundation arch.
    verbose: Optional[bool] = True,
    # Aug 2026: is_mosaic_for_branches was the wrong name, it really was for intermediate files for rolling
    # up by huc level (ag in tqdm) as intermediate files to help with final rollup.
    # Only inundate_nation used it. But it would have done it by default anyways, so it is pointless
    # is_mosaic_for_branches: Optional[bool] = False,
    # appends to the output file names. Really only has value if more than one huc is processed.
    # Name "per huc" but technically, it is per unit_attribute)
    is_mosaic_for_branches: Optional[bool] = False,  # usually the value of the huc number
    inundation_polygon: Optional[str] = None,  # Aug 2026: No scripts are using this, but leave it in for now
    # ) -> str:  (see note about return value below)
):
    """

        Mosaic inundation extents or depths


        Notes about raster_data arg: The raster_data coming in must be one of two things:
            1) A dataframe with the column names of "huc8", ("inundation_raster_path" or "depths_raster_path")
            2) A csv with headers same columns
            If extra column in, they will be ignored.
            This system can use which ever of the two pathing columns but not both at the same time.


            Parameters
            ----------
            raster_data : Union[str, pd.DataFrame]
                Either the path or dataframe of the files processed previously in inundation
            mosaic_attribute: str
                Attribute to mosaic the map files
            output_mosaic_path: str
                Name of final mosaicked inundation file
    #       mask_path: Optional[str], default = None
    #           Name of file to inclusively mask final output file
            unit_attribute_name: Optional[str], default = None
                Processing unit to mosaic inundation
            nodata: Optional[int], default = elev_raster_ndv
                Value to represent nodata
    #        num_threads: Optional[int], default = 1
    #            Number of parallel processes to use
            remove_intermediate_files: Optional[bool], default = False
                Whether to remove intermediate input files
    #       subset: Optional[str], default = None
    #           Path to file for subsetting inundation files
            verbose: Optional[bool], default = True
                Quiet output
            add_huc_to_mosaic_file_name: Optional[bool] = False,
                Whether to append branch name after output  # usually just appends the huc number to the file name
    #        inundation_polygon: Optional[str], default = None
    #            File path for inundation polygon

        # Returns
        #   -------
        #  str
        #    File name of mosaiced output - Was an error. If there were multiple hucs involved in the mosaic
        #    it was submitting on the very last ag_mosaic_output in each huc. Nothing was using it anyways.
        #

    """

    # mosaic_attribute is the column name from the incoming raster dataframe
    if mosaic_attribute not in ("inundation_raster_path", "depths_raster_path"):
        raise ValueError(
            "mosaic_attribute arg is the name of the column in the incoming raster dataframe."
            " which has to be either inundation_raster_path or depths_raster_path depending which you are mosaicking"
        )

    if not output_mosaic_path:
        raise ValueError("output mosiac raster path can not be empty")

    msg = f"Starting mosaic for {output_mosaic_path}. Note: if this includes multiple HUC being processed"
    " this file path will be used as a base file name and path and append the huc value to each huc output file."
    if verbose:
        logging.info(msg)
    else:
        logging.debug(msg)

    # if not os.path.isdir(os.path.dirname(output_mosaic_path)):
    os.makedirs(os.path.dirname(output_mosaic_path), exist_ok=True)

    try:
        # Can be passed in as a dataframe or a string to a file location for loading
        # Correct column names assumed, see notes above about raster data columns
        if isinstance(raster_data, pd.DataFrame):
            if raster_data.empty:
                raise Exception("The raster data arg appears to be a dataframe but it is empty")
            inundation_maps_df = raster_data
            del raster_data
        elif isinstance(raster_data, str):
            if not raster_data:
                raise ValueError("raster data path can not be an empty string")
            # inundation_maps_df = pd.read_csv(raster_data, dtype={unit_attribute_name: str, "branchID": str})
            # Aug 2026:; branch ID not needed. It was never used
            inundation_maps_df = pd.read_csv(raster_data, dtype={unit_attribute_name: str})
        else:
            raise TypeError(
                f"Pass Pandas Dataframe or file path string to csv for map_file argument - [{output_mosaic_path}]"
            )

        # Column name checks
        if "huc8" not in inundation_maps_df.columns:
            raise Exception("dataframe or csv is missing the huc8 column")

        # check to see if it is using either a column named inundation_raster_path or depths_raster_path
        # exists depending on the value of the mosaic_attribute
        if mosaic_attribute not in inundation_maps_df.columns:
            raise Exception(
                f"The mosaic_attribute value submitted was '{mosaic_attribute}', but that column name does not exist"
                " in the submitted dataframe or the csv path that was submitted."
            )

        # remove NaNs
        inundation_maps_df = inundation_maps_df.dropna(axis=0, how="all")

        # subset  (never used and didn't really have much value anyways)
        # if subset is not None:
        #     subset_mask = inundation_maps_df.loc[:, unit_attribute_name].isin(subset)
        #     inundation_maps_df = inundation_maps_df.loc[subset_mask, :]

        # unique aggregation units  (always huc8 for now)
        aggregation_units = inundation_maps_df.loc[:, unit_attribute_name].unique()

        # auto takes care of sorting (by index by default)   (always huc8 for now)
        # But the df might have data for more than one huc, and they all get mosiacked to one final output file
        inundation_maps_df = inundation_maps_df.set_index(unit_attribute_name, drop=True)

        tqdm_disable = True
        if len(aggregation_units) > 1:
            tqdm_disable = False
        # decide upon whether to display the progress bar
        # if verbose & len(aggregation_units) == 1:
        #     tqdm_disable = False
        # elif verbose:
        #     tqdm_disable = False
        # else:
        #     tqdm_disable = True

        remove_at_end = []

        # ag_key is likely "huc8" but could have been overridden
        # Aug 2026: Be super careful. In sythensize_test_case.py -> run_test_case.py -> produce_mosaicked_inundation
        # which has MP, it often submits mosaic sets with the exact same files names. This means the agkey
        # would be identical. The only differences is the base folder path is different as it has a magnitude
        # subfolder name. We need to keep an eye for that and it is part of the key reason why we can not have
        # thread in thread. But thread inside MP is ok, just have to watch for it.
        # Hopefully, we never get two of the exact same mosaic output paths identical by accident by an outside MP.

        # TODO: We can likely turn this into a MT as all of the files are HUC level sets so their would
        # not be collisions
        # Might check for dups in the inundation_maps which in theory should not happen
        for ag_key in tqdm(aggregation_units, disable=tqdm_disable, desc="Mosaicking FIMs"):
            if verbose:
                logging.info(f"Starting mosaic for {ag_key}")
            else:
                logging.debug(f"Starting mosaic for {ag_key}")
            try:
                inundation_maps_list = inundation_maps_df.loc[ag_key, mosaic_attribute].tolist()
            except AttributeError as ae:
                logging.critical(f"Attribute error when processing {ag_key} ")
                raise ae
                # do not supress, re-raise and stop processing
                # inundation_maps_list = [inundation_maps_df.loc[ag, mosaic_attribute]]

            #            # Some processes may have already added the ag value (if it is a huc) to
            #            # the file name, so don't re-add it.
            #            # Only add the huc into the name if branches are being processed, as
            #            # sometimes the mosaic is not for gms branches but maybe mosaic of an
            #            # fr set with a gms composite map.

            # Use the output mosaic path as a base file name with branch subsets if applicable
            ag_mosaic_output_path = output_mosaic_path
            # if (is_mosaic_for_branches) and (ag_key not in output_mosaic_path):
            if is_mosaic_for_branches:
                ag_mosaic_output_path = fh.append_id_to_file_name(output_mosaic_path, ag_key)

            # This is mostly for removing the branch intermediate tifs in the rollup to the huc here
            remove_list = mosaic_by_unit(
                inundation_maps_list,
                ag_mosaic_output_path,
                nodata,
                # num_threads=num_threads,
                remove_intermediate_files=remove_intermediate_files,
                # mask_path=mask_path,
                # verbose=verbose,
            )

            if remove_list is not None:
                remove_at_end.extend(remove_list)
                remove_at_end = list(set(remove_at_end))  # Ensures unique values

            logging.debug(f"Mosaic complete for {ag_key}")

        if inundation_polygon is not None:  # Aug 2026: No scripts use this at this time, but maybe later
            mosaic_final_inundation_extent_to_poly(ag_mosaic_output_path, inundation_polygon)

        if remove_intermediate_files:
            # if verbose:
            #     # fh.vprint("Removing inputs ...", verbose)
            #     logging.info(f"Removing interium raster files ... [{output_mosaic_path}]")
            # else:
            #     logging.debug(f"Removing interium raster files ... [{output_mosaic_path}]")

            for remove_file in remove_at_end:
                # Aug 2026: We are getting errors here saying the file does not exist. Must be something subtle
                # farther up the chain that assumed this file was here or something. Or colliding via
                # a parent MP or MT having the same huc nummbers. Ones like alpha testing do submit workers
                # that have dup HUCs but differnt paths, usually just a subfolder for magnitude.
                if os.path.exists(remove_file):
                    os.remove(remove_file)
                else:
                    logging.warning(
                        f"Somehow {remove_file} did not to be removed."
                        f" Output mosiac path is {output_mosaic_path} Research required."
                        " Maybe related to MP or MT?"
                    )
        # else:
        #     logging.debug(f"Skipping removing interium raster files ... [{output_mosaic_path}]")

    except Exception as ex:
        logging.critical(f"Critical Error while creating a mosaic for {output_mosaic_path}")
        logging.critical(traceback.format_exc())
        raise ex

    # Return file name and path of the final mosaic output file.
    # Might be empty.
    # return ag_mosaic_output_path
    # ag_mosaic_output_path - This would have the last adj ag_mosaic_output_path which is an error if you have
    # more than one HUC incoming
    # Unless it errors out, it would have the exact value as the input output_mosaic_path


# Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
# threading not applicable and can not be used as it would break.
def mosaic_by_unit(
    inundation_maps_list: list,
    mosaic_output_path: str,
    nodata: Optional[int] = elev_raster_ndv,
    # num_threads: Optional[int] = 1,
    remove_intermediate_files: Optional[bool] = False,
    # mask_path: Optional[str] = None,
    # verbose: Optional[bool] = False,
) -> Union[list, None]:
    """
        Mosaic inundation extents or depths

        Parameters
        ----------
        inundation_maps_list : list
            List of inundation maps to mosaic based on agkey if applicable
        mosaic_output_path: str
            Name of final mosaicked inundation file
        nodata: Optional[int], default = elev_raster_ndv
            Value to represent nodata
    #    workers: Optional[int], default = 1
    #        Number of parallel processes to use
        remove_intermediate_files: Optional[bool], default = False
            Whether to remove intermediate input files
    #     mask_path: Optional[str], default = None
    #        Name of file to inclusively mask final output file
    #    verbose: Optional[bool], default = False
    #        Quiet output

        Returns
        -------
        str
            File name of mosaicked output
    """

    #     if mosaic_output_path is not None:

    merge(inundation_maps_list, method='max', nodata=nodata, dst_path=mosaic_output_path)

    # if mask_path:
    #     # fh.vprint("Masking ...", verbose)
    #     if verbose:
    #         logging.info(f"Masking... for {mosaic_output_path} using {mask_path}")
    #     else:
    #         logging.debug(f"Masking... for {mosaic_output_path} using {mask_path}")

    #     mask_mosaic(mosaic_output_path, mask_path, outfile=mosaic_output_path, workers=num_threads)

    remove_list = []
    if remove_intermediate_files:
        # if verbose:
        #     #fh.vprint("Removing inputs ...", verbose)

        for inun_map in inundation_maps_list:
            if inun_map is not None and os.path.isfile(inun_map):
                remove_list.append(inun_map)

    return remove_list


# def _vprint(message, verbose):
#     if verbose:
#         print(message)


# def mask_mosaic(mosaic, mask_path, polys_layer=None, outfile=None, workers=4, quiet=True):
# Aug 2026: polys_layer never used and no way to use it unless a script calls directly to here and
# not via mosaic_iundation as it never used that arg
# Also.. verbose / quite was always hiding the fact that it was always failing.
# Masking was always failing and has not worked since it was added Oct 2025
#    We are going to fully remove it as it doesn't really have much value as it was always
#    masked by the huc wbd.gpkg when not coming in via interpolate_water_surface.py which never
#    sent in a mask_path so this was skipped anyways

# Some scripts were calling this and it was triggering a huge amount of errors but the exception
# was completely surpressed them

#  So.. lets drop all masking system wide.
'''
def mask_mosaic(mosaic_output_path, mask_path, outfile=None, workers=4, verbose=False):

    if not mosaic_output_path:
        raise Exception("mosaic_output_path can not be None or empty")
    # if isinstance(mosaic_output_path, str):
    with rasterio.open(mosaic_output_path, 'r') as rst:
        windows = [windows for _, windows in rst.block_windows()]
        profile = rst.profile
    # elif isinstance(mosaic, rasterio.DatasetReader):
    #     pass
    # else:
    #     raise TypeError("Pass rasterio dataset or filepath for mosaic")

    if isinstance(mask_path, str):
        # mask_path = gpd.read_file(mask_path, layer=polys_layer)
        mask_path = gpd.read_file(mask_path)
    elif isinstance(mask_path, gpd.GeoDataFrame):
        pass
    else:
        raise TypeError("Pass geopandas dataset or filepath for catchment polygons")

    mosaic_read = rxr.open_rasterio(mosaic_output_path)
    mosaic_read = mosaic_read.sel({'band': 1})
    geom = mask_path['geometry'].values[0]

    # None; Aug 1, 2026: This has failed for a long time the only calling member of this
    # in this function, attempts to pass in five args. Which means a bunch of code all of the way up
    # is invalid.
    def write_window(geom, window, wrst, lock):
        mosaic_slice = mosaic_output_path.isel(
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


    # TODO: If we keep this... Then upgrade this threadpool for better management
    # Aug 2026: Does this even make sense to have a threadpool for?
    executor = ThreadPoolExecutor(max_workers=workers)

    # TODO: Notice it submits  5 args into the thread pool,
    # but the write_window only accepts 4 args and should error out
    def __data_generator(windows, mosaic, geom, wrst, lock):
        for window in windows:
            yield mosaic, geom, window, wrst, lock

    lock = Lock()

    with rasterio.open(outfile, "r+", **profile) as wrst:
        dgen = __data_generator(windows, mosaic_read, geom, wrst, lock)

        # Aug 1, 2026: This has likely been broken for a long time and the error surpressed.
        # it calls write_window abovve, but it looking for four args and not the five submitted
        # This means this entire block in invalid and the entire masking system is invalid
        results = {executor.submit(write_window, *wg): 1 for wg in dgen}

        for future in as_completed(results):
            try:
                future.result()
            except Exception as exc:
                # This should not be supressed and was. It was failing for all recs with
                # as expected:  mask_mosaic.<locals>.write_window() takes 4 positional arguments but 5 were given
                # but it was
                logging.critical(f"The write_window has failed for {mosaic_output_path} - [{outfile}]")
                logging.critical(traceback.format_exc())
                # _vprint("Exception {} for {}".format(exc, results[future]), not quiet)
            else:
                if results[future] is not None:
                    # _vprint("... {} complete".format(results[future]), not quiet)
                    if not verbose:
                        logging.info(f"... {results[future]} complete - [{outfile}]")

                else:
                    if not verbose:
                        # _vprint("... complete", not quiet)
                        logging.info(f"... complete - [{outfile}]")
'''


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

    # Aug, 2026: If we want to use this feature, it needs updating as the function args were changed

    parser = argparse.ArgumentParser(description="Mosaic GMS Inundation Rasters")
    parser.add_argument(
        "-m",
        "--raster_data",
        help="File path string to CSV of inundation/depth maps to mosaic.",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--mosaic_attribute",
        help="Optional: Attribute name: should be the value of either inundation_rasters_path"
        " or depths_rasters_path. Defaults to 'inundation_rasters_path'",
        required=False,
        default="inundation_raster_path",
        type=str,
    )
    # parser.add_argument(
    #     "-a",
    #     "--mask",
    #     help="File path to vector polygon mask used to clip mosaic (optional). Default is None",
    #     required=False,
    #     default=None,
    #     type=str,
    # )
    parser.add_argument(
        "-u",
        "--unit-attribute-name",
        help="Unit attribute name (optional). Default is huc8",
        required=False,
        default="huc8",
        type=str,
    )
    # parser.add_argument(
    #     "-s",
    #     "--subset",
    #     help="Value(s) of unit attribute name used to subset (optional)",
    #     required=False,
    #     default=None,
    #     type=str,
    #     nargs="+",
    # )
    parser.add_argument(
        "-n", "--nodata", help="NODATA value for output raster", required=False, default=elev_raster_ndv
    )
    # parser.add_argument(
    #     "-w",
    #     "--workers",
    #     help="Number of Workers (optional). Default value is 1.",
    #     required=False,
    #     default=1,
    #     type=int,
    # )
    parser.add_argument(
        "-o",
        "--output-mosaic-path",
        help="Mosaiced inundation Maps file name and path",
        required=True,
        type=str,
    )
    # parser.add_argument(
    #     "-i",
    #     "--inundation-polygon",
    #     help="Filename of the final inundation extent polygon (optional). Default is None.",
    #     required=False,
    #     default=None,
    #     type=str,
    # )
    parser.add_argument(
        "-r",
        "--remove-intermediate-files",
        help="Remove original input inundation Maps (optional). Default is True",
        required=False,
        default=True,
        action="store_false",
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
        "--is_mosaic_for_branches",
        help="If the mosaic is for branchs, include this arg. If is_mosaic_for_branches is true, "
        "the mosaic output name will add the HUC into the output name for overwrite reasons.",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    Mosaic_inundation(**args)
