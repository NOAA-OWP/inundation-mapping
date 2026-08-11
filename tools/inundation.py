#!/usr/bin/env python3

# import argparse
import logging
import os
import traceback
import warnings

# from os.path import splitext
from typing import List, Optional, Tuple, Union

# import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from numba import njit, typed, types
from rasterio.mask import mask
from shapely.geometry import shape

import src.utils.shared_functions as sf


gpd.options.io_engine = "pyogrio"
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.getLogger('numba').setLevel(logging.WARNING)


# Aug 2026: No scripts call this directly. It is only called by inundate_gms and kinda has to be due
# to iterators and huc / branch management
# And with it really only callable via inundate_gms, much of this code becomes irrelavent.
# With masking being invalid, we do not need the catchments_poly_path anymore.
# This always a child of the ThreadProcessPool from inundate_gms and has to be now (based on on huc id and a branch id)
def inundate(
    huc: str,
    branch_id: int,
    rem_branch_path: str,
    catchments_branch_path: str,
    hydro_table_branch_df: pd.DataFrame,
    forecast_file_path: str,
    # catchment_poly_path: Optional[str] = None,
    # mask_type: Optional[Union[str, List[str]]] = None,
    # hucs: Optional[Union[str, fiona.Collection]] = None,  # replaced with single manditory huc arg
    # hucs_layerName: Optional[str] = None,  # n/a
    # subset_hucs: Optional[Union[str, List[str]]] = None,  n/a
    # num_workers: Optional[int] = 1,   # n/a
    # aggregate: Optional[bool] = False, # n/a
    inundation_raster_path: Optional[str] = None,
    depths_raster_path: Optional[str] = None,
    # src_table: Optional[str] = None, # n/a
    verbose: Optional[bool] = False,  # temp not in use
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
):
    """

    Run inundation on FIM >=3.0 outputs at job-level scale or aggregated scale

    Generate depths raster, inundation raster, and inundation polygon from FIM >=3.0 outputs.
    Be sure to pass a HUCs file to process in batch mode if passing aggregated products.

    # TODO: cleanup doc strings

    Parameters
    ----------
    huc: str,  Not optional
    branch_id: int, Not optional
    rem_branch_path : str
        File path to the Relative Elevation Model raster.
        Must have the same CRS as catchments raster.
    catchments_branch_path : str
        File path to the Catchments raster. Must have the same CRS as REM raster
    # catchment_poly_path : str
    #     File path to Catchments raster. Must have the same CRS as REM raster
    hydro_table_branch_df : pandas.DataFrame
        Pandas DataFrame object with correct indices and columns.
    forecast_file_path : str
        File path to forecast csv with correct column names.
    # mask_type : Optional[str], default=None
    #     How to mask the datasets for processing inundation
    # hucs : Optional[Union[str, fiona.Collection]], default=None
    #     Batch mode only. File path or fiona collection of vector polygons in HUC 4,6,or 8's to inundate on.
    #     Must have an attribute named as either "HUC4","HUC6", or "HUC8" with the associated values.
    # hucs_layerName : Optional[str], default=None
    #     Batch mode only. Layer name in hucs to use if multi-layer file is passed.
    # subset_hucs : Optional[Union[str, List[str]]], default=None
    #     Batch mode only. File path to line delimited file, HUC string, or list of HUC strings to
    #     further subset hucs file for inundating.
    # num_workers : Optional[int], default=1
    #     Batch mode only. Number of workers to use in batch mode. Must be 1 or greater.
    # aggregate : Optional[bool], default=False
    #     Batch mode only. Aggregates output rasters to VRT mosaic files and merges polygons to single GPKG file
    #     Currently not functional. Raises warning and sets to false. On to-do list.
    inundation_raster_path : Optional[str], default=None
        Path to optional inundation raster output. Appends HUC number if ran in batch mode.
    depths_raster_path : Optional[str], default=None
        Path to optional depths raster output. Appends HUC number if ran in batch mode.
    # src_table : Optional[str], default=None
    #     Table to subset main hydrotable.
    verbose : Optional[bool], default=False
        verbose output.
    precalb_option : Optional[bool], default=False
        Whether to use precalb discharge in hydrotable. If True, will use precalb_discharge_cms column
    windowed : Optional[bool], default=False
        Memory efficient operation to process inundation

    Returns
    -------
    inun_data : dict (see return below)
        Can also return None

    # Warns
    # -----
    # warn
    #     if aggregate set to true, will revert to false.

    """

    # Let it pick up the default logger even if it was never set up or was created with special handlers.
    # When a logger is attached to, even it if is not set up, it goes to console only.
    # This a handle only. With inundate mostly being called from indundate_gms via a threadpool
    # this helps with managing logging collisions and a memory built up of the logger
    # Notice: it is called "logger" and not "logging".
    # If we need it in child classes, pass the "logger"
    logger = logging.getLogger()

    # commented out as it fills the logs heavily (there are over 55,000 branches in a BED)
    # if verbose:
    #     logging.info(f"Start Inundating for {huc} - {branch_id}")
    # else:
    #     logging.debug(f"Start Inundating for {huc} - {branch_id}")


    if not os.path.isfile(rem_branch_path):
        raise Exception(f"[{huc}:{branch_id}] - Rem file of {rem_branch_path} does not exist")

    if not os.path.isfile(catchments_branch_path):
        raise Exception(f"[{huc}:{branch_id}] - Catchments file of {catchments_branch_path} does not exist")

    if hydro_table_branch_df is None or hydro_table_branch_df.empty:
        raise TypeError(f"[{huc}:{branch_id}] - hydro_table_branch_df is None or empty")

    # check for num_workers
    # Can not do iterators in this file
    # num_workers = int(num_workers)
    # assert num_workers >= 1, "Number of workers should be 1 or greater"
    # if (num_workers > 1) & (hucs is None):
    #     raise AssertionError("Pass a HUCs file to batch process inundation mapping")

    # check that aggregate is only done for hucs mode and was only ever called in "filter" mode, making this pointless.
    # aggregate = bool(aggregate)
    # if aggregate:
    #     warn("Aggregate feature currently not working. Setting to false for now.")  (legacy)
    #     aggregate = False
    # if hucs is None:
    #     assert not aggregate, "Pass HUCs file if aggregation is desired"

    # bool verbose
    # verbose = bool(verbose)

    inun_data = None

    if not depths_raster_path and not inundation_raster_path:
        raise ValueError(
            f"[{huc}:{branch_id}] - At least one raster path (depth or inundation) must be provided."
        )

    if (depths_raster_path != "" and depths_raster_path is not None) and (
        inundation_raster_path != "" and inundation_raster_path is not None
    ):
        raise ValueError(
            f"[{huc}:{branch_id}] - Can not supply both a depth and an inundation path. It needs to be only one"
        )

    if not os.path.exists(rem_branch_path):
        raise ValueError(f"[{huc}:{branch_id}] - {rem_branch_path} does not exist")
    #is_inundation_raster = False

    if not os.path.exists(catchments_branch_path):
        raise ValueError(f"[{huc}:{branch_id}] - {catchments_branch_path} does not exist")

    is_inundation_raster = False if not inundation_raster_path else True

    depth_rst = None  # Manages orphaned opened rasters
    inundation_rst = None  # Manages orphaned opened rasters

    # logging.debug("+++++++++++++++++")
    # logging.debug(f"Starting inundate for {inundation_raster_path}") 

    # inun_data = None

    try:

        with (
            rasterio.open(rem_branch_path) as rem_rst,
            rasterio.open(catchments_branch_path) as catchments_rst,
        ):

            # check for matching number of bands and single band only
            assert ((rem_rst.transform * (0, 0)) == (catchments_rst.transform * (0, 0))) & (
                (rem_rst.transform * (rem_rst.width, rem_rst.height))
                == (catchments_rst.transform * (catchments_rst.width, catchments_rst.height))
            ), f"[{huc}:{branch_id}] - REM and catchments rasters require same upper left and lower right extents"

            depths_profile = rem_rst.profile
            inundation_profile = catchments_rst.profile

            # A little of a weird way to check for oConus recs (19x..22x huc numbers)
            # else... we use it as int32
            is_int_16 = inundation_profile['dtype'] == 'int16'

            # logging.debug(f"len of hydro_table_branch_df is {len(hydro_table_branch_df)} for {inundation_raster_path}")

            # catchment stages dictionary
            catchment_stages_dict = __subset_hydroTable_to_forecast(
                hydro_table_branch_df, forecast_file_path, is_int_16, precalb_option
            )

            # TODO: Aug 2026: Did this really come back as len 0 every time? research required.
            # logging.debug(f"[{inundation_raster_path}] - Number of catchments in dict ar {len(catchment_stages_dict)}")

            # if len(catchment_stages_dict) == 0:
            #     logging.debug(f"[{huc}:{branch_id}] - There are no catchment stage records to process")
            #     return inun_data  # Empty

            # this could have failed sometims as the catchment_stages_dict was defined in the __subset_hydro...
            # if src_table is not None:
            #     create_src_subset_csv(hydro_table_branch_df, catchmentStagesDict, src_table)

            # TODO: Jun 2026: research this more. Does rasterio might want json args now, TBD
            # Jun 2026: Can't use blockxsize and blockysize (seeing as we are using COG GeoTiffs) ??

            # Hummm... does depth no data even make sense?
            nodata = 0
            if not is_inundation_raster:
                # Aug 2026: Not.. previously.. it assumed all depth profiles were int16 so may not have worked with oConus
                depths_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True)
                depth_rst = rasterio.open(depths_raster_path, "w+", **depths_profile)
                nodata = (
                    np.int16(depths_profile['nodata'])
                    if is_int_16
                    else np.int32(depths_profile['nodata'])
                )
            else:  
                inundation_profile.update(
                    driver='GTiff', blockxsize=256, blockysize=256, tiled=True, nodata=0
                )
                inundation_rst = rasterio.open(inundation_raster_path, "w+", **inundation_profile)
                nodata = (
                    np.int16(inundation_profile['nodata'])
                    if is_int_16
                    else np.int32(inundation_profile['nodata'])
                )

            # make windows generator.
            # The generator is really only ever called once per use of the inundate function
            # Which makes some of this semi pointless other then the windowed part.
            window_gen = __make_windows_generator(
                rem_rst=rem_rst,
                catchments_rst=catchments_rst,
                # catchment_poly_path=catchment_poly_path,
                # mask_type,
                catchment_stages_dict=catchment_stages_dict,
                # verbose=verbose,
                # hucs=hucs,
                # hucSet=hucSet,
                windowed=windowed,
                inundation_rst=inundation_rst,
                inundation_nodata=nodata,
                inundation_raster_path=inundation_raster_path,
                depth_rst=depth_rst,
                depths_raster_path=depths_raster_path,
                min_value=30 if is_int_16 else 0.03048,
            )

            inundation_rasters = []
            depth_rasters = []
            # inundation_polys = []

            for wg in window_gen:
                future = __inundate_in_huc(**wg)
                inundation_rasters += [future[0]]
                depth_rasters += [future[1]]
                # inundation_polys += [future[2]]

            # return inundation_rasters, depth_rasters, inundation_polys
            # inundation.py.__inundate_in_huc never returned a poly, it was hardcoded to None
            # TODO: Aug 2026: This is loose as this becomes the column names needed in mosaic_inundation
            inun_data = {
                "huc8": huc,
                "branchID": branch_id,
                "inundation_raster_paths": inundation_raster_path,
                "depths_raster_paths": depths_raster_path,
                # "inundation_polygons": inundation_polys_file_name,  # no longer applicable
            }

        return inun_data

    except Exception as ex:
        logger.critical(f"[{huc}:{branch_id}] - Critical Error while inundating for {forecast_file_path}."
                        f" Details = {ex}")
        # logger.critical(traceback.format_exc())
        raise ex  # yes, re-raise
    finally:
        if inundation_rst is not None:
            inundation_rst.close()
        if depth_rst is not None:
            depth_rst.close()

        # Closes the temp attachment to the logging handlers from outside the MT
        # Forces the logging handlers to flush before continuing and leaving late console messages
        for handler in logger.handlers:
            handler.flush()


def __inundate_in_huc(
    rem_array: np.ndarray,
    catchments_array: np.ndarray,
    depth_rst: rasterio.io.DatasetWriter,
    inundation_rst: rasterio.io.DatasetWriter,
    # hucCode: int,
    catchment_stages_dict: typed.Dict,
    depths_raster_path: str,
    inundation_raster_path: str,
    # verbose: Optional[bool] = False,
    window: Optional[bool] = None,
    inundation_nodata: Optional[int] = None,  # never will be None, Should min be zero?
    min_value=30,
) -> Tuple[str, str]:
    # ) -> Tuple[str, str, str]:
    """
        Inundate within the chosen scope

        Parameters
        ----------
        rem_array : np.ndarray
            File path to or rasterio dataset reader of Relative Elevation Model raster.
        catchments_array : np.ndarray
            File path to or rasterio dataset reader of Catchments raster.
        depth_rst : rasterio.io.DatasetWriter
            Dataset to write depth data to
        inundation_rst : rasterio.io.DatasetWriter
            Dataset to write inundation extent to
        # hucCode : str
        #     Catchment processing unit to inundate
        catchment_stages_dict : typed.Dict
            Numba compatible dictionary with HydroID as a key and flood stage as a value
        depths_raster_path : str
            Name of inundation depth dataset
        inundation_raster_path : str
            Name of inundation extent dataset
        # verbose : Optional[bool], default = None
        #     Whether to supress printed output
        window : Optional[bool], default = None
            Whether to use window memory optimization
        inundation_nodata : Optional[int], default = None
            Value for inundation extent nodata

        Returns
        -------
    #     Tuple[str, str, str]
    #         Name of depth raster, inundation extent raster, and inundation polygons (could be None)
        Tuple[str, str ]
            Name of depth raster, inundation extent raster (either could None, but not both)

    """

    # if depths_raster_path is not None:
    #     logging.debug(f"inundating for Depth raster of {depths_raster_path}")
    # else:
    #     logging.debug(f"inundating for iundation raster of {inundation_raster_path}")

    # verbose print
    # if hucCode is not None:
    #     __vprint("Inundating {} ...".format(hucCode), not verbose)

    # logging.debug(f"catchment_stages_dict count inside __inundate is {len(catchment_stages_dict)} for {inundation_raster_path}")


    rem, catchments = __go_fast_mapping(
        rem_array,
        catchments_array,
        catchment_stages_dict,
        rem_array.shape[1],
        rem_array.shape[0],
        inundation_nodata,
        min_value,
    )

    if depths_raster_path is not None:
        # logging.debug(f"Writing depths_  to {inundation_raster_path}")
        depth_rst.write(rem, window=window, indexes=1)

    if inundation_raster_path is not None:
        # logging.debug(f"Writing inundation_rst  to {inundation_raster_path}")
        inundation_rst.write(catchments, window=window, indexes=1)

    # return inundation_raster_path, depths_raster_path, None
    # Aug 2026: This is a little weird, but ok
    return inundation_raster_path, depths_raster_path


@njit(nogil=True, fastmath=True, cache=True)
def __go_fast_mapping(
    rem: np.ndarray,
    catchments: np.ndarray,
    catchment_stages_dict: typed.Dict,
    x: int,
    y: int,
    nodata_c: int,
    min_value: Union[int, float],
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Numba optimization for determining flood depth and flood

    Parameters
    ----------
    rem : np.ndarray
        Relative elevation model values which will be replaced by inundation depth values
    catchments : np.ndarray
        Rasterized catchments represented by HydroIDs to be replaced with inundation values
    catchment_stages_dict :  typed.Dict
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    x : int
        Shape of longitude coordinates
    y : int
        Shape of latitude coordinates
    nodata_c : int
        Nodata value to use for catchment values

    Returns
    -------
    Tuple[np.ndarray, np.ndarray]
        Arrays representing inundation depths and extents

    """
    # Iterate through each latitude and longitude
    for i in range(y):
        for j in range(x):
            # If catchments are nodata
            if catchments[i, j] != nodata_c:
                # catchments in stage dict
                if catchments[i, j] in catchment_stages_dict:

                    if rem[i, j] >= 0:

                        depth = catchment_stages_dict[catchments[i, j]] - rem[i, j]

                        # If the depth is greater than approximately 1/10th of a foot
                        if depth < min_value:
                            catchments[i, j] *= -1  # set HydroIDs to negative
                            rem[i, j] = 0
                        else:
                            rem[i, j] = depth
                    else:
                        rem[i, j] = 0
                        catchments[i, j] *= -1  # set HydroIDs to negative
                else:
                    rem[i, j] = 0
                    catchments[i, j] *= -1
            else:
                rem[i, j] = 0
                catchments[i, j] = nodata_c

    return rem, catchments


# This will always have exactly one huc
# Also, the code block that looks for "fossid" is a legacy from FIM3 and is no longer valid
# mask_type was always "filter", but was always overridden as "hucs" was always overridden
# earlier in the stack to be None, so most of this block was never used.
# This also makes catchment_poly un-necessary
# We do not need a huc value as this whole script can only handle one huc at a time, so we add it after generator
def __make_windows_generator(
    rem_rst: rasterio.io.DatasetReader,
    catchments_rst: rasterio.io.DatasetReader,
    # catchment_poly: Union[str, gpd.GeoDataFrame], # deprecated based on mask_type
    # mask_type: str,   but didn't work anyways (foss_fim column error) (fim3 column)
    catchment_stages_dict: typed.Dict,
    # verbose: bool,
    #    hucs: Optional[list] = None,
    #    hucSet: Optional[list] = None,
    windowed: Optional[bool] = False,
    # July 2026: The only script that passes in depth raster paths is interpolate_water_surface, but if
    # this was accidently changed to a string, that tool likely was not working.
    # depth_rst: Optional[str] = None,
    depth_rst: rasterio.io.DatasetReader = None,
    depths_raster_path: str = None,
    inundation_rst: rasterio.io.DatasetReader = None,
    inundation_nodata: Optional[int] = None,
    inundation_raster_path: str = None,
    min_value: int = 30,
):
    """
    Generator to split processing in to windows or different masked datasets

    Parameters
    ----------
    rem : DatasetReader
        Relative elevation model raster dataset
    catchments : DatasetReader
        Rasterized catchments represented by HydoIDs dataset
    # catchment_poly: Union[str, gpd.GeoDataFrame]
    #     File name or GeoDataFrame containing catchment polygon data
    # mask_type: str
    #     Specifies what type of mask procedure to use
    catchmentStagesDict : numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    inundation_raster : str
        Name of inundation extent raster to output
    depths : str
        Name of inundation depth raster to output
    verbose : bool
        Whether to suppress printed output or run in verbose mode
    # hucs : Optional[list], default = None
    #     HUC values to process
    # hucSet : Optional[list], default=None
    #     Prefixes of HUC to look for and process
    windowed: Optional[bool], default = False
        Whether to use memory optimized windows
    depths_raster_path: Optional[str], default = None
        Name of depth raster to output
    inundation_raster_path: Optional[str] = None
        Name of inundation raster to output
    inundation_nodata: Optional[int] = None
        Value of nodata value in inundation extent

    Returns
    -------
    Tuple of rioxarray Datasets/DataArrays and other data
    rem_array : np.ndarray
        Either full or masked dataset
    catchments_array : np.ndarray
        Either full or masked dataset
    depth_rst : rasterio.io.DatasetWriter
        Dataset to write depth data to
    inundation_rst : rasterio.io.DatasetWriter
        Dataset to write inundation extent data to
    # hucCode : str
    #     Code representing the huc processing unit
    catchmentStagesDict : typed.Dict
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    depths : str
        Name of inundation depth raster to output
    inundation_raster : str
        Name of inundation extent raster to output
    verbose: bool
        Whether to suppress printed output or run in verbose mode
    window : bool
        Whether to use memory optimization
    inundation_nodata : int
        Value for inundation extent nodata

    """

    # With this now being one and exactly one huc and no subset, most of this function is not needed
    '''
    if hucs is not None:
        # get attribute name for HUC column
        for huc in hucs:
            for hucColName in huc['properties'].keys():
                if 'HUC' in hucColName:
                    # hucSize = int(hucColName[-1])
                    break
            break

        # make windows
        for huc in hucs:
            # returns hucCode if current huc is in hucSet (at least starts with)
            def __return_huc_in_hucSet(hucCode, hucSet):
                for hs in hucSet:
                    if hs.startswith(hucCode):
                        return hucCode

                return None

            if __return_huc_in_hucSet(huc['properties'][hucColName], hucSet) is None:
                continue

            # Only came in with the value of "filter", so it always failed (foss_fim column never existed)
            # that is a fim3 carry over
            try:
                if mask_type == "huc":
                    # window = geometry_window(rem,shape(huc['geometry']))
                    rem_array, window_transform = mask(rem, shape(huc['geometry']), crop=True, indexes=1)
                    catchments_array = mask(catchments, shape(huc['geometry']), crop=True, indexes=1)
                elif mask_type == "filter":

                    if isinstance(catchment_poly, str):
                        catchment_poly = gpd.read_file(catchment_poly)
                    elif isinstance(catchment_poly, gpd.GeoDataFrame):
                        pass
                    elif isinstance(catchment_poly, None):
                        pass
                    else:
                        raise TypeError("Pass geopandas dataset or filepath for catchment polygons")

                    fossid = huc['properties']['fossid']
                    if catchment_poly.HydroID.dtype != 'str':
                        catchment_poly.HydroID = catchment_poly.HydroID.astype(str)
                    catchment_poly = catchment_poly[catchment_poly.HydroID.str.startswith(fossid)]

                    rem_array, window_transform = mask(rem, catchment_poly['geometry'], crop=True, indexes=1)
                    catchments_array, _ = mask(catchments, catchment_poly['geometry'], crop=True, indexes=1)
                    del catchment_poly
                elif mask_type is None:
                    pass
                else:
                    print("invalid mask type. Options are 'huc' or 'filter'")
            except ValueError:  # shape doesn't overlap raster
                continue  # skip to next HUC

            hucCode = huc['properties'][hucColName]

            yield {
                "rem_array": rem_array,
                "catchments_array": catchments_array,
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                "hucCode": hucCode,
                "catchmentStagesDict": catchmentStagesDict,
                "depths": depths,
                "inundation_raster": inundation_raster,
                "verbose": verbose,
                "window": None,
                "inundation_nodata": inundation_nodata,
                "min_value": min_value,
            }
    '''
    # This was never None and besides masking dropped out the need for the part above
    # else:
    #     hucCode = None

    if windowed is True:
        for __, window in rem_rst.block_windows():
            yield {
                "rem_array": rem_rst.read(1, window=window),
                "catchments_array": catchments_rst.read(1, window=window),
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                # "hucCode": hucCode,
                "catchment_stages_dict": catchment_stages_dict,
                "depths_raster_path": depths_raster_path,
                "inundation_raster_path": inundation_raster_path,
                # "verbose": verbose,
                "window": window,
                "inundation_nodata": inundation_nodata,
                "min_value": min_value,
            }
    else:
        yield {
            "rem_array": rem_rst.read(1),
            "catchments_array": catchments_rst.read(1),
            "depth_rst": depth_rst,
            "inundation_rst": inundation_rst,
            # "hucCode": hucCode,
            "catchment_stages_dict": catchment_stages_dict,
            "depths_raster_path": depths_raster_path,
            "inundation_raster_path": inundation_raster_path,
            # "verbose": verbose,
            "window": None,
            "inundation_nodata": inundation_nodata,
            "min_value": min_value,
        }


# Available via shared functions
# def __append_huc_code_to_file_name(fileName: str, hucCode: str) -> str:
#     """
#     Append huc code to a file name

#     Parameters
#     ----------
#     fileName : str
#         Name of the file
#     hucCode : str
#         HUC Code

#     Returns
#     -------
#     str
#         Filename with huc appended to the end
#     """
#     if hucCode is None:
#         return fileName

#     base_file_path, extension = splitext(fileName)

#     return "{}_{}{}".format(base_file_path, hucCode, extension)


def __subset_hydroTable_to_forecast(
    hydro_table_branch_df: pd.DataFrame,
    forecast_file_path: str,
    process_int16=True,
    precalb_option: bool = False,
) -> typed.Dict:
    """
    Subset hydrotable with forecast

    Parameters
    ----------
    hydro_table_branch_df: pd.DataFrame
    forecast_file_path: str
        Likely has more than this huc and branches
    process_int16: bool, default = True
        Whether to process inundation with int16 datatype

    Returns
    -------
    typed.Dict
        Numba catchment stages dictionary

    """
    # It is never a string
    # if isinstance(hydro_table_branch_df, str):
    #     htable_req_cols = [
    #         'HUC',
    #         'feature_id',
    #         'HydroID',
    #         'stage',
    #         'precalb_discharge_cms',
    #         'discharge_cms',
    #         'LakeID',
    #     ]
    #     file_ext = hydro_table_branch_df.split('.')[-1]
    #     if file_ext == 'csv':
    #         hydro_table_branch_df = pd.read_csv(
    #             hydro_table_branch_df,
    #             dtype={
    #                 'HUC': str,
    #                 'feature_id': str,
    #                 'HydroID': str,
    #                 'stage': float,
    #                 'precalb_discharge_cms': float,
    #                 'discharge_cms': float,
    #                 'LakeID': int,
    #                 'last_updated': object,
    #                 'submitter': object,
    #                 'obs_source': object,
    #             },
    #             low_memory=False,
    #             usecols=htable_req_cols,
    #         )
    #     elif file_ext == "feather":
    #         hydro_table_branch_df = pd.read_feather(hydro_table_branch_df, columns=htable_req_cols)
    #     # huc_error = hydroTable.HUC.unique()
    #     hydro_table_branch_df = hydro_table_branch_df.set_index(['HUC', 'feature_id', 'HydroID'])

    # elif isinstance(hydro_table_branch_df, pd.DataFrame):
    #     pass  # consider checking for correct dtypes, indices, and columns
    # else:
    #     raise TypeError("Pass path to hydro-table csv or Pandas DataFrame")

    hydro_table_branch_df = hydro_table_branch_df[
        hydro_table_branch_df["LakeID"] == -999
    ]  # Subset hydroTable to include only non-lake catchments.

    # raises error if hydroTable is empty due to all segments being lakes
    if hydro_table_branch_df.empty:
        raise sf.hydroTableHasOnlyLakes("All stream segments in HUC are within lake boundaries.")

    # if isinstance(forecast_file_path, str):
    # TODO: AUG 2026: We really do not need to keep reloading the forecast file over and over, just preload it
    # and pass it in. We can fix it later.
    try:
        forecast_file_path = pd.read_csv(forecast_file_path, dtype={'feature_id': str, 'discharge': float})
        forecast_file_path = forecast_file_path.set_index('feature_id')
    except UnicodeDecodeError:
        # If it fails with decodeing, we will try to load a different way
        forecast_file_path = read_nwm_forecast_file(forecast_file_path)

    # elif isinstance(forecast_file_path, pd.DataFrame):
    #     pass  # consider checking for dtypes, indices, and columns
    # else:
    #     raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    # if not hydro_table_branch_df.empty:
    # It tried to load it a second time.. (duplicate code)
    #     if isinstance(forecast_file_path, str):
    #         forecast_file_path = pd.read_csv(forecast_file_path, dtype={'feature_id': str, 'discharge': float})
    #         forecast_file_path = forecast_file_path.set_index('feature_id')
    #     elif isinstance(forecast_file_path, pd.DataFrame):
    #         pass  # consider checking for dtypes, indices, and columns
    #     else:
    #         raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    #     # susbset hucs if passed  (never used)
    #     if huc is not None:
    #         if isinstance(huc, list):
    #             if len(huc) == 1:
    #                 try:
    #                     huc = open(huc[0]).read().split('\n')
    #                 except FileNotFoundError:
    #                     pass
    #         elif isinstance(huc, str):
    #             try:
    #                 huc = open(huc).read().split('\n')
    #             except FileNotFoundError:
    #                 huc = [huc]

    #         # subsets HUCS
    #         subset_hucs_orig = huc.copy()
    #         huc = []
    #         for huc in np.unique(hydro_table_branch_df.index.get_level_values('HUC')):
    #             for sh in subset_hucs_orig:
    #                 if huc.startswith(sh):
    #                     huc += [huc]

    #         hydro_table_branch_df = hydro_table_branch_df[np.in1d(hydro_table_branch_df.index.get_level_values('HUC'), huc)]

    # join tables
    try:
        # We can not filter by huc or branch as the forecast file might have those columns
        # hydro_table_branch_df has already been filtered to branches
        hydro_table_branch_df = hydro_table_branch_df.join(forecast_file_path, on=['feature_id'], how='inner')
        hydro_table_branch_df = hydro_table_branch_df.reset_index()
    except AttributeError:
        raise sf.NoForecastFound("No forecast value found for the passed feature_ids in the Hydro-Table")
    except Exception as ex:
        raise ex  # yes. just re-raise... in theory, in why catch and rethrow? readability.

    # else:

    # initialize dictionary
    catchment_stages_dict = (
        typed.Dict.empty(types.int16, types.int16)
        if process_int16
        else typed.Dict.empty(types.int32, types.float32)
    )

    # print("------------------")
    # logging.info(hydro_table_branch_df.info())
    # logging.info(hydro_table_branch_df.index.names)
    # print("------------------")

    # interpolate stages
    for hid, sub_table in hydro_table_branch_df.groupby('HydroID'):
        if precalb_option:
            interpolated_stage = np.interp(
                sub_table.loc[:, 'discharge'].unique(),
                sub_table.loc[:, 'precalb_discharge_cms'],
                sub_table.loc[:, 'stage'],
            )
        else:
            interpolated_stage = np.interp(
                sub_table.loc[:, 'discharge'].unique(),
                sub_table.loc[:, 'discharge_cms'],
                sub_table.loc[:, 'stage'],
            )

        # add this interpolated stage to catchment stages dict
        h = round(interpolated_stage[0], 4)

        hid = types.int16(np.int16(str(hid)[4:])) if process_int16 else types.int32(hid)
        h = types.int16(np.round(h * 1000)) if process_int16 else types.float32(h)
        catchment_stages_dict[hid] = h

    # Can only ever be exactly one HUC, so hucset has no value
    # huc set
    # hucSet = [str(i) for i in hydro_table_branch_df.index.get_level_values('HUC').unique().to_list()]

    # logging.info("++++++++++++++")
    # logging.info(catchment_stages_dict)

    return catchment_stages_dict


def read_nwm_forecast_file(forecast_file, rename_headers: Optional[bool] = True) -> pd.DataFrame:
    """
    Reads NWM netcdf comp files and converts to forecast data frame

    Parameters
    ----------
    forecast_file: str
        Filepath for the forecast file
    rename_headers: Optional[bool], default = True
        Whether to rename the headers in the forecast file

    Returns
    -------
    pd.DataFrame
        Forecast DataFrame

    """

    flows_nc = xr.open_dataset(forecast_file, decode_cf='feature_id', engine='netcdf4')

    flows_df = flows_nc.to_dataframe()
    flows_df = flows_df.reset_index()

    flows_df = flows_df[['streamflow', 'feature_id']]

    if rename_headers:
        flows_df = flows_df.rename(columns={"streamflow": "discharge"})

    convert_dict = {'feature_id': str, 'discharge': float}
    flows_df = flows_df.astype(convert_dict)

    flows_df = flows_df.set_index('feature_id', drop=True)

    flows_df = flows_df.dropna()

    return flows_df


'''
def __vprint(message, verbose):

    if verbose:
        print(message)
'''

'''
def create_src_subset_csv(hydro_table: str, catchmentStagesDict: dict, src_table: str):
    """
    Create a subset synthetic rating curve table

    Parameters
    ----------
    hydro_table: str
        Filepath for synthetic rating curve
    catchmentStagesDict: dict
        Catchment stages dictionary
    src_table: str
        Output filepath for subset synthetic rating curve

    """
    src_df = pd.DataFrame.from_dict(catchmentStagesDict, orient='index')
    src_df = src_df.reset_index()
    src_df.columns = ['HydroID', 'stage_inund']
    htable_req_cols = ['HUC', 'feature_id', 'HydroID', 'stage', 'discharge_cms', 'LakeID']
    df_htable = pd.read_csv(
        hydro_table,
        dtype={
            'HydroID': int,
            'HUC': object,
            'branch_id': int,
            'last_updated': object,
            'submitter': object,
            'obs_source': object,
        },
        usecols=htable_req_cols,
    )
    df_htable = df_htable.merge(src_df, how='left', on='HydroID')
    df_htable['find_match'] = (df_htable['stage'] - df_htable['stage_inund']).abs()
    df_htable = df_htable.loc[df_htable.groupby('HydroID')['find_match'].idxmin()].reset_index(drop=True)
    df_htable.to_csv(src_table, index=False)
'''

# Jun 2026: This looks like it has not worked for a while.
# We want to come in via inundate_gms.py now anyways

# if __name__ == '__main__':
#     # parse arguments

# If we rebuild, it needs a number of changes including optionally add at logging system.
# See shared_functions.setup_file_logger
# Otherwise, a logger, either customized or by default will exist.

#     parser = argparse.ArgumentParser(
#         description='Rapid inundation mapping for FOSS FIM. Operates in single-HUC and batch modes.'
#     )
#     parser.add_argument(
#         '-r', '--rem', help='REM raster at job level or mosaic vrt. Must match catchments CRS.', required=True
#     )
#     parser.add_argument(
#         '-c',
#         '--catchments',
#         help='Catchments raster at job level or mosaic VRT. Must match rem CRS.',
#         required=True,
#     )
#     parser.add_argument('-b', '--catchment-poly', help='catchment_vector', required=True)
#     parser.add_argument('-t', '--hydro-table', help='Hydro-table in csv file format', required=True)
#     parser.add_argument('-f', '--forecast', help='Forecast discharges in CMS as CSV file', required=True)
#     parser.add_argument(
#         '-u',
#         '--hucs',
#         help='Batch mode only: HUCs file to process at. Must match CRS of input rasters',
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-l',
#         '--hucs-layerName',
#         help='Batch mode only. Layer name in HUCs file to use',
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-j',
#         '--num-workers',
#         help='Batch mode only. Number of concurrent processes',
#         required=False,
#         default=1,
#         type=int,
#     )
#     parser.add_argument(
#         '-s',
#         '--subset-hucs',
#         help="""Batch mode only. HUC code,
#             series of HUC codes (no quotes required), or line delimited of HUCs to run within
#             the hucs file that is passed""",
#         required=False,
#         default=None,
#         nargs='+',
#     )
#     parser.add_argument(
#         '-m',
#         '--mask-type',
#         help='Specify huc (FIM < 3) or filter (FIM >= 3) masking method',
#         required=False,
#         default="huc",
#     )
#     parser.add_argument(
#         '-a',
#         '--aggregate',
#         help="""Batch mode only. Aggregate outputs to VRT files.
#                         Currently, raises warning and sets to false if used.""",
#         required=False,
#         action='store_true',
#     )
#     parser.add_argument(
#         '-i',
#         '--inundation-raster',
#         help="""Inundation Raster output. Only writes if designated.
#                         Appends HUC code in batch mode.""",
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-p',
#         '--inundation-polygon',
#         help="""Inundation polygon output. Only writes if designated.
#                         Appends HUC code in batch mode.""",
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-d',
#         '--depths',
#         help="""Depths raster output. Only writes if designated.
#                         Appends HUC code in batch mode.""",
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-n',
#         '--src-table',
#         help="""Output table with the SRC lookup/interpolation.
#                         Only writes if designated. Appends HUC code in batch mode.""",
#         required=False,
#         default=None,
#     )
#     parser.add_argument(
#         '-q', '--verbose', help='verbose terminal output', required=False, default=False, action='store_true'
#     )

#     # extract to dictionary
#     args = vars(parser.parse_args())
#     # feature_id = 5253867
