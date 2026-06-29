#!/usr/bin/env python3
import argparse
import logging
import os
import traceback
from datetime import datetime
from os.path import splitext
from typing import List, Optional, Tuple, Union
from warnings import warn

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


class hydroTableHasOnlyLakes(Exception):
    """Raised when a Hydro-Table only has lakes"""

    pass


class NoForecastFound(Exception):
    """Raised when no forecast is available for a given Hydro-Table"""

    pass


# NOTE: Jun 2026: num_workers (n/a), hucs, hucs_layerName and subset_hucs was never used and was removed.
# Nothing came directly to inundate
# This inundates just one huc and its branch at a time, but is part of an MT coming from inundate_gms
# See notes in__make_windows_generator function about catchments_poly_path and mask_type arg
def inundate(
    huc: str,
    branch_id: int,
    rem_branch_path: str,
    catchments_file_path: str,
    catchments_poly_path: str,
    hydro_table: Union[str, pd.DataFrame],
    forecast_file_path: str,
    inundation_branch_raster_path: str,
    depths_branch_raster_path: Optional[str] = None,
    mask_type: Optional[Union[str, List[str]]] = None,
    verbose: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
):
    """
    Be sure to pass a HUCs file to process in batch mode if passing aggregated products.

    Parameters
    ----------
    huc: str
        Must have a valid huc in string format, already zero padded
    branch_id: int
        Must have the branch ID - can be zero (branch 0)
    rem_branch_path : str
        File path to the Relative Elevation Model raster.
        Must have the same CRS as catchments raster.
    catchments_file_path : str
        File path to the Catchments raster. Must have the same CRS as REM raster
    # catchments_poly_path : str
    #     File path. Must have the same CRS as REM raster
    hydro_table : str or pandas.DataFrame
        File path to hydro-table csv or Pandas DataFrame object with correct indices and columns.
        It should be the huc specific hydro_table
    forecast_file_path : str
        File path.
    # mask_type : Optional[str], default=None
    #    How to mask the datasets for processing inundation
    inundation_branch_raster_path : str
        Path to inundation raster output. Appends HUC number if ran in batch mode.
    depths_branch_raster_path : Optional[str], default=None
        Path to optional depths raster output. Appends HUC number if ran in batch mode.
    verbose : Optional[bool], default=False
        Quiet output.
    precalb_option : Optional[bool], default=False
        Whether to use precalb discharge in hydrotable. If True, will use precalb_discharge_cms column
    windowed : Optional[bool], default=False
        Memory efficient operation to process inundation

    Returns
    -------
    inun_data : dict (see return below)
        Can also return None

    Raises
    ------
    TypeError
        Wrong input data types
    AssertionError
        Wrong input data types

    Warns
    -----
    warn
        if aggregate set to true, will revert to false.

    Notes
    -----
    - Specifying a subset of the domain in rem or catchments to inundate on is achieved by the HUCs file or
        the forecast file.

    """
    # Let it pick up the default logger even if it was never set up or was created with special handlers.
    # When a logger is attached to, even it if is not set up, it goes to console only.
    # This a handle only. With inundate mostly being called from indundate_gms via a threadpool
    # this helps with managing logging collisions and a memory built up of the logger
    # Notice: it is called "logger" and not "logging".
    # If we need it in child classes, pass the "logger"
    logger = logging.getLogger()

    # commented out as it fills the logs heavily
    # if verbose:
    #     logging.info(f"Start Inundating for {huc} - {branch_id}")
    # else:
    #     logging.debug(f"Start Inundating for {huc} - {branch_id}")

    # Keep this off generally as it can create a TON of logs
    # logging.debug("+++++++++++++++++++++++++++++++")
    # logging.debug(f"Inundating based for {rem_path} - locals data")
    # logging.debug(locals())
    # logging.debug("+++++++++++++++++++++++++++++++")

    if huc is None or huc == "":
        raise Exception("huc value can not be None or empty")

    if not os.path.isfile(rem_branch_path):
        raise Exception(f"Rem file of {rem_branch_path} does not exist")

    if not os.path.isfile(catchments_file_path):
        raise Exception(f"Catchments file of {catchments_file_path} does not exist")

    # catchment stages dictionary
    if hydro_table is None:
        raise TypeError("Pass hydro table csv")

    inun_data = None

    try:
        # input rem
        # Load then into memory data in order to close the rasterio connection earlier

        # input rem
        # logger.debug(f"rem_path is {rem_branch_path} for {huc}/{branch_id}")
        with rasterio.open(rem_branch_path) as rem_rst, rasterio.open(catchments_file_path) as catchments_rst:

            # check for matching number of bands and single band only
            assert ((rem_rst.transform * (0, 0)) == (catchments_rst.transform * (0, 0))) & (
                (rem_rst.transform * (rem_rst.width, rem_rst.height))
                == (catchments_rst.transform * (catchments_rst.width, catchments_rst.height))
            ), "REM and catchments rasters require same upper left and lower right extents"

            depths_profile = rem_rst.profile.copy()
            inundation_profile = catchments_rst.profile.copy()

            # logger.debug(f"Depth Profile for {hucs} is {depths_profile} - pre update")
            # logger.debug(f"Depth inundation_profile for {hucs} is {inundation_profile} - pre update")

            int_16 = inundation_profile['dtype'] == 'int16'

            # catchment stages dictionary
            catchmentStagesDict, ___ = __subset_hydroTable_to_forecast(
                huc, hydro_table, forecast_file_path, int_16, precalb_option
            )

            if catchmentStagesDict is not None:

                # TODO: Jun 2026: research this more. Does rasterio might want json args now, TBD
                # Jun 2026: Can't use blockxsize and blockysize (seeing as we are using COG GeoTiffs) ??
                depths_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True)
                inundation_profile.update(
                    driver='GTiff', blockxsize=256, blockysize=256, tiled=True, nodata=0
                )

                # depths_profile.update(driver='GTiff', blocksize=256, tiled=True)
                # inundation_profile.update(driver='GTiff', blocksize=256, tiled=True, nodata=0)

                # logger.debug(f"Depth Profile for {hucs} is {depths_profile} - post update")
                # logger.debug(f"Depth inundation_profile for {hucs} is {inundation_profile} - post update")
                # logger.debug("*******************")

                # depth_rst = rasterio.open(depths, "w+", **depths_profile) if depths is not None else None

                depth_rst = (
                    rasterio.open(depths_branch_raster_path, "w+", **depths_profile)
                    if depths_branch_raster_path is not None
                    else None
                )
                inundation_rst = (
                    rasterio.open(inundation_branch_raster_path, "w+", **inundation_profile)
                    if (inundation_branch_raster_path is not None and inundation_profile is not None)
                    else None
                )

                nodata = (
                    np.int16(inundation_profile['nodata'])
                    if int_16
                    else np.int32(inundation_profile['nodata'])
                )

                # make windows generator
                # Jun 2026: See notes in the __make_windows_generator function.
                window_gen = __make_windows_generator(
                    rem_rst,
                    catchments_rst,
                    # catchments_poly_path,
                    catchmentStagesDict,
                    inundation_branch_raster_path,
                    inundation_rst,
                    nodata,
                    depths_branch_raster_path,
                    depth_rst,
                    verbose,
                    windowed=windowed,
                    min_value=30 if int_16 else 0.03048,
                )

                inundation_rasters = []
                depth_rasters = []
                inundation_polys = []

                # Temporarily incurring serial processing
                for wg in window_gen:
                    future = __inundate_in_huc(**wg)
                    inundation_rasters += [future[0]]
                    depth_rasters += [future[1]]
                    inundation_polys += [future[2]]

                if depth_rst is not None:
                    depth_rst.close()
                if inundation_rst is not None:
                    inundation_rst.close()

                # if verbose:
                #     logger.info(f"Done Inundating based on {forecast} and {rem_path}")
                # else:
                #     logger.debug(f"Done Inundating based on {forecast} and {rem_path}")

                # Jun 2026: in earlier versions for this, the three raster/polys columns had dozens
                # and dozens of dup records. I think it was one per catchments but the returned
                # inundation had the rollup raster names dozens fo times over.
                # However, when this was returned to inundate_gms, it took care of dups
                # Now, we just drop dups manually (again, in inundate_gms)
                # Just take the first rec of each of three objects to ger uniqueness

                inundation_rasters_file_name = None
                if len(inundation_rasters) > 0:
                    inundation_rasters_file_name = inundation_rasters[0]

                depth_rasters_file_name = None
                if len(depth_rasters) > 0:
                    depth_rasters_file_name = depth_rasters[0]

                inundation_polys_file_name = None
                if len(inundation_polys) > 0:
                    inundation_polys_file_name = inundation_polys[0]

                inun_data = {
                    "huc8": huc,
                    "branchID": branch_id,
                    "inundation_rasters": inundation_rasters_file_name,
                    "depths_rasters": depth_rasters_file_name,
                    "inundation_polygons": inundation_polys_file_name,
                }

        return inun_data

    except (hydroTableHasOnlyLakes, NoForecastFound) as hex:
        error_type = type(hex).__name__
        logger.warning(f"{error_type} - Error while inundating for {huc} / {branch_id}")
        return None
    except Exception as ex:
        logger.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logger.critical(f"Critical Error while inundating for {forecast_file_path} / {branch_id}")
        logger.critical(traceback.format_exc())
        raise ex  # yes, re-raise
    finally:
        # forces the logging handlers to flush before continuing and leaving late console messages
        for handler in logger.handlers:
            handler.flush()


# Jun 2026: see various notes about hucCode. verbose temp not in use
def __inundate_in_huc(
    rem_array: np.ndarray,
    catchments_array: np.ndarray,
    depth_rst: rasterio.io.DatasetWriter,
    inundation_rst: rasterio.io.DatasetWriter,
    # hucCode: str, Jun 2026: was always None
    catchmentStagesDict: typed.Dict,
    depths_branch_raster_path: str,
    inundation_branch_raster_path: str,
    verbose: Optional[bool] = False,
    window: Optional[bool] = None,
    inundation_nodata: Optional[int] = None,
    min_value=30,
    # logger = logging.Logger  # Jun 2026.. temp disabled, possibly perm
) -> Tuple[str, str, str]:
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
    catchmentStagesDict : typed.Dict
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    depths_branch_raster_path : str
        Name of inundation depth dataset
    inundation_raster : str
        Name of inundation extent dataset
    verbose : Optional[bool], default = False
        Whether to supress printed output
    window : Optional[bool], default = None
        Whether to use window memory optimization
    inundation_nodata : Optional[int], default = None
        Value for inundation extent nodata
    logger: The active logger. It may not have been explicitly set up but it does exist as
        it was created or attached to via inundate function.

    Returns
    -------
    Tuple[str, str, str]
        Name of depth raster, inundation extent raster, and inundation polygons (could be None)

    """
    # verbose print
    # if hucCode is not None:
    #     msg = "Inundating {} ...".format(hucCode)
    #     if verbose:
    #         logger.info(msg)
    #     else:
    #         logger.debug(msg)

    rem, catchments = __go_fast_mapping(
        rem_array,
        catchments_array,
        catchmentStagesDict,
        rem_array.shape[1],
        rem_array.shape[0],
        inundation_nodata,
        min_value,
    )

    if depths_branch_raster_path is not None:
        depth_rst.write(rem, window=window, indexes=1)

    if inundation_branch_raster_path is not None:
        inundation_rst.write(catchments, window=window, indexes=1)

    return inundation_branch_raster_path, depths_branch_raster_path, None


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


# Jun 2026: The only code that ever came to inundation.py came through inundate_gms.py. In the past,
# in the inundate_gms function and its generator, it always overrode the args of hucs, hucs_layerName
# and subset_hucs to None. This means the code block in here for "if hucs is not None:". was never used
# which was good as there is a bug in that code block that would have thrown an exception as the
# fossid column is not valid.
# With those columns now being invalid, it also means the catchments_poly (catchments_poly_path) arg
# and the mask_type column are also no longer needed
def __make_windows_generator(
    rem_rst: rasterio.io.DatasetReader,
    catchments_rst: rasterio.io.DatasetReader,
    # catchments_poly_path: str,
    # mask_type: str,
    catchmentStagesDict: typed.Dict,
    inundation_branch_raster_path: str,
    inundation_rst: rasterio.io.DatasetReader,
    inundation_nodata: Optional[int] = None,
    depths_branch_raster_path: Optional[str] = None,
    depth_rst: Optional[rasterio.io.DatasetReader] = None,
    # hucs: Optional[list] = None,
    verbose: Optional[bool] = False,
    windowed: Optional[bool] = False,
    min_value: int = 30,
):
    """
    Generator to split processing in to windows or different masked datasets

    Parameters
    ----------
    rem_rst : rasterio.io.DatasetReader
        rasterio dataset reader of Relative Elevation Model raster.
        Must have the same CRS as catchments raster.
    catchments_rst : rasterio.io.DatasetReader
        rasterio dataset reader of Catchments raster. Must have the same CRS as REM raster
    catchmentStagesDict : numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    inundation_branch_raster_path : str
        Name of inundation extent raster to output
    inundation_rst: rasterio.io.DatasetReader
        rem loaded branch raster
    inundation_nodata: Optional[int] = None
        Value of nodata value in inundation extent
    depths_branch_raster_path : str
        Name of inundation depth raster to output
    depth_rst: Optional[str], default = None
        Name of depth raster to output
    verbose : bool
        Whether to suppress printed output or run in verbose mode
    windowed: Optional[bool], default = False
        Whether to use memory optimized windows

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
    catchmentStagesDict : typed.Dict
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    depths : str
        Name of inundation depth raster to output
    inundation_raster : str
        Name of inundation extent raster to output
    quiet: bool
        Whether to suppress printed output or run in verbose mode
    window : bool
        Whether to use memory optimization
    inundation_nodata : int
        Value for inundation extent nodata

    """

    # if hucs is not None:
    #     # get attribute name for HUC column
    #     for huc in hucs:
    #         for hucColName in huc['properties'].keys():
    #             if 'HUC' in hucColName:
    #                 # hucSize = int(hucColName[-1])
    #                 break
    #         break

    #     # make windows
    #     for huc in hucs:
    #         # returns hucCode if current huc is in hucSet (at least starts with)
    #         def __return_huc_in_hucSet(hucCode, hucSet):
    #             for hs in hucSet:
    #                 if hs.startswith(hucCode):
    #                     return hucCode

    #             return None

    #         if __return_huc_in_hucSet(huc['properties'][hucColName], hucSet) is None:
    #             continue

    #         try:
    #             if mask_type == "huc":
    #                 # window = geometry_window(rem,shape(huc['geometry']))
    #                 rem_array, window_transform = mask(rem_rst, shape(huc['geometry']), crop=True, indexes=1)
    #                 catchments_array = mask(catchments_rst, shape(huc['geometry']), crop=True, indexes=1)
    #             elif mask_type == "filter":

    #                 catchment_poly = gpd.read_file(catchments_poly_path)

    #                 fossid = huc['properties']['fossid']
    #                 if catchment_poly.HydroID.dtype != 'str':
    #                     catchment_poly.HydroID = catchment_poly.HydroID.astype(str)
    #                 catchment_poly = catchment_poly[catchment_poly.HydroID.str.startswith(fossid)]

    #                 rem_array, window_transform = mask(rem_rst, catchment_poly['geometry'], crop=True, indexes=1)
    #                 catchments_array, _ = mask(catchments_rst, catchment_poly['geometry'], crop=True, indexes=1)
    #                 del catchment_poly
    #             elif mask_type is None:
    #                 pass
    #             else:
    #                 print("invalid mask type. Options are 'huc' or 'filter'")
    #         except ValueError:  # shape doesn't overlap raster
    #             continue  # skip to next HUC

    #         hucCode = huc['properties'][hucColName]

    #         yield {
    #             "rem_array": rem_array,
    #             "catchments_array": catchments_array,
    #             "depth_rst": depth_rst,
    #             "inundation_rst": inundation_rst,
    #             "hucCode": hucCode,
    #             "catchmentStagesDict": catchmentStagesDict,
    #             "depths": depths_branch_raster_path,
    #             "inundation_raster": inundation_branch_raster_path,
    #             "quiet": not verbose,
    #             "window": None,
    #             "inundation_nodata": inundation_nodata,
    #             "min_value": min_value,
    #         }
    # else:
    # hucCode = None

    if windowed is True:
        for ij, window in rem_rst.block_windows():
            yield {
                "rem_array": rem_rst.read(1, window=window),
                "catchments_array": catchments_rst.read(1, window=window),
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                # "hucCode": hucCode,
                "catchmentStagesDict": catchmentStagesDict,
                "depths_branch_raster_path": depths_branch_raster_path,
                "inundation_branch_raster_path": inundation_branch_raster_path,
                "verbose": verbose,
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
            "catchmentStagesDict": catchmentStagesDict,
            "depths_branch_raster_path": depths_branch_raster_path,
            "inundation_branch_raster_path": inundation_branch_raster_path,
            "verbose": verbose,
            "window": None,
            "inundation_nodata": inundation_nodata,
            "min_value": min_value,
        }


def __subset_hydroTable_to_forecast(
    huc: str,
    hydroTable: Union[str, pd.DataFrame],
    forecast: Union[str, pd.DataFrame],
    process_int16=True,
    precalb_option: bool = False,
) -> Tuple[typed.Dict, List[str]]:
    """
    Subset hydrotable with forecast
    Note: logger not sent in as an arg. If you need a logger, add it as an arg.
    See example at __inundate_in_huc

    Parameters
    ----------
    huc: str
        Assumed to be zero padded
    hydroTable: Union[str, pd.DataFrame]
        Filepath for the forecast file.  It is already the HUC version of the hydrotable
    forecast: Union[str, pd.DataFrame]
        Whether to rename the headers in the forecast file
    process_int16: bool, default = True
        Whether to process inundation with int16 datatype

    Returns
    -------
    Tuple[typed.Dict, List[str]]
        Numba catchment stages dictionary and list of hucs

    """
    if isinstance(hydroTable, str):

        htable_req_cols = [
            'HUC',
            'feature_id',
            'HydroID',
            'stage',
            'precalb_discharge_cms',
            'discharge_cms',
            'LakeID',
        ]
        file_ext = hydroTable.split('.')[-1]
        if file_ext == 'csv':
            hydroTable = pd.read_csv(
                hydroTable,
                dtype={
                    'HUC': str,
                    'feature_id': str,
                    'HydroID': str,
                    'stage': float,
                    'precalb_discharge_cms': float,
                    'discharge_cms': float,
                    'LakeID': int,
                    'last_updated': object,
                    'submitter': object,
                    'obs_source': object,
                },
                low_memory=False,
                usecols=htable_req_cols,
            )
        elif file_ext == "feather":
            hydroTable = pd.read_feather(hydroTable, columns=htable_req_cols)
        # huc_error = hydroTable.HUC.unique()
        hydroTable = hydroTable.set_index(['HUC', 'feature_id', 'HydroID'])

    elif isinstance(hydroTable, pd.DataFrame):
        pass  # consider checking for correct dtypes, indices, and columns
    else:
        raise TypeError("Pass path to hydro-table csv or Pandas DataFrame")

    hydroTable = hydroTable[
        hydroTable["LakeID"] == -999
    ]  # Subset hydroTable to include only non-lake catchments.

    # raises error if hydroTable is empty due to all segments being lakes
    if hydroTable.empty:
        raise hydroTableHasOnlyLakes("All stream segments in HUC are within lake boundaries.")

    if isinstance(forecast, str):
        try:
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        except UnicodeDecodeError:
            forecast = read_nwm_forecast_file(forecast)

    elif isinstance(forecast, pd.DataFrame):
        pass  # consider checking for dtypes, indices, and columns
    else:
        raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    if not hydroTable.empty:
        if isinstance(forecast, str):
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        elif isinstance(forecast, pd.DataFrame):
            pass  # consider checking for dtypes, indices, and columns
        else:
            raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    # join tables
    try:
        hydroTable = hydroTable.join(forecast, on=['feature_id'], how='inner')
    except AttributeError:
        raise NoForecastFound(
            "No forecast values found for the passed feature_ids in the Hydro-Table for"
            f"huc of {huc} and forecast "
        )

    else:  # more/less a "finally keyword" ???

        # initialize dictionary
        catchmentStagesDict = (
            typed.Dict.empty(types.int16, types.int16)
            if process_int16
            else typed.Dict.empty(types.int32, types.float32)
        )

        # interpolate stages
        for hid, sub_table in hydroTable.groupby(level='HydroID'):
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
            catchmentStagesDict[hid] = h

        # huc set
        hucSet = [str(i) for i in hydroTable.index.get_level_values('HUC').unique().to_list()]

        return catchmentStagesDict, hucSet


def read_nwm_forecast_file(forecast_file, rename_headers: Optional[bool] = True) -> pd.DataFrame:
    """
    Reads NWM netcdf comp files and converts to forecast data frame

    Note: logger not sent in as an arg. If you need a logger, add it as an arg.
    See example at __inundate_in_huc

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


# Jun 2026: commented out __main__ : does not appear to have worked for a while
# Consider using inundate_gms.py
'''
if __name__ == '__main__':
    # parse arguments

    # TODO: Jun 2026: This looks like it has not worked for a while.
    # We may want to consider removing it favor of coming in via inundate_gms.py

    # We may want to create a logger if coming in via command line. See shared_functions.setup_file_logger
    # Otherwise, a logger, either customized or by default will exist.

    parser = argparse.ArgumentParser(
        description='Rapid inundation mapping for FOSS FIM. Operates in single-HUC and batch modes.'
    )
    parser.add_argument(
        '-r', '--rem', help='REM raster at job level or mosaic vrt. Must match catchments CRS.', required=True
    )
    parser.add_argument(
        '-c',
        '--catchments',
        help='Catchments raster at job level or mosaic VRT. Must match rem CRS.',
        required=True,
    )
    parser.add_argument('-b', '--catchment-poly', help='catchment_vector', required=True)
    parser.add_argument('-t', '--hydro-table', help='Hydro-table in csv file format', required=True)
    parser.add_argument('-f', '--forecast', help='Forecast discharges in CMS as CSV file', required=True)
    parser.add_argument(
        '-u',
        '--hucs',
        help='Batch mode only: HUCs file to process at. Must match CRS of input rasters',
        required=False,
        default=None,
    )
    parser.add_argument(
        '-l',
        '--hucs-layerName',
        help='Batch mode only. Layer name in HUCs file to use',
        required=False,
        default=None,
    )
    parser.add_argument(
        '-s',
        '--subset-hucs',
        help="""Batch mode only. HUC code,
            series of HUC codes (no quotes required), or line delimited of HUCs to run within
            the hucs file that is passed""",
        required=False,
        default=None,
        nargs='+',
    )
    parser.add_argument(
        '-m',
        '--mask-type',
        help='Specify huc (FIM < 3) or filter (FIM >= 3) masking method',
        required=False,
        default="huc",
    )
    parser.add_argument(
        '-a',
        '--aggregate',
        help="""Batch mode only. Aggregate outputs to VRT files.
                        Currently, raises warning and sets to false if used.""",
        required=False,
        action='store_true',
    )
    parser.add_argument(
        '-i',
        '--inundation-raster',
        help="""Inundation Raster output. Only writes if designated.
                        Appends HUC code in batch mode.""",
        required=False,
        default=None,
    )
    parser.add_argument(
        '-p',
        '--inundation-polygon',
        help="""Inundation polygon output. Only writes if designated.
                        Appends HUC code in batch mode.""",
        required=False,
        default=None,
    )
    parser.add_argument(
        '-d',
        '--depths',
        help="""Depths raster output. Only writes if designated.
                        Appends HUC code in batch mode.""",
        required=False,
        default=None,
    )
    parser.add_argument(
        '-n',
        '--src-table',
        help="""Output table with the SRC lookup/interpolation.
                        Only writes if designated. Appends HUC code in batch mode.""",
        required=False,
        default=None,
    )
    parser.add_argument(
        '-vr', '--verbose', help='Quiet terminal output', required=False, default=False, action='store_true'
    )

    # extract to dictionary
    args = vars(parser.parse_args())
    # feature_id = 5253867
'''
