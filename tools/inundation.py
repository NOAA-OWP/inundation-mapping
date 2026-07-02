#!/usr/bin/env python3
import logging
import os
import traceback
from datetime import datetime
# from os.path import splitext
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
os.environ["GDAL_CACHEMAX"] = "0"


# NOTE: Jun 2026: num_workers (n/a), hucs, hucs_layerName and subset_hucs was never used and was removed.
# Nothing came directly to inundate
# This inundates just one huc and its branch at a time, but is part of an MT coming from inundate_gms
# See notes in__make_windows_generator function about catchments_poly_path and mask_type arg
# subset_hucs arg is no longer relevant as only one HUC and its branches are processed here for
# performance and memory reasons.
# At this time, inundate is called via the ThreadPool in inundate_gms only.
# Catchments poly path does not seem to have been used for quite a while.
# Proof of it not working for quite a while is that is is looking for a column named 'foss_id' and would
# error when it did not find it. That also was part of the validaton that mask_type was never used.
# The only thing that came straight to inundate was the fim3 version of run_test_case. Everything else
# is through inundate_gms.
def inundate(
    huc: str,
    branch_id: int,
    rem_branch_path: str,
    catchments_file_path: str,
    catchments_poly_path: str,
    hydro_table_path: str,
    flow_file_path: str,
    inundation_raster_path: str,
    depths_branch_raster_path: Optional[str] = None,
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
    catchments_poly_path : str
        File path. Must have the same CRS as REM raster
    hydro_table_path : str
        File path to hydro-table csv.
    flow_file_path : str
        File path.
    inundation_branch_raster_path : str
        Path to inundation raster output.
    depths_branch_raster_path : Optional[str], default=None
        Path to optional depths raster output.
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
    
    # commented out as it fills the logs heavily
    if verbose:
        logging.info(f"Start Inundating for {huc}-{branch_id} - {flow_file_path}")
    else:
        logging.debug(f"Start Inundating for {huc}-{branch_id} - {flow_file_path}")

    if huc is None or huc == "":
        raise Exception("huc value can not be None or empty")

    if not os.path.isfile(rem_branch_path):
        raise Exception(f"Rem file of {rem_branch_path} does not exist")

    if not os.path.isfile(catchments_file_path):
        raise Exception(f"Catchments file of {catchments_file_path} does not exist")

    # catchment stages dictionary
    if hydro_table_path is None or hydro_table_path == "":
        raise TypeError("Pass hydro table csv")

    inun_data = None
    try:
        # input rem
        # Load then into memory data in order to close the rasterio connection earlier

        # catchment stages dictionary
        catchmentStagesDict = __subset_hydroTable_to_forecast(
            huc, hydro_table_path, flow_file_path, int_16, precalb_option
        )

        # TODO: Jun 2026: We open a bunch of rasters, they don't have very good scope control
        # and can be leaked. They are passed into a generator, zooiks
        catchments_rst = rasterio.open(catchments_file_path)
        rem_rst = rasterio.open(rem_branch_path) 

        # input rem
        # logger.debug(f"rem_path is {rem_branch_path} for {huc}/{branch_id}")

        # check for matching number of bands and single band only
        assert ((rem_rst.transform * (0, 0)) == (catchments_rst.transform * (0, 0))) & (
            (rem_rst.transform * (rem_rst.width, rem_rst.height))
            == (catchments_rst.transform * (catchments_rst.width, catchments_rst.height))
        ), "REM and catchments rasters require same upper left and lower right extents"

        depths_profile = rem_rst.profile.copy()
        inundation_profile = catchments_rst.profile.copy()
        int_16 = inundation_profile['dtype'] == 'int16'


        # TODO: Jun 2026: research this more. Does rasterio might want json args now, TBD
        # Jun 2026: Can't use blockxsize and blockysize (seeing as we are using COG GeoTiffs) ??
        depths_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True)
        inundation_profile.update(
            driver='GTiff', blockxsize=256, blockysize=256, tiled=True, nodata=0
        )

        # depths_profile.update(driver='GTiff', blocksize=256, tiled=True)
        # inundation_profile.update(driver='GTiff', blocksize=256, tiled=True, nodata=0)
        # depth_rst = rasterio.open(depths, "w+", **depths_profile) if depths is not None else None

        depth_rst = (
            rasterio.open(depths_branch_raster_path, "w+", **depths_profile)
            if depths_branch_raster_path is not None
            else None
        )
        inundation_rst = (
            rasterio.open(inundation_raster_path, "w+", **inundation_profile)
            if (inundation_raster_path is not None and inundation_profile is not None)
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
            catchmentStagesDict,
            inundation_rst,
            nodata,
            depth_rst,
            verbose,
            windowed=windowed,
            min_value=30 if int_16 else 0.03048,
        )

        inundation_rasters = []
        depth_rasters = []
        inundation_polys = []

        # Temporarily incurring serial processing
        # Jun 2026: inudation_polys are always returning None from __inudate_in_hucs
        # Left is anyways for now
        # This always comes back with a huge amount of dups, as in __go_fast_mapping
        # it was working with the CatchmentStageDict
        for wg in window_gen:
            future = __inundate_in_huc(**wg)
            inundation_rasters += [future[0]]
            depth_rasters += [future[1]]
            inundation_polys += [future[2]]

        if depth_rst is not None:
            depth_rst.close()
        if inundation_rst is not None:
            inundation_rst.close()

        # Jun 2026: in earlier versions for this, the three raster/polys columns had dozens
        # and dozens of dup records. I think it was one per catchments but the returned
        # inundation had the rollup raster names dozens fo times over.
        # However, when this was returned to inundate_gms, it took care of dups
        # Now, we just drop dups manually (again, in inundate_gms)
        # Just take the first rec of each of three objects to ger uniqueness
        # from window arg?

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

        if verbose:
            logging.info(f"Completed Inundating for {huc}-{branch_id} - {flow_file_path}")
        else:
            logging.debug(f"Completed Inundating for {huc}-{branch_id} - {flow_file_path}")
    
        return huc, branch_id, inun_data

    except (sf.hydroTableHasOnlyLakes, sf.NoForecastFound) as hex:
        error_type = type(hex).__name__
        logging.warning(f"{error_type} - Error while inundating for {huc}-{branch_id}")
        return huc, branch_id, None
    except Exception as ex:
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical(f"Critical Error while inundating for {flow_file_path} : {huc}-{branch_id}")
        logging.critical(traceback.format_exc())
        raise ex  # yes, re-raise


# Jun 2026: see various notes about hucCode. verbose temp not in use
def __inundate_in_huc(
    rem_array: np.ndarray,
    catchments_array: np.ndarray,
    depth_rst: rasterio.io.DatasetWriter,
    inundation_rst: rasterio.io.DatasetWriter,
    catchmentStagesDict: typed.Dict,
    depths_branch_raster_path: str,
    inundation_branch_raster_path: str,
    window: Optional[bool] = None,
    inundation_nodata: Optional[int] = None,
    min_value=30,
) -> Tuple[str, str, str]:
    
    # Note: return Tuple:
    #   is depth raster, inundation extent raster and inundation polygons
    #   but inundation polygons was hardcoded to None for some reason

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
    window : Optional[bool], default = None
        Whether to use window memory optimization
    inundation_nodata : Optional[int], default = None
        Value for inundation extent nodata

    Returns
    -------
    Tuple[str, str]
        depth raster, inundation extent raster (inundation polygons removed Jul 2, 2026: was always None)

    """
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
# and subset_hucs to None. 
# catchments_poly_path has not been used for a long time, possible FIM 3. The column of "foss_id"
# hasn't existed for a long time. Errors if passed in with a value in hucs, which was always set to None.
# And with the mask_type never having value (always None or path to the full huc.gpkg, it has no value either)
def __make_windows_generator(
    rem_rst: rasterio.io.DatasetReader,
    catchments_rst: rasterio.io.DatasetReader,
    catchmentStagesDict: typed.Dict,
    inundation_rst: rasterio.io.DatasetReader,
    inundation_nodata: Optional[int] = None,
    depth_rst: Optional[rasterio.io.DatasetReader] = None,
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
    inundation_rst: rasterio.io.DatasetReader
        rem loaded branch raster
    inundation_nodata: Optional[int] = None
        Value of nodata value in inundation extent
    depth_rst: Optional[str], default = None
        Name of depth raster to output
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
    window : bool
        Whether to use memory optimization
    inundation_nodata : int
        Value for inundation extent nodata

    """

    if windowed is True:
        for ij, window in rem_rst.block_windows():
            yield {
                "rem_array": rem_rst.read(1, window=window),
                "catchments_array": catchments_rst.read(1, window=window),
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                "catchmentStagesDict": catchmentStagesDict,
                "depths_branch_raster_path": depths_branch_raster_path,
                "inundation_branch_raster_path": inundation_branch_raster_path,
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
            "catchmentStagesDict": catchmentStagesDict,
            "depths_branch_raster_path": depths_branch_raster_path,
            "inundation_branch_raster_path": inundation_branch_raster_path,
            "window": None,
            "inundation_nodata": inundation_nodata,
            "min_value": min_value,
        }


# Note: subset_hucs no longer relavent as each huc is being processed one at a time now
# for performance and memory reasons
# By default, most of the hydrotables are from the branch level, but it is possible to pass
# a different hydrotable through the chain.
def __subset_hydroTable_to_forecast(
    huc: str,
    hydro_table_path: str,
    flow_file_path: str,
    process_int16=True,
    precalb_option: bool = False,
):
    """
    Subset hydrotable with forecast
    Note: logger not sent in as an arg. If you need a logger, add it as an arg.
    See example at __inundate_in_huc

    Parameters
    ----------
    huc: str
        Assumed to be zero padded
    hydroTable: str
        Filepath for the hydrotble file.  It is already the HUC version of the hydrotable
    flow_file_path : str
        Path to file with streamflow associated with feature id
    process_int16: bool, default = True
        Whether to process inundation with int16 datatype

    Returns
    -------
    typed.Dict
        Numba catchment stages dictionary

    """
    htable_req_cols = [
        'HUC',
        'feature_id',
        'HydroID',
        'stage',
        'precalb_discharge_cms',
        'discharge_cms',
        'LakeID',
    ]

    # ------------------------------
    # Load Hydrotable data first
    # The hydrotable may or may not be already filtered to a branch or huc
    hydro_table_df = pd.read_csv(
        hydro_table_path,
        dtype={
            'HUC': str,
            'feature_id': str,
            'HydroID': str,
            'stage': float,
            'precalb_discharge_cms': float,
            'discharge_cms': float,
            'LakeID': int,
        },
        low_memory=False,
        usecols=htable_req_cols,
        )
    
    if not os.path.exists(hydro_table_path):
        raise ValueError(f"{hydro_table_path} file is missing")

    if huc_hydrotable_df.empty:  # should not be empty at this point.
        raise Exception(f"{hydro_table_path} is Empty")

    if not 'HUC' in hydro_table_df.columns:
        raise ValueError(f"{hydro_table_path} is missing a column named 'HUC'")

    huc_hydrotable_df = hydro_table_df[hydro_table_df["HUC"] == huc]

    if huc_hydrotable_df.empty:
        raise ValueError(f"{hydro_table_path}'s HUC column does not have records for {huc}")

    huc_hydrotable_df = huc_hydrotable_df[
        huc_hydrotable_df["LakeID"] == -999
    ]  # Subset hydroTable to include only non-lake catchments.

    # raises error if hydroTable is empty due to all segments being lakes
    if huc_hydrotable_df.empty:
        raise sf.hydroTableHasOnlyLakes("All stream segments in HUC are within lake boundaries.")

        # huc_error = hydroTable.HUC.unique()
    huc_hydrotable_df = huc_hydrotable_df.set_index(['HUC', 'feature_id', 'HydroID'])

    # huc_error = hydroTable.HUC.unique()

    # Jun 2026: Moved test from inundate_gms
    if precalb_option:
        if "precalb_discharge_cms" not in huc_hydrotable_df.columns:
            raise ValueError("Missing expected column 'precalb_discharge_cms' in hydrotable.")
        missing_count = huc_hydrotable_df["precalb_discharge_cms"].isna().sum()
        if missing_count > 0:
            huc_hydrotable_df["precalb_discharge_cms"].fillna(
                huc_hydrotable_df["discharge_cms"], inplace=True
            )

    # ------------------------------
    # Now load the flow data and join df
    try:
        flow_file_df = pd.read_csv(flow_file_path, dtype={'feature_id': str, 'discharge': float})
        flow_file_df = flow_file_df.set_index('feature_id')
    except UnicodeDecodeError:
        flow_file_df = read_nwm_forecast_file(flow_file_path)

    # join tables
    try:
        huc_hydrotable_df = huc_hydrotable_df.join(flow_file_df, on=['feature_id'], how='inner')
    except AttributeError:
        raise sf.NoForecastFound(
            "No forecast values found for the passed feature_ids in the Hydro-Table for"
            f"huc of {huc} and forecast "
        )

    if huc_hydrotable_df.empty:
        raise Exception(f"Something went wrong joining {hydro_table_path} and {flow_file_path} on feature_id")

    # ------------------------------
    # initialize dictionary
    catchmentStagesDict = (
        typed.Dict.empty(types.int16, types.int16)
        if process_int16
        else typed.Dict.empty(types.int32, types.float32)
    )

    # interpolate stages
    for hid, sub_table in huc_hydrotable_df.groupby(level='HydroID'):
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

    return catchmentStagesDict


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
