#!/usr/bin/env python3

import argparse
import gc
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import splitext
from typing import Union
from warnings import warn

import fiona
import geopandas as gpd
import numba.typed.typeddict
import numpy as np
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from gval.homogenize.spatial_alignment import _matching_spatial_indices
from numba import njit, prange, typed, types
from rasterio.mask import mask
from shapely.geometry import shape


gpd.options.io_engine = "pyogrio"


class hydroTableHasOnlyLakes(Exception):
    """Raised when a Hydro-Table only has lakes"""

    pass


class NoForecastFound(Exception):
    """Raised when no forecast is available for a given Hydro-Table"""

    pass


def inundate(
    rem: Union[str, xr.DataArray, xr.Dataset],
    catchments: Union[str, xr.DataArray, xr.Dataset],
    catchment_poly,
    hydro_table,
    forecast,
    mask_type,
    hucs=None,
    hucs_layerName=None,
    subset_hucs=None,
    num_workers=1,
    aggregate=False,
    inundation_raster=None,
    depths=None,
    src_table=None,
    quiet: bool = False,
    windowed=False,
):
    """

    Run inundation on FIM >=3.0 outputs at job-level scale or aggregated scale

    Generate depths raster, inundation raster, and inundation polygon from FIM >=3.0 outputs.
    Can use the FIM 3.0 outputs at native HUC level or the aggregated products.
    Be sure to pass a HUCs file to process in batch mode if passing aggregated products.

    Parameters
    ----------
    rem : str or rioxarray DataSet or DataArray
        File path to or rasterio dataset reader of Relative Elevation Model raster.
        Must have the same CRS as catchments raster.
    catchments : str or rioxarray DataSet or DataArray
        File path to or rasterio dataset reader of Catchments raster. Must have the same CRS as REM raster
    hydro_table : str or pandas.DataFrame
        File path to hydro-table csv or Pandas DataFrame object with correct indices and columns.
    forecast : str or pandas.DataFrame
        File path to forecast csv or Pandas DataFrame with correct column names.
    hucs : str or fiona.Collection, optional
        Batch mode only. File path or fiona collection of vector polygons in HUC 4,6,or 8's to inundate on.
        Must have an attribute named as either "HUC4","HUC6", or "HUC8" with the associated values.
    hucs_layerName : str, optional
        Batch mode only. Layer name in hucs to use if multi-layer file is passed.
    subset_hucs : str or list of str, optional
        Batch mode only. File path to line delimited file, HUC string, or list of HUC strings to
        further subset hucs file for inundating.
    num_workers : int, optional
        Batch mode only. Number of workers to use in batch mode. Must be 1 or greater.
    aggregate : bool, optional
        Batch mode only. Aggregates output rasters to VRT mosaic files and merges polygons to single GPKG file
        Currently not functional. Raises warning and sets to false. On to-do list.
    inundation_raster : str, optional
        Path to optional inundation raster output. Appends HUC number if ran in batch mode.
    inundation_polygon : str, optional
        Path to optional inundation vector output. Only accepts GPKG right now.
        Appends HUC number if ran in batch mode.
    depths : str, optional
        Path to optional depths raster output. Appends HUC number if ran in batch mode.
    quiet : bool, optional
        Quiet output.

    Returns
    -------
    error_code : int
        Zero for successful completion.

    Raises
    ------
    TypeError
        Wrong input data types
    AssertionError
        Wrong input data types

    Warns
    -----
    warn
        if aggregrate set to true, will revert to false.

    Notes
    -----
    - Specifying a subset of the domain in rem or catchments to inundate on is achieved by the HUCs file or
        the forecast file.

    """

    # check for num_workers
    num_workers = int(num_workers)
    assert num_workers >= 1, "Number of workers should be 1 or greater"
    if (num_workers > 1) & (hucs is None):
        raise AssertionError("Pass a HUCs file to batch process inundation mapping")

        # check that aggregate is only done for hucs mode
    aggregate = bool(aggregate)
    if aggregate:
        warn("Aggregate feature currently not working. Setting to false for now.")
        aggregate = False
    if hucs is None:
        assert not aggregate, "Pass HUCs file if aggregation is desired"

    # bool quiet
    quiet = bool(quiet)

    # input rem
    if isinstance(rem, str):
        rem = rasterio.open(rem)
    elif isinstance(rem, xr.Dataset) or isinstance(rem, xr.DataArray):
        pass
    else:
        raise TypeError("Pass rioxarray Dataset/DataArray or filepath for rem")

    # input catchments grid
    if isinstance(catchments, str):
        catchments = rasterio.open(catchments)
    elif isinstance(catchments, xr.Dataset) or isinstance(catchments, xr.DataArray):
        pass
    else:
        raise TypeError("Pass rioxarray Dataset/DataArray or filepath for catchments")

    # check for matching number of bands and single band only
    assert ((rem.transform * (0, 0)) == (catchments.transform * (0, 0))) & (
        (rem.transform * (rem.width, rem.height))
        == (catchments.transform * (catchments.width, catchments.height))
    ), "REM and catchments rasters require same upper left and lower right extents"

    # open hucs
    if hucs is None:
        pass
    elif isinstance(hucs, str):
        hucs = fiona.open(hucs, 'r', layer=hucs_layerName)
    elif isinstance(hucs, fiona.Collection):
        pass
    else:
        raise TypeError("Pass fiona collection or filepath for hucs")

    # catchment stages dictionary
    if hydro_table is None:
        raise TypeError("Pass hydro table csv")

    # catchment stages dictionary
    if hydro_table is not None:
        catchmentStagesDict, hucSet = __subset_hydroTable_to_forecast(hydro_table, forecast, subset_hucs)
    else:
        raise TypeError("Pass hydro table csv")

    if catchmentStagesDict is not None:
        if src_table is not None:
            create_src_subset_csv(hydro_table, catchmentStagesDict, src_table)

        depths_profile = rem.profile
        inundation_profile = catchments.profile

        depths_profile.update(
            driver='GTiff',
            blockxsize=256,
            blockysize=256,
            tiled=True,
            # compress='lzw'
        )
        inundation_profile.update(
            driver='GTiff',
            blockxsize=256,
            blockysize=256,
            tiled=True,
            # compress='lzw'
            dtype='int8',
            nodata=0,
        )

        depth_rst = rasterio.open(depths, "w+", **depths_profile) if depths is not None else None
        inundation_rst = (
            rasterio.open(inundation_raster, "w+", **inundation_profile)
            if (inundation_profile is not None)
            else None
        )

        # make windows generator
        window_gen = __make_windows_generator(
            rem,
            catchments,
            catchment_poly,
            mask_type,
            catchmentStagesDict,
            inundation_raster,
            depths,
            quiet,
            hucs=hucs,
            hucSet=hucSet,
            windowed=windowed,
            depth_rst=depth_rst,
            inundation_rst=inundation_rst,
            depth_nodata=depths_profile['nodata'],
            inundation_nodata=inundation_profile['nodata'],
        )

        # executor = ThreadPoolExecutor(max_workers=num_workers)
        #
        # # submit jobs
        # results = {executor.submit(__inundate_in_huc, *wg): wg[6] for wg in window_gen}
        #
        inundation_rasters = []
        depth_rasters = []
        inundation_polys = []
        # for future in as_completed(results):
        #     try:
        #         future.result()
        #     except Exception as exc:
        #         __vprint("Exception {} for {}".format(exc, results[future]), not quiet)
        #     else:
        #         if results[future] is not None:
        #             __vprint("... {} complete".format(results[future]), not quiet)
        #         else:
        #             __vprint("... complete", not quiet)
        #
        #         inundation_rasters += [future.result()[0]]
        #         depth_rasters += [future.result()[1]]
        #         inundation_polys += [future.result()[2]]
        #
        # # power down pool
        # executor.shutdown(wait=True)

        # Temprorarily incurring serial processing
        for wg in window_gen:
            future = __inundate_in_huc(*wg)
            inundation_rasters += [future[0]]
            depth_rasters += [future[1]]
            inundation_polys += [future[2]]

        # power down pool
        # executor.shutdown(wait=True)
        depth_rst.close()
        inundation_rst.close()
    return inundation_rasters, depth_rasters, inundation_polys


def __inundate_in_huc(
    rem_array,
    catchments_array,
    depth_rst,
    inundation_rst,
    hucCode,
    catchmentStagesDict,
    depths,
    inundation_raster,
    quiet=False,
    window=None,
    depth_nodata=None,
    inundation_nodata=None,
):
    """
    Inundate within the chosen scope

    Parameters
    ----------
    rem : Union[str, xr.DataArray, xr.Dataset]
        File path to or rasterio dataset reader of Relative Elevation Model raster.
    catchments : Union[str, xr.DataArray, xr.Dataset]
        File path to or rasterio dataset reader of Catchments raster.
    hucCode : str
        Catchment processing unit to inundate
    catchmentStagesDict :  numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    depths : str
        Name of inundation depth dataset
    inundation_raster : str
        Name of inundation extent dataset
    quiet : bool
        Whether to supress printed output

    """
    # verbose print
    if hucCode is not None:
        __vprint("Inundating {} ...".format(hucCode), not quiet)

    # out_array = np.tile(np.int8(0), rem_array.shape)
    rem, catchments = __go_fast_mapping(
        rem_array,
        catchments_array,
        catchmentStagesDict,
        rem_array.shape[1],
        rem_array.shape[0],
        np.int16(depth_nodata),
        np.int16(inundation_nodata),
    )

    # print(inundation_raster, type(final_array[0][0]), np.max(final_array))

    if depths is not None:
        depth_rst.write(rem, window=window, indexes=1)

    if inundation_raster is not None:
        inundation_rst.write(catchments, window=window, indexes=1)

    return inundation_raster, depths, None


@njit(nogil=True, fastmath=True, parallel=True, cache=True)
def __go_fast_mapping(rem, catchments, catchmentStagesDict, x, y, nodata_r, nodata_c):
    """
    Numba optimization for determining flood depth and flood

    Parameters
    ----------
    rem : numpy array
        Relative elevation model values which will be replaced by inundation depth values
    catchments : numpy array
        Rasterized catchments represented by HydoIDs to be replaced with inundation values
    catchmentStagesDict :  numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    x : int
        Shape of longitude coordinates
    y : int
        Shape of latitude coordinates
    nodata_r : int
        Nodata value to use for depth values
    nodata_c : int
        Nodata value to use for catchment values

    """
    # Iterate through each latitude and longitude
    for i in prange(y):
        for j in prange(x):
            # If catchments are nodata
            if catchments[i, j] != nodata_c:
                # catchments in stage dict
                if catchments[i, j] in catchmentStagesDict:
                    # if elevation is zero or greater
                    if rem[i, j] >= 0:
                        depth = catchmentStagesDict[catchments[i, j]] - rem[i, j]

                        # If the depth is greater than approximately 1/10th of a foot
                        if depth < 30:
                            catchments[i, j] *= -1  # set HydroIDs to negative
                            rem[i, j] = 0
                        else:
                            rem[i, j] = depth
                    else:
                        rem[i, j] = 0
                        catchments[i, j] *= -1  # set HydroIDs to negative
                else:
                    rem[i, j] = 0
                    catchments[i, j] = nodata_c
            else:
                rem[i, j] = 0
                catchments[i, j] = nodata_c

    return rem, catchments


def __make_windows_generator(
    rem,
    catchments,
    catchment_poly,
    mask_type,
    catchmentStagesDict,
    inundation_raster,
    depths,
    quiet,
    hucs=None,
    hucSet=None,
    windowed=False,
    depth_rst=None,
    inundation_rst=None,
    depth_nodata=None,
    inundation_nodata=None,
):
    """
    Generator to split processing in to windows or different masked datasets

    Parameters
    ----------
    rem : rioxarray DataArray
        Relative elevation model raster dataset
    catchments : rioxarray DataArray
        Rasterized catchments represented by HydoIDs
    catchmentStagesDict : numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    inundation_raster : str
        Name of inundation extent raster to output
    depths : str
        Name of inundation depth raster to output
    quiet : bool
        Whether to suppress printed output or run in verbose mode
    hucs : list, optional
        HUC values to process
    hucSet : list, optional
        Prefixes of HUC to look for and process

    Returns
    -------
    Tuple of rioxarray Datasets/DataArrays and other data
    rem : rioxarray DataArray
        Either full or masked dataset
    catchments : rioxarray DataArray
        Either full or masked dataset
    hucCode : str
        Code representing the huc processing unit
    catchmentStagesDict : numba dictionary
        Numba compatible dictionary with HydroID as a key and flood stage as a value
    depths : str
        Name of inundation depth raster to output
    inundation_raster, : str
        Name of inundation extent raster to output
    quiet: bool
        Whether to suppress printed output or run in verbose mode

    """

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

            yield (
                rem_array,
                catchments_array,
                depth_rst,
                inundation_rst,
                hucCode,
                catchmentStagesDict,
                depths,
                inundation_raster,
                quiet,
                None,
                depth_nodata,
                inundation_nodata,
            )

    else:
        hucCode = None

        if windowed is True:
            for ij, window in rem.block_windows():

                yield (
                    rem.read(1, window=window),
                    catchments.read(1, window=window),
                    depth_rst,
                    inundation_rst,
                    hucCode,
                    catchmentStagesDict,
                    depths,
                    inundation_raster,
                    quiet,
                    window,
                    depth_nodata,
                    inundation_nodata,
                )
        else:

            yield (
                rem.read(1),
                catchments.read(1),
                depth_rst,
                inundation_rst,
                hucCode,
                catchmentStagesDict,
                depths,
                inundation_raster,
                quiet,
                None,
                depth_nodata,
                inundation_nodata,
            )


def __append_huc_code_to_file_name(fileName, hucCode):
    if hucCode is None:
        return fileName

    base_file_path, extension = splitext(fileName)

    return "{}_{}{}".format(base_file_path, hucCode, extension)


def __subset_hydroTable_to_forecast(hydroTable, forecast, subset_hucs=None):
    if isinstance(hydroTable, str):
        htable_req_cols = ['HUC', 'feature_id', 'HydroID', 'stage', 'discharge_cms', 'LakeID']
        file_ext = hydroTable.split('.')[-1]
        if file_ext == 'csv':
            hydroTable = pd.read_csv(
                hydroTable,
                dtype={
                    'HUC': str,
                    'feature_id': str,
                    'HydroID': str,
                    'stage': float,
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

        # susbset hucs if passed
        if subset_hucs is not None:
            if isinstance(subset_hucs, list):
                if len(subset_hucs) == 1:
                    try:
                        subset_hucs = open(subset_hucs[0]).read().split('\n')
                    except FileNotFoundError:
                        pass
            elif isinstance(subset_hucs, str):
                try:
                    subset_hucs = open(subset_hucs).read().split('\n')
                except FileNotFoundError:
                    subset_hucs = [subset_hucs]

            # subsets HUCS
            subset_hucs_orig = subset_hucs.copy()
            subset_hucs = []
            for huc in np.unique(hydroTable.index.get_level_values('HUC')):
                for sh in subset_hucs_orig:
                    if huc.startswith(sh):
                        subset_hucs += [huc]

            hydroTable = hydroTable[np.in1d(hydroTable.index.get_level_values('HUC'), subset_hucs)]

    # join tables
    try:
        hydroTable = hydroTable.join(forecast, on=['feature_id'], how='inner')
    except AttributeError:
        # print("FORECAST ERROR")
        raise NoForecastFound("No forecast value found for the passed feature_ids in the Hydro-Table")

    else:
        # initialize dictionary
        catchmentStagesDict = typed.Dict.empty(types.int16, types.uint16)

        # interpolate stages
        for hid, sub_table in hydroTable.groupby(level='HydroID'):
            interpolated_stage = np.interp(
                sub_table.loc[:, 'discharge'].unique(),
                sub_table.loc[:, 'discharge_cms'],
                sub_table.loc[:, 'stage'],
            )

            # add this interpolated stage to catchment stages dict
            h = round(interpolated_stage[0], 4)

            hid = types.int16(np.int16(str(hid)[4:]))
            h = types.int16(np.round(h * 1000))
            catchmentStagesDict[hid] = h

        # huc set
        hucSet = [str(i) for i in hydroTable.index.get_level_values('HUC').unique().to_list()]

        return catchmentStagesDict, hucSet


def read_nwm_forecast_file(forecast_file, rename_headers=True):
    """Reads NWM netcdf comp files and converts to forecast data frame"""

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


def __vprint(message, verbose):

    if verbose:
        print(message)


def create_src_subset_csv(hydro_table, catchmentStagesDict, src_table):
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


if __name__ == '__main__':
    # parse arguments
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
        '-j',
        '--num-workers',
        help='Batch mode only. Number of concurrent processes',
        required=False,
        default=1,
        type=int,
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
        '-q', '--quiet', help='Quiet terminal output', required=False, default=False, action='store_true'
    )

    # extract to dictionary
    args = vars(parser.parse_args())
    # feature_id = 5253867
