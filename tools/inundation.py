#!/usr/bin/env python3

import argparse
import os
from os.path import splitext
from typing import List, Optional, Tuple, Union
from warnings import warn

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from numba import njit, typed, types
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
    rem: Union[str, rasterio.io.DatasetReader],
    catchments: Union[str, rasterio.io.DatasetReader],
    catchment_poly: Union[str, pd.DataFrame],
    hydro_table: Union[str, pd.DataFrame],
    forecast: Union[str, pd.DataFrame],
    mask_type: Optional[Union[str, List[str]]] = None,
    hucs: Optional[Union[str, fiona.Collection]] = None,
    hucs_layerName: Optional[str] = None,
    subset_hucs: Optional[Union[str, List[str]]] = None,
    num_workers: Optional[int] = 1,
    aggregate: Optional[bool] = False,
    inundation_raster: Optional[str] = None,
    depths: Optional[str] = None,
    src_table: Optional[str] = None,
    quiet: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
) -> Tuple[List[str], List[str], List[str]]:

    # check for num_workers
    num_workers = int(num_workers)
    assert num_workers >= 1, "Number of workers should be 1 or greater"
    if (num_workers > 1) & (hucs is None):
        raise AssertionError("Pass a HUCs file to batch process inundation mapping")

    aggregate = bool(aggregate)
    if aggregate:
        warn("Aggregate feature currently not working. Setting to false for now.")
        aggregate = False
    if hucs is None:
        assert not aggregate, "Pass HUCs file if aggregation is desired"

    quiet = bool(quiet)

    # input rem
    if isinstance(rem, str):
        rem = rasterio.open(rem)
    elif isinstance(rem, rasterio.io.DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio DatasetReader or filepath for rem")

    # input catchments grid
    if isinstance(catchments, str):
        catchments = rasterio.open(catchments)
    elif isinstance(catchments, rasterio.io.DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio DatasetReader or filepath for catchments")

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

    if hydro_table is None:
        raise TypeError("Pass hydro table csv")

    depths_profile = rem.profile.copy()
    inundation_profile = catchments.profile.copy()

    int_16 = inundation_profile['dtype'] == 'int16'

    # catchment stages dictionary
    if hydro_table is not None:
        catchmentStagesDict, hucSet = __subset_hydroTable_to_forecast(
            hydro_table, forecast, subset_hucs, int_16, precalb_option
        )
    else:
        raise TypeError("Pass hydro table csv")

    if catchmentStagesDict is not None:
        if src_table is not None:
            create_src_subset_csv(hydro_table, catchmentStagesDict, src_table)

        depths_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True)

        # CRITICAL FIX: Set nodata=0 in inundation_profile BEFORE rasterio.open()
        inundation_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True, nodata=0)

        # Explicitly set out_nodata to 0 for the array writer
        out_nodata = 0
        nodata = np.int16(out_nodata) if int_16 else np.int32(out_nodata)

        depth_rst = rasterio.open(depths, "w+", **depths_profile) if depths is not None else None
        inundation_rst = (
            rasterio.open(inundation_raster, "w+", **inundation_profile)
            if (inundation_raster is not None and inundation_profile is not None)
            else None
        )

        # Match nodata output explicitly to inundation_profile['nodata'] (0)
        out_nodata = inundation_profile['nodata']
        nodata = np.int16(out_nodata) if int_16 else np.int32(out_nodata)

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
            inundation_nodata=nodata,
            min_value=30 if int_16 else 0.03048,
        )

        inundation_rasters = []
        depth_rasters = []
        inundation_polys = []

        for wg in window_gen:
            future = __inundate_in_huc(**wg)
            inundation_rasters += [future[0]]
            depth_rasters += [future[1]]
            inundation_polys += [future[2]]

        if depth_rst is not None:
            depth_rst.close()
        if inundation_rst is not None:
            inundation_rst.close()

    return inundation_rasters, depth_rasters, inundation_polys


def __inundate_in_huc(
    rem_array: np.ndarray,
    catchments_array: np.ndarray,
    depth_rst: rasterio.io.DatasetWriter,
    inundation_rst: rasterio.io.DatasetWriter,
    hucCode: int,
    catchmentStagesDict: typed.Dict,
    depths: str,
    inundation_raster: str,
    quiet: Optional[bool] = False,
    window: Optional[bool] = None,
    inundation_nodata: Optional[int] = None,
    min_value=30,
) -> Tuple[str, str, str]:

    if hucCode is not None:
        __vprint("Inundating {} ...".format(hucCode), not quiet)

    # EXACTLY 7 ARGUMENTS PASSED HERE
    rem, catchments = __go_fast_mapping(
        rem_array,
        catchments_array,
        catchmentStagesDict,
        rem_array.shape[1],
        rem_array.shape[0],
        inundation_nodata,
        min_value,
    )

    if depths is not None:
        depth_rst.write(rem, window=window, indexes=1)

    if inundation_raster is not None:
        inundation_rst.write(catchments, window=window, indexes=1)

    return inundation_raster, depths, None


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

    for i in range(y):
        for j in range(x):
            val = catchments[i, j]

            # Valid HydroID must be strictly positive and not equal to nodata
            if val > 0 and val != nodata_c:
                if val in catchment_stages_dict:

                    if rem[i, j] >= 0:
                        depth = catchment_stages_dict[val] - rem[i, j]

                        if depth < min_value:
                            catchments[i, j] = val * -1  # DRY (Negative HydroID)
                            rem[i, j] = 0
                        else:
                            rem[i, j] = depth  # WET (Positive HydroID)
                    else:
                        rem[i, j] = 0
                        catchments[i, j] = val * -1  # DRY
                else:
                    rem[i, j] = 0
                    catchments[i, j] = val * -1  # DRY
            else:
                rem[i, j] = 0
                catchments[i, j] = nodata_c  # Background padding set to nodata_c (0)

    return rem, catchments


def __make_windows_generator(
    rem: rasterio.io.DatasetReader,
    catchments: rasterio.io.DatasetReader,
    catchment_poly: Union[str, gpd.GeoDataFrame],
    mask_type: str,
    catchmentStagesDict: typed.Dict,
    inundation_raster: str,
    depths: str,
    quiet: bool,
    hucs: Optional[list] = None,
    hucSet: Optional[list] = None,
    windowed: Optional[bool] = False,
    depth_rst: Optional[str] = None,
    inundation_rst: Optional[str] = None,
    inundation_nodata: Optional[int] = None,
    min_value: int = 30,
):
    # Retrieve input raster's internal nodata dynamically
    catchment_src_nodata = catchments.nodata if catchments.nodata is not None else inundation_nodata

    if hucs is not None:
        for huc in hucs:
            # ... (HUC generator preamble) ...
            try:
                if mask_type == "huc":
                    rem_array, window_transform = mask(
                        rem, shape(huc['geometry']), crop=True, indexes=1, nodata=rem.nodata
                    )
                    catchments_array = mask(
                        catchments, shape(huc['geometry']), crop=True, indexes=1, nodata=inundation_nodata
                    )
                elif mask_type == "filter":
                    # ... (vector loading logic) ...
                    fossid = huc['properties']['fossid']
                    if catchment_poly.HydroID.dtype != 'str':
                        catchment_poly.HydroID = catchment_poly.HydroID.astype(str)
                    catchment_poly = catchment_poly[catchment_poly.HydroID.str.startswith(fossid)]

                    rem_array, window_transform = mask(
                        rem, catchment_poly['geometry'], crop=True, indexes=1, nodata=rem.nodata
                    )
                    # DYNAMIC: Mask window padding directly with the target inundation_nodata
                    catchments_array, _ = mask(
                        catchments, catchment_poly['geometry'], crop=True, indexes=1, nodata=inundation_nodata
                    )
                    del catchment_poly
            except ValueError:
                continue

            yield {
                "rem_array": rem_array,
                "catchments_array": catchments_array,
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                "hucCode": hucCode,
                "catchmentStagesDict": catchmentStagesDict,
                "depths": depths,
                "inundation_raster": inundation_raster,
                "quiet": quiet,
                "window": None,
                "inundation_nodata": inundation_nodata,  # Dynamic target nodata
                "catchment_src_nodata": catchment_src_nodata,  # Dynamic input source nodata
                "min_value": min_value,
            }
    else:
        hucCode = None

        if windowed is True:
            for ij, window in rem.block_windows():
                yield {
                    "rem_array": rem.read(1, window=window),
                    "catchments_array": catchments.read(1, window=window),
                    "depth_rst": depth_rst,
                    "inundation_rst": inundation_rst,
                    "hucCode": hucCode,
                    "catchmentStagesDict": catchmentStagesDict,
                    "depths": depths,
                    "inundation_raster": inundation_raster,
                    "quiet": quiet,
                    "window": window,
                    "inundation_nodata": inundation_nodata,
                    "min_value": min_value,
                }
        else:
            yield {
                "rem_array": rem.read(1),
                "catchments_array": catchments.read(1),
                "depth_rst": depth_rst,
                "inundation_rst": inundation_rst,
                "hucCode": hucCode,
                "catchmentStagesDict": catchmentStagesDict,
                "depths": depths,
                "inundation_raster": inundation_raster,
                "quiet": quiet,
                "window": None,
                "inundation_nodata": inundation_nodata,
                "min_value": min_value,
            }


def __append_huc_code_to_file_name(fileName: str, hucCode: str) -> str:
    if hucCode is None:
        return fileName

    base_file_path, extension = splitext(fileName)
    return "{}_{}{}".format(base_file_path, hucCode, extension)


def __subset_hydroTable_to_forecast(
    hydroTable: Union[str, pd.DataFrame],
    forecast: Union[str, pd.DataFrame],
    subset_hucs=None,
    process_int16=True,
    precalb_option: bool = False,
) -> Tuple[typed.Dict, List[str]]:

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

        hydroTable = hydroTable.set_index(['HUC', 'feature_id', 'HydroID'])

    elif isinstance(hydroTable, pd.DataFrame):
        pass
    else:
        raise TypeError("Pass path to hydro-table csv or Pandas DataFrame")

    hydroTable = hydroTable[hydroTable["LakeID"] == -999]

    if hydroTable.empty:
        raise hydroTableHasOnlyLakes("All stream segments in HUC are within lake boundaries.")

    if isinstance(forecast, str):
        try:
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        except UnicodeDecodeError:
            forecast = read_nwm_forecast_file(forecast)
    elif isinstance(forecast, pd.DataFrame):
        pass
    else:
        raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

    if not hydroTable.empty:
        if isinstance(forecast, str):
            forecast = pd.read_csv(forecast, dtype={'feature_id': str, 'discharge': float})
            forecast = forecast.set_index('feature_id')
        elif isinstance(forecast, pd.DataFrame):
            pass
        else:
            raise TypeError("Pass path to forecast file csv or Pandas DataFrame")

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

            subset_hucs_orig = subset_hucs.copy()
            subset_hucs = []
            for huc in np.unique(hydroTable.index.get_level_values('HUC')):
                for sh in subset_hucs_orig:
                    if huc.startswith(sh):
                        subset_hucs += [huc]

            hydroTable = hydroTable[np.in1d(hydroTable.index.get_level_values('HUC'), subset_hucs)]

    try:
        hydroTable = hydroTable.join(forecast, on=['feature_id'], how='inner')
    except AttributeError:
        raise NoForecastFound("No forecast value found for the passed feature_ids in the Hydro-Table")

    else:
        catchmentStagesDict = (
            typed.Dict.empty(types.int16, types.int16)
            if process_int16
            else typed.Dict.empty(types.int32, types.float32)
        )

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

            h = round(interpolated_stage[0], 4)

            hid = types.int16(np.int16(str(hid)[4:])) if process_int16 else types.int32(hid)
            h = types.int16(np.round(h * 1000)) if process_int16 else types.float32(h)
            catchmentStagesDict[hid] = h

        hucSet = [str(i) for i in hydroTable.index.get_level_values('HUC').unique().to_list()]

        return catchmentStagesDict, hucSet


def read_nwm_forecast_file(forecast_file, rename_headers: Optional[bool] = True) -> pd.DataFrame:
    flows_nc = xr.open_dataset(forecast_file, decode_cf='feature_id', engine='netcdf4')
    flows_df = flows_nc.to_dataframe().reset_index()
    flows_df = flows_df[['streamflow', 'feature_id']]

    if rename_headers:
        flows_df = flows_df.rename(columns={"streamflow": "discharge"})

    convert_dict = {'feature_id': str, 'discharge': float}
    flows_df = flows_df.astype(convert_dict).set_index('feature_id', drop=True).dropna()

    return flows_df


def __vprint(message, verbose):
    if verbose:
        print(message)


def create_src_subset_csv(hydro_table: str, catchmentStagesDict: dict, src_table: str):
    src_df = pd.DataFrame.from_dict(catchmentStagesDict, orient='index').reset_index()
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
    parser = argparse.ArgumentParser(
        description='Rapid inundation mapping for FOSS FIM. Operates in single-HUC and batch modes.'
    )
    parser.add_argument('-r', '--rem', help='REM raster at job level or mosaic vrt.', required=True)
    parser.add_argument('-c', '--catchments', help='Catchments raster.', required=True)
    parser.add_argument('-b', '--catchment-poly', help='catchment_vector', required=True)
    parser.add_argument('-t', '--hydro-table', help='Hydro-table csv', required=True)
    parser.add_argument('-f', '--forecast', help='Forecast CSV', required=True)
    parser.add_argument('-u', '--hucs', help='Batch mode HUCs file', required=False, default=None)
    parser.add_argument('-l', '--hucs-layerName', help='Layer name in HUCs', required=False, default=None)
    parser.add_argument('-j', '--num-workers', help='Number of workers', required=False, default=1, type=int)
    parser.add_argument('-s', '--subset-hucs', help='Subset HUCs', required=False, default=None, nargs='+')
    parser.add_argument('-m', '--mask-type', help='Specify mask type', required=False, default="huc")
    parser.add_argument('-a', '--aggregate', action='store_true')
    parser.add_argument('-i', '--inundation-raster', help='Inundation Raster', required=False, default=None)
    parser.add_argument('-p', '--inundation-polygon', help='Inundation Polygon', required=False, default=None)
    parser.add_argument('-d', '--depths', help='Depths raster', required=False, default=None)
    parser.add_argument('-n', '--src-table', help='SRC table output', required=False, default=None)
    parser.add_argument(
        '-q', '--quiet', help='Quiet output', required=False, default=False, action='store_true'
    )

    args = vars(parser.parse_args())
