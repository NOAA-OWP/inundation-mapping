#!/usr/bin/env python3

import argparse
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import splitext
from warnings import warn

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from numba import njit, typed, types
from rasterio.features import shapes
from rasterio.io import DatasetReader, DatasetWriter
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
    rem,
    catchments,
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
    inundation_polygon=None,
    depths=None,
    out_raster_profile=None,
    out_vector_profile=None,
    src_table=None,
    quiet=False,
):
    """

    Run inundation on FIM >=3.0 outputs at job-level scale or aggregated scale

    Generate depths raster, inundation raster, and inundation polygon from FIM >=3.0 outputs.
    Can use the FIM 3.0 outputs at native HUC level or the aggregated products.
    Be sure to pass a HUCs file to process in batch mode if passing aggregated products.

    Parameters
    ----------
    rem : str or rasterio.DatasetReader
        File path to or rasterio dataset reader of Relative Elevation Model raster.
        Must have the same CRS as catchments raster.
    catchments : str or rasterio.DatasetReader
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
    out_raster_profile : str or dictionary, optional
        Override the default raster profile for outputs.
        See Rasterio profile documentation for more information.
    out_vector_profile : str or dictionary
        Override the default kwargs passed to fiona.Collection including crs, driver, and schema.
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
    elif isinstance(rem, DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio dataset or filepath for rem")

    # input catchments grid
    if isinstance(catchments, str):
        catchments = rasterio.open(catchments)
    elif isinstance(catchments, DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio dataset or filepath for catchments")

    # check for matching number of bands and single band only
    assert (
        rem.count == catchments.count == 1
    ), "REM and catchments rasters are required to be single band only"

    # check for matching raster sizes
    assert (rem.width == catchments.width) & (
        rem.height == catchments.height
    ), "REM and catchments rasters required same shape"

    # check for matching projections
    # assert (
    #     rem.crs.to_proj4() == catchments.crs.to_proj4()
    # ), "REM and Catchment rasters require same CRS definitions"

    # check for matching bounds
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

    # check for matching projections
    # assert (
    #     to_string(hucs.crs) == rem.crs.to_proj4() == catchments.crs.to_proj4()
    # ), "REM, Catchment, and HUCS CRS definitions must match"

    # catchment stages dictionary
    if hydro_table is not None:
        catchmentStagesDict, hucSet = __subset_hydroTable_to_forecast(hydro_table, forecast, subset_hucs)
    else:
        raise TypeError("Pass hydro table csv")

    if catchmentStagesDict is not None:
        if src_table is not None:
            create_src_subset_csv(hydro_table, catchmentStagesDict, src_table)

        # make windows generator
        window_gen = __make_windows_generator(
            rem,
            catchments,
            catchment_poly,
            mask_type,
            catchmentStagesDict,
            inundation_raster,
            inundation_polygon,
            depths,
            out_raster_profile,
            out_vector_profile,
            quiet,
            hucs=hucs,
            hucSet=hucSet,
        )

        # start up thread pool
        # executor = ThreadPoolExecutor(max_workers=num_workers)

        # start up thread pool
        # executor = ThreadPoolExecutor(max_workers=num_workers)

        # submit jobs
        # results = {executor.submit(__inundate_in_huc, *wg): wg[6] for wg in window_gen}

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
        for wg in window_gen:
            future = __inundate_in_huc(*wg)
            inundation_rasters += [future[0]]
            depth_rasters += [future[1]]
            inundation_polys += [future[2]]

        # power down pool
        # executor.shutdown(wait=True)

    # close datasets
    rem.close()
    catchments.close()

    return (inundation_rasters, depth_rasters, inundation_polys)


def __inundate_in_huc(
    rem_array,
    catchments_array,
    crs,
    window_transform,
    rem_profile,
    catchments_profile,
    hucCode,
    catchmentStagesDict,
    depths,
    inundation_raster,
    inundation_polygon,
    out_raster_profile,
    out_vector_profile,
    quiet,
):
    # verbose print
    if hucCode is not None:
        __vprint("Inundating {} ...".format(hucCode), not quiet)

    # save desired profiles for outputs
    depths_profile = rem_profile
    inundation_profile = catchments_profile

    # update output profiles from inputs
    if isinstance(out_raster_profile, dict):
        depths_profile.update(**out_raster_profile)
        inundation_profile.update(**out_raster_profile)
    elif out_raster_profile is None:
        depths_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True, compress='lzw')
        inundation_profile.update(driver='GTiff', blockxsize=256, blockysize=256, tiled=True, compress='lzw')
    else:
        raise TypeError("Pass dictionary for output raster profiles")

    # update profiles with width and heights from array sizes
    depths_profile.update(height=rem_array.shape[0], width=rem_array.shape[1])
    inundation_profile.update(height=catchments_array.shape[0], width=catchments_array.shape[1])

    # update transforms of outputs with window transform
    depths_profile.update(transform=window_transform)
    inundation_profile.update(transform=window_transform)
    # open output depths
    if isinstance(depths, str):
        depths = __append_huc_code_to_file_name(depths, hucCode)
        depths = rasterio.open(depths, "w", **depths_profile)
    elif isinstance(depths, DatasetWriter):
        pass
    elif depths is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output depths, or None.")

    # open output inundation raster
    if isinstance(inundation_raster, str):
        inundation_raster = __append_huc_code_to_file_name(inundation_raster, hucCode)
        inundation_raster = rasterio.open(inundation_raster, "w", **inundation_profile)
    elif isinstance(inundation_raster, DatasetWriter):
        pass
    elif inundation_raster is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output inundation raster, or None.")

    # prepare output inundation polygons schema
    if inundation_polygon is not None:
        if out_vector_profile is None:
            out_vector_profile = {'crs': crs, 'driver': 'GPKG'}

        out_vector_profile['schema'] = {
            'geometry': 'Polygon',
            'properties': OrderedDict([('HydroID', 'int')]),
        }

        # open output inundation polygons
        if isinstance(inundation_polygon, str):
            inundation_polygon = __append_huc_code_to_file_name(inundation_polygon, hucCode)
            inundation_polygon = fiona.open(inundation_polygon, 'w', **out_vector_profile)
        elif isinstance(inundation_polygon, fiona.Collection):
            pass
        else:
            raise TypeError("Pass fiona collection or file path as inundation_polygon")

    # save desired array shape
    desired_shape = rem_array.shape

    # flatten
    rem_array = rem_array.ravel()
    catchments_array = catchments_array.ravel()

    # create flat outputs
    depths_array = rem_array.copy()
    inundation_array = catchments_array.copy()

    # reset output values
    depths_array[depths_array != depths_profile['nodata']] = 0
    inundation_array[inundation_array != inundation_profile['nodata']] = (
        inundation_array[inundation_array != inundation_profile['nodata']] * -1
    )

    # make output arrays
    inundation_array, depths_array = __go_fast_mapping(
        rem_array, catchments_array, catchmentStagesDict, inundation_array, depths_array
    )

    # reshape output arrays
    inundation_array = inundation_array.reshape(desired_shape)
    depths_array = depths_array.reshape(desired_shape)

    # write out inundation and depth rasters
    if isinstance(inundation_raster, DatasetWriter):
        inundation_raster.write(inundation_array, indexes=1)
    if isinstance(depths, DatasetWriter):
        depths.write(depths_array, indexes=1)

    # polygonize inundation
    if isinstance(inundation_polygon, fiona.Collection):
        # make generator for inundation polygons

        inundation_polygon_generator = shapes(
            inundation_array, mask=inundation_array > 0, connectivity=8, transform=window_transform
        )

        # generate records
        records = []
        for i, (g, h) in enumerate(inundation_polygon_generator):
            record = dict()
            record['geometry'] = g
            record['properties'] = {'HydroID': int(h)}
            records += [record]

        # write out
        inundation_polygon.writerecords(records)

    if isinstance(depths, DatasetWriter):
        depths.close()
    if isinstance(inundation_raster, DatasetWriter):
        inundation_raster.close()
    if isinstance(inundation_polygon, fiona.Collection):
        inundation_polygon.close()
    # if isinstance(hucs,fiona.Collection): inundation_polygon.close()

    # return file names of outputs for aggregation. Handle Nones
    try:
        ir_name = inundation_raster.name
    except AttributeError:
        ir_name = None

    try:
        d_name = depths.name
    except AttributeError:
        d_name = None

    try:
        ip_name = inundation_polygon.path
    except AttributeError:
        ip_name = None

    # print(ir_name)
    # yield(ir_name,d_name,ip_name)

    if isinstance(depths, DatasetWriter):
        depths.close()
    if isinstance(inundation_raster, DatasetWriter):
        inundation_raster.close()
    if isinstance(inundation_polygon, fiona.Collection):
        inundation_polygon.close()

    return (ir_name, d_name, ip_name)


@njit
def __go_fast_mapping(rem, catchments, catchmentStagesDict, inundation, depths):
    for i, (r, cm) in enumerate(zip(rem, catchments)):
        if cm in catchmentStagesDict:
            if r >= 0:
                depth = catchmentStagesDict[cm] - r
                depths[i] = max(depth, 0)  # set negative depths to 0
            else:
                depths[i] = 0

            if depths[i] > 0:  # set positive depths to positive
                inundation[i] *= -1
            # else: # set positive depths to value of positive catchment value
            # inundation[i] = cm

    return (inundation, depths)


def __make_windows_generator(
    rem,
    catchments,
    catchment_poly,
    mask_type,
    catchmentStagesDict,
    inundation_raster,
    inundation_polygon,
    depths,
    out_raster_profile,
    out_vector_profile,
    quiet,
    hucs=None,
    hucSet=None,
):
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
                    rem_array, window_transform = mask(rem, [shape(huc['geometry'])], crop=True, indexes=1)
                    catchments_array, _ = mask(catchments, [shape(huc['geometry'])], crop=True, indexes=1)
                elif mask_type == "filter":
                    # input catchments polygon
                    if isinstance(catchment_poly, str):
                        catchment_poly = gpd.read_file(catchment_poly)
                    elif isinstance(catchment_poly, DatasetReader):
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
                rem.crs.wkt,
                window_transform,
                rem.profile,
                catchments.profile,
                hucCode,
                catchmentStagesDict,
                depths,
                inundation_raster,
                inundation_polygon,
                out_raster_profile,
                out_vector_profile,
                quiet,
            )

    else:
        hucCode = None
        # window = Window(col_off=0,row_off=0,width=rem.width,height=rem.height)

        yield (
            rem.read(1),
            catchments.read(1),
            rem.crs.wkt,
            rem.transform,
            rem.profile,
            catchments.profile,
            hucCode,
            catchmentStagesDict,
            depths,
            inundation_raster,
            inundation_polygon,
            out_raster_profile,
            out_vector_profile,
            quiet,
        )


def __append_huc_code_to_file_name(fileName, hucCode):
    if hucCode is None:
        return fileName

    base_file_path, extension = splitext(fileName)

    return "{}_{}{}".format(base_file_path, hucCode, extension)


def __subset_hydroTable_to_forecast(hydroTable, forecast, subset_hucs=None):
    if isinstance(hydroTable, str):
        htable_req_cols = ['HUC', 'feature_id', 'HydroID', 'stage', 'discharge_cms', 'LakeID']
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
        catchmentStagesDict = typed.Dict.empty(types.int32, types.float64)

        # interpolate stages
        for hid, sub_table in hydroTable.groupby(level='HydroID'):
            interpolated_stage = np.interp(
                sub_table.loc[:, 'discharge'].unique(),
                sub_table.loc[:, 'discharge_cms'],
                sub_table.loc[:, 'stage'],
            )

            # add this interpolated stage to catchment stages dict
            h = round(interpolated_stage[0], 4)

            hid = types.int32(hid)
            h = types.float32(h)
            catchmentStagesDict[hid] = h

        # huc set
        hucSet = [str(i) for i in hydroTable.index.get_level_values('HUC').unique().to_list()]

        return (catchmentStagesDict, hucSet)


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

    # call function
    inundate(**args)
