#!/usr/bin/env python3

# TODO: Aug 2026: Considering the volume of imports, consider moving some into specific functions
# to handle system wide load values. Any call made to any function here loads all of these
# Note: Rasterio now has a quasi-memory leak with python 3.12 and newer versions. It is not a true
# Memory leak but is much slower to put in the python GC (garbage collection) and can result in growing
# memory usage. One way to manage this is to put that import inside the fuction it needs.
# Note: Some of the imports are already inside key functions.
# import csv
import datetime as dt

# import gc
import json
import logging
import os
import pathlib
import psutil
import time
import traceback
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
import rioxarray as rxr
import urllib3
import xarray as xr
from dotenv import load_dotenv
from gval import CatStats
from rasterio import features
from rasterio.features import geometry_mask
from rasterio.warp import Resampling, calculate_default_transform, reproject
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, ReadTimeout
from shapely.geometry import MultiPolygon, Polygon, shape
from tools_shared_variables import (
    ACCEPTED_NAD27_SPELLINGS,
    ACCEPTED_NAD83_SPELLINGS,
    ACCEPTED_NAVD88_SPELLINGS,
    ACCEPTED_NGVD29_SPELLINGS,
    UNKNOWN_DATUM_SPELLINGS,
    WRDS_METADATA_COL_TYPES,
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
    acceptable_coord_acc_code_list,
    acceptable_coord_method_code_list,
    acceptable_site_type_list,
)
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry


gpd.options.io_engine = "pyogrio"


def correct_datum_typos(crs, vcs):
    """
    Check for typos in the horizontal datum data
    Temp workaround to handle incorrect WRDS horizontal datum entries. TODO: Remove once WRDS error is corrected?

    These rules were developed by looking at the various ways that the horizontal datum was misspelled in the WRDS data
    in March 2026. Should be periodically updated to make sure we capture additional "creative" spellings.

    Possible typos that we aren't currently accepting:
    - 'NGVD83' (because it mixes up NGVD29 and NAD83, so I'm not sure what it means)

    - 'NADVD88', 'NAD983', 'NAC83', 'NAVD27', NGVD1988, NAD84, they're just a bit far off from the correct values such that
      we aren't sure if they are typos or just something else. We can add them if we see them more
      in the future.

    Arguments
    ---------
    crs - str
        The horizontal datum to check for typos (or None).
    vcs -  str
        The vertical datum to check for typos (or None).

    Returns
    -------
    crs_corrected - str or None
        The corrected horizontal datum, if a typo was found, or None if no typos were found.
    vcs_corrected - str or None
        The corrected vertical datum, if a typo was found, or None if no typos were found.
    uncorrected_crs_error - bool
        True if a typo was found in the horizontal datum that we were unable to correct, False otherwise.
    uncorrected_vcs_error - bool
        True if a typo was found in the vertical datum that we were unable to correct, False otherwise.
    msgs - list of str
        A list of messages describing any typos that were found and how they were corrected, or any uncorrected errors.

    crs_corrected, vcs_corrected, uncorrected_crs_error, uncorrected_vcs_error, msgs = correct_datum_typos(crs, vcs)

    """

    # Preferred CRS or VCS values
    crs_nad27 = 'NAD27'
    crs_nad83 = 'NAD83'

    vcs_ngvd29 = 'NGVD29'
    vcs_navd88 = 'NAVD88'

    # Initialize values
    crs_corrected = None
    vcs_corrected = None
    uncorrected_crs_error = False
    uncorrected_vcs_error = False
    msgs = []

    known_crs_list = [crs_nad27, crs_nad83, 'WGS84', 'EPSG:4326']
    known_vcs_list = [vcs_ngvd29, vcs_navd88, 'LMSL']

    # ----
    # Check the CRS (horizontal datum) for typos (if it is not None or already an acceptable CRS)
    if crs is not None and crs.upper() not in known_crs_list:
        try:
            float(crs)
            numeric = True
        except ValueError:
            numeric = False

        # Fix misspelled CRSs that are actually NAD27
        if crs.upper() in ACCEPTED_NAD27_SPELLINGS:
            crs_corrected = crs_nad27
            msg = f"Changing CRS from {crs} to {crs_corrected}"
            msgs.append(msg)

        elif crs.upper() in ACCEPTED_NGVD29_SPELLINGS:
            crs_corrected = crs_nad27
            msg = f"Vertical datum supplied in lieu of horizontal CRS, changing {crs} to {crs_corrected}"
            msgs.append(msg)

        # Fix misspelled CRSs that are actually NAD83
        elif crs.upper() in ACCEPTED_NAD83_SPELLINGS:
            crs_corrected = crs_nad83
            msg = f"Changing CRS from {crs} to {crs_corrected}"
            msgs.append(msg)

        elif crs.upper() in ACCEPTED_NAVD88_SPELLINGS:
            crs_corrected = crs_nad83
            msg = f"Vertical datum supplied in lieu of horizontal CRS, changing {crs} to {crs_corrected}"
            msgs.append(msg)

        # Check if the CRS is a number
        elif numeric == True:
            msg = f"Unable to correct CRS, CRS is a number ({crs}) and not an acceptable value (i.e. NAD83)"
            msgs.append(msg)
            uncorrected_crs_error = True

        # Check if the CRS is unknown, blank, or other
        elif crs.upper() in UNKNOWN_DATUM_SPELLINGS:
            msg = "Unable to correct CRS, CRS is unknown and not an acceptable value (i.e. NAD83)"
            msgs.append(msg)
            uncorrected_crs_error = True

        # If the CRS is not a number, not unknown, and not a known misspelling, then we don't know what it is and we can't correct it
        else:
            msg = f"Unable to correct CRS, CRS is not an acceptable value (i.e. NAD83), does not match any known misspellings or other common issues. CRS:{crs}"
            msgs.append(msg)
            uncorrected_crs_error = True

    # else:
    # CRS is provided but is already in an acceptable format

    # -----
    # Check the VCS (vertical datum) for typos (if it is not None or already an acceptable VCS)
    if vcs is not None and vcs.upper() not in known_vcs_list:
        try:
            float(vcs)
            numeric = True
        except ValueError:
            numeric = False

        # Fix misspelled vertical datums that are actually NGVD29
        if vcs.upper() in ACCEPTED_NGVD29_SPELLINGS:
            vcs_corrected = vcs_ngvd29
            msg = f"Changing vertical datum from {vcs} to {vcs_corrected}"
            msgs.append(msg)

        elif vcs.upper() in ACCEPTED_NAD27_SPELLINGS:
            vcs_corrected = vcs_ngvd29
            msg = f"Horizontal CRS supplied in lieu of vertical datum, changing {vcs} to {vcs_corrected}"
            msgs.append(msg)

        # Fix misspelled CRSs that are actually NAVD88
        elif vcs.upper() in ACCEPTED_NAVD88_SPELLINGS:
            vcs_corrected = vcs_navd88
            msg = f"Changing vertical datum from {vcs} to {vcs_corrected}"
            msgs.append(msg)

        elif vcs.upper() in ACCEPTED_NAD83_SPELLINGS:
            vcs_corrected = vcs_navd88
            msg = f"Horizontal CRS supplied in lieu of vertical datum, changing {vcs} to {vcs_corrected}"
            msgs.append(msg)

        # Check if the VCS is a number
        elif numeric == True:
            msg = f"Typo found in vertical datum, vcs is a number ({vcs}) and not an acceptable value (i.e. NGVD29)"
            msgs.append(msg)
            uncorrected_vcs_error = True

        # Check if the VCS is unknown, blank, or other
        elif vcs.upper() in UNKNOWN_DATUM_SPELLINGS:
            msg = "Typo found in vertical datum, vcs is unknown and not an acceptable value (i.e. NGVD29)"
            msgs.append(msg)
            uncorrected_vcs_error = True

        # If the VCS is not a number, not unknown, and not a known misspelling, then we don't know what it is and we can't correct it
        else:
            msg = f"Unable to correct VCS, VCS is not an acceptable value (i.e. NGVD29), does not match any known misspellings or other common issues. VCS:{vcs}"
            msgs.append(msg)
            uncorrected_vcs_error = True

    # else:
    # VCS is provided but is already in an acceptable format

    return crs_corrected, vcs_corrected, uncorrected_crs_error, uncorrected_vcs_error, msgs


# TODO: Jun 2025: Change this to have a path to the config via an arg.
# See get_usgs_reting_curves for an example
def get_env_paths():
    load_dotenv()
    # import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, WBD_LAYER


def filter_nwm_segments_by_stream_order(unfiltered_segments, desired_order, nwm_flows_df):
    """
    This function uses the WRDS API to filter out NWM segments from a list if their stream order is different than
    the target stream order.

    Args:
        unfiltered_segments (list):  A list of NWM feature_id strings.
        desired_order (str): The desired stream order.
    Returns:
        filtered_segments (list): A list of NWM feature_id strings, paired down to only those that share the target order.

    """

    # feature ID of 0 is getting passed to WRDS and returns empty results,
    # which can cause failures on next()
    #    if '0' in unfiltered_segments:
    #        unfiltered_segments = unfiltered_segments.remove('0')
    #    if unfiltered_segments is None:
    #        return filtered_segments

    filtered_segments = []

    for feature_id in unfiltered_segments:
        try:
            stream_order = nwm_flows_df.loc[nwm_flows_df['ID'] == int(feature_id), 'order_'].values[0]

            if stream_order == desired_order:
                filtered_segments.append(feature_id)

        except Exception as e:
            # This isn't actually a big deal, it just means that the given feature ID does not
            # match any of the segments provided.
            print(f'WARNING: Exception occurred during filter_nwm_segments_by_stream_order():{e}')

    return filtered_segments


def filter_usgs_by_acceptance_criteria(input_df):
    '''
    This function filters a USGS dataframe according the acceptance criteria.

    Parameters
    ----------
    input_df: pd.DataFrame
        Dataframe of USGS data to be filtered. Must contain the following columns:
        - usgs_data_coord_accuracy_code
        - usgs_data_coord_method_code
        - usgs_data_alt_method_code
        - usgs_data_site_type
        - usgs_data_alt_accuracy_code

    Returns
    -------
    output_df: pd.DataFrame
        Filtered DataFrame of USGS data that meets the acceptance criteria.

    # TODO: Could add an option to adjust which acceptable codes are used, but for now this is hardcoded.

    '''
    input_df['acceptable_codes'] = (
        input_df['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)
        & input_df['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)
        & input_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)
        & input_df['usgs_data_site_type'].isin(acceptable_site_type_list)
    )
    input_df = input_df.astype({'usgs_data_alt_accuracy_code': float})

    # the input_df is very fragmented at this point due the filter above. Make a temp unfragamented version
    # else, we get a PerformanceWarning: DataFrame is highly fragmented
    defrag_input_df = input_df.copy()

    defrag_input_df['acceptable_alt_error'] = np.where(
        defrag_input_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh, True, False
    )
    output_df = defrag_input_df[
        (defrag_input_df['acceptable_codes'] == True) & (defrag_input_df['acceptable_alt_error'] == True)
    ]

    return output_df


def mask_out_lakes(input_array, huc, raster_src, fim_run_dir):
    '''
    This function is used in CatFIM to mask out lakes from inundated tifs.

    Parameters
    ----------
    input_array: xarray DataArray
        DataArray with inundation values that need lakes removed
    huc: str
        HUC8 id (string), needed to get the correct lakes file
    raster_src: rasterio DatasetReader
        src from a raster that should be uses for getting the correct raster dimensions
    fim_run_dir: str
        path to the fim run directory where the lakes shapefile is located

    Returns
    -------
    masked_array: xarray
        DataArray with lakes masked out
    mask_status: string,
        status of whether lake shapefile was available

    '''
    # Read in waterbodies geopackage
    preclip_lakes_path = os.path.join(fim_run_dir, huc, 'nwm_lakes_proj_subset.gpkg')

    if not os.path.exists(preclip_lakes_path):
        # If the lakes shapefile does not exist, return the input array without masking
        mask_status = (
            f'No lakes shapefile found at {preclip_lakes_path}, returning original array without lake masking'
        )
        return input_array, mask_status
    else:
        # Read in the lakes shapefile
        preclip_lakes_gdf = gpd.read_file(preclip_lakes_path)

        # Create a binary raster using the shapefile geometry
        lake_mask = geometry_mask(
            preclip_lakes_gdf.geometry,
            transform=raster_src.transform,
            invert=False,
            out_shape=(raster_src.height, raster_src.width),
        )

        # Set values within the lake geometry to zero, masking them out of the FIM
        masked_array = input_array * lake_mask
        mask_status = 'Lakes shapefile available, masked out lake'
        return masked_array, mask_status


def check_for_regression(
    stats_json_to_test, previous_version, previous_version_stats_json_path, regression_test_csv=None
):
    difference_dict = {}

    # Compare stats_csv to previous_version_stats_file
    stats_dict_to_test = json.load(open(stats_json_to_test))
    previous_version_stats_dict = json.load(open(previous_version_stats_json_path))

    for stat, value in stats_dict_to_test.items():
        previous_version_value = previous_version_stats_dict[stat]
        stat_value_diff = value - previous_version_value
        difference_dict.update({stat + '_diff': stat_value_diff})

    return difference_dict


def compute_contingency_stats_from_rasters(
    predicted_raster_path: str,
    benchmark_raster_path: str,
    agreement_raster: str = None,
    stats_csv: str = None,
    stats_json: str = None,
    mask_dict: dict = {},
):
    """
    This function contains FIM-specific logic to prepare raster datasets for use in the generic
    get_stats_table_from_binary_rasters() function. This function also calls the generic
    compute_stats_from_contingency_table() function and writes the results to CSV and/or JSON, depending on user input.

    Parameters
    ----------
    predicted_raster_path: str
        The path to the predicted, or modeled, FIM extent raster.
    benchmark_raster_path: str
        The path to the benchmark, or truth, FIM extent raster.
    agreement_raster: str, optional
        An agreement raster will be written to this path. 0: True Negatives, 1: False Negative, 2: False Positive,
        3: True Positive.
    stats_csv: str, optional
        Performance statistics will be written to this path. CSV allows for readability and other tabular processes.
    stats_json: str, optional
        Performance statistics will be written to this path. JSON allows for quick ingestion into Python dictionary
        in other processes.
    mask_dict: dict
        Dictionary with inclusionary and/or exclusionary masks asn options.

    Returns
    -------
    dict
        A dictionary of statistics produced by compute_stats_from_contingency_table(). Statistic names are keys and
        statistic values are the values.
    """

    # print('candidate', predicted_raster_path)
    # print('benchmark', benchmark_raster_path)

    # Get statistics table from two rasters.
    stats_dictionary = get_stats_table_from_binary_rasters(
        benchmark_raster_path, predicted_raster_path, agreement_raster, mask_dict=mask_dict
    )

    for stats_mode in stats_dictionary:
        # Write the mode_stats_dictionary to the stats_csv.
        if stats_csv != None:
            stats_csv = os.path.join(os.path.split(stats_csv)[0], stats_mode + '_stats.csv')
            df = pd.DataFrame.from_dict(stats_dictionary[stats_mode], orient="index", columns=['value'])
            df.to_csv(stats_csv)

        # Write the mode_stats_dictionary to the stats_json.
        if stats_json != None:
            stats_json = os.path.join(os.path.split(stats_csv)[0], stats_mode + '_stats.json')
            with open(stats_json, "w") as outfile:
                json.dump(stats_dictionary[stats_mode], outfile)

    return stats_dictionary


def profile_test_case_archive(archive_to_check, magnitude, stats_mode):
    """
    This function searches multiple directories and locates previously produced performance statistics.

    Args:
        archive_to_check (str): The directory path to search.
        magnitude (str): Because a benchmark dataset may have multiple magnitudes, this argument defines
                               which magnitude is to be used when searching for previous statistics.
    Returns:
        archive_dictionary (dict): A dictionary of available statistics for previous versions of the domain and magnitude.
                                  {version: {agreement_raster: agreement_raster_path, stats_csv: stats_csv_path, stats_json: stats_json_path}}
                                  *Will only add the paths to files that exist.

    """
    archive_dictionary = {}

    # List through previous version and check for available stats and maps. If available, add to dictionary.
    available_versions_list = os.listdir(archive_to_check)

    if len(available_versions_list) == 0:
        print("Cannot compare with -c flag because there are no data in the previous_versions directory.")
        return

    for version in available_versions_list:
        version_magnitude_dir = os.path.join(archive_to_check, version, magnitude)
        stats_json = os.path.join(version_magnitude_dir, stats_mode + '_stats.json')

        if os.path.exists(stats_json):
            archive_dictionary.update({version: {'stats_json': stats_json}})

    return archive_dictionary


def compute_stats_from_contingency_table(
    true_negatives, false_negatives, false_positives, true_positives, cell_area=None, masked_count=None
):
    """
    This generic function takes contingency table metrics as arguments and returns a dictionary of contingency table statistics.
    Much of the calculations below were taken from older Python files. This is evident in the inconsistent use of case.

    Parameters
    ----------
    true_negatives: int
        The true negatives from a contingency table.
    false_negatives: int
        The false negatives from a contingency table.
    false_positives: int
        The false positives from a contingency table.
    true_positives: int
        The true positives from a contingency table.
    cell_area: float, default = None
        This optional argument allows for area-based statistics to be calculated, in the case that contingency
        table metrics were derived from areal analysis.
    masked_count: int, default = None
        Amount of pixels masked out of array

    Returns
    -------
    dict
        A dictionary of statistics. Statistic names are keys and statistic values are the values.
        Refer to dictionary definition in bottom of function for statistic names.

    """

    vals, keys = CatStats.process_statistics(
        func_names="all", tp=true_positives, tn=true_negatives, fp=false_positives, fn=false_negatives
    )
    alt_keys = ['band', 'tn', 'fn', 'fp', 'tp']
    alt_vals = [1, true_negatives, false_negatives, false_positives, true_positives]

    for k, v in zip(alt_keys[::-1], alt_vals[::-1]):
        keys.insert(0, k)
        vals.insert(0, v)

    metrics_table = pd.DataFrame({x: [y] for x, y in zip(keys, vals)})

    return cross_walk_gval_fim(metric_df=metrics_table, cell_area=cell_area, masked_count=masked_count)


def cross_walk_gval_fim(metric_df: pd.DataFrame, cell_area: int, masked_count: int) -> dict:
    """
    Crosswalks metrics made from GVAL to standard FIM names and conventions

    Parameters
    ----------
    metric_df: pd.DataFrame
        Dataframe for getting
    cell_area: int
        Area in meters of squared resolution
    masked_count: int
        How many pixels are masked

    Returns
    -------
    dict
        Dictionary of statistical metrics
    """
    # Remove band entry
    metric_df = metric_df.iloc[:, 1:]

    # Dictionary to crosswalk column names
    crosswalk = {
        'tn': 'true_negatives_count',
        'fn': 'false_negatives_count',
        'fp': 'false_positives_count',
        'tp': 'true_positives_count',
        'accuracy': 'ACC',
        'balanced_accuracy': 'Bal_ACC',
        'critical_success_index': 'CSI',
        'equitable_threat_score': 'EQUITABLE_THREAT_SCORE',
        'f_score': 'F1_SCORE',
        'false_discovery_rate': 'FAR',
        'false_negative_rate': 'PND',
        'false_omission_rate': 'FALSE_OMISSION_RATE',
        'false_positive_rate': 'FALSE_POSITIVE_RATE',
        'fowlkes_mallows_index': 'FOWLKES_MALLOW_INDEX',
        'matthews_correlation_coefficient': 'MCC',
        'negative_likelihood_ratio': 'NEGATIVE_LIKELIHOOD_RATIO',
        'negative_predictive_value': 'NPV',
        'overall_bias': 'BIAS',
        'positive_likelihood_ratio': 'POSITIVE_LIKELIHOOD_RATIO',
        'positive_predictive_value': 'PPV',
        'prevalence': 'PREVALENCE',
        'prevalence_threshold': 'PREVALENCE_THRESHOLD',
        'true_negative_rate': 'TNR',
        'true_positive_rate': 'TPR',
    }

    metric_df.columns = [crosswalk[x] for x in metric_df.columns]

    # Build
    tn, fn, tp, fp = (
        metric_df['true_negatives_count'].values[0],
        metric_df['false_negatives_count'].values[0],
        metric_df['true_positives_count'].values[0],
        metric_df['false_positives_count'].values[0],
    )
    total_population = tn + fn + tp + fp
    metric_df['contingency_tot_count'] = total_population

    metric_df['TP_perc'] = (tp / total_population) * 100 if total_population > 0 else "NA"
    metric_df['FP_perc'] = (fp / total_population) * 100 if total_population > 0 else "NA"
    metric_df['TN_perc'] = (tn / total_population) * 100 if total_population > 0 else "NA"
    metric_df['FN_perc'] = (fn / total_population) * 100 if total_population > 0 else "NA"

    predPositive = tp + fp
    predNegative = tn + fn
    obsPositive = tp + fn
    obsNegative = tn + fp

    metric_df['cell_area_m2'] = cell_area
    sq_km_converter = 1000000

    # This checks if a cell_area has been provided, thus making areal calculations possible.
    metric_df['TP_area_km2'] = (tp * cell_area) / sq_km_converter if cell_area is not None else None
    metric_df['FP_area_km2'] = (fp * cell_area) / sq_km_converter if cell_area is not None else None
    metric_df['TN_area_km2'] = (tn * cell_area) / sq_km_converter if cell_area is not None else None
    metric_df['FN_area_km2'] = (fn * cell_area) / sq_km_converter if cell_area is not None else None
    metric_df['contingency_tot_area_km2'] = (
        (total_population * cell_area) / sq_km_converter if cell_area is not None else None
    )

    metric_df['predPositive_area_km2'] = (
        (predPositive * cell_area) / sq_km_converter if cell_area is not None else None
    )
    metric_df['predNegative_area_km2'] = (
        (predNegative * cell_area) / sq_km_converter if cell_area is not None else None
    )
    metric_df['obsPositive_area_km2'] = (
        (obsPositive * cell_area) / sq_km_converter if cell_area is not None else None
    )
    metric_df['obsNegative_area_km2'] = (
        (obsNegative * cell_area) / sq_km_converter if cell_area is not None else None
    )
    metric_df['positiveDiff_area_km2'] = (
        (metric_df['predPositive_area_km2'] - metric_df['obsPositive_area_km2'])[0]
        if cell_area is not None
        else None
    )

    total_pop_and_mask_pop = total_population + masked_count if masked_count > 0 else None
    metric_df['masked_count'] = masked_count if masked_count > 0 else 0
    metric_df['masked_perc'] = (masked_count / total_pop_and_mask_pop) * 100 if masked_count > 0 else 0
    metric_df['masked_area_km2'] = (masked_count * cell_area) / sq_km_converter if masked_count > 0 else 0
    metric_df['predPositive_perc'] = (predPositive / total_population) * 100 if total_population > 0 else "NA"
    metric_df['predNegative_perc'] = (predNegative / total_population) * 100 if total_population > 0 else "NA"
    metric_df['obsPositive_perc'] = (obsPositive / total_population) * 100 if total_population > 0 else "NA"
    metric_df['obsNegative_perc'] = (obsNegative / total_population) * 100 if total_population > 0 else "NA"
    metric_df['positiveDiff_perc'] = (
        metric_df['predPositive_perc'].values[0] - metric_df['obsPositive_perc'].values[0]
        if total_population > 0
        else "NA"
    )

    return {x: y for x, y in zip(metric_df.columns, metric_df.values[0])}


def get_stats_table_from_binary_rasters(
    benchmark_raster_path: str, candidate_raster_path: str, agreement_raster: str = None, mask_dict: dict = {}
):
    """
    Produces categorical statistics table from 2 rasters and returns it. Also exports an agreement raster classified as:
        0: True Negatives
        1: False Negative
        2: False Positive
        3: True Positive
        4: Masked
        10: Nodata

    Parameters
    ----------
    benchmark_raster_path: str
        Path to the binary benchmark raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.
    candidate_raster_path: str
        Path to the predicted raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.
    agreement_raster: str, default = None.
        Path to save agreement raster/s
    mask_dict : dict, default = {}
        Dictionary with inclusionary and/or exclusionary masks asn options.

    Returns
    -------
    dict
        A Python dictionary of a contingency table. Key/value pair formatted as:
        {true_negatives: int, false_negatives: int, false_positives: int, true_positives: int}

    """

    # Jun 2026: gc.collect() now can create memory deadlocks depending on what memory dependencies are currently in place
    # especially if it a shared pointer in an MP/MT condition. Let 'del' take care of it and also use "with" even if it
    # creates an

    # Jun 2026: gc.collect() now can create memory deadlocks depending on what memory dependencies are currently in place
    # especially if it a shared pointer in an MP/MT condition. Let 'del' take care of it and also use "with" even if it
    # creates an

    # Load benchmark and candidate data
    # Load it into a local memory object so it will release the rxr which holds the file open
    # which can create collisions if that file is in use via MP or MT
    try:
        # TODO: Jun 2026: As benchmark file is a heavily used file in alpha testing, we should look into
        # adjustments to be open/closed as quick as possible.
        # rioxarray and rasterio use lazyloaded and can keep a file open.

        with (
            rxr.open_rasterio(benchmark_raster_path) as benchmark_raster,
            rxr.open_rasterio(candidate_raster_path) as candidate_raster,
        ):
            cell_area = np.abs(np.prod(benchmark_raster.rio.resolution()))
            benchmark_raster.data = xr.where(
                benchmark_raster == benchmark_raster.rio.nodata, 10, benchmark_raster
            )
            benchmark_raster = benchmark_raster.rio.write_nodata(10)

            candidate_raster.data = xr.where(
                (candidate_raster != candidate_raster.rio.nodata) & (candidate_raster >= 0),
                1,
                candidate_raster,
            )
            candidate_raster.data = xr.where(
                (candidate_raster != candidate_raster.rio.nodata) & (candidate_raster < 0),
                0,
                candidate_raster,
            )
            candidate_raster.data = xr.where(
                candidate_raster == candidate_raster.rio.nodata, 10, candidate_raster
            )
            candidate_raster = candidate_raster.rio.write_nodata(10)

            pairing_dictionary = {
                (0, 0): 0,
                (0, 1): 1,
                (0, 10): 10,
                (1, 0): 2,
                (1, 1): 3,
                (1, 10): 5,
                (4, 0): 4,
                (4, 1): 4,
                (4, 10): 10,
                (10, 0): 10,
                (10, 1): 10,
                (10, 10): 10,
            }

            # Loop through exclusion masks and mask the agreement_array.
            all_masks_df = None
            if mask_dict != {}:
                for poly_layer in mask_dict:
                    operation = mask_dict[poly_layer]['operation']

                    if operation == 'exclude':
                        poly_path = mask_dict[poly_layer]['path']
                        buffer_val = (
                            0 if mask_dict[poly_layer]['buffer'] is None else mask_dict[poly_layer]['buffer']
                        )

                        # Read mask bounds with candidate boundary box
                        # NOTE: Jun 2026: our gpd is using pyogrio which auto uses pyarrow as a memory
                        # cache. Sometimes, if a file collision hits showing things like
                        # OSError: database disk image is malformed, you can bypass pyarrow. It will slow it
                        # down fractionlly, but should help with the file collisions.
                        # Adding use_arrow=False forces pyogrio to use Numpy arrays (so careful on python package
                        # versioning. (see Dockerfile, numpy version reload to lower versions. TBD))
                        poly_all = gpd.read_file(
                            poly_path, bbox=candidate_raster.rio.bounds(), use_arrow=False
                        )

                        # Make sure features are present in bounding box area before projecting.
                        # Continue to next layer if features are absent.
                        if poly_all.empty:
                            del poly_all
                            continue

                        # Project layer to reference crs.
                        poly_all_proj = poly_all.to_crs(candidate_raster.rio.crs)

                        # Buffer if buffer val exists
                        poly_all_proj = poly_all_proj.buffer(buffer_val) if buffer_val != 0 else poly_all_proj

                        if all_masks_df is not None and not all_masks_df.empty:
                            all_masks_df = pd.concat([all_masks_df, poly_all_proj])
                        else:
                            all_masks_df = poly_all_proj

                        del poly_all, poly_all_proj

            stats_table_dictionary = {}  # Initialize empty dictionary.

            c_aligned, b_aligned = candidate_raster.gval.homogenize(benchmark_raster, target_map="candidate")

        # Jun 2026: Note: raster.close is the most important to releaseing memory
        # calling del after it has little value as it just releases the pointer but it is still
        # in memory. And never, ever call gc.collect()
        # if candidate_raster is not None:
        #     candidate_raster.close()
        #     del candidate_raster
        # if benchmark_raster is not None:
        #     benchmark_raster.close()
        #     del benchmark_raster  # really don't need del, it does not do much

        agreement_map = c_aligned.gval.compute_agreement_map(
            b_aligned, comparison_function='pairing_dict', pairing_dict=pairing_dictionary
        )

        c_aligned.close()
        b_aligned.close()
        del c_aligned, b_aligned

        agreement_map_og = agreement_map.copy()
        agreement_map.rio.write_nodata(4, inplace=True)

        # Mask if mask_dict is provided
        if all_masks_df is not None and not all_masks_df.empty:
            agreement_map = agreement_map.rio.clip(all_masks_df['geometry'], invert=True)
            agreement_map.data = xr.where(
                agreement_map_og.sel({'x': agreement_map.coords['x'], 'y': agreement_map.coords['y']}) == 10,
                10,
                agreement_map,
            )

        crosstab_table = agreement_map.gval.compute_crosstab()

        metrics_table = crosstab_table.gval.compute_categorical_metrics(
            positive_categories=[1], negative_categories=[0], metrics="all"
        )

        # Only write the agreement raster if user-specified.
        if agreement_raster != None:
            agreement_map_write = agreement_map.rio.write_nodata(10, encoded=True)
            agreement_map_write.rio.to_raster(agreement_raster, dtype=np.uint8, driver="COG")
            agreement_map_write.close()
            del agreement_map_write

            # Write legend text file
            legend_txt = os.path.join(os.path.split(agreement_raster)[0], 'read_me.txt')

            now = dt.datetime.now()
            current_time = now.strftime("%m/%d/%Y %H:%M:%S")

            with open(legend_txt, 'w') as f:
                f.write("%s\n" % '0: True Negative')
                f.write("%s\n" % '1: False Negative')
                f.write("%s\n" % '2: False Positive')
                f.write("%s\n" % '3: True Positive')
                f.write(
                    "%s\n" % '4: Masked area (excluded from contingency table analysis). '
                    'Mask layers: {mask_dict}'.format(mask_dict=mask_dict)
                )
                f.write("%s\n" % 'Results produced at: {current_time}'.format(current_time=current_time))

        # Store summed pixel counts in dictionary.
        stats_table_dictionary.update(
            {
                'total_area': cross_walk_gval_fim(
                    metric_df=metrics_table, cell_area=cell_area, masked_count=np.sum(agreement_map.data == 4)
                )
            }
        )

        del crosstab_table, metrics_table

        # After agreement_array is masked with default mask layers, check for inclusion masks in mask_dict.
        if mask_dict != {}:
            for poly_layer in mask_dict:
                operation = mask_dict[poly_layer]['operation']

                if operation == 'include':
                    poly_path = mask_dict[poly_layer]['path']
                    buffer_val = (
                        0 if mask_dict[poly_layer]['buffer'] is None else mask_dict[poly_layer]['buffer']
                    )

                    # Read mask bounds with candidate boundary box
                    poly_all = gpd.read_file(poly_path, bbox=agreement_map.rio.bounds())

                    # Make sure features are present in bounding box area before projecting.
                    # Continue to next layer if features are absent.
                    if poly_all.empty:
                        del poly_all
                        continue

                    poly_all_proj = poly_all.to_crs(agreement_map.rio.crs)

                    # Buffer if buffer val exists
                    poly_all_proj = poly_all_proj.buffer(buffer_val) if buffer_val != 0 else poly_all_proj

                    poly_handle = poly_layer + '_b' + str(buffer_val) + 'm'

                    # Do analysis on inclusion masked area
                    agreement_map_include = agreement_map.rio.clip(poly_all_proj['geometry'])
                    agreement_map_include.data = xr.where(
                        agreement_map_og.sel(
                            {'x': agreement_map_include.coords['x'], 'y': agreement_map_include.coords['y']}
                        )
                        == 10,
                        10,
                        agreement_map_include,
                    )

                    crosstab_table = agreement_map_include.gval.compute_crosstab()

                    metrics_table = crosstab_table.gval.compute_categorical_metrics(
                        positive_categories=[1], negative_categories=[0], metrics="all"
                    )

                    if agreement_raster != None:
                        # Write the layer_agreement_raster.
                        layer_agreement_raster = os.path.join(
                            os.path.split(agreement_raster)[0], poly_handle + '_agreement.tif'
                        )
                        logging.debug(f"layer_agreement_raster is {layer_agreement_raster}")
                        agreement_map_write = agreement_map_include.rio.write_nodata(10, encoded=True)
                        # TODO: Jun 2026: Was occasionally throwing rasterio errors.
                        # ie) 	rasterio._err.CPLE_AppDefinedError: Deleting /data/test_cases/{... a path}/sadn4_b0m_agreement.tif
                        # failed: No such file or directory. Looks like it is a reference open from agreement_map_include
                        # We might need to research this more.
                        agreement_map_write.rio.to_raster(
                            layer_agreement_raster, dtype=np.uint8, driver="COG"
                        )
                        agreement_map_write.close()
                        del agreement_map_write

                    # Update stats table dictionary
                    stats_table_dictionary.update(
                        {
                            poly_handle: cross_walk_gval_fim(
                                metric_df=metrics_table,
                                cell_area=cell_area,
                                masked_count=np.sum(agreement_map_include.data == 4),
                            )
                        }
                    )
                    if agreement_map_include is not None:
                        agreement_map_include.close()

                    del poly_all, poly_all_proj, metrics_table, crosstab_table

        if agreement_map is not None:
            agreement_map.close()

    except Exception as ex:
        logging.critical(f"An error occurred while loading stats from {benchmark_raster_path}")
        logging.critical(traceback.format_exc())
        raise ex

    return stats_table_dictionary


########################################################################
########################################################################
# Functions related to categorical fim and ahps evaluation
########################################################################
# TODO: Jun 2026: if it makes sense, it is possible to use a native logger
# ie) logging.info.  If a logger has not specifically been created, native python logging
# just prints to screen instead.
def get_metadata(
    metadata_url,
    select_by,
    selector,
    must_include=None,
    upstream_trace_distance=None,
    downstream_trace_distance=None,
):
    '''
    Retrieve metadata for a site or list of sites.

    Parameters
    ----------
    metadata_url : STR
        metadata base URL.
    select_by : STR
        Location search option. Options include: 'state', TODO: test 'nws_lid'
    selector : LIST
        Value to match location data against. Supplied as a LIST.
    must_include : STR, optional
        What attributes are required to be valid response. The default is None.
    upstream_trace_distance : INT, optional
        Distance in miles upstream of site to trace NWM network. The default is None.
    downstream_trace_distance : INT, optional
        Distance in miles downstream of site to trace NWM network. The default is None.

    Returns
    -------
    metadata_list : LIST
        Dictionary or list of dictionaries containing metadata at each site.
    metadata_dataframe : Pandas DataFrame
        Dataframe of metadata for each site.
    err_msg : STR
        Error message, if applicable (or blank string).
    '''
    # Initialize error message
    err_msg = ''

    # Format selector variable in case multiple selectors supplied
    format_selector = '%2C'.join(selector)

    # Define the url
    url = f'{metadata_url}/{select_by}/{format_selector}/'

    # Assign optional parameters to a dictionary
    params = {}
    params['must_include'] = must_include
    params['upstream_trace_distance'] = upstream_trace_distance
    params['downstream_trace_distance'] = downstream_trace_distance

    # Suppress Insecure Request Warning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    try:
        # Request data from API URL
        response = requests.get(url, params=params, verify=False)

    except requests.RequestException as e:
        # If the API request is not completed, output an error message and return empty outputs
        err_msg = f'Metadata API request failed: {e}'
        return [], pd.DataFrame(), err_msg

    # If the API request was successful, process the data
    if response.ok:
        # Convert data response to a json
        metadata_json = response.json()

        # Get the metadata list from the json
        metadata_list = metadata_json['locations']

        # Add timestamp of WRDS and sources retrieval
        timestamp = response.headers['Date']
        timestamp_list = metadata_json['data_sources']['metadata_sources']

        # Default timestamps to "Not available" and overwrite with real values if possible.
        nwis_timestamp, nrldb_timestamp = "Not available", "Not available"
        for timestamp in timestamp_list:
            if "NWIS" in timestamp:
                nwis_timestamp = timestamp
            if "NRLDB" in timestamp:
                nrldb_timestamp = timestamp

        # Get crosswalk info (always last dictionary in list)
        crosswalk_info = metadata_json['data_sources']

        # Update each dictionary with timestamp and crosswalk info
        for metadata in metadata_list:
            metadata.update({"wrds_timestamp": timestamp})
            metadata.update({"nrldb_timestamp": nrldb_timestamp})
            metadata.update({"nwis_timestamp": nwis_timestamp})
            metadata.update(crosswalk_info)

        # Convert metadata list of dictionaries to a dataframe
        metadata_dataframe = pd.json_normalize(metadata_list)

        # Replace all periods with underscores in column names
        metadata_dataframe.columns = metadata_dataframe.columns.astype(str).str.replace('.', '_')

    else:
        # If the request was completed but produced no data, output an error message and return empty outputs
        err_msg = f'Code: {response.status_code}\nMessage: {response.reason}\nURL: {response.url}'
        metadata_list = []
        metadata_dataframe = pd.DataFrame()

    return metadata_list, metadata_dataframe, err_msg


########################################################################
# Function to assign HUC code using the WBD spatial layer using a spatial join
########################################################################
def aggregate_wbd_hucs(metadata_list, wbd_huc8_path, retain_attributes=False, huc_list=list()):
    '''
    Assigns the proper FIM HUC 08 code to each site in the input DataFrame.
    Converts input DataFrame to a GeoDataFrame using lat/lon attributes
    with sites containing null nws_lid/lat/lon removed. Reprojects GeoDataFrame
    to same CRS as the HUC 08 layer. Performs a spatial join to assign the
    HUC 08 layer to the GeoDataFrame. Sites that are not assigned a HUC
    code removed as well as sites in Alaska and Canada.

    Parameters
    ----------
    metadata_list: List of Dictionaries
        Output list from get_metadata
    wbd_huc8_path : pathlib Path
        Path to HUC8 wbd layer (assumed to be geopackage format)
    retain_attributes ; Bool OR List
        Flag to define attributes of output GeoDataBase. If True, retain
        all attributes. If False, the site metadata will be trimmed to a
        default list. If a list of desired attributes is supplied these
        will serve as the retained attributes.
    Returns
    -------
    dictionary : DICT
        Dictionary with HUC (key) and corresponding AHPS codes (values).
    all_gdf: GeoDataFrame
        GeoDataFrame of all NWS_LID sites.

    '''
    # TODO: (May 2026) Update print statements with logging.info() once all apps that use this script have logging plugged in
    # eval_plots does not have logging yet, maybe others as well

    # Import huc8 layer as geodataframe and retain necessary columns
    print("Reading WBD...")
    huc8_all = gpd.read_file(wbd_huc8_path, layer='WBDHU8')
    # print("WBD read.")

    huc8 = huc8_all[['HUC8', 'name', 'states', 'geometry']]

    # Filter HUCs by HUC list and sort numerically
    if len(huc_list) > 0:
        huc8 = huc8[huc8['HUC8'].isin(huc_list)]
    huc8 = huc8.sort_values(by='HUC8', ascending=True)

    # Create empty geodataframe and define CRS for potential horizontal datums (default of NAD83 if unassigned)
    metadata_gdf = gpd.GeoDataFrame()
    crs_lookup = {'NAD27': 'EPSG:4267', 'NAD83': 'EPSG:4269', 'WGS84': 'EPSG:4326'}

    # Iterate through each site
    print("Iterating through metadata list...")

    columns_with_NA_missing_dtype = []
    dtype_warning_list = []

    for metadata in metadata_list:
        # Convert metadata to json
        df = pd.json_normalize(metadata)

        # Columns have periods due to nested dictionaries, replace with '_'
        df.columns = df.columns.str.replace('.', '_')

        # Drop any metadata sites that don't have lat/lon populated
        df.dropna(
            subset=['identifiers_nws_lid', 'usgs_preferred_latitude', 'usgs_preferred_longitude'],
            inplace=True,
        )

        # If dataframe still has data
        if not df.empty:
            # Get horizontal datum
            h_datum = df['usgs_preferred_latlon_datum_name'].item()

            # Look up EPSG code, if not returned Assume NAD83 as default.
            dict_crs = crs_lookup.get(h_datum, 'EPSG:4269_ Assumed')

            # We want to know what sites were assumed, hence the split.
            src_crs, *message = dict_crs.split('_')

            # Convert dataframe to geodataframe using lat/lon (USGS). Add attribute of assigned crs (label ones that are assumed)
            site_gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df['usgs_preferred_longitude'], df['usgs_preferred_latitude']),
                crs=src_crs,
            )

            ## TEMP DEBUG: Temporarily removing this section which deals with columns types etc,
            ## becuase I'm worried it might've introduced errors in the USGS calibration TODO: reimplement if needed
            # # Add data type to columns, if needed (usually the ones that are sometimes/always NA)
            # for colname, new_dtype in WRDS_METADATA_COL_TYPES.items():
            #     if colname in site_gdf.columns:
            #         site_gdf[colname] = site_gdf[colname].astype(new_dtype)

            # # # Replace 'None' and 'nan' with None
            # # site_gdf = site_gdf.replace(['None', 'nan', np.nan], None)

            # # Record colnames of cols with NA vals and a vague col type (object)
            # # because these columns could cause future warnings and errors
            # for colname in site_gdf.columns:  # TEMP DEBUG
            #     if site_gdf[colname].isna().any() and site_gdf[colname].dtype == object:
            #         columns_with_NA_missing_dtype.append(colname)

            # Field to indicate if a latlon datum was assumed
            site_gdf['assigned_crs'] = src_crs + ''.join(message)

            # Reproject to huc8 crs
            site_gdf = site_gdf.to_crs(huc8.crs)

            # Append site geodataframe to metadata geodataframe (and catch warnings if applicable)
            with warnings.catch_warnings(record=True) as caught_warnings:
                warnings.simplefilter("always", category=FutureWarning)

                metadata_gdf = pd.concat([metadata_gdf, site_gdf], ignore_index=True)

                # Extract the warning message as a string
                if caught_warnings:
                    warning_message = str(caught_warnings[-1].message)
                    dtype_warning_list.append(warning_message)

    # Debugging information about NA columns relating to FutureWarning (May 2026)
    columns_with_NA_missing_dtype = list(set(columns_with_NA_missing_dtype))
    if len(columns_with_NA_missing_dtype) > 0:
        print(
            f"Columns in sites GDF with NA values AND object dtype (even after fix): {columns_with_NA_missing_dtype}"
        )

    if len(dtype_warning_list) > 0:
        print(f"Captured {len(dtype_warning_list)} warnings with with the following info:")
        for warning in list(set(dtype_warning_list)):
            print(warning)

    # Exit if there's no metadata compiled
    if metadata_gdf.empty:
        return None, gpd.GeoDataFrame()

    # Trim metadata to only have certain fields.
    if not retain_attributes:
        metadata_gdf = metadata_gdf[
            ['identifiers_nwm_feature_id', 'identifiers_nws_lid', 'identifiers_usgs_site_code', 'geometry']
        ]
    # If a list of attributes is supplied then use that list.
    #    elif isinstance(retain_attributes,list):
    #        metadata_gdf = metadata_gdf[retain_attributes]
    print("Performing spatial and tabular operations on geodataframe...")

    # Perform a spatial join to get the WBD HUC 8 assigned to each AHPS
    joined_gdf = gpd.sjoin(
        metadata_gdf, huc8, how='inner', predicate='intersects', lsuffix='ahps', rsuffix='wbd'
    )
    joined_gdf = joined_gdf.drop(columns='index_wbd')

    # Create a dictionary of huc [key] and nws_lid[value]
    dictionary = joined_gdf.groupby('HUC8')['identifiers_nws_lid'].apply(list).to_dict()

    return dictionary, joined_gdf


########################################################################
# Function to define the mainstems network
########################################################################
def mainstem_nwm_segs(metadata_url, list_of_sites):
    '''
    Define the mainstems network. Currently a 4 pass approach that probably needs refined.
    Once a final method is decided the code can be shortened. Passes are:
        1) Search downstream of gages designated as upstream. This is done to hopefully reduce the issue of mapping starting at the nws_lid. 91038 segments
        2) Search downstream of all LID that are rfc_forecast_point = True. Additional 48,402 segments
        3) Search downstream of all evaluated sites (sites with detailed FIM maps) Additional 222 segments
        4) Search downstream of all sites in HI/PR (locations have no rfc_forecast_point = True) Additional 408 segments

    Parameters
    ----------
    metadata_url : STR
        URL of API.
    list_of_sites : LIST
        List of evaluated sites.

    Returns
    -------
    ms_nwm_segs_set : SET
        Mainstems network segments as a set.

    '''

    # Define the downstream trace distance
    downstream_trace_distance = 'all'

    # Trace downstream from all 'headwater' usgs gages
    select_by = 'tag'
    selector = ['usgs_gages_ii_ref_headwater']
    must_include = None
    gages_list, gages_dataframe, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Trace downstream from all rfc_forecast_point.
    select_by = 'nws_lid'
    selector = ['all']
    must_include = 'nws_data.rfc_forecast_point'
    fcst_list, fcst_dataframe, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Trace downstream from all evaluated ahps sites.
    select_by = 'nws_lid'
    selector = list_of_sites
    must_include = None
    eval_list, eval_dataframe, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Trace downstream from all sites in HI/PR.
    select_by = 'state'
    selector = ['HI', 'PR']
    must_include = None
    islands_list, islands_dataframe, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Combine all lists of metadata dictionaries into a single list.
    combined_lists = gages_list + fcst_list + eval_list + islands_list
    # Define list that will contain all segments listed in metadata.
    all_nwm_segments = []
    # For each lid metadata dictionary in list
    for lid in combined_lists:
        # get all downstream segments
        downstream_nwm_segs = lid.get('downstream_nwm_features')
        # Append downstream segments
        if downstream_nwm_segs:
            all_nwm_segments.extend(downstream_nwm_segs)
        # Get the nwm feature id associated with the location
        location_nwm_seg = lid.get('identifiers').get('nwm_feature_id')
        if location_nwm_seg:
            # Append nwm segment (conver to list)
            all_nwm_segments.extend([location_nwm_seg])
    # Remove duplicates by assigning to a set.
    ms_nwm_segs_set = set(all_nwm_segments)

    return ms_nwm_segs_set


##############################################################################
# Function to create list of NWM segments
###############################################################################
def get_nwm_segs(metadata):
    '''
    Using the metadata output from "get_metadata", output the NWM segments.

    Parameters
    ----------
    metadata : DICT
        Dictionary output from "get_metadata" function.

    Returns
    -------
    all_segments : LIST
        List of all NWM segments.

    '''

    nwm_feature_id = metadata.get('identifiers').get('nwm_feature_id')
    upstream_nwm_features = metadata.get('upstream_nwm_features')
    downstream_nwm_features = metadata.get('downstream_nwm_features')

    all_segments = []
    # Convert NWM feature id segment to a list (this is always a string or empty)
    if nwm_feature_id:
        nwm_feature_id = [nwm_feature_id]
        all_segments.extend(nwm_feature_id)
    # Add all upstream segments (always a list or empty)
    if upstream_nwm_features:
        all_segments.extend(upstream_nwm_features)
    # Add all downstream segments (always a list or empty)
    if downstream_nwm_features:
        all_segments.extend(downstream_nwm_features)

    return all_segments


#######################################################################
# Thresholds
#######################################################################
# Feb 24, 2026: TODO: The call to the api, should have a "with" and proper try/catch added.
# See run_vdatum_for_region for an example
def get_thresholds(threshold_url, select_by, selector, threshold='all', source_crs_availability=None):
    '''
    Get nws_lid threshold stages and flows (i.e. bankfull, action, minor,
    moderate, major). Returns a dictionary for stages and one for flows.

    Parameters
    ----------
    threshold_url : STR
        WRDS threshold API.
    select_by : STR
        Type of site (nws_lid, usgs_site_code etc).
    selector : STR
        Site for selection. Must be a single site.
    threshold : STR, optional
        Threshold option. The default is 'all'.
    source_crs_availability : list or None (the default is None), optional
        List of sources where the CRS is available. This is used to determine
        which source to pull threshold data from if multiple sources are available.
        The default is None (source CRS availability is not considered).

    Returns
    -------
    stages : DICT
        Dictionary of stages at each threshold.
    flows : DICT
        Dictionary of flows at each threshold.
    status_msg : STR
        Status of API call and data availability.

    '''
    if source_crs_availability == None:
        source_crs_availability = []

    params = {}
    params['threshold'] = threshold
    url = f'{threshold_url}/{select_by}/{selector}'

    # Initialize status message
    status_msg = f"Selector: {selector}: "

    # response = requests.get(url, params=params, verify=False)

    # Call the API
    session = requests.Session()

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)

    response = session.get(url, params=params, verify=False)  # TODO: Put this in a try/except block

    if response.status_code == 200:
        thresholds_json = response.json()

        # Get metadata
        thresholds_info = thresholds_json['value_set']
        threshold_count = thresholds_json['_metrics']['threshold_count']
        status_msg += f"WRDS response sucessful. {threshold_count} threshold types available. "

        # Initialize stages/flows dictionaries
        stages = {}
        flows = {}
        # Check if thresholds information is populated. If site is non-existent thresholds info is blank
        if thresholds_info:
            # Get all rating sources and corresponding indexes in a dictionary
            rating_sources = {
                i.get('calc_flow_values').get('rating_curve').get('source'): index
                for index, i in enumerate(thresholds_info)
            }

            # If projection source info isn't provided (or no valid projections were found so the list is blank),
            # just pick the threshold data based on threshold availability
            if len(source_crs_availability) == 0:

                # Get threshold data by source
                # Use NRLDB if available (priority), otherwise USGS Rating Depot (switched Dec '25)
                if 'NRLDB' in rating_sources:
                    threshold_data = thresholds_info[rating_sources['NRLDB']]
                elif 'USGS Rating Depot' in rating_sources:
                    threshold_data = thresholds_info[rating_sources['USGS Rating Depot']]
                # If neither USGS or NRLDB is available, use first dictionary to get stage values.
                else:
                    threshold_data = thresholds_info[0]

            # If projection source info IS provided, use that alongside threshold availability to get threshold data
            else:
                if 'NRLDB' in source_crs_availability and 'NRLDB' in rating_sources:
                    threshold_data = thresholds_info[rating_sources['NRLDB']]
                elif 'USGS Rating Depot' in source_crs_availability and 'USGS Rating Depot' in rating_sources:
                    threshold_data = thresholds_info[rating_sources['USGS Rating Depot']]

                # If there isn't a match between source_crs_availability and rating_sources, just get threshold data
                # based on availability with NRLDB as priority over USGS (same as above logic).
                # The site will probably error out down the line.
                elif 'NRLDB' in rating_sources:
                    threshold_data = thresholds_info[rating_sources['NRLDB']]

                # If neither USGS or NRLDB is available, use first dictionary to get stage values.
                else:
                    threshold_data = thresholds_info[0]

            # Get stages and flows for each threshold
            if threshold_data:
                status_msg += "Thresholds available. "

                stages = threshold_data['stage_values']
                flows = threshold_data['calc_flow_values']
                # Add source information to stages and flows. Flows source inside a nested dictionary. Remove key once source assigned to flows.
                stages['source'] = threshold_data.get('metadata').get('threshold_source')
                flows['source'] = flows.get('rating_curve', {}).get('source')
                flows.pop('rating_curve', None)
                # Add timestamp WRDS data was retrieved.
                stages['wrds_timestamp'] = response.headers['Date']
                flows['wrds_timestamp'] = response.headers['Date']
                # Add Site information
                stages['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                flows['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                stages['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                flows['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                stages['units'] = threshold_data.get('metadata').get('stage_units')
                flows['units'] = threshold_data.get('metadata').get('calc_flow_units')
        return stages, flows, status_msg
    else:
        status_msg += "WRDS response error."
        print(status_msg)
        stages = None
        flows = None

        return stages, flows, status_msg


########################################################################
# Function to write flow file
########################################################################
def flow_data(segments, flows, convert_to_cms=True):
    '''
    Given a list of NWM segments and a flow value in cfs, convert flow to
    cms and return a DataFrame that is set up for export to a flow file.

    Parameters
    ----------
    segments : LIST
        List of NWM segments.
    flows : FLOAT
        Flow in CFS.
    convert_to_cms : BOOL
        Flag to indicate if supplied flows should be converted to metric.
        Default value is True (assume input flows are CFS).

    Returns
    -------
    flow_data : DataFrame
        Dataframe ready for export to a flow file.

    '''
    if convert_to_cms:
        # Convert cfs to cms
        cfs_to_cms = 0.3048**3
        flows_cms = round(flows * cfs_to_cms, 2)
    else:
        flows_cms = round(flows, 2)

    flow_data = pd.DataFrame({'feature_id': segments, 'discharge': flows_cms})
    flow_data = flow_data.astype({'feature_id': int, 'discharge': float})
    return flow_data


#######################################################################
# Function to get datum information
#######################################################################
def get_datum(metadata):
    '''
    Given a record from the metadata endpoint, retrieve important information
    related to the datum and site from both NWS and USGS sources. This information
    is saved to a dictionary with common keys. USGS has more data available so
    it has more keys.

    Parameters
    ----------
    metadata : DICT
        Single record from the get_metadata function. Must iterate through
        the get_metadata output list.

    Returns
    -------
    nws_datums : DICT
        Dictionary of NWS data.
    usgs_datums : DICT
        Dictionary of USGS Data.

    '''
    # Get site and datum information from nws sub-dictionary. Use consistent naming between USGS and NWS sources.
    nws_datums = {}
    nws_datums['nws_lid'] = metadata['identifiers']['nws_lid']
    nws_datums['usgs_site_code'] = metadata['identifiers']['usgs_site_code']
    nws_datums['state'] = metadata['nws_data']['state']
    nws_datums['datum'] = metadata['nws_data']['zero_datum']
    nws_datums['vcs'] = metadata['nws_data']['vertical_datum_name']
    nws_datums['lat'] = metadata['nws_data']['latitude']
    nws_datums['lon'] = metadata['nws_data']['longitude']
    nws_datums['crs'] = metadata['nws_data']['horizontal_datum_name']
    nws_datums['source'] = 'nws_data'

    # Get site and datum information from usgs_data sub-dictionary. Use consistent naming between USGS and NWS sources.
    usgs_datums = {}
    usgs_datums['nws_lid'] = metadata['identifiers']['nws_lid']
    usgs_datums['usgs_site_code'] = metadata['identifiers']['usgs_site_code']
    usgs_datums['active'] = metadata['usgs_data']['active']
    usgs_datums['state'] = metadata['usgs_data']['state']
    usgs_datums['datum'] = metadata['usgs_data']['altitude']
    usgs_datums['vcs'] = metadata['usgs_data']['alt_datum_code']
    usgs_datums['datum_acy'] = metadata['usgs_data']['alt_accuracy_code']
    usgs_datums['datum_meth'] = metadata['usgs_data']['alt_method_code']
    usgs_datums['lat'] = metadata['usgs_data']['latitude']
    usgs_datums['lon'] = metadata['usgs_data']['longitude']
    usgs_datums['crs'] = metadata['usgs_data']['latlon_datum_name']
    usgs_datums['source'] = 'usgs_data'

    return nws_datums, usgs_datums


########################################################################
# Function to convert horizontal datums
########################################################################
def convert_latlon_datum(lat, lon, src_crs, dest_crs):
    '''
    Converts latitude and longitude datum from a source CRS to a dest CRS
    using geopandas and returns the projected latitude and longitude coordinates.

    Parameters
    ----------
    lat : FLOAT
        Input Latitude.
    lon : FLOAT
        Input Longitude.
    src_crs : STR
        CRS associated with input lat/lon. Geopandas must recognize code.
    dest_crs : STR
        Target CRS that lat/lon will be projected to. Geopandas must recognize code.

    Returns
    -------
    new_lat : FLOAT
        Reprojected latitude coordinate in dest_crs.
    new_lon : FLOAT
        Reprojected longitude coordinate in dest_crs.

    '''

    # Create a temporary DataFrame containing the input lat/lon.
    temp_df = pd.DataFrame({'lat': [lat], 'lon': [lon]})
    # Convert dataframe to a GeoDataFrame using the lat/lon coords. Input CRS is assigned.
    temp_gdf = gpd.GeoDataFrame(temp_df, geometry=gpd.points_from_xy(temp_df.lon, temp_df.lat), crs=src_crs)
    # Reproject GeoDataFrame to destination CRS.
    reproject = temp_gdf.to_crs(dest_crs)
    # Get new Lat/Lon coordinates from the geometry data.
    new_lat, new_lon = [reproject.geometry.y.item(), reproject.geometry.x.item()]
    return new_lat, new_lon


#######################################################################
# Function to get conversion adjustment NGVD to NAVD in FEET
#######################################################################
def ngvd_to_navd_ft(datum_info):
    '''
    Given the lat/lon, retrieve the adjustment from NGVD29 to NAVD88 in feet.
    Uses NOAA tidal API to get conversion factor. Requires that lat/lon is
    in NAD27 crs. If input lat/lon are not NAD27 then these coords are
    reprojected to NAD27 and the reproject coords are used to get adjustment.
    There appears to be an issue when region is not in contiguous US.
    TODO: Test outside of CONUS and resolve if needed.

    Parameters
    ----------
    datum_info : DICT
        Dictionary containing site information. Must contain the following keys:
        - 'crs': CRS of lat/lon (e.g. 'NAD27', 'NAD83', 'WGS84').
        - 'state': State of site (e.g. 'Alaska', 'California').
        - 'lat': Latitude of site.
        - 'lon': Longitude of site.

    Returns
    -------
    datum_adj_ft : FLOAT
        Vertical adjustment in feet, from NGVD29 to NAVD88, and rounded to nearest hundredth. Returns None if API call is unsuccessful.
    err_msg : STR
        Error message if applicable, otherwise blank string.

    '''

    # Feb 2026: All print commands are getting lost in MP, so we need to return the message
    # They do show in the console but not the logs without returning the message
    err_msg = ""

    # Correct the CRS, if needed (this likely also done outside the function, but the redundancy is ok)
    crs_corrected, __, uncorrected_crs_error, __, msgs = correct_datum_typos(datum_info['crs'], None)

    if len(msgs) > 0:
        for msg in msgs:
            print(msg)  # Printing for debug purposes, won't show up in logs

    if crs_corrected is not None:
        datum_info['crs'] = crs_corrected

    if uncorrected_crs_error is True:
        err_msg += f"Unable to correct CRS spelling (CRS is {datum_info['crs']})\n"

    # If crs is not NAD 27, convert crs to NAD27 and get adjusted lat lon
    if datum_info['crs'] != 'NAD27':
        print(f"Input lat/lon is in {datum_info['crs']} datum. Converting to NAD27 for VDatum API...")
        try:
            lat, lon = convert_latlon_datum(datum_info['lat'], datum_info['lon'], datum_info['crs'], 'NAD27')

        except Exception as ex:
            # Exit if an error occurs here because the code assumes lat lon is NAD27 so the code can't proceed otherwise
            msg = f"Error occurred while trying to convert CRS to NAD27 for VDatum API (input CRS: {datum_info['crs']})\nError: {ex}"
            err_msg += msg
            print(msg)
            adjustment_ft = None

            return adjustment_ft, err_msg

    else:
        # Otherwise assume lat/lon is in NAD27.
        lat = datum_info['lat']
        lon = datum_info['lon']

    # --------------------------------------

    # Define url for datum API
    datum_url = 'https://vdatum.noaa.gov/vdatumweb/api/convert'

    # Define parameters. Hard code most parameters to convert NGVD to NAVD.
    params = {}
    params['s_x'] = lon  # source x, longitude
    params['s_y'] = lat  # source y, latitude
    params['s_h_frame'] = 'NAD27'  # Source CRS
    params['s_v_frame'] = 'NGVD29'  # Source vertical coord datum
    params['s_vertical_unit'] = 'm'  # Source vertical units
    params['src_height'] = 0.0  # Source vertical height
    params['t_v_frame'] = 'NAVD88'  # Target vertical datum
    params['tar_vertical_unit'] = 'm'  # Target vertical height

    err_msg_vdatum = ""

    # Run Vdatum with region-specific parameters
    if datum_info['state'] == 'Alaska':
        params['s_v_geoid'] = 'geoid12b'  # Source geoid (AK-specific)
        params['t_v_geoid'] = 'geoid12b'  # Target geoid (AK-specific)

        response, success, err_msg_vdatum = run_vdatum_for_region(params, 'AK', datum_url)

        # If AK region fails, try running calling API with SEAK region
        if success == False:
            response, success, err_msg_vdatum = run_vdatum_for_region(params, 'SEAK', datum_url)

    else:
        # For CONUS, use default geoid
        response, success, err_msg_vdatum = run_vdatum_for_region(params, 'contiguous', datum_url)

    # Get adjustment in feet if Vdatum API call is successful
    if success == True:
        # Get adjustment in meters (NGVD29 to NAVD88)
        results = response.json()
        adjustment = results['t_z']

        # Convert meters to feet
        adjustment_ft = round(float(adjustment) * 3.28084, 2)

    else:
        # If the VDatum call was unsuccessful, set the error message to be the one from the vdatum run if it exists, otherwise set a default error message
        if err_msg_vdatum == "":
            err_msg = "VDatum API call was not successful, but no error message was returned from the API."
        else:
            # Set the error message to be the one from the vdatum run
            err_msg = err_msg_vdatum

        adjustment_ft = None
        print(err_msg)

    return adjustment_ft, err_msg


def run_vdatum_for_region(params, region, datum_url):
    '''
    Run API for a given region.

    Parameters
    ----------
    params : DICT
        Dictionary of parameters to run VDatum API.
    region : STR
        Region to run VDatum API for. Options are 'AK', 'SEAK', and
        'contiguous'.
    datum_url : STR
        URL for VDatum API.

    Returns
    -------
    response : requests.Response or STR
        Response from API call if successful, otherwise a string with a default error message.
    success : BOOL
        Flag to indicate whether API call was successful and VDatum returned results without an error message.
    err_msg : STR
        Error message if applicable, otherwise blank string.

    '''
    params['region'] = region

    time.sleep(1)  # pause for 1 second before each request so we don't overwhelm the API

    # Suppress Insecure Request Warning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    # Adjust logging level for urllib3
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    response = "Internal Error: response message not set"
    success = False
    err_msg = ""

    try:
        # Call the API
        retry = Retry(connect=5, backoff_factor=5, total=4, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)

        # with requests.Session() as session:
        session = requests.Session()
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        response = session.get(datum_url, params=params, verify=False, timeout=(5, 20))

        # Check whether API call was successful (indicated by a 200 response code)
        if response.status_code == 200:
            results = response.json()

            if "errorCode" in results:
                # API run sucessful but VDatum returned an error message, so we can't proceed with getting the adjustment

                # Feb 24, 2026: It is possible for the response header to return a 200, but there could
                # be a controlled message from the API, such as  {'errorCode': 412, 'message': 'Uncaught error, please contact NOAA VDatum Program Support team.'}

                if "message" in results:
                    err_msg = (
                        f"Error {results['errorCode']} returned from NOAA VDatum API: {results['message']}"
                    )
                else:
                    err_msg = f"Error {results['errorCode']} returned from NOAA VDatum API, but no message provided. Full response: {results}"
            else:
                # API run was successful AND VDatum returned results without an error message, so we can proceed with getting the adjustment
                success = 't_z' in results
        else:
            # If API call was not sucessful (i.e. response code is not 200), return an error message with the status code and description from the response
            err_msg = f"API call failed while calling NOAA vDatum service: Status code {response.status_code}; Description: {response.reason}"
            print(err_msg)

    except ReadTimeout as err:
        err_msg = f"Error occured while calling NOAA VDatum: {err}"
        print(err_msg)

    except HTTPError as err:
        # These are for catastropic errors calling NOAA
        err_msg = f"Error occured while calling NOAA VDatum: {err}"
        print(err_msg)

    except requests.exceptions.RequestException as err:
        # These are for catastropic errors calling NOAA
        err_msg = f"Error occured while calling NOAA VDatum: {err}"
        print(err_msg)

    return response, success, err_msg


#######################################################################
# Function to download rating curve from API
#######################################################################
def get_rating_curve(rating_curve_url, location_ids):
    '''
    Given list of location_ids (nws_lids, usgs_site_codes, etc) get the
    rating curve from WRDS API and export as a DataFrame.

    Parameters
    ----------
    rating_curve_url : STR
        URL to retrieve rating curve
    location_ids : LIST
        List of location ids. Can be nws_lid or usgs_site_codes.

    Returns
    -------
    all_curves : pandas DataFrame
        Rating curves from input list as well as other site information.

    '''
    # Define DataFrame to contain all returned curves.
    all_curves = pd.DataFrame()
    err_msg = ""

    # print(location_ids)
    # Define call to retrieve all rating curve information from WRDS.
    joined_location_ids = '%2C'.join(location_ids)
    url = f'{rating_curve_url}/{joined_location_ids}'

    try:
        # Call the API
        response = requests.get(url, verify=False)

    except requests.RequestException as e:
        # If the API request is not completed, output an error message and return empty outputs
        err_msg = f'Rating curve API request failed: {e}'
        return all_curves, err_msg

    # If successful
    if response.ok:
        # Write return to json and extract the rating curves
        site_json = response.json()
        rating_curves_list = site_json['rating_curves']

        # For each curve returned
        for curve in rating_curves_list:
            # Check if a curve was populated (e.g wasn't blank)
            if curve:
                # Write rating curve to pandas dataframe as well as site attributes
                curve_df = pd.DataFrame(curve['rating_curve'], dtype=float)

                # Add other information such as site, site type, source, units, and timestamp.
                curve_df['location_id'] = curve['metadata']['location_id']
                curve_df['location_type'] = curve['metadata']['id_type']
                curve_df['source'] = curve['metadata']['source']
                curve_df['flow_units'] = curve['metadata']['flow_unit']
                curve_df['stage_units'] = curve['metadata']['stage_unit']
                curve_df['wrds_timestamp'] = response.headers['Date']

                # Append rating curve to DataFrame containing all curves
                all_curves = pd.concat([all_curves, curve_df])
            else:
                continue

    return all_curves, err_msg


#######################################################################
# Following Functions used for preprocesing of AHPS sites (NWS and USGS)
########################################################################


#######################################################################
# Function to return a correct maps.
########################################################################
def select_grids(dataframe, stages, datum88, buffer):
    '''
    Given a DataFrame (in a specific format), and a dictionary of stages, and the datum (in navd88).
    loop through the available inundation datasets and find the datasets that are equal to or immediately above the thresholds and only return 1 dataset per threshold (if any).

    Parameters
    ----------
    dataframe : DataFrame
        DataFrame that has to be in a specific format and contains the stages and paths to the inundation datasets.
    stages : DICT
        Dictionary of thresholds (key) and stages (values)
    datum88: FLOAT
        The datum associated with the LID that is pre-converted to NAVD88 (if needed)
    buffer: Float
        Interval which the uppder bound can be assigned. For example, Threshold + buffer = upper bound. Recommended to make buffer 0.1 greater than desired interval as code selects maps < and not <=

    Returns
    -------
    maps : DICT
        Dictionary of threshold (key) and inundation dataset path (value)
    map_flows: DICT
        Dictionary of threshold (key) and flows in CFS rounded to the nearest whole number associated with the selected maps (value)

    '''
    # Define threshold categories
    thresholds = ['action', 'minor', 'moderate', 'major']
    maps = {}
    map_flows = {}
    # For each threshold, pick the appropriate map for analysis.
    for i, threshold in enumerate(thresholds):
        # Check if stage is None
        if not stages[threshold] is None:
            # Define the threshold floor elevation (navd88).
            lower_bound = round((stages[threshold] + datum88), 1)
            # Define the threshold ceiling (navd88)
            upper_bound = round((stages[threshold] + datum88 + buffer), 1)
            # For thresholds that are action, minor, moderate
            if threshold in ['action', 'minor', 'moderate']:
                # Make sure the next threshold has a valid stage
                if stages[thresholds[i + 1]] is None:
                    next_threshold = upper_bound
                else:
                    # Determine what the next threshold elevation is.
                    next_threshold = round((stages[thresholds[i + 1]] + datum88), 1)
                # Make sure the upper_bound is not greater than the next threshold, if it is then reassign upper_bound.
                if upper_bound > next_threshold:
                    upper_bound = next_threshold
                # Get the single map which meets the criteria.
                value = dataframe.query(f'({lower_bound}<=elevation) & (elevation<{upper_bound})')[
                    'elevation'
                ].min()
            # For major threshold
            else:
                # Get the single map which meets criteria.
                value = dataframe.query(f'({lower_bound}<=elevation) & (elevation<{upper_bound})')[
                    'elevation'
                ].min()

            # If the selected value is a number
            if np.isfinite(value):
                # Get the map path and the flow associated with the map (rounded to nearest whole number)
                map_path = dataframe.query(f'elevation == {value}')['path'].item()
                map_flow = round(dataframe.query(f'elevation == {value}')['flow'].item(), 0)
                # Check to see if map_flow is valid (if beyond rating_curve it is nan)
                if not np.isfinite(map_flow):
                    map_path = 'No Flow'
                    map_flow = 'No Flow'

            # If the selected value is not a number (or interpolated flow is nan caused by elevation of map which is beyond rating curve range), then map_path and map_flows are both set to 'No Map'.
            else:
                map_path = 'No Map'
                map_flow = 'No Map'
        else:
            map_path = 'No Threshold'
            map_flow = 'No Threshold'

        # Write map paths and flows to dictionary
        maps[threshold] = map_path
        map_flows[threshold] = map_flow

    # Get the maximum inundation map (using elevation) and this will be the domain extent
    max_value = dataframe['elevation'].max()
    map_path = dataframe.query(f'elevation == {max_value}')['path'].item()
    map_flow = 'Not Used'
    maps['extent'] = map_path
    map_flows['extent'] = map_flow

    return maps, map_flows


#######################################################################
# Process AHPS Extent Grid (Fill Holes)
#######################################################################
def process_extent(extent, profile, output_raster=False):
    '''
    Convert raster to feature (using raster_to_feature), the footprint is used so all raster values are set to 1 where there is data.
    fill all donut holes in resulting feature.
    Filled geometry is then converted back to raster using same raster properties as input profile.
    Output raster will have be encoded as follows:
        filled footprint (wet) = 1
        remaining area in raster domain (dry) = 0
        NoData = 3

    Parameters
    ----------
    extent : Rasterio Dataset Reader
        Path to extent raster
    extent_profile: Rasterio Profile
        profile related to the extent argument
    output_raster: STR
        Path to output raster. If no path supplied, then no raster is written to disk. default = False

    Returns (If no output raster specified)
    -------
    extent_filled_raster : rasterio dataset
        Extent raster with filled donut holes
    profile : rasterio profile
        Profile associated with extent_filled_raster

    '''

    # Convert extent to feature and explode geometry
    poly_extent = raster_to_feature(extent, profile, footprint_only=True)
    poly_extent = poly_extent.explode(index_parts=True)

    # Fill holes in extent
    poly_extent_fill_holes = MultiPolygon(Polygon(p.exterior) for p in poly_extent['geometry'])
    # loop through the filled polygons and insert the new geometry
    for i, part in enumerate(poly_extent_fill_holes.geoms):
        poly_extent.loc[i, 'geometry'] = part

    # Dissolve filled holes with main map and explode
    poly_extent['dissolve_field'] = 1
    poly_extent = poly_extent.dissolve(by='dissolve_field')
    poly_extent = poly_extent.explode(index_parts=True)
    poly_extent = poly_extent.reset_index()

    # Convert filled polygon back to raster
    extent_filled_raster = features.rasterize(
        ((geometry, 1) for geometry in poly_extent['geometry']),
        fill=0,
        dtype='int32',
        transform=profile['transform'],
        out_shape=(profile['height'], profile['width']),
    )

    # Update profile properties (dtype and no data)
    profile.update(dtype=rasterio.int32)
    profile.update(nodata=0)

    # Check if output raster is specified. If so, the write extent filled raster to disk.
    if output_raster:
        # Create directory
        Path(output_raster).parent.mkdir(parents=True, exist_ok=True)
        with rasterio.Env():
            with rasterio.open(output_raster, 'w', **profile) as dst:
                dst.write(extent_filled_raster, 1)
    # If no output raster is supplied the return the rasterio array and profile.
    else:
        return extent_filled_raster, profile


########################################################################
# Convert raster to polygon
########################################################################
def raster_to_feature(grid, profile_override=False, footprint_only=False):
    '''
    Given a grid path, convert to vector, dissolved by grid value, in GeoDataFrame format.

    Parameters
    ----------
    grid_path : pathlib path OR rasterio Dataset Reader
        Path to grid or a rasterio Dataset Reader
    profile_override: rasterio Profile
        Default is False, If a rasterio Profile is supplied, it will dictate the transform and crs.
    footprint_only: BOOL
        If true, dataset will be divided by itself to remove all unique values. If False, all values in grid will be carried through on raster to feature conversion. default = False

    Returns
    -------
    dissolve_geodatabase : GeoDataFrame
        Dissolved (by gridvalue) vector data in GeoDataFrame.

    '''
    # TODO: Jun 2026: Change all rasterio.open commands to better scope contol.
    # Using either the "with" syntax, or open the file / read / explicit close or better control.
    # Even adding try/except woudl help. See inundate.py -> inundate for yet another option.

    # Determine what format input grid is:
    # If a pathlib path, open with rasterio
    if isinstance(grid, pathlib.PurePath):
        dataset = rasterio.open(grid)
    # If a rasterio dataset object, assign to dataset
    elif isinstance(grid, rasterio.DatasetReader):
        dataset = grid

    # Get data/mask and profile properties from dataset
    data = dataset.read(1)
    msk = dataset.read_masks(1)
    data_transform = dataset.transform
    coord_sys = dataset.crs

    # If a profile override was supplied, use it to get the transform and coordinate system.
    if profile_override:
        data_transform = profile_override['transform']
        coord_sys = profile_override['crs']

    # If a footprint of the raster is desired, convert all data values to 1
    if footprint_only:
        data[msk == 255] = 1

    # Convert grid to feature
    spatial = []
    values = []
    for geom, val in rasterio.features.shapes(data, mask=msk, transform=data_transform):
        spatial.append(shape(geom))
        values.append(val)
    spatial_geodataframe = gpd.GeoDataFrame({'values': values, 'geometry': spatial}, crs=coord_sys)
    dissolve_geodataframe = spatial_geodataframe.dissolve(by='values')
    return dissolve_geodataframe


########################################################################
# Create AHPS Benchmark Grid
########################################################################
def process_grid(benchmark, benchmark_profile, domain, domain_profile, reference_raster):
    '''
    Given a benchmark grid and profile, a domain rasterio dataset and profile, and a reference raster,
    Match the benchmark dataset to the domain extent and create a classified grid convert to:
        0 (no data footprint of domain)
        1 (data footprint of domain)
        2 (data footprint of benchmark)
    Then reproject classified benchmark grid to match reference grid resolution and crs.
    Output is an array of values and a profile.

    Parameters
    ----------
    benchmark : rasterio dataset
        Rasterio dataset of the benchmark dataset for a given threshold
    benchmark_profile : rasterio profile
        A potentially modified profile to the benchmark dataset.
    domain: rasterio dataset
        Rasterio dataset of the domain grid (the maximum available grid for a given site)
    domain_profile: rasterio profile
        A potentially modified profile of the domain dataset.
    reference_raster : pathlib Path
        Path to reference dataset.

    Returns
    -------
    boolean_benchmark : numpy Array
        Array of values for the benchmark_boolean grid.
    profile : rasterio profile
        Updated, final profile of the boolean_benchmark grid.

    '''

    # Make benchmark have same dimensions as domain (Assume domain has same CRS as benchmark)
    # Get source CRS (benchmark and domain assumed to be same CRS)
    source_crs = benchmark_profile['crs'].to_wkt()
    # Get domain data
    domain_arr = domain.read(1)
    # Get benchmark data
    benchmark_arr = benchmark.read(1)
    # Create empty array with same dimensions as domain
    benchmark_fit_to_domain = np.empty(domain_arr.shape)
    # Make benchmark have same footprint as domain (Assume domain has same CRS as benchmark)
    reproject(
        benchmark_arr,
        destination=benchmark_fit_to_domain,
        src_transform=benchmark.transform,
        src_crs=source_crs,
        src_nodata=benchmark.nodata,
        dst_transform=domain.transform,
        dst_crs=source_crs,
        dst_nodata=benchmark.nodata,
        dst_resolution=source_crs,
        resampling=Resampling.bilinear,
    )
    # Convert fitted benchmark dataset to boolean. 0 = NODATA Regions and 1 = Data Regions
    benchmark_fit_to_domain_bool = np.where(benchmark_fit_to_domain == benchmark.nodata, 0, 1)
    # Merge domain datamask and benchmark data mask. New_nodata_value (2) = Domain NO DATA footprint, 0 = NO DATA for benchmark (within data region of domain), 1 = DATA region of benchmark.
    new_nodata_value = 2
    classified_benchmark = np.where(
        domain_arr == domain.nodata, new_nodata_value, benchmark_fit_to_domain_bool
    )

    # TODO: Jun 2026: Change all rasterio.open commands to better scope contol.
    # Using either the "with" syntax, or open the file / read / explicit close or better control.
    # Even adding try/except woudl help. See inundate.py -> inundate for yet another option.

    ## Reproject classified benchmark to reference raster crs and resolution.
    # Read in reference raster
    reference = rasterio.open(reference_raster)
    # Determine the new transform and dimensions of reprojected/resampled classified benchmark dataset whos width, height, and bounds are same as domain dataset.
    new_benchmark_transform, new_benchmark_width, new_benchmark_height = calculate_default_transform(
        source_crs, reference.crs, domain.width, domain.height, *domain.bounds, resolution=reference.res
    )
    # Define an empty array that is same dimensions as output by the "calculate_default_transform" command.
    classified_benchmark_projected = np.empty((new_benchmark_height, new_benchmark_width), dtype=np.uint8)
    # Reproject and resample the classified benchmark dataset. Nearest Neighbor resampling due to integer values of classified benchmark.
    reproject(
        classified_benchmark,
        destination=classified_benchmark_projected,
        src_transform=domain.transform,
        src_crs=source_crs,
        src_nodata=new_nodata_value,
        dst_transform=new_benchmark_transform,
        dst_crs=reference.crs,
        dst_nodata=new_nodata_value,
        dst_resolution=reference.res,
        resampling=Resampling.nearest,
    )

    # Update profile using reference profile as base (data type, NODATA, transform, width/height).
    profile = reference.profile
    profile.update(transform=new_benchmark_transform)
    profile.update(dtype=rasterio.uint8)
    profile.update(nodata=new_nodata_value)
    profile.update(width=new_benchmark_width)
    profile.update(height=new_benchmark_height)

    return classified_benchmark_projected, profile


# TODO: Jun 2026: Does not appear to be in use. Consider removing it. Deprecated ??
def calculate_metrics_from_agreement_raster(agreement_raster):
    '''Calculates metrics from an agreement raster'''

    agreement_encoding_digits_to_names = {0: "TN", 1: "FN", 2: "FP", 3: "TP"}

    # TODO: Jun 2026: Change all rasterio.open commands to better scope contol.
    # Using either the "with" syntax, or open the file / read / explicit close

    if isinstance(agreement_raster, rasterio.DatasetReader):
        pass
    elif isinstance(agreement_raster, str):
        agreement_raster = rasterio.open(agreement_raster)
    else:
        raise TypeError(f"{agreement_raster} is not a Rasterio Dataset Reader or a filepath to a raster")

    # cycle through blocks
    totals = dict.from_keys(list(range(4)), 0)
    for idx, wind in agreement_raster.block_windows(1):
        window_data = agreement_raster.read(1, window=wind)
        values, counts = np.unique(window_data, return_counts=True)
        for val, cts in zip(values, counts):
            totals[val] += cts

    results = dict()
    for digit, count in totals.items():
        results[agreement_encoding_digits_to_names[digit]] = count

    return results


# evaluation metric fucntions
# TODO: Jun 2026: Does not appear to be in use. Consider removing it. Deprecated ??
def csi(TP, FP, FN, TN=None):
    '''Critical Success Index'''

    return TP / (FP + FN + TP)

# TODO: Jun 2026: Does not appear to be in use. Consider removing it. Deprecated ??
def tpr(TP, FP, FN, TN=None):
    '''True Positive Rate'''
    return TP / (TP + FN)

# TODO: Jun 2026: Does not appear to be in use. Consider removing it. Deprecated ??
def far(TP, FP, FN, TN=None):
    '''False Alarm Rate'''

    return FP / (TP + FP)

# TODO: Jun 2026: Does not appear to be in use. Consider removing it. Deprecated ??
def mcc(TP, FP, FN, TN=None):
    '''Matthew's Correlation Coefficient'''
    return (TP * TN - FP * FN) / np.sqrt((TP + FP) * (TP + FN) * (TN + FP) * (TN + FN))


##########################
'''
Aug 2026:
  As python 3.12, gdal and other packages were upgraded, gdal handles memory different
  and can be slower to add memory to python GC (garbage collection).
  This can trigger a quasi-memory leak of memory growing as it can not be released fast enough.

  This function can be used for calcs with MP and MT's or even both.
  but is designed to be plugged it inside the child workers.

  Notes:
    - MP will share this gdal cach max value for all objects and MT's inside it.
    - Combined gdal calls like rasterio which includes sharing inside MT's.
    - This helps manage memory used with gdal related packages such as rasterio,
      geopandas, xarray/rioxarray, fiona, and others via dependencies like gval
    - When a object is called, it will not limit or error if the max is hit but will simply
      help manage the memory and release it quicker.
'''
def set_dynamic_gdal_cache(num_parent_workers=1, num_child_workers=1):

    from osgeo import gdal  # Yes.. function level.

    # Get total system memory in bytes
    total_memory_bytes = psutil.virtual_memory().total
    
    # Convert bytes to Megabytes (1 MB = 1024 * 1024 bytes)
    total_memory_mb = total_memory_bytes // (1024 * 1024)

    # Add a memory buffer for the OS and misc other python packages in the script other items
    # (max 70% of available). Remember, docker, VScode, terminals, RDP Connections all share
    # this memory.
    total_memory_mb = total_memory_bytes // (1024 * 1024)
    max_memory_allowed = total_memory_mb * 0.70

    # Now share it between parent and child workers.
    # (per parent and child combined)
    num_workers_used = num_parent_workers * num_child_workers
    max_memory_used_per_job = int(max_memory_allowed / num_workers_used)
    
    # 4. Apply to GDAL for the current Python process (in MB)
    gdal.SetConfigOption("GDAL_CACHEMAX", str(max_memory_used_per_job))
    # print(f"GDAL_CACHEMAX set to: {max_memory_used_per_job} MB")