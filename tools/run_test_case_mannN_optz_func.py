#!/usr/bin/env python3

import errno
import json
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy
from inundation import inundate
from mosaic_inundation import Mosaic_inundation
from scipy.ndimage import generic_filter
from tools_shared_functions import compute_contingency_stats_from_rasters
from tools_shared_variables import (  # INPUTS_DIR,
    AHPS_BENCHMARK_CATEGORIES,
    MAGNITUDE_DICT,
    OUTPUTS_DIR,
    PREVIOUS_FIM_DIR,
    TEST_CASES_DIR,
    elev_raster_ndv,
)
from tqdm import tqdm

from utils.shared_functions import FIM_Helpers as fh


# *******************************************************
# def list_all_test_cases(huc, version, archive, benchmark_categories=[]):
def list_all_test_cases(huc, version, archive, benchmark_categories, output_dir):
    """Returns a complete list of all benchmark category test cases as classes.

    Parameters
    ----------
    version : str
        Version of FIM to which this test_case belongs. This should correspond to the fim directory
        name in either `/data/previous_fim/` or `/outputs/`.
    archive : bool
        If true, this test case outputs will be placed into the `official_versions` folder
        and the FIM model will be read from the `/data/previous_fim` folder.
        If false, it will be saved to the `testing_versions/` folder and the FIM model
        will be read from the `/outputs/` folder.
    """
    # if not benchmark_categories:
    #     benchmark_categories = list(MAGNITUDE_DICT.keys())
    # benchmark_categories = ['nws', 'usgs'] # ['ble']

    test_case_list = []
    for bench_cat in benchmark_categories:

        test_id = f'{huc}_{bench_cat}'
        test_case_list.append(Test_Case(test_id, version, archive, huc, output_dir))

    return test_case_list


# *********************************************************
def Test_Case(test_id, version, archive, huc, output_dir):
    """Class that handles test cases, specifically running the alpha test.

    Parameters
    ----------
    test_id : str
        ID of the test case in huc8_category format, e.g. `12090201_ble`.
    version : str
        Version of FIM to which this test_case belongs. This should correspond to the fim directory
        name in either `/data/previous_fim/` or `/outputs/`.
    archive : bool
        If true, this test case outputs will be placed into the `official_versions` folder
        and the FIM model will be read from the `/data/previous_fim` folder.
        If false, it will be saved to the `testing_versions/` folder and the FIM model
        will be read from the `/outputs/` folder.

    """

    huc_unused, benchmark_cat = test_id.split('_')
    is_ahps = True if benchmark_cat in AHPS_BENCHMARK_CATEGORIES else False
    # FIM run directory path - uses HUC 8
    fim_dir = os.path.join(PREVIOUS_FIM_DIR if archive else output_dir, version, huc)
    # Test case directory path
    dir_tc = os.path.join(
        TEST_CASES_DIR,
        f'{benchmark_cat}_test_cases',
        test_id,
        'official_versions' if archive else 'testing_versions',
        version,
    )
    if not os.path.exists(dir_tc):
        os.makedirs(dir_tc)

    # Benchmark data path
    validation_data = os.path.join(
        TEST_CASES_DIR, f'{benchmark_cat}_test_cases', f'validation_data_{benchmark_cat}'
    )
    benchmark_dir = os.path.join(validation_data, huc)
    if huc[:2] == '19':
        mask_dict = {
            'levees': {
                'path': os.getenv('input_nld_levee_protected_areas_Alaska'),
                'buffer': None,
                'operation': 'exclude',
            },
            'waterbodies': {
                # 'path': '/data/inputs/nwm_hydrofabric/nwm_lakes.gpkg',
                'path': os.getenv('input_nwm_lakes_Alaska'),
                'buffer': None,
                'operation': 'exclude',
            },
        }
    else:
        mask_dict = {
            'levees': {
                'path': os.getenv(
                    'input_nld_levee_protected_areas'
                ),  # '/data/inputs/nld_vectors/Levee_protected_areas.gpkg',
                'buffer': None,
                'operation': 'exclude',
            },
            'waterbodies': {
                'path': os.getenv('input_nwm_lakes'),  # '/data/inputs/nwm_hydrofabric/nwm_lakes.gpkg',
                'buffer': None,
                'operation': 'exclude',
            },
        }

    magnitude_class = data(huc, benchmark_cat)

    if not os.path.exists(benchmark_dir):
        # raise ImportError(f"path {self.benchmark_dir} does not exist!")
        print(f"{benchmark_dir} does not exist!")
        # Create list of shapefile paths to use as exclusion areas.
        return
    else:
        test_case_dic = {
            'huc': huc,
            'fim_dir': fim_dir,
            'dir_tc': dir_tc,
            'validation_data': validation_data,
            'benchmark_dir': benchmark_dir,
            'benchmark_cat': benchmark_cat,
            'test_id': test_id,
            'is_ahps': is_ahps,
            'magnitude_class': magnitude_class,
            'mask_dict': mask_dict,
        }
        return test_case_dic


# *********************************************************
def magnitudes(category):
    '''Returns the magnitudes associated with the benchmark category.'''
    return MAGNITUDE_DICT[category]


def data(huc, category):
    '''
    Returns a dict of magnitudes and sites for a given huc. Sites will be AHPS lids for
    AHPS sites and empty strings for non-AHPS sites.
    '''
    category = category.lower()
    validation_data = os.path.join(TEST_CASES_DIR, f'{category}_test_cases', f'validation_data_{category}')
    is_ahps = True if category in AHPS_BENCHMARK_CATEGORIES else False

    huc_dir = os.path.join(validation_data, huc)
    if not os.path.isdir(huc_dir):
        return {}
    if is_ahps:
        lids = os.listdir(huc_dir)
        mag_dict = {}
        for lid in lids:
            lid_dir = os.path.join(huc_dir, lid)
            for mag in [file for file in os.listdir(lid_dir) if file in magnitudes(category)]:
                if mag in mag_dict:
                    mag_dict[mag].append(lid)
                else:
                    mag_dict[mag] = [lid]

        return mag_dict
    else:
        mags = list(os.listdir(huc_dir))
        return {mag: [''] for mag in mags}


# *********************************************************
def alpha_test(
    test_case_dic,
    hydroTable_all,
    calibrated=False,
    model='',
    mask_type='huc',
    inclusion_area='',
    inclusion_area_buffer=0,
    overwrite=True,
    verbose=False,
    gms_workers=1,
):
    '''Compares a FIM directory with benchmark data from a variety of sources.

    Parameters
    ----------
    calibrated : bool
        Whether or not this FIM version is calibrated.
    model : str
        MS or FR extent of the model. This value will be written to the eval_metadata.json.
    mask_type : str
        Mask type to feed into inundation.py.
    inclusion_area : int
        Area to include in agreement analysis.
    inclusion_area_buffer : int
        Buffer distance in meters to include outside of the model's domain.
    overwrite : bool
        If True, overwites pre-existing test cases within the test_cases directory.
    verbose : bool
        If True, prints out all pertinent data.
    gms_workers : int
        Number of worker processes assigned to GMS processing.
    '''
    try:

        if not overwrite and os.path.isdir(test_case_dic['dir_tc']):
            print(
                f"Metrics for {test_case_dic['dir_tc']} already exist. Use overwrite flag (-o) to overwrite metrics."
            )
            return

        fh.vprint(f"Starting alpha test for {test_case_dic['dir_tc']}", verbose)

        stats_modes_list = ['total_area']
        # Create paths to fim_run outputs for use in inundate()
        if model != 'GMS':
            rem = os.path.join(test_case_dic['fim_dir'], 'rem_zeroed_masked.tif')
            if not os.path.exists(rem):
                rem = os.path.join(test_case_dic['fim_dir'], 'rem_clipped_zeroed_masked.tif')
            catchments = os.path.join(
                test_case_dic['fim_dir'], 'gw_catchments_reaches_filtered_addedAttributes.tif'
            )
            if not os.path.exists(catchments):
                catchments = os.path.join(
                    test_case_dic['fim_dir'], 'gw_catchments_reaches_clipped_addedAttributes.tif'
                )
            mask_type = mask_type
            # if mask_type == 'huc':
            #     catchment_poly = ''
            # else:
            #     catchment_poly = os.path.join(
            #         test_case_dic['fim_dir'],
            #         'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg',
            #     )

        # Map necessary inputs for inundate().
        if inclusion_area != '':
            inclusion_area_name = os.path.split(inclusion_area)[1].split('.')[0]  # Get layer name
            test_case_dic['mask_dict'].update(
                {
                    inclusion_area_name: {
                        'path': inclusion_area,
                        'buffer': int(inclusion_area_buffer),
                        'operation': 'include',
                    }
                }
            )
            # Append the concatenated inclusion_area_name and buffer.
            if inclusion_area_buffer is None:
                inclusion_area_buffer = 0
            stats_modes_list.append(inclusion_area_name + '_b' + str(inclusion_area_buffer) + 'm')

        # Get the magnitudes and lids for the current huc and loop through them
        magnitude_class = test_case_dic['magnitude_class']
        for magnitude in magnitude_class:
            for instance in magnitude_class[magnitude]:
                # instance will be the lid for AHPS sites and '' for other sites
                # For each site, inundate the REM and compute aggreement raster with stats
                _inundate_and_compute(
                    hydroTable_all,
                    test_case_dic,
                    magnitude,
                    instance,
                    model=model,
                    verbose=verbose,
                    gms_workers=gms_workers,
                )

            # Clean up 'total_area' outputs from AHPS sites
            if test_case_dic['is_ahps']:
                clean_ahps_outputs(os.path.join(test_case_dic['dir_tc'], magnitude))

        # Write out evaluation meta-data
        write_metadata(test_case_dic['dir_tc'], calibrated, model)

    except KeyboardInterrupt:
        print("Program aborted via keyboard interrupt")
        sys.exit(1)
    except Exception as ex:
        print(ex)
        # Temporarily adding stack trace
        print(f"trace for {test_case_dic['test_id']} -------------\n", traceback.format_exc())
        sys.exit(1)


def _inundate_and_compute(
    hydroTable_all, test_case_dic, magnitude, lid, compute_only=False, model='', verbose=False, gms_workers=1
):
    '''Method for inundating and computing contingency rasters as part of the alpha_test.
    Used by both the alpha_test() and composite() methods.

        Parameters
        ----------
        magnitude : str
            Magnitude of the current benchmark site.
        lid : str
            lid of the current benchmark site. For non-AHPS sites, this should be an empty string ('').
        compute_only : bool
            If true, skips inundation and only computes contingency stats.
    '''
    # Output files
    fh.vprint("Creating output files", verbose)

    huc = test_case_dic['huc']
    test_id = test_case_dic['test_id']
    test_case_out_dir = os.path.join(test_case_dic['dir_tc'], magnitude)
    print(f"Processing test_id: {test_id}, lid: {lid}, magnitude: {magnitude}")

    inundation_prefix = lid + '_' if lid else ''
    inundation_path = os.path.join(test_case_out_dir, f'{inundation_prefix}inundation_extent.tif')
    predicted_raster_path = inundation_path.replace('.tif', f'_{huc}.tif')

    agreement_raster = os.path.join(
        test_case_out_dir, (f'ahps_{lid}' if lid else '') + 'total_area_agreement.tif'
    )
    stats_json = os.path.join(test_case_out_dir, 'stats.json')
    stats_csv = os.path.join(test_case_out_dir, 'stats.csv')

    # Create directory
    if not os.path.isdir(test_case_out_dir):
        os.mkdir(test_case_out_dir)

    # Benchmark raster and flow files
    benchmark_rast = (
        f'ahps_{lid}' if lid else test_case_dic['benchmark_cat']
    ) + f'_huc_{huc}_extent_{magnitude}.tif'  # huc
    benchmark_rast = os.path.join(test_case_dic['benchmark_dir'], lid, magnitude, benchmark_rast)
    benchmark_flows = benchmark_rast.replace(f'_extent_{magnitude}.tif', f'_flows_{magnitude}.csv')
    mask_dict_indiv = test_case_dic['mask_dict'].copy()
    if test_case_dic['is_ahps']:  # add domain shapefile to mask for AHPS sites
        domain = os.path.join(test_case_dic['benchmark_dir'], lid, f'{lid}_domain.shp')
        mask_dict_indiv.update({lid: {'path': domain, 'buffer': None, 'operation': 'include'}})
    # Check to make sure all relevant files exist
    if (
        not os.path.isfile(benchmark_rast)
        or not os.path.isfile(benchmark_flows)
        or (test_case_dic['is_ahps'] and not os.path.isfile(domain))
    ):
        return -1

    # Inundate REM
    if not compute_only:  # composite alpha tests don't need to be inundated
        if model == "GMS":
            produce_mosaicked_inundation(
                os.path.dirname(test_case_dic['fim_dir']),
                hydroTable_all,
                huc,
                benchmark_flows,
                inundation_raster=predicted_raster_path,
                mask=os.path.join(test_case_dic['fim_dir'], "wbd.gpkg"),
                verbose=verbose,
            )

    # Create contingency rasters and stats
    fh.vprint("Begin creating contingency rasters and stats", verbose)
    if os.path.isfile(predicted_raster_path):
        compute_contingency_stats_from_rasters(
            predicted_raster_path,
            benchmark_rast,
            agreement_raster,
            stats_csv=stats_csv,
            stats_json=stats_json,
            mask_dict=mask_dict_indiv,
        )
    return


def write_metadata(dir_tc, calibrated, model):
    '''Writes metadata files for a test_case directory.'''
    with open(os.path.join(dir_tc, 'eval_metadata.json'), 'w') as meta:
        eval_meta = {'calibrated': calibrated, 'model': model}
        meta.write(json.dumps(eval_meta, indent=2))


def clean_ahps_outputs(magnitude_directory):
    '''Cleans up `total_area` files from an input AHPS magnitude directory.'''
    output_file_list = [os.path.join(magnitude_directory, of) for of in os.listdir(magnitude_directory)]
    for output_file in output_file_list:
        if "total_area" in output_file:
            os.remove(output_file)


# *********************************************************
# from inundate_mosaic_wrapper import produce_mosaicked_inundation
# Transfer produce_mosaicked_inundation from inundate_mosaic_wrapper to this script
def produce_mosaicked_inundation(
    fim_dir,
    hydroTable_all,
    huc,  # hucs
    flow_file,
    inundation_raster=None,
    inundation_polygon=None,
    depths_raster=None,
    map_filename=None,
    mask=None,
    unit_attribute_name="huc8",
    num_workers=1,
    remove_intermediate=True,
    verbose=False,
    is_mosaic_for_branches=False,
):
    """
    This function calls Inundate_gms and Mosaic_inundation to produce inundation maps.
    Possible outputs include inundation rasters encoded by HydroID (negative HydroID for dry and positive
    HydroID for wet), polygons depicting extent, and depth rasters. The function requires a flow file
    organized by NWM feature_id and discharge in cms. "feature_id" and "discharge" columns MUST be present
    in the flow file.

    Args:
        fim_dir (str):            Path to fim directory where FIM outputs were written by
                                    fim_pipeline.
        huc (str):                The HUC for which to produce mosaicked inundation files.
        flow_file (str):          Path to flow file to be used for inundation.
                                    feature_ids in flow_file should be present in supplied HUC.
        inundation_raster (str):  Full path to output inundation raster
                                    (encoded by positive and negative HydroIDs).
        inuntation_polygon (str): Full path to output inundation polygon. Optional.
        depths_raster (str):      Full path to output depths_raster. Pixel values will be in meters. Optional.
        num_workers (int):        Number of parallel jobs to run.
        keep_intermediate (bool): Option to keep intermediate files.
        verbose (bool):           Print verbose messages to screen. Not tested.
    """

    # Check that inundation_raster or depths_raster is supplied
    if inundation_raster is None and depths_raster is None:
        raise ValueError("Must supply either inundation_raster or depths_raster.")

    # Check that output directory exists. Notify user that output directory will be created if not.
    for output_file in [inundation_raster, inundation_polygon, depths_raster]:
        if output_file is None:
            continue
        parent_dir = os.path.split(output_file)[0]
        if not os.path.exists(parent_dir):
            fh.vprint(
                "Parent directory for "
                + os.path.split(output_file)[1]
                + " does not exist. The parent directory will be produced.",
                verbose,
            )
            os.makedirs(parent_dir)

    # Check that fim_dir exists
    if not os.path.exists(fim_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_dir)

    # If the "hucs" argument is really one huc, convert it to a list
    if type(huc) is str:  # hucs
        # hucs = [hucs]
        huc = [huc]

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Check job numbers and raise error if necessary
    total_cpus_available = os.cpu_count() - 1
    if num_workers > total_cpus_available:
        raise ValueError(
            "The number of workers (-w), {}, "
            "exceeds your machine's available CPU count minus one ({}). "
            "Please lower the num_workers.".format(num_workers, total_cpus_available)
        )

    # Call Inundate_gms
    map_file = Inundate_gms(
        fim_dir=fim_dir,
        hydroTable_all=hydroTable_all,
        hucs=huc,
        forecast=flow_file,
        num_workers=num_workers,
        inundation_raster=inundation_raster,
        depths_raster=depths_raster,
        verbose=verbose,
    )

    # Write map file if designated
    if map_filename is not None:
        if not os.path.isdir(os.path.dirname(map_filename)):
            os.makedirs(os.path.dirname(map_filename))

        map_file.to_csv(map_filename, index=False)

    fh.vprint("Mosaicking extent...", verbose)

    for mosaic_attribute in ["depths_rasters", "inundation_rasters"]:
        mosaic_output = None
        if mosaic_attribute == "inundation_rasters":
            if inundation_raster is not None:
                mosaic_output = inundation_raster
        elif mosaic_attribute == "depths_rasters":
            if depths_raster is not None:
                mosaic_output = depths_raster

    if mosaic_output is not None:
        # Call Mosaic_inundation
        mosaic_file_path = Mosaic_inundation(
            map_file.copy(),
            mosaic_attribute=mosaic_attribute,
            mosaic_output=mosaic_output,
            mask=mask,
            unit_attribute_name=unit_attribute_name,
            nodata=elev_raster_ndv,
            remove_inputs=remove_intermediate,
            verbose=verbose,
            is_mosaic_for_branches=is_mosaic_for_branches,
            inundation_polygon=inundation_polygon,
        )

    fh.vprint("Mosaicking complete.", verbose)

    return mosaic_file_path


def Inundate_gms(
    fim_dir,
    hydroTable_all,
    forecast,
    num_workers=1,
    hucs=None,
    inundation_raster=None,
    # inundation_polygon=None,
    depths_raster=None,
    verbose=False,
    log_file=None,
    output_fileNames=None,
):
    # per huc modification
    # input handling
    if hucs is not None:
        try:
            _ = (i for i in hucs)
        except TypeError:
            raise ValueError("hucs argument must be an iterable")

    if isinstance(hucs, str):
        hucs = [hucs]
    if type(hucs) is str:
        hucs = [hucs]

    num_workers = int(num_workers)

    # log file
    if log_file is not None:
        if os.path.exists(log_file):
            os.remove(log_file)

        if verbose:
            print("HUC8,BranchID,Exception", file=open(log_file, "w"))

    # load fim inputs
    hucs_branches = pd.read_csv(os.path.join(fim_dir, "fim_inputs.csv"), header=None, dtype={0: str, 1: str})

    if hucs is not None:
        hucs = set(hucs)
        huc_indices = hucs_branches.loc[:, 0].isin(hucs)
        hucs_branches = hucs_branches.loc[huc_indices, :]

    # get number of branches
    number_of_branches = len(hucs_branches)

    # make inundate generator
    inundate_input_generator = __inundate_gms_generator(
        hucs_branches,
        hydroTable_all,
        fim_dir,
        inundation_raster,
        # inundation_polygon,
        depths_raster,
        forecast,
        verbose=False,
    )

    # start up process pool
    # better results with Process pool
    executor = ProcessPoolExecutor(max_workers=num_workers)

    # collect output filenames
    inundation_raster_fileNames = [None] * number_of_branches
    inundation_polygon_fileNames = [None] * number_of_branches
    depths_raster_fileNames = [None] * number_of_branches
    hucCodes = [None] * number_of_branches
    branch_ids = [None] * number_of_branches

    executor_generator = {executor.submit(inundate, **inp): ids for inp, ids in inundate_input_generator}
    idx = 0
    for future in tqdm(
        as_completed(executor_generator),
        total=len(executor_generator),
        desc=f"Inundating branches with {num_workers} workers",
        disable=(not verbose),
    ):
        hucCode, branch_id = executor_generator[future]

        try:
            future.result()

        except Exception as exc:
            if log_file is not None:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
            else:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
        else:
            hucCodes[idx] = hucCode
            branch_ids[idx] = branch_id

            try:
                # print(hucCode,branch_id,future.result()[0][0])
                inundation_raster_fileNames[idx] = future.result()[0][0]
            except TypeError:
                pass

            try:
                depths_raster_fileNames[idx] = future.result()[1][0]
            except TypeError:
                pass

            try:
                inundation_polygon_fileNames[idx] = future.result()[2][0]
            except TypeError:
                pass

            idx += 1

    # power down pool
    executor.shutdown(wait=True)

    # make filename dataframe
    output_fileNames_df = pd.DataFrame(
        {
            "huc8": hucCodes,
            "branchID": branch_ids,
            "inundation_rasters": inundation_raster_fileNames,
            "depths_rasters": depths_raster_fileNames,
            "inundation_polygons": inundation_polygon_fileNames,
        }
    )

    if output_fileNames is not None:
        output_fileNames_df.to_csv(output_fileNames, index=False)

    # print(output_fileNames_df["inundation_rasters"])
    return output_fileNames_df


def __inundate_gms_generator(
    huc_branches,
    hydroTable_all,
    fim_dir,
    inundation_raster,
    # inundation_polygon,
    depths_raster,
    forecast,
    verbose=False,
    windowed=False,
):
    # Iterate over branches
    for idx, row in huc_branches.iterrows():
        huc = str(row.iloc[0])
        branch_id = str(row.iloc[1])

        huc_dir = os.path.join(fim_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_branch = os.path.join(branch_dir, catchments_file_name)

        hydroTable_branch = hydroTable_all.loc[hydroTable_all["branch_id"] == int(branch_id)]

        xwalked_file_name = f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
        catchment_poly = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there
        if (inundation_raster is not None) and (huc not in inundation_raster):
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, [huc, branch_id])
        else:
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, branch_id)

        # if (inundation_polygon is not None) and (huc not in inundation_polygon):
        #     inundation_branch_polygon = fh.append_id_to_file_name(inundation_polygon, [huc, branch_id])
        # else:
        #     inundation_branch_polygon = fh.append_id_to_file_name(inundation_polygon, branch_id)

        if (depths_raster is not None) and (huc not in depths_raster):
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, [huc, branch_id])
        else:
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, branch_id)

        # identifiers
        identifiers = (huc, branch_id)

        # inundate input
        inundate_input = {
            "rem": rem_branch,
            "catchments": catchments_branch,
            "catchment_poly": catchment_poly,
            "hydro_table": hydroTable_branch,
            "forecast": forecast,
            "mask_type": "filter",
            "hucs": None,
            "hucs_layerName": None,
            "subset_hucs": None,
            "num_workers": 1,
            "aggregate": False,
            "inundation_raster": inundation_branch_raster,
            # "inundation_polygon": inundation_branch_polygon,
            "depths": depths_branch_raster,
            # "out_raster_profile": None,
            # "out_vector_profile": None,
            "quiet": not verbose,
            "windowed": windowed,
        }
        # print(list(inundate_input.items())[3])
        yield inundate_input, identifiers


# *********************************************************
# *********************************************************
# -------------------------------------------------------
# Functions for incorporating nonmonotonic flow adjustment
# into the optomization. This functions have been modified
# for the purpose of the optimization. Please see the
# original functions in /src/ folder
# -------------------------------------------------------
# Analysing each HydroID SRC for nonmonotonic SRC
def analyze_nonmonotonic_src(srcs_df, strm_order):  # , thalweg_hydroids

    # Only apply on stream orders >= strm_order
    if srcs_df['order_'].iloc[0] < strm_order:
        return srcs_df

    srcs_df.loc[srcs_df['stage'] == 0, 'discharge_cms'] = 0

    cond_chan = srcs_df['bankfull_proxy'] == 'channel'
    srcs_df_chan = srcs_df[cond_chan]
    non_monotonic_index = srcs_df_chan.index[srcs_df_chan['discharge_cms'].diff().lt(0)].tolist()

    # Recalculate 'Discharge' values before the last non-monotonic row
    # Note: No change has been applied on WetArea, Volume, LENGTHKM
    if non_monotonic_index:
        # Get the target values from the last non-monotonic index
        target_idx = non_monotonic_index[-1]
        target_numCells = srcs_df.loc[target_idx, 'Number of Cells']
        target_SurfaceArea = srcs_df.loc[target_idx, 'SurfaceArea (m2)']
        target_BedArea = srcs_df.loc[target_idx, 'BedArea (m2)']

        # Define the slice (up to but not including target_idx)
        row_slice = slice(0, target_idx)

        # Assign target values to the selected rows
        srcs_df.loc[row_slice, 'Number of Cells'] = target_numCells
        srcs_df.loc[row_slice, 'SurfaceArea (m2)'] = target_SurfaceArea
        srcs_df.loc[row_slice, 'BedArea (m2)'] = target_BedArea

        # Recalculate discharge variables
        length_km = srcs_df.loc[row_slice, 'LENGTHKM']
        # Avoid division by zero
        length_km = length_km.replace(0, np.nan)

        target_TopWidth = target_SurfaceArea / length_km / 1000
        target_WettedPerimeter = target_BedArea / length_km / 1000

        wet_area = srcs_df.loc[row_slice, 'WetArea (m2)']
        target_HydraulicRadius = wet_area / target_WettedPerimeter

        srcs_df.loc[row_slice, 'TopWidth (m)'] = target_TopWidth
        srcs_df.loc[row_slice, 'WettedPerimeter (m)'] = target_WettedPerimeter
        srcs_df.loc[row_slice, 'HydraulicRadius (m)'] = target_HydraulicRadius
        srcs_df['HydraulicRadius (m)'] = srcs_df['HydraulicRadius (m)'].fillna(0)

        # Recalculate discharge_cms for the selected rows
        srcs_df.loc[row_slice, 'discharge_cms'] = (
            wet_area
            * (srcs_df.loc[row_slice, 'HydraulicRadius (m)'] ** (2.0 / 3))
            * (srcs_df.loc[row_slice, 'SLOPE'] ** 0.5)
            / srcs_df.loc[row_slice, 'channel_n']
        )

    return srcs_df


# -------------------------------------------------------
# Correcting nonmonotonic SRC
# Main function for the non-monotonic adjustment
def correct_nonmonotonic_src(ht_df):  # , bankfull_flows_file
    """
    Function for correcting nonmonotonic synthetic rating curves.
    For GMS branches, it will correct each hydroID SRC in serial based
    that shows nonmonotonic behavior within in-channel stages.

    """
    strm_order = 4
    # Defining integer columns
    cols_int = ['Number of Cells', 'SurfaceArea (m2)', 'HydroID', 'NextDownID', 'order_', 'feature_id']

    # Update parameters for nonmonotonic SRC
    ht_df2 = ht_df.copy()
    cols_int = ['Number of Cells', 'SurfaceArea (m2)', 'HydroID', 'NextDownID', 'order_', 'feature_id']
    ht_df2[cols_int] = ht_df2[cols_int].astype(int)
    ht_df2 = ht_df2.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)

    # Defining integer columns
    ht_df3 = ht_df2.copy()

    # Adjusting ht tables for nonmonotonic hts
    ht_df4 = ht_df3.groupby('HydroID', group_keys=False)[ht_df3.columns].apply(
        analyze_nonmonotonic_src, strm_order=strm_order
    )

    # Make sure nonmonotonic adjustment just applied within in-channel stages
    cond_bankfull = ht_df2['bankfull_proxy'] == 'floodplain'
    if 'subdiv_discharge_cms' in ht_df2.columns:
        ht_df4.loc[cond_bankfull, 'subdiv_discharge_cms'] = ht_df2.loc[cond_bankfull, 'subdiv_discharge_cms']
    ht_df4.loc[cond_bankfull, 'discharge_cms'] = ht_df2.loc[cond_bankfull, 'discharge_cms']
    ht_df4.loc[cond_bankfull, 'SurfaceArea (m2)'] = ht_df2.loc[cond_bankfull, 'SurfaceArea (m2)']
    ht_df4.loc[cond_bankfull, 'BedArea (m2)'] = ht_df2.loc[cond_bankfull, 'BedArea (m2)']
    ht_df4.loc[cond_bankfull, 'TopWidth (m)'] = ht_df2.loc[cond_bankfull, 'TopWidth (m)']
    ht_df4.loc[cond_bankfull, 'WettedPerimeter (m)'] = ht_df2.loc[cond_bankfull, 'WettedPerimeter (m)']
    ht_df4.loc[cond_bankfull, 'HydraulicRadius (m)'] = ht_df2.loc[cond_bankfull, 'HydraulicRadius (m)']

    # Force zero stage to have zero discharge
    ht_df4.loc[ht_df4['stage'] == 0, 'discharge_cms'] = 0
    if 'subdiv_discharge_cms' in ht_df4.columns:
        ht_df4.loc[ht_df4['stage'] == 0, 'subdiv_discharge_cms'] = 0

    # Make sure there is no nan values
    ht_df4['channel_n'] = ht_df4.groupby('HydroID')['channel_n'].ffill()
    ht_df4['overbank_n'] = ht_df4.groupby('HydroID')['overbank_n'].ffill()

    # Write src back to file
    ht_df = ht_df4.copy()

    ht_df = ht_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
    ht_df[cols_int] = ht_df[cols_int].astype(int)

    return ht_df


# *********************************************************
# *********************************************************
# -------------------------------------------------------
# Functions for Subdivision and bankfull functions for
# one branch into the optomization. This functions have
# been modified for the purpose of the optimization.
# Please see the original functions in /src/ folder
# -------------------------------------------------------
# *********************************************************
# Subdivision and bankfull functions for one branch
def src_bankfull_lookup(
    ht_df, df_bflows, huc, branch_id  # df_src src_full_filename,  # bankfull_flow_filepath
):
    ## NWM recurr rename discharge var
    df_bflows = df_bflows.rename(columns={'discharge': 'bankfull_flow'})

    ## Combine the nwm bankfull estimated flows into the SRC via feature_id
    ht_df = ht_df.merge(df_bflows, how='left', on='feature_id')

    ## Check if there are any missing data, negative or zero flow values in the bankfull_flow
    check_null = ht_df['bankfull_flow'].isnull().sum()

    if check_null > 0:
        ## Fill missing/nan nwm bankfull_flow values with -999 to handle later
        ht_df['bankfull_flow'] = ht_df['bankfull_flow'].fillna(-999)

    invalid_bflow_mask = (ht_df['bankfull_flow'] <= 0) & (ht_df['bankfull_flow'] != -999)

    if 'LakeID' in ht_df.columns:
        lake_id = pd.to_numeric(ht_df['LakeID'], errors='coerce').fillna(0)
        lake_mask = lake_id > 0
    else:
        lake_mask = pd.Series(False, index=ht_df.index)

    unexpected_invalid_bflow_mask = invalid_bflow_mask & ~lake_mask

    if unexpected_invalid_bflow_mask.any():
        bad_feature_ids = (
            ht_df.loc[unexpected_invalid_bflow_mask, 'feature_id'].drop_duplicates().astype(str).tolist()
        )

        print(
            f"WARNING: HUC: {huc}  branch id: {branch_id} --> "
            f"{len(bad_feature_ids)} non-lake feature(s) have negative or zero bankfull_flow: "
            f"{', '.join(bad_feature_ids[:10])}\n"
        )

    ## Define the channel geometry variable names to use from the src
    hradius_var = 'HydraulicRadius (m)'
    volume_var = 'Volume (m3)'
    surface_area_var = 'SurfaceArea (m2)'
    bedarea_var = 'BedArea (m2)'

    ## Locate the closest SRC discharge value to the NWM bankfull estimated flow
    ht_df['Q_bfull_find'] = (ht_df['bankfull_flow'] - ht_df['discharge_cms']).abs()

    ## Check for any missing/null entries in the input SRC
    # There may be null values for lake or coastal flow lines
    # (need to set a value to do groupby idxmin below)
    if ht_df['Q_bfull_find'].isnull().values.any():
        ht_df['Q_bfull_find'] = ht_df['Q_bfull_find'].fillna(999999)
    if ht_df['HydroID'].isnull().values.any():
        print(
            'WARNING: HUC: '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + ' --> Null values found in "HydroID"... \n'
        )

    df_bankfull_calc = ht_df[
        ['stage', 'HydroID', bedarea_var, volume_var, hradius_var, surface_area_var, 'Q_bfull_find']
    ]  # create new subset df to perform the Q_1_5 lookup
    df_bankfull_calc = df_bankfull_calc[
        df_bankfull_calc['stage'] > 0.0
    ]  # Ensure bankfull stage is greater than stage=0
    df_bankfull_calc = df_bankfull_calc.reset_index(drop=True)
    # find the index of the Q_bfull_find (closest matching flow)
    df_bankfull_calc = df_bankfull_calc.loc[
        df_bankfull_calc.groupby('HydroID')['Q_bfull_find'].idxmin()
    ].reset_index(drop=True)
    # rename volume to use later for channel portion calc
    df_bankfull_calc = df_bankfull_calc.rename(
        columns={
            'stage': 'Stage_bankfull',
            bedarea_var: 'BedArea_bankfull',
            volume_var: 'Volume_bankfull',
            hradius_var: 'HRadius_bankfull',
            surface_area_var: 'SurfArea_bankfull',
        }
    )
    ht_df = ht_df.merge(
        df_bankfull_calc[
            [
                'Stage_bankfull',
                'HydroID',
                'BedArea_bankfull',
                'Volume_bankfull',
                'HRadius_bankfull',
                'SurfArea_bankfull',
            ]
        ],
        how='left',
        on='HydroID',
    )
    ht_df = ht_df.drop(['Q_bfull_find'], axis=1)

    ## mask bankfull variables when the bankfull estimated flow value is <= 0
    ht_df['Stage_bankfull'] = ht_df['Stage_bankfull'].mask(ht_df['bankfull_flow'] <= 0.0)

    ## Create a new column to identify channel/floodplain via the bankfull stage value
    ht_df.loc[ht_df['stage'] <= ht_df['Stage_bankfull'], 'bankfull_proxy'] = 'channel'
    ht_df.loc[ht_df['stage'] > ht_df['Stage_bankfull'], 'bankfull_proxy'] = 'floodplain'
    ht_df['bankfull_proxy'] = ht_df['bankfull_proxy'].fillna('channel')

    return ht_df


def subdiv_geometry(df_src):
    ## Calculate in-channel volume & bed area
    df_src['Volume_chan (m3)'] = np.where(
        df_src['stage'] <= df_src['Stage_bankfull'],
        df_src['Volume (m3)'],
        (
            df_src['Volume_bankfull']
            + ((df_src['stage'] - df_src['Stage_bankfull']) * df_src['SurfArea_bankfull'])
        ),
    )
    df_src['BedArea_chan (m2)'] = np.where(
        df_src['stage'] <= df_src['Stage_bankfull'], df_src['BedArea (m2)'], df_src['BedArea_bankfull']
    )
    df_src['WettedPerimeter_chan (m)'] = np.where(
        df_src['stage'] <= df_src['Stage_bankfull'],
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000),
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000)
        + ((df_src['stage'] - df_src['Stage_bankfull']) * 2),
    )

    ## Calculate overbank volume & bed area
    df_src['Volume_obank (m3)'] = np.where(
        df_src['stage'] > df_src['Stage_bankfull'], (df_src['Volume (m3)'] - df_src['Volume_chan (m3)']), 0.0
    )
    df_src['BedArea_obank (m2)'] = np.where(
        df_src['stage'] > df_src['Stage_bankfull'],
        (df_src['BedArea (m2)'] - df_src['BedArea_chan (m2)']),
        0.0,
    )
    df_src['WettedPerimeter_obank (m)'] = df_src['BedArea_obank (m2)'] / df_src['LENGTHKM'] / 1000

    return df_src


def subdiv_mannings_eq(df_src):
    ## Calculate discharge (channel) using Manning's equation
    df_src = df_src.drop(
        ['WetArea_chan (m2)', 'HydraulicRadius_chan (m)', 'Discharge_chan (m3s-1)', 'Velocity_chan (m/s)'],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_chan (m2)'] = df_src['Volume_chan (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_chan (m)'] = df_src['WetArea_chan (m2)'] / df_src['WettedPerimeter_chan (m)']
    df_src['HydraulicRadius_chan (m)'] = df_src['HydraulicRadius_chan (m)'].fillna(0)
    df_src['Discharge_chan (m3s-1)'] = (
        df_src['WetArea_chan (m2)']
        * pow(df_src['HydraulicRadius_chan (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['manningN_ch_optz']
    )

    ## Calculate discharge (overbank) using Manning's equation
    df_src = df_src.drop(
        [
            'WetArea_obank (m2)',
            'HydraulicRadius_obank (m)',
            'Discharge_obank (m3s-1)',
            'Velocity_obank (m/s)',
        ],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_obank (m2)'] = df_src['Volume_obank (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_obank (m)'] = df_src['WetArea_obank (m2)'] / df_src['WettedPerimeter_obank (m)']
    df_src = df_src.replace([np.inf, -np.inf], np.nan)  # need to replace inf instances (divide by 0)
    df_src['HydraulicRadius_obank (m)'] = df_src['HydraulicRadius_obank (m)'].fillna(0)
    df_src['Discharge_obank (m3s-1)'] = (
        df_src['WetArea_obank (m2)']
        * pow(df_src['HydraulicRadius_obank (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['manningN_ob_optz']
    )

    ## Calcuate the total of the subdivided discharge (channel + overbank)
    df_src = df_src.drop(
        ['Discharge (m3s-1)_subdiv'], axis=1, errors='ignore'
    )  # drop these cols (in case subdiv was previously performed)
    df_src['Discharge (m3s-1)_subdiv'] = df_src['Discharge_chan (m3s-1)'] + df_src['Discharge_obank (m3s-1)']
    df_src.loc[df_src['stage'] == 0, ['Discharge (m3s-1)_subdiv']] = 0

    return df_src
