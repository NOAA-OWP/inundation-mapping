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

from src.utils.shared_functions import FIM_Helpers as fh


# NOTE: Jun 2026:  With major updates being made to inudation files, this tool will no longer
# work and requires major updates including optimizations.
# It was agreed that it was ok to defer as this script might not even be used anymore

# TODO: Jun 2026: Why do we have so much duplication to run_test_cases.py
# if we do want to keep this and not rebuild... consider rebuilding it to use our
# standard logging system. See synthesize_test_case.py, run_test_case.py and various
# inundation files under it.
# At a min, review the part that calls inundation.inundate and compare to inundate_gms.py
# But this script needs to be rebuilt anyways and call inundate_gms.py and not duplicate it here.
# This script is now very out of date, including the need to remove FIM 3 references.


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
    mask_dict = {
        'levees': {
            'path': '/data/inputs/nld_vectors/Levee_protected_areas.gpkg',
            'buffer': None,
            'operation': 'exclude',
        },
        'waterbodies': {
            'path': '/data/inputs/nwm_hydrofabric/nwm_lakes.gpkg',
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
# TODO: Jun 2026: Along with comparing and updating this against run_test_case.py, also
# adjust for workers. Temp disabled here.
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
        Number of worker processes assigned to processing.
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
                    hydroTable_all, test_case_dic, magnitude, instance, model=model, verbose=verbose
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
    hydroTable_all, test_case_dic, magnitude, lid, compute_only=False, model='', verbose=False
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
        # TODO: Jun 2026: Mosaic_inundation has our standard logging added to it
        # To test: Will it blow up now with logging not being added here?  Likely not
        # A python logger default, (not ours in shared_functions), knows how
        # to compensate if a log file does not exist. Google "python logger what if file is not set"
        mosaic_file_path = Mosaic_inundation(
            map_file.copy(),
            mosaic_attribute=mosaic_attribute,
            mosaic_output=mosaic_output,
            # mask=mask,
            unit_attribute_name=unit_attribute_name,
            nodata=elev_raster_ndv,
            remove_inputs=remove_intermediate,
            verbose=verbose,
            is_mosaic_for_branches=is_mosaic_for_branches,
            inundation_polygon=inundation_polygon,
            show_progress_bar=False,
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

    # TODO: Jun 2026: We likely need a test to ensure hucs has at least one value. It likely always has only one. TBD

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
    # TODO: Jun 2026: This needs a major upgrade to manage memory leaks which can easily occur here.
    # See the run_test_case chain and inundate_gms for examples of both multi-procs and multi-thread
    # That update should also update how TQDM is used in order to not leave hung TQDM threads which
    # have been seen.
    # This can trigger memory leaks as is.
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
        # TODO: Jun 2026: This wil break as inundate hucCode and branch_id will not exist
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
        huc = str(row[0])
        branch_id = str(row[1])

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
            "aggregate": False,
            "inundation_raster": inundation_branch_raster,
            # "inundation_polygon": inundation_branch_polygon,
            "depths": depths_branch_raster,
            # "out_raster_profile": None,
            # "out_vector_profile": None,
            "verbose": verbose,
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
# Reseting stage column in SRCs
def reset_stage(srcs_df):

    srcs_df = srcs_df.sort_values('stage').reset_index(drop=True)
    step = 0.3048
    srcs_df['stage'] = np.array([round(i * step, 4) for i in range(len(srcs_df))])

    return srcs_df


# -------------------------------------------------------
# Extending src_df with linear_extrapolation for missing stages in thalweg notches
def extend_src_linear_extrapolation(srcs_df, stages_full):

    # Number of the last rows of src to include in extrapolation
    num_rows = 3
    # Identify all value columns except 'stage'
    src_cols = [col for col in srcs_df.columns if col not in ['stage']]

    existing_stages = srcs_df['stage'].values
    # If already complete, return early
    if len(existing_stages) == len(stages_full):
        return srcs_df

    # Prepare existing src_df
    existing_src = srcs_df.set_index('stage')

    # Build DataFrame for all target stages
    extended_src = pd.DataFrame({'stage': stages_full})
    extended_src['HydroID'] = srcs_df['HydroID'].iloc[0]
    extended_src = extended_src.set_index('stage')

    # For each value column, interpolate/extrapolate as needed
    for col in src_cols:
        # Columns to extrapolate
        col_variables = [
            'Number of Cells',
            'SurfaceArea (m2)',
            'BedArea (m2)',
            'Volume (m3)',
            'TopWidth (m)',
            'WettedPerimeter (m)',
            'WetArea (m2)',
            'HydraulicRadius (m)',
            'discharge_cms',
        ]
        if col in col_variables:
            # Only use non-NaN values for fitting
            mask = ~np.isnan(existing_src.index.values[-num_rows:]) & ~np.isnan(
                existing_src[col].values[-num_rows:]
            )
            x = existing_src.index.values[-num_rows:][mask]
            y = existing_src[col].values[-num_rows:][mask]

            if len(x) >= 2 and np.var(x) > 1e-8:  # Ensure valid data & non-constant x
                try:
                    coeffs = np.polyfit(x, y, 1)
                    extended_src[col] = np.polyval(coeffs, extended_src.index.values)
                except np.linalg.LinAlgError:
                    # Fallback: Use last valid value if linear fit fails
                    last_valid = y[-1] if len(y) > 0 else existing_src[col].iloc[-1]
                    extended_src[col] = last_valid
            else:  # Not enough data or constant x-values
                last_valid = existing_src[col].iloc[-1]
                extended_src[col] = last_valid

            # Overwrite with original values where available
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]

        else:  # Repeat last value for missing
            # Fill with NaN first
            extended_src[col] = np.nan
            # Assign existing values
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]
            # Find missing stages and fill with last value
            existing_stages_sorted = np.sort(existing_src.index.values)
            last_value = existing_src[col].loc[existing_stages_sorted[-1]]

            for stage in stages_full:
                if pd.isna(extended_src.at[stage, col]):
                    extended_src.at[stage, col] = last_value

    extended_src = extended_src.reset_index()

    return extended_src


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
            * (srcs_df.loc[row_slice, 'HydraulicRadius_chan (m)'] ** (2.0 / 3))
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
    ht_df2 = ht_df2.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)

    # Removing thalweg notche rows from SRCs and
    # Applying extend_src_linear_extrapolation to add missing rows
    cond_ThalwegNRows = (ht_df2['Number of Cells'] == 0) & (ht_df2['stage'] > 0)
    if cond_ThalwegNRows.sum() > 0:
        ht_df_skipTwNRows = ht_df2[~cond_ThalwegNRows].copy()
        ht_df_skipTwNRows_gb = (
            ht_df_skipTwNRows.groupby('HydroID', group_keys=False).apply(reset_stage).reset_index(drop=True)
        )

        ht_df3 = ht_df_skipTwNRows_gb.copy()

        # Applying extend_src_linear_extrapolation to add missing rows
        # Identify the standard stages
        step = 0.3048
        stages_full = np.array([round(i * step, 4) for i in range(84)])

        # Apply extend_src_linear_extrapolation to each ht_df
        ht_df3 = (
            ht_df3.groupby('HydroID', group_keys=False)
            .apply(lambda src_g: extend_src_linear_extrapolation(src_g, stages_full))
            .sort_values(['HydroID', 'stage'])
            .reset_index(drop=True)
        )
        ht_df3[cols_int] = ht_df3[cols_int].astype(int)

    else:
        ht_df3 = ht_df2.copy()

    # Adjusting src tables for nonmonotonic SRCs
    # Excluding hydroIDs that already fixed in thalweg notches adjustment
    ht_df4 = ht_df3.groupby('HydroID', group_keys=False).apply(
        analyze_nonmonotonic_src, strm_order=strm_order  # , thalweg_hydroids = thalweg_hydroids
    )

    # Make sure nonmonotonic adjustment just applied within in-channel stages
    cond_bankfull = ht_df3['bankfull_proxy'] == 'floodplain'

    ht_df4.loc[cond_bankfull, 'discharge_cms'] = ht_df3.loc[cond_bankfull, 'discharge_cms']
    ht_df4.loc[cond_bankfull, 'SurfaceArea (m2)'] = ht_df3.loc[cond_bankfull, 'SurfaceArea (m2)']
    ht_df4.loc[cond_bankfull, 'BedArea (m2)'] = ht_df3.loc[cond_bankfull, 'BedArea (m2)']
    ht_df4.loc[cond_bankfull, 'TopWidth (m)'] = ht_df3.loc[cond_bankfull, 'TopWidth (m)']
    ht_df4.loc[cond_bankfull, 'WettedPerimeter (m)'] = ht_df3.loc[cond_bankfull, 'WettedPerimeter (m)']
    ht_df4.loc[cond_bankfull, 'HydraulicRadius (m)'] = ht_df3.loc[cond_bankfull, 'HydraulicRadius (m)']

    # Force zero stage to have zero discharge
    ht_df4.loc[ht_df4['stage'] == 0, 'discharge_cms'] = 0

    # Write src back to file
    ht_df = ht_df4.copy()

    ht_df = ht_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
    ht_df[cols_int] = ht_df[cols_int].astype(int)

    return ht_df


# -------------------------------------------------------
# Functions for incorporating longitudinal flow adjustment
# into the optomization. This functions have been modified
# for the purpose of the optimization. Please see the
# original functions in /src/ folder
# -------------------------------------------------------
def extract_longitudinal_variables(src_df, hydroid, stage):
    """
    Function for extracting hydraulic variable to longitudinal smooth
    along a stream in synthetic rating curves.
    Candidate_variables_to_smooth_longitudinally = [
        'discharge_cms'
    ]
        Parameters
        ----------
        src_df : dataframe
            Synthetic rating curve dataframe.
        hydroid : str
            Fim hydroid string.
        stage : float
            Fim stage.

        Returns
        ----------
        voi_hid_stage : list

    """

    src = src_df.loc[src_df.HydroID == hydroid]

    if src.LakeID.iloc[0] > 0:
        return [np.nan]
    else:
        flow = round(np.interp(stage, src.stage, src['discharge_cms']), 2)

    voi_hid_stage = [flow]

    return voi_hid_stage


# -------------------------------------------------------
def min_ignore_zeros(lst):
    """
    Function for calculation non-zero minimumns.

        Parameters
        ----------
        lst : list

        Returns
        ----------
        minimum : float

    """
    nonzero = lst[lst > 0]
    if nonzero.size > 0:
        return np.min(nonzero)
    else:
        return 0


# -------------------------------------------------------
def filter_voi(voi_array):
    """
    Function for a gaussian and minimum filtering on an array.

        Parameters
        ----------
        voi_array : array

        Returns
        ----------
        gfilter

    """
    minfilter = generic_filter(voi_array, min_ignore_zeros, size=4)
    gfilter = scipy.ndimage.gaussian_filter1d(minfilter, sigma=2, radius=2)
    return gfilter


# -------------------------------------------------------
# Main function for the longitudinal flow adjustment
def filter_longitudinal_discharge_jitters(fim_dir, ht_df):
    """
    Function for smoothing longitudinal jitters in any variables
    of interest along a stream in synthetic rating curves.
    This will only correct GMS branch's SRCs based on the hydro_ids.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.

        Returns
        ----------
        log_text : str

    """
    huc = str(int(float(ht_df['HUC'].iloc[0])))
    branch = str(int(float(ht_df['branch'].iloc[0])))
    if len(huc) != 8:
        huc = '0' + huc

    if int(branch) > 0:  # Just for GMS branches
        ht_df = ht_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
        cathment_gpkg = join(
            fim_dir,
            huc,
            'branches',
            branch,
            f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.gpkg',
        )
        # Longitudinally adjust srcs for WSE
        catchment_gdf0 = gpd.read_file(cathment_gpkg)
        catchment_gdf = catchment_gdf0.drop_duplicates(subset=['HydroID'], keep='first')

        stages = [round(num, 4) for num in ht_df['stage'][0:84]]

        # Defining stages with discharge = 0 and Number of Cells = 0 for later masking
        Q0_mask = ht_df['discharge_cms'] == 0
        nocell0_mask = ht_df['Number of Cells'] == 0

        headwaters_rows = catchment_gdf.loc[
            ~catchment_gdf.HydroID.isin(catchment_gdf.NextDownID.astype(int)),
        ]
        # Remove headwaters with lakeID
        headwaters = list(headwaters_rows[headwaters_rows['LakeID'] < 0]['HydroID'])

        # Build hydroid chain first
        hydroid_chain_mhws = []
        for headwater in headwaters:
            hydroid_chain = [headwater]
            nexthydroid = headwater
            # While loop to create the list of hydroids
            while catchment_gdf.HydroID.isin([nexthydroid]).any():
                nexthydroid = int(
                    catchment_gdf.loc[catchment_gdf.HydroID == nexthydroid, "NextDownID"].item()
                )
                hydroid_chain.append(nexthydroid)

            if len(hydroid_chain[:-1]) > 2:  # Excluding headwaters with len 2 or smaller
                hydroid_chain_mhws.append(hydroid_chain)

        # Makes a logitudinal dataframes of variables of interests
        keys = ['discharge_cms']
        original_all_voi = {}
        filtered_all_voi = {}
        if len(hydroid_chain_mhws) > 0:
            for ikey in range(len(keys[0:1])):  # Just apply to discharge
                voi2smooth_mhws = []
                filtered_voi_mhws = []
                for hydroid_chain in hydroid_chain_mhws:
                    voi2smooth_df = dict()
                    filtered_voi_df = dict()
                    long_index = 0
                    for nexthydroid in hydroid_chain[:-1]:  # Excluding the last HydroID
                        voi2smooth_list = []
                        for stage in stages:
                            voi2smooth_list.append(
                                extract_longitudinal_variables(ht_df, nexthydroid, stage)[ikey]
                            )
                        voi2smooth_df[nexthydroid] = voi2smooth_list + [long_index]
                        long_index += 1

                    stages_cols = [str(istg) for istg in stages]
                    voi2smooth_df = pd.DataFrame.from_dict(
                        voi2smooth_df, orient="index", columns=stages_cols + ['long_position']
                    )
                    # Applies 2 filters of minimum and gaussian
                    # on the logitudinal surface area, volume and bedArea
                    for stage in stages_cols:
                        filtered_voi_array = filter_voi(voi2smooth_df[stage])
                        filtered_voi_df[stage] = list(filtered_voi_array)

                    # Convert filtered_voi_df to a DataFrame
                    filtered_voi_df = pd.DataFrame.from_dict(filtered_voi_df, orient="columns")
                    # Align indices and add "long_position"
                    filtered_voi_df.index = voi2smooth_df.index  # Ensure indices match
                    filtered_voi_df["long_position"] = voi2smooth_df["long_position"]

                    voi2smooth_mhws.append(voi2smooth_df)
                    filtered_voi_mhws.append(filtered_voi_df)

                voi2smooth_mhws_df = pd.concat(voi2smooth_mhws)
                filtered_voi_mhws_df = pd.concat(filtered_voi_mhws)

                # Add the dataframe to the dictionary
                original_all_voi[keys[ikey]] = voi2smooth_mhws_df
                filtered_all_voi[keys[ikey]] = filtered_voi_mhws_df

            # Defining a lake_discharge dataframe
            Q_lake_hydroID = ht_df[['HydroID', 'LakeID', 'stage', 'discharge_cms']]
            for jkey in range(len(keys[0:1])):  # Just apply to discharge
                # Reshaping variables of interest (voi) to be included in hydrotable
                filtered_voi = filtered_all_voi[keys[jkey]].drop('long_position', axis=1)
                reshaped_filtered_voi = filtered_voi.reset_index().melt(
                    id_vars='index', var_name='stage', value_name=f'Filtered_{keys[jkey]}'
                )
                # print(reshaped_filtered_voi)
                reshaped_filtered_voi.rename(columns={'index': 'HydroID'}, inplace=True)
                reshaped_filtered_voi['stage'] = reshaped_filtered_voi['stage'].astype(float)

                # Adding filtered SurfaceArea, volume and bedarea to src
                ht_df = ht_df.merge(
                    reshaped_filtered_voi[['HydroID', 'stage', f'Filtered_{keys[jkey]}']],
                    on=['HydroID', 'stage'],
                    how='left',
                )
                # Update voi including SurfaceArea (m2), 'BedArea (m2)' and 'Volume (m3)' in src
                # Update ht_df where LakeID > 0 and stage matches
                mask_src = (ht_df[f'Filtered_{keys[jkey]}'].notna()) & (ht_df['LakeID'] < 0)
                ht_df.loc[mask_src, keys[jkey]] = ht_df.loc[mask_src, f'Filtered_{keys[jkey]}']

            # Refining Discharge for lake hydroIDs with the original Q
            # Merge with ht_df
            ht_df_merged = ht_df.merge(
                Q_lake_hydroID, on=['HydroID', 'LakeID', 'stage'], how='left', suffixes=('', '_lake')
            )
            # Update ht_df where LakeID > 0 and stage matches
            mask = (ht_df_merged['LakeID'] > 0) & (ht_df_merged['discharge_cms_lake'].notnull())
            ht_df.loc[mask, 'discharge_cms'] = ht_df_merged.loc[mask, 'discharge_cms_lake']
            ht_df = ht_df.round(5)

            # Set Hydraulic properties of original stages with discharge = 0 back to 0
            ht_df.loc[Q0_mask, 'discharge_cms'] = 0

            # Set cahnnel properties of original stages with Number of Cells = 0 back to 0
            ht_df.loc[nocell0_mask, 'Number of Cells'] = 0
            ht_df.loc[nocell0_mask, 'SurfaceArea (m2)'] = 0
            ht_df.loc[nocell0_mask, 'TopWidth (m)'] = 0

            # Set nans to 0
            ht_df.loc[ht_df['stage'] == 0, 'discharge_cms'] = 0
            ht_df = ht_df.drop(columns=[f'Filtered_{keys[jkey]}'], axis=1, errors='ignore')

    return ht_df
