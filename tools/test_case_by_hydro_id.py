#!/usr/bin/env python3

import argparse
import os
import shutil
import sys
import traceback
import warnings
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from pixel_counter import zonal_stats
from run_test_case import Test_Case
from shapely.validation import make_valid
from tools_shared_functions import compute_stats_from_contingency_table
from tqdm import tqdm

import utils.fim_logger as fl
from utils.shared_variables import VIZ_PROJECTION


warnings.filterwarnings("ignore", category=FutureWarning, module="gdal")
gpd.options.io_engine = "pyogrio"

# global RLOG
FLOG = fl.FIM_logger()  # the non mp version

"""
This module uses zonal stats to subdivide alpha metrics by each HAND catchment.
The output is a vector geopackage and is also known as the "FIM Performance" layer
when loaded into HydroVIS. At the time of this commit, it takes approximately
20 to 32 hours to complete.

"""


#####################################################
# Perform zonal stats is a funtion stored in pixel_counter.py.
# The input huc_gpkg is a single huc8 geopackage, the second input argument must be input as a dict.
# For the purposes of assembling the alpha metrics by hydroid, always use agreement_raster total
#   area agreement tiff.
# This function is called automatically. Returns stats dict of pixel counts.
#####################################################
def perform_zonal_stats(huc_gpkg, agree_rast):
    stats = zonal_stats(huc_gpkg, {"agreement_raster": agree_rast}, nodata_value=10)
    return stats


#####################################################
# Creates a pandas df containing Alpha stats by hydroid.
# Stats input is the output of zonal_stats function.
# Huc8 is the huc8 string and is passed via the directory loop during execution.
# Mag is the magnitude (100y, action, minor etc.) is passed via the directory loop.
# Bench is the benchmark source.
#####################################################
def assemble_hydro_alpha_for_single_huc(stats, huc8, mag, bench):
    in_mem_df = pd.DataFrame(
        columns=[
            'HydroID',
            'huc8',
            'contingency_tot_area_km2',
            'CSI',
            'FAR',
            'TPR',
            'TNR',
            'PPV',
            'NPV',
            'Bal_ACC',
            'MCC',
            'EQUITABLE_THREAT_SCORE',
            'PREVALENCE',
            'BIAS',
            'F1_SCORE',
            'masked_perc',
            'MAG',
            'BENCH',
        ]
    )

    FLOG.trace(f"Assemble hydro for huc is {huc8} for {mag} and  {bench}")

    for dicts in stats:
        tot_pop = dicts['tn'] + dicts['fn'] + dicts['fp'] + dicts['tp']
        if tot_pop == 0:
            continue

        stats_dictionary = compute_stats_from_contingency_table(
            dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count=dicts['mp']
        )

        HydroID = dicts['HydroID']

        contingency_tot_area_km2 = float(stats_dictionary['contingency_tot_area_km2'])
        if contingency_tot_area_km2 != 'NA':
            contingency_tot_area_km2 = round(contingency_tot_area_km2, 2)

        CSI = stats_dictionary['CSI']
        if CSI != 'NA':
            CSI = round(CSI, 2)

        FAR = stats_dictionary['FAR']
        if FAR != 'NA':
            FAR = round(FAR, 2)

        TPR = stats_dictionary['TPR']
        if TPR != 'NA':
            TPR = round(TPR, 2)

        TNR = stats_dictionary['TNR']
        if TNR != 'NA':
            TNR = round(TNR, 2)

        PPV = stats_dictionary['PPV']
        if PPV != 'NA':
            PPV = round(PPV, 2)

        NPV = stats_dictionary['NPV']
        if NPV != 'NA':
            NPV = round(NPV, 2)

        Bal_ACC = stats_dictionary['Bal_ACC']
        if Bal_ACC != 'NA':
            Bal_ACC = round(Bal_ACC, 2)

        MCC = float(stats_dictionary['MCC'])
        if MCC != 'NA':
            MCC = round(MCC, 2)

        EQUITABLE_THREAT_SCORE = stats_dictionary['EQUITABLE_THREAT_SCORE']
        if EQUITABLE_THREAT_SCORE != 'NA':
            EQUITABLE_THREAT_SCORE = round(EQUITABLE_THREAT_SCORE, 2)

        PREVALENCE = stats_dictionary['PREVALENCE']
        if PREVALENCE != 'NA':
            PREVALENCE = round(PREVALENCE, 2)

        BIAS = stats_dictionary['BIAS']
        if BIAS != 'NA':
            BIAS = round(BIAS, 2)

        F1_SCORE = stats_dictionary['F1_SCORE']
        if F1_SCORE != 'NA':
            F1_SCORE = round(F1_SCORE, 2)

        masked_perc = stats_dictionary['masked_perc']
        if masked_perc != 'NA':
            masked_perc = round(masked_perc, 2)

        # HydroID = stats_dictionary['HydroID']

        dict_with_list_values = {
            'HydroID': [HydroID],
            'huc8': [huc8],
            'contingency_tot_area_km2': [contingency_tot_area_km2],
            'CSI': [CSI],
            'FAR': [FAR],
            'TPR': [TPR],
            'TNR': [TNR],
            'PPV': [PPV],
            'NPV': [NPV],
            'Bal_ACC': [Bal_ACC],
            'MCC': [MCC],
            'EQUITABLE_THREAT_SCORE': [EQUITABLE_THREAT_SCORE],
            'PREVALENCE': [PREVALENCE],
            'BIAS': [BIAS],
            'F1_SCORE': [F1_SCORE],
            'masked_perc': [masked_perc],
            'MAG': [mag],
            'BENCH': [bench],
        }

        dict_to_df = pd.DataFrame(
            dict_with_list_values,
            columns=[
                'HydroID',
                'huc8',
                'contingency_tot_area_km2',
                'CSI',
                'FAR',
                'TPR',
                'TNR',
                'PPV',
                'NPV',
                'Bal_ACC',
                'MCC',
                'EQUITABLE_THREAT_SCORE',
                'PREVALENCE',
                'BIAS',
                'F1_SCORE',
                'masked_perc',
                'MAG',
                'BENCH',
            ],
        )

        concat_list = [in_mem_df, dict_to_df]
        in_mem_df = pd.concat(concat_list, sort=False)

    return in_mem_df


def catchment_zonal_stats(benchmark_category, version, output_file_name):
    # Execution code
    csv_output = gpd.GeoDataFrame(
        columns=[
            'HydroID',
            'huc8',
            'contingency_tot_area_km2',
            'CSI',
            'FAR',
            'TPR',
            'TNR',
            'PPV',
            'NPV',
            'Bal_ACC',
            'MCC',
            'EQUITABLE_THREAT_SCORE',
            'PREVALENCE',
            'BIAS',
            'F1_SCORE',
            'masked_perc',
            'MAG',
            'BENCH',
            'geometry',
        ],
        geometry='geometry',
    ).set_crs(VIZ_PROJECTION)

    # This funtion, relies on the Test_Case class defined in run_test_case.py to list all available test cases
    all_test_cases = Test_Case.list_all_test_cases(
        version=version,
        archive=True,
        benchmark_categories=[] if benchmark_category == "all" else [benchmark_category],
    )

    num_test_cases = len(all_test_cases)
    FLOG.lprint("")
    FLOG.lprint(f'Processing {num_test_cases} test cases...')

    missing_hucs = []

    # easier to filter by one huc for debugging purposes
    # debug_test_hucs = ["12090301", "07100007", "19020302"]
    # tqdm will likely not work with all of the printd
    for test_case_class in tqdm(all_test_cases, desc=f'Running {len(all_test_cases)} test cases'):
        if not os.path.exists(test_case_class.fim_dir):
            FLOG.warning(f'{test_case_class.fim_dir} does not exist')
            missing_hucs.append(test_case_class.huc)
            continue

        # DEBUGGING CODE
        # huc = test_case_class.huc
        # if huc not in  debug_test_hucs:
        #     print(f"skipped {huc}")
        #     continue

        FLOG.lprint(f"Processing {test_case_class.test_id}")
        test_case_processing_start = datetime.now(timezone.utc)

        agreement_dict = test_case_class.get_current_agreements()

        # We are only using branch 0 catchments to define boundaries for zonal stats
        catchment_gpkg = os.path.join(
            test_case_class.fim_dir,
            'branches',
            '0',
            "gw_catchments_reaches_filtered_addedAttributes_crosswalked_0.gpkg",
        )

        catchment_geom = gpd.read_file(catchment_gpkg)
        catchment_geom['geometry'] = catchment_geom.apply(lambda row: make_valid(row.geometry), axis=1)

        for agree_rast in agreement_dict:

            define_mag = agree_rast.split(version)
            define_mag_1 = define_mag[1].split('/')
            mag = define_mag_1[1]

            FLOG.trace(f"Processing {test_case_class.test_id}: {mag}")
            FLOG.trace(define_mag[1])
            FLOG.trace(f"catchment_gpkg : {catchment_gpkg} - agree_rast is {agree_rast}")
            stats = perform_zonal_stats(catchment_gpkg, agree_rast)
            if stats == []:
                FLOG.lprint(f"{test_case_class.test_id}: No zonal stats for {mag}")
                continue

            hydro_geom_df = catchment_geom[["HydroID", "geometry"]]

            in_mem_df = assemble_hydro_alpha_for_single_huc(
                stats, test_case_class.huc, mag, test_case_class.benchmark_cat
            )

            FLOG.trace(f"merging geom output: {test_case_class.test_id}: magnitude = {mag}")

            geom_output = hydro_geom_df.merge(in_mem_df, on='HydroID', how='inner').to_crs(VIZ_PROJECTION)

            csv_output = pd.concat([geom_output, csv_output], sort=False)

        FLOG.lprint(f"Processing complete for {test_case_class.test_id}")
        # calculate duration per test case
        test_case_processing_end = datetime.now(timezone.utc)
        time_duration = test_case_processing_end - test_case_processing_start
        FLOG.lprint(f".. Duration: {str(time_duration).split('.')[0]}")

    if missing_hucs:
        FLOG.warning(f"There were {len(missing_hucs)} HUCs missing from the input FIM version")
        FLOG.warning(missing_hucs)

    csv_output.sort_values(by=['huc8', 'BENCH'])
    # Add version information to csv_output dataframe
    csv_output['version'] = version

    # Change the index into a named column of "oid" (for HV), then move it to the front
    csv_output["oid"] = csv_output.index
    oid_col = csv_output.pop("oid")
    csv_output.insert(0, oid_col.name, oid_col)

    # we don't want to save this group by, just show the size
    FLOG.lprint(f"total grouped by benchmark: {csv_output.groupby('BENCH').size()}")
    FLOG.lprint("------------------------------------")

    FLOG.lprint(f'Writing geopackage {output_file_name}')
    csv_output.to_file(output_file_name, index=False, driver="GPKG", engine='fiona')

    FLOG.lprint('Writing to CSV')
    csv_path = output_file_name.replace(".gpkg", ".csv")
    csv_output.to_csv(csv_path, index=False)  # Save to CSV


if __name__ == "__main__":

    """
    Example usage:
    python /foss_fim/tools/test_case_by_hydro_id.py \
        -b all \
        -v fim_4_5_11_1 \
        -g /outputs/fim_performance/hand_4_5_11_1/fim_performance_catchments.gpkg
    """

    parser = argparse.ArgumentParser(description='Produces alpha metrics by hydro id.')

    parser.add_argument(
        '-b',
        '--benchmark_category',
        help='Choice of truth data. Options are: all, ble, ifc, nws, usgs, ras2fim',
        required=True,
    )
    parser.add_argument(
        '-v',
        '--version',
        help='The fim version to use. eg) hand_4_5_11_1. Note: folder must be in the previous_fim folder.',
        required=True,
    )
    parser.add_argument(
        '-g',
        '--gpkg',
        help='Filepath and filename to hold exported gpkg file.'
        ' eg. /data/fim_performance/hand_4_5_11_1/fim_performance_catchments.gpkg.'
        ' A CSV with the same name will also be written.',
        required=True,
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    benchmark_category = args['benchmark_category']
    version = args['version']
    gpkg_file = args['gpkg']

    # TODO: Oct 2024: This logic below should be moved into a function
    # leaving nothing here but just loading args and passing them to the function

    print("================================")
    print("Start test_case_by_hydroid.py")

    # =======================
    # Validate and setup enviro

    # adjust and check file extension
    # Why change extension to lower case? Makes it easier to make a log file name from it

    gpkg_file_name = os.path.basename(gpkg_file)
    file_name_segs = os.path.splitext(gpkg_file_name)

    if len(file_name_segs) != 2:
        raise Exception(f"file name of {gpkg_file_name} appears invalid (missing extension)")
    orig_ext = file_name_segs[1]
    new_ext = orig_ext.lower()
    if new_ext != ".gpkg":
        raise Exception(f"file name of {gpkg_file_name} appears invalid (not a gpkg extension)")

    # we are going to replace the .gpkg extension lower
    gpkg_file_name.replace(orig_ext, new_ext)
    output_folder = os.path.dirname(gpkg_file)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    # adjusted file name with extension as final output file
    output_file_name = os.path.join(output_folder, gpkg_file_name)
    FLOG.lprint(f"Output file will be {output_file_name}")
    log_folder_path = os.path.join(output_folder, "logs")

    log_output_file = FLOG.calc_log_name_and_path(log_folder_path, "test_by_hydro_id")
    # file names looks like this ie: {prefix}_{yyyy}_{mm}_{dd}-{hr_min_sec}
    # (folder path}/logs/test_by_hydro_id_2024_10_26_-13_21_43.log
    FLOG.setup(log_output_file)

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    FLOG.lprint("================================")
    FLOG.lprint(f"Start Test Case by Hydro ID - (UTC): {dt_string}")
    FLOG.lprint("")

    FLOG.trace(f'Runtime args: {args}')
    print(f"log file being created as {FLOG.LOG_FILE_PATH}")

    try:
        catchment_zonal_stats(benchmark_category, version, output_file_name)
    except Exception:
        # FLOG.critical generally means stop the program
        # FLOG.error means major problem but execution continues (sometimes in MP)
        FLOG.critical("Execution failed")
        FLOG.critical(traceback.format_exc())
        sys.exit(1)

    FLOG.lprint("================================")
    FLOG.lprint("End test_case_by_hydroid")

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    FLOG.lprint(f"Ended (UTC): {dt_string}")

    # calculate duration
    time_duration = overall_end_time - overall_start_time
    FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    print()
