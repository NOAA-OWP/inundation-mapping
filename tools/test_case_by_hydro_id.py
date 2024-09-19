#!/usr/bin/env python3

import argparse
import os
import traceback
from datetime import datetime

import geopandas as gpd
import pandas as pd
from pixel_counter import zonal_stats
from run_test_case import Test_Case
from shapely.validation import make_valid
from tools_shared_functions import compute_stats_from_contingency_table
from tqdm import tqdm


gpd.options.io_engine = "pyogrio"

"""
This module uses zonal stats to subdivide alpha metrics by each HAND catchment.
The output is a vector geopackage and is also known as the "FIM Performance" layer
when loaded into HydroVIS. At the time of this commit, it takes approximately
32 hours to complete.

Example usage:
python /foss_fim/tools/test_case_by_hydro_id.py \
    -b all \
    -v fim_4_5_2_11 \
    -g /outputs/fim_performance_v4_5_2_11.gpkg \
    -l
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

    for dicts in stats:
        tot_pop = dicts['tn'] + dicts['fn'] + dicts['fp'] + dicts['tp']
        if tot_pop == 0:
            continue

        stats_dictionary = compute_stats_from_contingency_table(
            dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count=dicts['mp']
        )
        # Calls compute_stats_from_contingency_table from run_test_case.py

        hydroid = dicts['HydroID']
        stats_dictionary['HydroID'] = hydroid

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

        HydroID = stats_dictionary['HydroID']

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


def catchment_zonal_stats(benchmark_category, version, csv, log):
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
    ).set_crs('EPSG:3857')

    # This funtion, relies on the Test_Case class defined in run_test_case.py to list all available test cases
    all_test_cases = Test_Case.list_all_test_cases(
        version=version,
        archive=True,
        benchmark_categories=[] if benchmark_category == "all" else [benchmark_category],
    )
    print(f'Found {len(all_test_cases)} test cases')
    if log:
        log.write(f'Found {len(all_test_cases)} test cases...\n')
    missing_hucs = []

    for test_case_class in tqdm(all_test_cases, desc=f'Running {len(all_test_cases)} test cases'):
        if not os.path.exists(test_case_class.fim_dir):
            print(f'{test_case_class.fim_dir} does not exist')
            missing_hucs.append(test_case_class)
            if log:
                log.write(f'{test_case_class.fim_dir} does not exist\n')
            continue

        if log:
            log.write(test_case_class.test_id + '\n')

        agreement_dict = test_case_class.get_current_agreements()

        for agree_rast in agreement_dict:

            # We are only using branch 0 catchments to define boundaries for zonal stats
            catchment_gpkg = os.path.join(
                test_case_class.fim_dir,
                'branches',
                "gw_catchments_reaches_filtered_addedAttributes_crosswalked_0.gpkg",
            )

            define_mag = agree_rast.split(version)
            define_mag_1 = define_mag[1].split('/')
            mag = define_mag_1[1]

            if log:
                log.write(f'  {define_mag[1]}\n')

            stats = perform_zonal_stats(catchment_gpkg, agree_rast)
            if stats == []:
                continue

            get_geom = gpd.read_file(catchment_gpkg)

            get_geom['geometry'] = get_geom.apply(lambda row: make_valid(row.geometry), axis=1)

            in_mem_df = assemble_hydro_alpha_for_single_huc(
                stats, test_case_class.huc, mag, test_case_class.benchmark_cat
            )

            hydro_geom_df = get_geom[["HydroID", "geometry"]]

            geom_output = hydro_geom_df.merge(in_mem_df, on='HydroID', how='inner').to_crs('EPSG:3857')

            concat_df_list = [geom_output, csv_output]

            csv_output = pd.concat(concat_df_list, sort=False)

    if missing_hucs:
        log.write(
            f"There were {len(missing_hucs)} HUCs missing from the input FIM version:\n"
            + "\n".join([h.fim_dir for h in missing_hucs])
        )

    print()
    print(csv_output.groupby('BENCH').size())
    print(f'total     {len(csv_output)}')
    log.write("\n------------------------------------\n")
    csv_output.groupby('BENCH').size().to_string(log)
    log.write(f'\ntotal     {len(csv_output)}\n')

    print('Writing to GPKG')
    log.write(f'Writing geopackage {csv}\n')
    csv_output.to_file(csv, driver="GPKG", engine='fiona')

    # Add version information to csv_output dataframe
    csv_output['version'] = version

    print('Writing to CSV')
    csv_path = csv.replace(".gpkg", ".csv")
    log.write(f'Writing CSV {csv_path}\n')
    csv_output.to_csv(csv_path)  # Save to CSV


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produces alpha metrics by hyrdoid.')

    parser.add_argument(
        '-b',
        '--benchmark_category',
        help='Choice of truth data. Options are: all, ble, ifc, nws, usgs, ras2fim',
        required=True,
    )
    parser.add_argument(
        '-v', '--version', help='The fim version to use. Should be similar to fim_3_0_24_14_ms', required=True
    )
    parser.add_argument(
        '-g',
        '--gpkg',
        help='Filepath and filename to hold exported gpkg file. '
        'Similar to /data/path/fim_performance_catchments.gpkg. A CSV with the same name will also be written.',
        required=True,
    )
    parser.add_argument(
        '-l',
        '--log',
        help='Optional flag to write a log file with the same name as the --GPKG.',
        required=False,
        default=None,
        action='store_true',
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    benchmark_category = args['benchmark_category']
    version = args['version']
    csv = args['gpkg']
    log = args['log']

    print("================================")
    print("Start test_case_by_hydroid.py")
    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"started: {dt_string}")
    print()

    ## Initiate log file
    if log:
        log = open(csv.replace('.gpkg', '.log'), "w")
        log.write('START TIME: ' + str(start_time) + '\n')
        log.write('#########################################################\n\n')
        log.write('')
        log.write(f'Runtime args:\n {args}\n\n')

    # This is the main execution -- try block is to catch and log errors
    try:
        catchment_zonal_stats(benchmark_category, version, csv, log)
    except Exception as ex:
        print(f"ERROR: Execution failed. Please check the log file for details. \n {log.name if log else ''}")
        if log:
            log.write(f"ERROR -->\n{ex}")
        traceback.print_exc(file=log)
        if log:
            log.write(f'Errored at: {str(datetime.now().strftime("%m/%d/%Y %H:%M:%S"))} \n')

    end_time = datetime.now()
    dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
    tot_run_time = end_time - start_time
    if log:
        log.write(f'END TIME: {str(end_time)} \n')
        log.write(f'TOTAL RUN TIME: {str(tot_run_time)} \n')
        log.close()

    print("================================")
    print("End test_case_by_hydroid.py")

    print(f"ended: {dt_string}")

    print(f"Duration: {str(tot_run_time).split('.')[0]}")
    print()
