import argparse
import csv
import json
import os
import re
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import partial
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
from run_test_case_mannN_optz_func import (
    alpha_test,
    correct_nonmonotonic_src,
    filter_longitudinal_discharge_jitters,
    list_all_test_cases,
)
from scipy.optimize import NonlinearConstraint, differential_evolution
from tools_shared_variables import MAGNITUDE_DICT, TEST_CASES_DIR


AHPS_BENCHMARK_CATEGORIES = ["usgs", "nws"]


# *********************************************************
def initialize_mannN(fim_dir, huc, mannN_file):

    # Load AI-based manning number data
    mannN_data = pd.read_csv(mannN_file)
    # aib_mannN_data.columns
    mannN_data_df = mannN_data[["feature_id", "channel_n", "overbank_n"]]

    # Clip manningN for HUC8
    fim_huc_dir = join(fim_dir, huc)
    path_nwm_streams = join(fim_huc_dir, "nwm_subset_streams.gpkg")
    nwm_stream = gpd.read_file(path_nwm_streams)

    wbd8_path = join(fim_huc_dir, 'wbd.gpkg')
    wbd8 = gpd.read_file(wbd8_path, engine="pyogrio", use_arrow=True)
    nwm_stream_clp = nwm_stream.clip(wbd8)

    mannN_data_df_huc = mannN_data_df.merge(nwm_stream_clp, left_on='feature_id', right_on='ID')
    mannN_df = mannN_data_df_huc.drop_duplicates(subset=['feature_id'], keep='first')
    mannN_df.index = range(len(mannN_df))
    mannN_df = mannN_df.drop(columns=['ID', 'order_'])
    mannN_df = mannN_df.rename(columns={'channel_n': 'ManningN'})
    mannN_df = mannN_df.rename(columns={'overbank_n': 'MannN_obank'})

    initial_mannN_df = mannN_df[['feature_id', 'ManningN', 'MannN_obank']]

    return initial_mannN_df


# *********************************************************
def read_initial_hydroTables(fim_dir, huc):

    huc_dir = join(fim_dir, huc)
    # Get hydro_table and src_full from each branch
    ht_all_branches_path = []
    src_all_branches_path = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        # if int(branch) > 0:
        ht_full = join(huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        if os.path.isfile(ht_full):
            ht_all_branches_path.append(ht_full)
        src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')

        if os.path.isfile(src_full):
            src_all_branches_path.append(src_full)

    initial_hydroTables_ls = []
    for pth in range(len(src_all_branches_path)):  # ht_path in ht_all_branches_path:
        ht_name = os.path.basename(ht_all_branches_path[pth])
        branch = ht_name.split(".")[0].split("_")[-1]
        ht_df_ini = pd.read_csv(ht_all_branches_path[pth], dtype={'feature_id': 'int64'}, low_memory=False)
        ht_df_ini["branch_id"] = branch

        src_df = pd.read_csv(src_all_branches_path[pth], dtype={'feature_id': 'int64'}, low_memory=False)
        src_geo_df = src_df[
            [
                'WetArea_chan (m2)',
                'HydraulicRadius_chan (m)',
                'WetArea_obank (m2)',
                'HydraulicRadius_obank (m)',
                'bankfull_proxy',
            ]
        ]

        ht_df = pd.concat([ht_df_ini, src_geo_df], axis=1)
        ht_df['branch'] = branch
        initial_hydroTables_ls.append(ht_df)

    return initial_hydroTables_ls


# *********************************************************
def recalculate_Q_with_mannN_and_update_hydroTables_ls(
    hydroTables_ls, mannN_ch_values, mannN_ob_values, feature_ids, fim_dir
):

    updated_hydroTables_ls = []
    for ht_df in hydroTables_ls:
        if 'manningN_ch_optz' in ht_df.columns:
            ht_df = ht_df.drop(
                [
                    'manningN_ch_optz',
                    'manningN_ob_optz',
                    'Discharge(cms)_optzN',
                    'optzN_on',
                    'overbank_n',
                    'channel_n',
                    'discharge_cms',
                ],
                axis=1,
            )

        # Create a temporary dataframe with updated ManningN values
        temp_mannN_df = pd.DataFrame(
            {
                'feature_id': feature_ids,
                'manningN_ch_optz': mannN_ch_values,
                'manningN_ob_optz': mannN_ob_values,
            }
        )

        # Merge the updated ManningN values
        ht_df = ht_df.merge(temp_mannN_df, how='left', on='feature_id')

        # Calculations of channel and overbank discharges
        Q_ch = (
            ht_df['WetArea_chan (m2)']
            * pow(ht_df['HydraulicRadius_chan (m)'], 2.0 / 3)
            * pow(ht_df['SLOPE'], 0.5)
            / ht_df['manningN_ch_optz']
        )
        Q_ob = (
            ht_df['WetArea_obank (m2)']
            * pow(ht_df['HydraulicRadius_obank (m)'], 2.0 / 3)
            * pow(ht_df['SLOPE'], 0.5)
            / ht_df['manningN_ob_optz']
        )
        ht_df['Discharge(cms)_optzN'] = Q_ob + Q_ch

        ht_df['optzN_on'] = True
        ht_df['discharge_cms'] = ht_df['Discharge(cms)_optzN']
        ht_df['channel_n'] = ht_df['manningN_ch_optz']
        ht_df['overbank_n'] = ht_df['manningN_ob_optz']

        ht_df1 = correct_nonmonotonic_src(ht_df)
        ht_df2 = filter_longitudinal_discharge_jitters(fim_dir, ht_df1)

        updated_hydroTables_ls.append(ht_df2)

    return updated_hydroTables_ls


# *********************************************************
def reformat_hydroTable_for_alpha_test(hydroTables_ls):

    hydroTables_df = pd.concat(hydroTables_ls, ignore_index=True)
    htable_req_cols = ["HUC", "branch_id", "feature_id", "HydroID", "stage", "discharge_cms", "LakeID"]
    data_types = {
        "HUC": str,
        "branch_id": int,
        "feature_id": str,
        "HydroID": str,
        "stage": float,
        "discharge_cms": float,
        "LakeID": int,
    }
    hydroTable_all = hydroTables_df[htable_req_cols]
    hydroTable_all = hydroTable_all.astype(data_types)
    hydroTable_all.set_index(["HUC", "feature_id", "HydroID"], inplace=True)

    return hydroTable_all


# *********************************************************
def create_master_metrics_df(fim_version, huc):
    """
    This function searches for and collates metrics into a single dataframe that can queried database-style.
        This function automatically looks for metrics produced for official versions and loads them into
            memory to be written to the output CSV.
    Args:
        master_metrics_csv_output (str)    : Full path to CSV output.
                                                If a file already exists at this path, it will be overwritten.
        dev_versions_to_include_list (list): A list of non-official FIM version names.
                                                If a user supplied information on the command line using the
                                                -dc flag, then this function will search for metrics in the
                                                "testing_versions" library of metrics and include them in
                                                the CSV output.
    """
    # Default to processing all possible versions in PREVIOUS_FIM_DIR.
    config = "DEV"

    # prev_versions_to_include_list = []
    dev_versions_to_include_list = []

    # prev_versions_to_include_list = os.listdir(PREVIOUS_FIM_DIR)
    if config == 'DEV':  # development fim model results
        dev_versions_to_include_list = [fim_version]

    # Construct header
    metrics_to_write = [
        'true_negatives_count',
        'false_negatives_count',
        'true_positives_count',
        'false_positives_count',
        'contingency_tot_count',
        'cell_area_m2',
        'TP_area_km2',
        'FP_area_km2',
        'TN_area_km2',
        'FN_area_km2',
        'contingency_tot_area_km2',
        'predPositive_area_km2',
        'predNegative_area_km2',
        'obsPositive_area_km2',
        'obsNegative_area_km2',
        'positiveDiff_area_km2',
        'CSI',
        'FAR',
        'TPR',
        'TNR',
        'PND',
        'PPV',
        'NPV',
        'ACC',
        'Bal_ACC',
        'MCC',
        'EQUITABLE_THREAT_SCORE',
        'PREVALENCE',
        'BIAS',
        'F1_SCORE',
        'TP_perc',
        'FP_perc',
        'TN_perc',
        'FN_perc',
        'predPositive_perc',
        'predNegative_perc',
        'obsPositive_perc',
        'obsNegative_perc',
        'positiveDiff_perc',
        'masked_count',
        'masked_perc',
        'masked_area_km2',
    ]

    # Create table header
    additional_header_info_prefix = ['version', 'nws_lid', 'magnitude', 'huc']
    list_to_write = [
        additional_header_info_prefix
        + metrics_to_write
        + ['full_json_path']
        + ['flow']
        + ['benchmark_source']
        + ['extent_config']
        + ["calibrated"]
    ]

    # Iterate through 5 benchmark sources
    for benchmark_source in ['ble', 'nws', 'usgs']:  # [] #['ble' 'nws', 'usgs', 'ifc', 'ras2fim']
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        # test_cases_list = [d for d in os.listdir(benchmark_test_case_dir) if re.match(r'\d{8}_\w{3,7}', d)]
        test_cases_list = [f"{huc}_{benchmark_source}"]

        if benchmark_source in ['ble', 'ifc', 'ras2fim']:
            magnitude_list = MAGNITUDE_DICT[benchmark_source]

            # Iterate through available test cases
            for each_test_case in test_cases_list:
                try:
                    # "testing" refers to the development model version(s) being evaluated
                    versions_to_crawl = os.path.join(
                        benchmark_test_case_dir, each_test_case, 'testing_versions'
                    )
                    versions_to_aggregate = dev_versions_to_include_list

                    # Pull version info from filepath
                    for magnitude in magnitude_list:
                        for version in versions_to_aggregate:
                            extent_config = 'COMP'
                            if "_c" in version and version.split('_c')[1] == "":
                                calibrated = "yes"
                            else:
                                calibrated = "no"
                            version_dir = os.path.join(versions_to_crawl, version)
                            magnitude_dir = os.path.join(version_dir, magnitude)

                            # Add metrics from file to metrics table ('list_to_write')
                            if os.path.exists(magnitude_dir):
                                magnitude_dir_list = os.listdir(magnitude_dir)
                                for f in magnitude_dir_list:
                                    if '.json' in f:
                                        flow = 'NA'
                                        nws_lid = "NA"
                                        sub_list_to_append = [version, nws_lid, magnitude, huc]
                                        full_json_path = os.path.join(magnitude_dir, f)
                                        if os.path.exists(full_json_path):
                                            stats_dict = json.load(open(full_json_path))
                                            # print("checking if it does solve the json just for 1 huc or not")
                                            # print(stats_dict)

                                            for metric in metrics_to_write:
                                                sub_list_to_append.append(stats_dict[metric])

                                            sub_list_to_append.append(full_json_path)
                                            sub_list_to_append.append(flow)
                                            sub_list_to_append.append(benchmark_source)
                                            sub_list_to_append.append(extent_config)
                                            sub_list_to_append.append(calibrated)

                                            list_to_write.append(sub_list_to_append)
                except ValueError:
                    pass

        # Iterate through AHPS benchmark data
        if benchmark_source in AHPS_BENCHMARK_CATEGORIES:
            # test_cases_list = os.listdir(benchmark_test_case_dir)
            test_cases_list_dir = os.path.join(benchmark_test_case_dir, f"{huc}_{benchmark_source}")

            if os.path.exists(test_cases_list_dir):
                test_cases_list = [f"{huc}_{benchmark_source}"]

                for each_test_case in test_cases_list:
                    try:
                        # Get HUC id
                        int(each_test_case.split('_')[0])

                        # Update filepaths based on whether the official or dev versions should be included
                        # iteration == "testing"
                        # "testing" refers to the development model version(s) being evaluated
                        versions_to_crawl = os.path.join(
                            benchmark_test_case_dir, each_test_case, 'testing_versions'
                        )
                        versions_to_aggregate = dev_versions_to_include_list

                        # Pull model info from filepath
                        for magnitude in ['action', 'minor', 'moderate', 'major']:
                            for version in versions_to_aggregate:
                                if '_ms' in version:
                                    extent_config = 'MS'
                                elif ('_fr' in version) or (version == 'fim_2_3_3'):
                                    extent_config = 'FR'
                                else:
                                    extent_config = 'COMP'
                                if "_c" in version and version.split('_c')[1] == "":
                                    calibrated = "yes"
                                else:
                                    calibrated = "no"

                                version_dir = os.path.join(versions_to_crawl, version)
                                magnitude_dir = os.path.join(version_dir, magnitude)

                                if os.path.exists(magnitude_dir):
                                    magnitude_dir_list = os.listdir(magnitude_dir)
                                    for f in magnitude_dir_list:
                                        if '.json' in f and 'total_area' not in f:
                                            nws_lid = f[:5]
                                            sub_list_to_append = [version, nws_lid, magnitude, huc]
                                            full_json_path = os.path.join(magnitude_dir, f)
                                            flow = ''
                                            if os.path.exists(full_json_path):
                                                # Get flow used to map
                                                flow_file = os.path.join(
                                                    benchmark_test_case_dir,
                                                    'validation_data_' + benchmark_source,
                                                    huc,
                                                    nws_lid,
                                                    magnitude,
                                                    'ahps_'
                                                    + nws_lid
                                                    + '_huc_'
                                                    + huc
                                                    + '_flows_'
                                                    + magnitude
                                                    + '.csv',
                                                )
                                                if os.path.exists(flow_file):
                                                    with open(flow_file, newline='') as csv_file:
                                                        reader = csv.reader(csv_file)
                                                        next(reader)
                                                        for row in reader:
                                                            flow = row[1]

                                                # Add metrics from file to metrics table ('list_to_write')
                                                stats_dict = json.load(open(full_json_path))
                                                for metric in metrics_to_write:
                                                    sub_list_to_append.append(stats_dict[metric])
                                                sub_list_to_append.append(full_json_path)
                                                sub_list_to_append.append(flow)
                                                sub_list_to_append.append(benchmark_source)
                                                sub_list_to_append.append(extent_config)
                                                sub_list_to_append.append(calibrated)
                                                list_to_write.append(sub_list_to_append)
                    except ValueError:
                        pass

    df_to_write = pd.DataFrame(list_to_write)
    df_to_write.columns = df_to_write.iloc[0]
    df_to_write = df_to_write[1:]

    return df_to_write


# *********************************************************
def synthesize_test_cases(huc, fim_version, hydroTable_all, job_number_branch, benchmark_cat, output_dir):
    """
    This function
    """
    print("=" * 40)
    print("Start synthesize test cases")
    start_time = datetime.now()

    # Default to processing all possible versions in PREVIOUS_FIM_DIR.
    fim_version = fim_version  # "all"

    # Create a list of all test_cases for which we have validation data
    archive_results = False
    benchmark_category = benchmark_cat  # ['ble', 'nws', 'usgs']# all
    all_test_cases = list_all_test_cases(
        huc=huc,
        version=fim_version,
        archive=archive_results,
        benchmark_categories=benchmark_category,  # [benchmark_category]
        output_dir=output_dir,
    )
    # print(hydroTable_all)
    model = "GMS"
    # job_number_huc = 1
    overwrite = True
    verbose = False
    calibrated = False
    for test_case_class in all_test_cases:
        if test_case_class is not None:
            # print(test_case_class)
            # if not os.path.exists(test_case_class['fim_dir']):
            #     continue
            alpha_test(
                test_case_dic=test_case_class,
                hydroTable_all=hydroTable_all,
                calibrated=calibrated,
                model=model,
                mask_type='huc',
                overwrite=overwrite,
                verbose=verbose,
                gms_workers=job_number_branch,
            )
    # job_number_branch = 6
    # Set up multiprocessor
    # with ProcessPoolExecutor(max_workers=1) as executor:  # job_number_huc
    #     # Loop through all test cases,
    #     # build the alpha test arguments,
    #     # and submit them to the process pool
    #     executor_dict = {}
    #     for test_case_class in all_test_cases:
    #         if test_case_class is not None:
    #             if not os.path.exists(test_case_class['fim_dir']):
    #                 continue
    #             alpha_test_args = {
    #                 'test_case_dic': test_case_class,
    #                 'hydroTable_all': hydroTable_all,
    #                 'calibrated': calibrated,
    #                 'model': model,
    #                 'mask_type': 'huc',
    #                 'overwrite': overwrite,
    #                 'verbose': verbose,
    #                 'gms_workers': job_number_branch,
    #             }
    #             try:
    #                 future = executor.submit(alpha_test, **alpha_test_args)

    #                 executor_dict[future] = test_case_class['test_id']
    #             except Exception as ex:
    #                 print(f"*** {ex}")
    #                 traceback.print_exc()
    #                 sys.exit(1)

    # # Send the executor to the progress bar and wait for all MS tasks to finish
    # progress_bar_handler(
    #     executor_dict, True, f"Running {model} alpha test cases with {job_number_huc} workers"
    # )
    metrics_df = create_master_metrics_df(fim_version, huc)

    print("=" * 40)
    print("End synthesize test cases")

    end_time = datetime.now()
    # dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    # print(f"ended: {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()

    return metrics_df


# *********************************************************
class EarlyStopper:
    def __init__(self, tol=0.01):
        self.tol = tol
        self.prev_x = None
        self.stop = False

    def __call__(self, xk, convergence=None):
        if self.prev_x is not None:
            diff = np.abs(np.array(xk) - np.array(self.prev_x))
            if np.all(diff < self.tol):
                self.stop = True
                return True  # Tells differential_evolution to stop
        self.prev_x = np.array(xk)
        return False


# *********************************************************
# Global variable to store the best solution that meets our criteria
# global global_loss
def objective_function(
    mannN_coef,
    feature_ids,
    initial_hydroTables_ls,
    fim_version,
    huc,
    fim_dir,
    # target_loss,
    iteration,
    job_number_branch,
    bench_cat,
):
    mannN_ch_coef = mannN_coef[0]
    mannN_ob_coef = mannN_coef[1]

    print(f'Iteration {iteration}: Current mannN coefficients for HUC {huc}: {mannN_coef}')

    mannN_ch_values_ini = np.zeros(len(feature_ids), dtype=float)
    mannN_ch_values_ini.fill(0.060)
    mannN_ch_values = mannN_ch_values_ini * mannN_ch_coef

    mannN_ob_values_ini = np.zeros(len(feature_ids), dtype=float)
    mannN_ob_values_ini.fill(0.120)
    mannN_ob_values = mannN_ob_values_ini * mannN_ob_coef

    updated_hydroTables_ls = recalculate_Q_with_mannN_and_update_hydroTables_ls(
        initial_hydroTables_ls, mannN_ch_values, mannN_ob_values, feature_ids, fim_dir
    )

    hydroTable_all = reformat_hydroTable_for_alpha_test(updated_hydroTables_ls)
    output_dir = os.path.dirname(os.path.dirname(fim_dir))

    if bench_cat == 'ahps':

        benchmark_cat = ['nws', 'usgs']  # ['ble', 'nws', 'usgs']
        metrics_df = synthesize_test_cases(
            huc, fim_version, hydroTable_all, job_number_branch, benchmark_cat, output_dir
        )

        # AHPS sites
        fn_count_all = np.sum(metrics_df['false_negatives_count'])
        fp_count_all = np.sum(metrics_df['false_positives_count'])
        loss_mannN = fn_count_all + fp_count_all
    else:
        # BLE sites/ ras sites
        benchmark_cat = ['ble']  # ['ble', 'nws', 'usgs']
        metrics_df = synthesize_test_cases(
            huc, fim_version, hydroTable_all, job_number_branch, benchmark_cat, output_dir
        )
        fn_count_100 = metrics_df['false_negatives_count'][metrics_df['magnitude'] == '100yr']
        fp_count_100 = metrics_df['false_positives_count'][metrics_df['magnitude'] == '100yr']
        loss_mannN = fn_count_100[1] + fp_count_100[1]

    # benchmark_cat = ['nws', 'usgs']  # ['ble', 'nws', 'usgs']
    # metrics_df = synthesize_test_cases(huc, fim_version, hydroTable_all, job_number_branch, benchmark_cat, output_dir)
    # # AHPS sites
    # fn_count_all = np.sum(metrics_df['false_negatives_count'])
    # fp_count_all = np.sum(metrics_df['false_positives_count'])
    # loss_mannN = fn_count_all + fp_count_all

    print(f'Current loss: {loss_mannN}')
    print("")

    metrics_df['iteration'] = iteration[0]  # *len(metrics_df)
    metrics_df['total_loss'] = loss_mannN
    metrics_df['mannN_ch_coef'] = mannN_coef[0]
    metrics_df['mannN_ob_coef'] = mannN_coef[1]
    metrics_df['mannN_ch'] = mannN_coef[0] * 0.06
    metrics_df['mannN_ob'] = mannN_coef[1] * 0.12

    metrics_dir = os.path.join(fim_dir, 'optz_metrics')
    if not os.path.exists(metrics_dir):
        os.mkdir(metrics_dir)

    metrics_csv_path = os.path.join(metrics_dir, f'optz_iteration_metrics_{huc}.csv')
    if not os.path.exists(metrics_csv_path):
        metrics_df.to_csv(metrics_csv_path, index=False)
    else:
        existing_metrix_df = pd.read_csv(metrics_csv_path)
        combined_df = pd.concat([existing_metrix_df, metrics_df])
        combined_df.to_csv(metrics_csv_path, index=False)

    iteration[0] += 1

    return loss_mannN


# *********************************************************
def constraint1(params):
    mannN_ch_coef, mannN_ob_coef = params
    return (0.12 * mannN_ob_coef) - (0.06 * mannN_ch_coef)


# *********************************************************
def partial_optimization_huc(
    fim_dir,
    huc,
    mannN_file,
    # target_loss,
    job_number_branch,
    bench_cat,
):

    metrics_dir = os.path.join(fim_dir, 'optz_metrics')
    metrics_csv_path = os.path.join(metrics_dir, f'optz_metrics_{huc}.csv')

    if not os.path.exists(metrics_csv_path):
        print("*" * 50)
        print(f"Start Optimizing MannN for HUC {huc}")
        start_time = datetime.now()

        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print(f"started: {dt_string}")
        print()

        initial_mannN_df = initialize_mannN(fim_dir, huc, mannN_file)

        feature_ids = initial_mannN_df['feature_id'].values
        fim_version = os.path.basename(os.path.normpath(fim_dir))
        initial_hydroTables_ls = read_initial_hydroTables(fim_dir, huc)

        bounds = [(0.1, 3.0), (0.15, 2.0)]

        iteration = [0]  # Use a list to allow modification inside the callback function

        # Create a partial function for the objective function with fixed arguments
        obj_func_partial = partial(
            objective_function,
            feature_ids=feature_ids,
            initial_hydroTables_ls=initial_hydroTables_ls,
            fim_version=fim_version,
            huc=huc,
            fim_dir=fim_dir,
            # target_loss=target_loss_value,
            iteration=iteration,
            job_number_branch=job_number_branch,
            bench_cat=bench_cat,
        )

        cons = NonlinearConstraint(constraint1, 0, np.inf)

        initial_population = [
            [1.0, 1.0],
            [0.4, 0.7],  # Your original guess
            [0.9, 0.9],
            [1.0, 0.5],
            [1.0, 1.5],
            [1.5, 1.0],
            [0.5, 1.5],
            [0.5, 1.0],
            [0.5, 0.5],
            [1.0, 1.9],
            [1.9, 1.0],
            [0.2, 1.9],
            [0.2, 1.0],
            [0.2, 0.2],
            [0.16, 0.5],
            [0.16, 1.0],
            [0.16, 0.8],
            # [1.0, 0.9], [1.0, 1.1], [1.1, 0.9], [1.1, 1.0], [1.1, 1.1],
            # [0.9, 1.1], [0.9, 1.0], [1.5, 1.5], [1.9, 1.9],
        ]

        # Fill the rest of the population with random values
        popusize = 20
        for _ in range(popusize - len(initial_population)):
            initial_population.append([np.random.uniform(low, high) for low, high in bounds])

        try:
            early_stopper = EarlyStopper(tol=0.01)
            optimization_result = differential_evolution(
                obj_func_partial,
                bounds,
                popsize=popusize,
                init=initial_population,  # 'latinhypercube',
                callback=early_stopper,
                constraints=(cons,),
                maxiter=15,
                strategy='best1bin',
                tol=1e-3,
                mutation=(0.5, 1),
                recombination=0.7,
                disp=True,
                polish=True,
                atol=1e-4,
                updating='immediate',
            )

            # Extract the optimized coefficients
            optimal_mannN_coef = optimization_result.x
            total_loss = optimization_result.fun

            print(f'Optimal manningN_coefs are {[optimal_mannN_coef]} for HUC: {huc}')
            print(f'Optimal total_loss is {[total_loss]} for HUC: {huc}')

            optimized_metrics_df = pd.DataFrame(
                [
                    {
                        'HUC': huc,
                        'total_loss': total_loss,
                        'optimal_mannN_ch_coef': optimal_mannN_coef[0],
                        'optimal_mannN_ob_coef': optimal_mannN_coef[1],
                        'optimal_mannN_ch': optimal_mannN_coef[0] * 0.06,
                        'optimal_mannN_ob': optimal_mannN_coef[1] * 0.12,
                    }
                ]
            )

            optimized_metrics_df.to_csv(metrics_csv_path)

            end_time = datetime.now()
            dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            print(f"ended: {dt_string}")

            # Calculate duration
            time_duration = end_time - start_time
            print(f"Duration: {str(time_duration).split('.')[0]}")
            print()

        except Exception as e:
            print(e)

    else:
        print("*" * 50)
        print(f"MannN for HUC{huc} is already optimized")


# *********************************************************
def multi_process_optimization(
    fim_dir,
    mannN_file,
    # target_loss_file,
    job_number_branch,
    job_number_huc,
    bench_cat,
):
    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"MannN optimization started: {dt_string}")
    print()

    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]

    with ProcessPoolExecutor(max_workers=int(job_number_huc)) as executor:
        # Loop through all test cases,
        # build the alpha test arguments,
        # and submit them to the process pool
        executor_dict = {}
        for huc in fim_hucs:
            par_optimization_args = {
                'fim_dir': fim_dir,
                'huc': huc,
                'mannN_file': mannN_file,
                'job_number_branch': job_number_branch,
                'bench_cat': bench_cat,
            }
            try:
                future = executor.submit(partial_optimization_huc, **par_optimization_args)
                executor_dict[future] = huc
            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"MannN optimization ended: {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()


# *********************************************************
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Implement user provided Manning's n values for in-channel vs. overbank flow. "
        "Recalculate Manning's eq for discharge"
    )
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-mann',
        '--mannN_file',
        help="Path to a csv file containing initial Manning's n values by featureid",
        required=True,
        type=str,
    )
    parser.add_argument('-jb', '--job_number_branch', help="job_number_branch", required=True, type=str)
    parser.add_argument('-jh', '--job_number_huc', help="job_number_huc", required=True, type=str)
    parser.add_argument(
        '-bc',
        '--bench_cat',
        help="Benchmark catagory that could be AHPS sites or BLE/sites",
        required=True,
        type=str,
    )

    args = vars(parser.parse_args())
    fim_dir = args['fim_dir']
    mannN_file = args['mannN_file']
    job_number_branch = args['job_number_branch']
    job_number_huc = args['job_number_huc']
    bench_cat = args['bench_cat']

    multi_process_optimization(fim_dir, mannN_file, job_number_branch, job_number_huc, bench_cat)
