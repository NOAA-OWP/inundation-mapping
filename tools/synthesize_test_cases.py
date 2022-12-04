#!/usr/bin/env python3

import os
import argparse
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import json
import csv
import ast
from tqdm import tqdm
from glob import glob
from collections import OrderedDict
import shutil

from run_test_case import run_alpha_test

from tools_shared_variables import TEST_CASES_DIR, PREVIOUS_FIM_DIR, OUTPUTS_DIR, AHPS_BENCHMARK_CATEGORIES, MAGNITUDE_DICT


def create_master_metrics_csv(master_metrics_csv_output, dev_versions_to_include_list):
    """
    This function searches for and collates metrics into a single CSV file that can queried database-style. The
    CSV is an input to eval_plots.py. This function automatically looks for metrics produced for official versions
    and loads them into memory to be written to the output CSV.
    
    Args:
        master_metrics_csv_output (str): Full path to CSV output. If a file already exists at this path, it will be overwritten.
        dev_versions_to_include_list (list): A list of non-official FIM version names. If a user supplied information on the command
                                            line using the -dc flag, then this function will search for metrics in the "testing_versions"
                                            library of metrics and include them in the CSV output.
    
    """
    
    # Construct header
    metrics_to_write = ['true_negatives_count',
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
                        'masked_area_km2'
                        ]

    additional_header_info_prefix = ['version', 'nws_lid', 'magnitude', 'huc']
    list_to_write = [additional_header_info_prefix + metrics_to_write + ['full_json_path'] + ['flow'] + ['benchmark_source'] + ['extent_config'] + ["calibrated"]]

    versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)

    if len(dev_versions_to_include_list) > 0:
        iteration_list = ['official', 'comparison']
    else:
        iteration_list = ['official']

    for benchmark_source in ['ble', 'nws', 'usgs', 'ifc']:
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        if benchmark_source in ['ble', 'ifc']:
            
            if benchmark_source == 'ble':
                magnitude_list = MAGNITUDE_DICT['ble']
            if benchmark_source == 'ifc':
                magnitude_list = MAGNITUDE_DICT['ifc']
            try:
                test_cases_list = os.listdir(benchmark_test_case_dir)
            except FileNotFoundError:
                continue
            
            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])

                    huc = test_case.split('_')[0]

                    for iteration in iteration_list:

                        if iteration == "official":
                            versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'official_versions')
                            versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)
                        if iteration == "comparison":
                            versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'testing_versions')
                            versions_to_aggregate = dev_versions_to_include_list

                        for magnitude in magnitude_list:
                            print(versions_to_aggregate)
                            for version in versions_to_aggregate:
                                if '_fr' in version:
                                    extent_config = 'FR'
                                elif '_ms' in version:
                                    extent_config = 'MS'
                                else:
                                    extent_config = 'FR'
                                if "_c" in version and version.split('_c')[1] == "":
                                    calibrated = "yes"
                                else:
                                    calibrated = "no"
                                version_dir = os.path.join(versions_to_crawl, version)
                                magnitude_dir = os.path.join(version_dir, magnitude)

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
        
        if benchmark_source in AHPS_BENCHMARK_CATEGORIES:
            test_cases_list = os.listdir(benchmark_test_case_dir)

            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])

                    huc = test_case.split('_')[0]

                    for iteration in iteration_list:

                        if iteration == "official":
                            versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'official_versions')
                            versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)
                        if iteration == "comparison":
                            versions_to_crawl = os.path.join(benchmark_test_case_dir, test_case, 'testing_versions')

                        for magnitude in ['action', 'minor', 'moderate', 'major']:
                            for version in versions_to_aggregate:
                                if '_fr' in version:
                                    extent_config = 'FR'
                                elif '_ms' in version:
                                    extent_config = 'MS'
                                else:
                                    extent_config = 'FR'
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

                                                # Get flow used to map.
                                                flow_file = os.path.join(benchmark_test_case_dir, 'validation_data_' + benchmark_source, huc, nws_lid, magnitude, 'ahps_' + nws_lid + '_huc_' + huc + '_flows_' + magnitude + '.csv')
                                                if os.path.exists(flow_file):
                                                    with open(flow_file, newline='') as csv_file:
                                                        reader = csv.reader(csv_file)
                                                        next(reader)
                                                        for row in reader:
                                                            flow = row[1]

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
    
    with open(master_metrics_csv_output, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=False,default='DEV')
    parser.add_argument('-l','--calibrated',help='Denotes use of calibrated n values. This should be taken from meta-data from hydrofabric dir',required=False, default=False,action='store_true')
    parser.add_argument('-e','--model',help='Denotes model used. FR, MS, or GMS allowed. This should be taken from meta-data in hydrofabric dir.',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all",nargs='+')
    parser.add_argument('-jh','--job-number-huc',help='Number of processes to use for HUC scale operations. HUC and Batch job numbers should multiply to no more than one less than the CPU count of the machine.',required=False, default=1,type=int)
    parser.add_argument('-jb','--job-number-branch',help='Number of processes to use for Branch scale operations. HUC and Batch job numbers should multiply to no more than one less than the CPU count of the machine.',required=False, default=1,type=int)
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='A benchmark category to specify. Defaults to process all categories.',required=False, default="all")
    parser.add_argument('-o','--overwrite',help='Overwrite all metrics or only fill in missing metrics.',required=False, action="store_true")
    parser.add_argument('-dc', '--dev-version-to-compare', nargs='+', help='Specify the name(s) of a dev (testing) version to include in master metrics CSV. Pass a space-delimited list.',required=False)
    parser.add_argument('-m','--master-metrics-csv',help='Define path for master metrics CSV file.',required=False,default=None)
    parser.add_argument('-d','--fr-run-dir',help='Name of test case directory containing FIM for FR model',required=False,default=None)
    parser.add_argument('-vr','--verbose',help='Verbose',required=False,default=None,action='store_true')
    parser.add_argument('-vg','--gms-verbose',help='GMS Verbose Progress Bar',required=False,default=None,action='store_true')

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    config = args['config']
    fim_version = args['fim_version']
    job_number_huc = args['job_number_huc']
    job_number_branch = args['job_number_branch']
    special_string = args['special_string']
    benchmark_category = args['benchmark_category']
    overwrite = args['overwrite']
    dev_versions_to_compare = args['dev_version_to_compare']
    master_metrics_csv = args['master_metrics_csv']
    fr_run_dir = args['fr_run_dir']
    calibrated = args['calibrated']
    model = args['model']
    verbose = args['verbose']
    gms_verbose = args['gms_verbose']

    # check job numbers
    total_cpus_requested = job_number_huc * job_number_branch
    total_cpus_available = os.cpu_count() - 1
    if total_cpus_requested > total_cpus_available:
        raise ValueError('The HUC job number, {}, multiplied by the branch job number, {}, '\
                          'exceeds your machine\'s available CPU count minus one. '\
                          'Please lower the job_number_huc or job_number_branch'\
                          'values accordingly.'.format(job_number_huc,job_number_branch)
                        )

    # Default to processing all possible versions in PREVIOUS_FIM_DIR. Otherwise, process only the user-supplied version.
    #if (fim_version != "all") & (not isinstance(fim_version,list)):
    # changed default behaviour above
    if not isinstance(fim_version,list):
        previous_fim_list = [fim_version]
    else:
        previous_fim_list = fim_version.copy()
    """
    else:
        if config == 'PREV':
            previous_fim_list = os.listdir(PREVIOUS_FIM_DIR)
        elif config == 'DEV':
            previous_fim_list = os.listdir(OUTPUTS_DIR)
    """

    #print(previous_fim_list);exit()
    # Define whether or not to archive metrics in "official_versions" or "testing_versions" for each test_id.
    if config == 'PREV':
        archive_results = True
    elif config == 'DEV':
        archive_results = False
    else:
        print('Config (-c) option incorrectly set. Use "DEV" or "PREV"')

    # List all available benchmark categories and test_cases.
    test_cases_dir_list = os.listdir(TEST_CASES_DIR)
    benchmark_category_list = []
    if benchmark_category == "all":
        for d in test_cases_dir_list:
            if 'test_cases' in d:
                benchmark_category_list.append(d.replace('_test_cases', ''))
    else:
        benchmark_category_list = [benchmark_category]

    # Loop through benchmark categories.
    procs_list = [] ; procs_dict = OrderedDict()
    for bench_cat in benchmark_category_list:
        
        # Map path to appropriate test_cases folder and list test_ids into bench_cat_id_list.
        bench_cat_test_case_dir = os.path.join(TEST_CASES_DIR, bench_cat + '_test_cases')
        bench_cat_id_list = os.listdir(bench_cat_test_case_dir)
        
        # temp
        #bench_cat_id_list = ['07060003_ifc']

        #if job_number_huc == 1:
            # something wrong about this lengtu
            #pb = tqdm(total=len(bench_cat_id_list))
        # Loop through test_ids in bench_cat_id_list.
        for test_id in bench_cat_id_list:
            if 'validation' and 'other' not in test_id:
                current_huc = test_id.split('_')[0]
                current_benchmark_category = test_id.split('_')[1]
                if current_benchmark_category in bench_cat:
                    # Loop through versions.
                    for version in previous_fim_list:
                        
                        #print(version,type(version),previous_fim_list,type(previous_fim_list));exit()
                        version = os.path.basename(version)
                        
                        if config == 'DEV':
                            #glob(os.path.join(OUTPUTS_DIR,version))
                            fim_run_dir = os.path.join(OUTPUTS_DIR, version, current_huc)
                        elif config == 'PREV':
                            fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc)
                       
                        # check for huc12 scale data
                        if not os.path.exists(fim_run_dir):
                            fim_run_dir = glob(os.path.join(OUTPUTS_DIR, version, current_huc+'*'))
                        
                        if not fim_run_dir:
                            continue
                        
                        # For previous versions of HAND computed at HUC6 scale
                        """
                        if not os.path.exists(fim_run_dir):
                            if config == 'DEV':
                                if os.path.exists(os.path.join(OUTPUTS_DIR, version, current_huc[:6])):
                                    fim_run_dir = os.path.join(OUTPUTS_DIR, version, current_huc[:6])
                            elif config == 'PREV':
                                if os.path.exists(os.path.join(PREVIOUS_FIM_DIR, version, current_huc[:6])):
                                    fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc[:6])
                        """
                        
                        # For current versions of HAND computed at HUC12 scale
                        """
                        print(fim_run_dir)
                        breakpoint()
                        #if (not os.path.exists(fim_run_dir)) & (len(current_huc) == 12):
                        if config == 'DEV':
                            fim_run_dir = glob(os.path.join(OUTPUTS_DIR, version, current_huc+'*'))
                        elif config == 'PREV':
                            fim_run_dir = glob(os.path.join(PREVIOUS_FIM_DIR, version, current_huc+'*'))
                        else:
                            continue
                        """
                        
                        try:
                            if os.path.exists(fim_run_dir):
                                # If a user supplies a special_string (-s), then add it to the end of the created dirs.
                                if special_string != "":
                                    version = version + '_' + special_string
                        except TypeError:
                            ver = version
                            version = [ver + '_' + special_string for _ in fim_run_dir]

                        # Define the magnitude lists to use, depending on test_id.
                        if 'ble' == current_benchmark_category:
                            magnitude = MAGNITUDE_DICT['ble']
                        elif ('usgs' == current_benchmark_category) | ('nws' == current_benchmark_category):
                            magnitude = ['action', 'minor', 'moderate', 'major']
                        elif 'ifc' == current_benchmark_category:
                            magnitude_list = MAGNITUDE_DICT['ifc']
                        else:
                            continue

                        # handle HUC12's
                        if isinstance(fim_run_dir,list):
                           
                           if len(fim_run_dir) == 0:
                               continue

                           # get unique huc12s in huc8
                           all_huc12s_in_current_huc = [ os.path.basename(frd) for frd in fim_run_dir ]
                           
                           last_huc12 = all_huc12s_in_current_huc[-1]
                           first_huc12 = all_huc12s_in_current_huc[0]

                           for frd,ver in zip(fim_run_dir,version):
                               
                               alpha_test_args = { 
                                                    'fim_run_dir': frd, 
                                                    'version': ver, 
                                                    'test_id': test_id, 
                                                    'magnitude': magnitude, 
                                                    'calibrated': calibrated,
                                                    'model': model,
                                                    'compare_to_previous': not archive_results, 
                                                    'archive_results': archive_results, 
                                                    'mask_type': 'huc',
                                                    'overwrite': overwrite,
                                                    'all_huc12s_in_current_huc' : all_huc12s_in_current_huc,
                                                    'last_huc12': last_huc12,
                                                    'fr_run_dir': fr_run_dir, 
                                                    'gms_workers': job_number_branch,
                                                    'verbose': False,
                                                    'gms_verbose': False
                                                  }
                               
                               procs_dict[os.path.basename(frd)] = alpha_test_args

                        # for HUC8s
                        else:

                            alpha_test_args = { 
                                                'fim_run_dir': fim_run_dir, 
                                                'version': version, 
                                                'test_id': test_id, 
                                                'magnitude': magnitude, 
                                                'calibrated': calibrated,
                                                'model': model,
                                                'compare_to_previous': not archive_results, 
                                                'archive_results': archive_results, 
                                                'mask_type': 'huc',
                                                'overwrite': overwrite, 
                                                'fr_run_dir': fr_run_dir, 
                                                'gms_workers': job_number_branch,
                                                'verbose': False,
                                                'gms_verbose': False
                                              }
                            procs_dict[current_huc] = alpha_test_args

    #print(procs_dict.keys());exit()
    # delete version dirs with HUC12
    if overwrite:
        for ch,ata in procs_dict.items():
            try:
                if ata['all_huc12s_in_current_huc'] is not None:
                    path_to_remove = os.path.join( TEST_CASES_DIR,
                                                   ata['test_id'].split('_')[1]+'_test_cases',
                                                   ata['test_id'],
                                                   'testing_versions',ata['version']
                                                  )
                    shutil.rmtree( path_to_remove,
                                  ignore_errors=True
                                 )
            except KeyError:
                pass
    
    if job_number_huc == 1:
        
        number_of_hucs = len(procs_dict)
        verbose_by_huc = not number_of_hucs == 1
        
        for current_huc, alpha_test_args in tqdm(procs_dict.items(),total=number_of_hucs,disable=(not verbose_by_huc)):
            alpha_test_args.update({'gms_verbose': not verbose_by_huc})

            try:
                run_alpha_test(**alpha_test_args)
            except Exception as exc:
                print('{}, {}, {}'.format(test_id,exc.__class__.__name__,exc))

    # Multiprocess alpha test runs.
    if job_number_huc > 1:
        
        #print(procs_dict);exit()
        executor = ProcessPoolExecutor(max_workers=job_number_huc)
        
        executor_generator = { 
                              executor.submit(run_alpha_test,**inp) : ids for ids,inp in procs_dict.items()
                             }

        for future in tqdm(as_completed(executor_generator),
                           total=len(executor_generator),
                           disable=(not verbose),
                           desc="Running test cases with {} HUC workers "\
                                "and {} Branch workers".format(job_number_huc,
                                                               job_number_branch),
                          ):
        

            hucCode = executor_generator[future]

            try:
                future.result()
            except Exception as exc:
                print('{}, {}, {}'.format(hucCode,exc.__class__.__name__,exc))
    
        # power down pool
        executor.shutdown(wait=True)

    if config == 'DEV':
        if dev_versions_to_compare != None:
            dev_versions_to_include_list = dev_versions_to_compare + [version]
        else:
            dev_versions_to_include_list = [version]
    if config == 'PREV':
        dev_versions_to_include_list = []

    if master_metrics_csv is not None:
        # Do aggregate_metrics.
        print("Creating master metrics CSV...")

        # this function is not compatible with GMS
        create_master_metrics_csv(master_metrics_csv_output=master_metrics_csv, versions_to_include_list=dev_versions_to_include_list)
