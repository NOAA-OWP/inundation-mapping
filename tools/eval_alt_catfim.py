
import os
import argparse
from multiprocessing import Pool
import csv
import json

from tools_shared_variables import TEST_CASES_DIR
from tools_shared_functions import compute_contingency_stats_from_rasters


def create_master_metrics_csv_alt(master_metrics_csv_output, json_list, version):

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



    for full_json_path in json_list:
         
         # Parse variables from json path.
        split_json_handle = os.path.split(full_json_path)[1].split('_')
        
        benchmark_source = split_json_handle[2]
        huc = split_json_handle[1]
        nws_lid = split_json_handle[0]
        magnitude = split_json_handle[3].replace('.json', '')
                  
        real_json_path = os.path.join(os.path.split(full_json_path)[0], nws_lid + '_b0m_stats.json')
        
        sub_list_to_append = [version, nws_lid, magnitude, huc]
        
        stats_dict = json.load(open(real_json_path))
        for metric in metrics_to_write:
            sub_list_to_append.append(stats_dict[metric])
        sub_list_to_append.append(real_json_path)
        sub_list_to_append.append('NA')
        sub_list_to_append.append(benchmark_source)
        sub_list_to_append.append('MS')
        sub_list_to_append.append('yes')
        
        list_to_write.append(sub_list_to_append)


    with open(master_metrics_csv_output, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)


def process_alt_comparison(args):
    
    predicted_raster_path = args[0]
    benchmark_raster_path = args[1]
    agreement_raster = args[2]
    stats_csv = args[3]
    stats_json = args[4]
    mask_values = args[5]
    stats_modes_list = args[6]
    test_id = args[7]
    mask_dict = args[8]

    compute_contingency_stats_from_rasters(predicted_raster_path,
                                           benchmark_raster_path,
                                           agreement_raster,
                                           stats_csv=stats_csv,
                                           stats_json=stats_json,
                                           mask_values=[],
                                           stats_modes_list=stats_modes_list,
                                           test_id=test_id,
                                           mask_dict=mask_dict,
                                           )

    print("Finished processing " + agreement_raster)


if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Produces metrics for alternative CatFIM.')
    parser.add_argument('-d','--catfim-directory',help='Path to directory storing CatFIM outputs. This is the most parent dir, usually named by a version.',required=True)
    parser.add_argument('-w','--output-workspace',help='Add a special name to the end of the branch.',required=True, default="")
    parser.add_argument('-m','--master-metrics-csv',help='Define path for master metrics CSV file.',required=False,default=None)
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    catfim_dir = args['catfim_directory']
    output_workspace = args['output_workspace']
    job_number = int(args['job_number'])
    master_metrics_csv = args['master_metrics_csv']
    
    if master_metrics_csv == None:
        master_metrics_csv = os.path.join(output_workspace, 'master_metrics.csv')
    
    if not os.path.exists(catfim_dir):
        print("CatFIM directory: " + catfim_dir + " does not exist.")
        quit
    
    if not os.path.exists(output_workspace):
        os.mkdir(output_workspace)
    
    catfim_dir_list = os.listdir(catfim_dir)
    
    procs_list = []
    json_list = []
    
    for huc in catfim_dir_list:
        if len(huc) == 8:
            
            huc_workspace = os.path.join(output_workspace, huc)
            if not os.path.exists(huc_workspace):
                os.mkdir(huc_workspace)

            huc_dir_path = os.path.join(catfim_dir, huc)
            
            # List AHPS sites.
            site_list = os.listdir(huc_dir_path)
            
            # Loop through AHPS sites.
            for site in site_list:
                site_dir = os.path.join(huc_dir_path, site)
                
                site_workspace = os.path.join(huc_workspace, site)
                if not os.path.exists(site_workspace):
                    os.mkdir(site_workspace)
                
                for category in ['action', 'minor', 'moderate', 'major']:
                    # Presumptiously define inundation grid path.
                    category_grid_path = os.path.join(site_dir, site + '_' + category + '_extent_' + huc + '.tif')
                
                    if os.path.exists(category_grid_path):

                        site_category_workspace = os.path.join(site_workspace, category)
                        if not os.path.exists(site_category_workspace):
                            os.mkdir(site_category_workspace)
                            
                        # Map path to benchmark data, both NWS and USGS.
                        for benchmark_type in ['nws', 'usgs']:
                            benchmark_grid = os.path.join(TEST_CASES_DIR, benchmark_type + '_test_cases', 'validation_data_' + benchmark_type, huc, site, category, 'ahps_' + site + '_huc_' + huc + '_extent_' + category + '.tif')
 
                            if os.path.exists(benchmark_grid):
                                
                                # Create dir in output workspace for results.                                
                                file_handle = site + '_' + huc + '_' + benchmark_type + '_' + category 
                                
                                predicted_raster_path = category_grid_path
                                benchmark_raster_path = benchmark_grid
                                agreement_raster = os.path.join(site_category_workspace, file_handle + '.tif')
                                stats_csv = os.path.join(site_category_workspace, file_handle + '.csv')
                                stats_json = os.path.join(site_category_workspace, file_handle + '.json')
                                mask_values=None
                                stats_modes_list=['total_area']
                                test_id=''
                                mask_dict={'levees': {'path': '/data/test_cases/other/zones/leveed_areas_conus.shp', 'buffer': None, 'operation': 'exclude'}, 
                                            'waterbodies': {'path': '/data/test_cases/other/zones/nwm_v2_reservoirs.shp', 'buffer': None, 'operation': 'exclude'},
                                            site: {'path': '/data/test_cases/{benchmark_type}_test_cases/validation_data_{benchmark_type}/{huc}/{site}/{site}_domain.shp'.format(benchmark_type=benchmark_type, site=site, category=category, huc=huc), 'buffer': None, 'operation': 'include'}}
                                
                                json_list.append(stats_json)
                                
                                # Either add to list to multiprocess or process serially, depending on user specification.
                                if job_number > 1:
                                    procs_list.append([predicted_raster_path, benchmark_raster_path, agreement_raster,stats_csv,stats_json,mask_values,stats_modes_list,test_id, mask_dict])
                                else:
                                    process_alt_comparison([predicted_raster_path, benchmark_raster_path, agreement_raster,stats_csv,stats_json, mask_values,stats_modes_list,test_id, mask_dict])

    # Multiprocess.
    if job_number > 1:
        with Pool(processes=job_number) as pool:
            pool.map(process_alt_comparison, procs_list)
            
    # Merge stats into single file.
    version = os.path.split(output_workspace)[1]
    create_master_metrics_csv_alt(master_metrics_csv, json_list, version)

            
    
    