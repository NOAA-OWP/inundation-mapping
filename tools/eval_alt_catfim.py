
import os
import argparse
from multiprocessing import Pool


from tools_shared_variables import TEST_CASES_DIR
from tools_shared_functions import compute_contingency_stats_from_rasters


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
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    catfim_dir = args['catfim_directory']
    output_workspace = args['output_workspace']
    job_number = int(args['job_number'])
    
    if not os.path.exists(catfim_dir):
        print("CatFIM directory: " + catfim_dir + " does not exist.")
        quit
    
    if not os.path.exists(output_workspace):
        os.mkdir(output_workspace)
    
    catfim_dir_list = os.listdir(catfim_dir)
    
    procs_list = []
    
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
                                
                                # Either add to list to multiprocess or process serially, depending on user specification.
                                if job_number > 1:
                                    procs_list.append([predicted_raster_path, benchmark_raster_path, agreement_raster,stats_csv,stats_json,mask_values,stats_modes_list,test_id, mask_dict])
                                else:
                                    process_alt_comparison([predicted_raster_path, benchmark_raster_path, agreement_raster,stats_csv,stats_json, mask_values,stats_modes_list,test_id, mask_dict])

    # Multiprocess.
    if job_number > 1:
        with Pool(processes=job_number) as pool:
            pool.map(process_alt_comparison, procs_list)
            
                                        
            
    
    