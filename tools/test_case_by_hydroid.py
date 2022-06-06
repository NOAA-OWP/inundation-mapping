
#contains logic for create csv of alpha stats by hydroid per huc
import sys
from osgeo import gdal, ogr
from osgeo.gdalconst import *
# Import numerical data library
import numpy as np
# Import file management library
import sys
import os
# Import data analysis library
import pandas as pd
import argparse
from pandas import DataFrame
import copy
import pathlib
import tempfile
import geopandas as gpd


from pixel_counter import zonal_stats

from tools_shared_functions import compute_stats_from_contingency_table
from run_test_case import test_case

#test_case_by_hydroid.py -g"/dev_fim_share/foss_fim/previous_fim/fim_3_0_24_14_ms/12090301/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg" -a"/dev_fim_share/foss_fim/test_cases/ble_test_cases/12090301_ble/official_versions/fim_3_0_24_14_ms/100yr/inundation_extent_12090301.tif" 




def perform_zonal_stats(huc_gpkg,agree_rast):
    stats = zonal_stats(huc_gpkg,{"agreement_raster":agree_rast})
    return stats

def assemble_hydro_alpha_for_single_huc(stats,huc8,mag,bench):

    in_mem_df = pd.DataFrame(columns=['HydroID','CSI','FAR','TPR','TNR','PND','HUC8','MAG','BENCH'])

    for dicts in stats:
        stats_dictionary = compute_stats_from_contingency_table(dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count= dicts['mp'])
        hydroid = dicts['HydroID']
        stats_dictionary['HydroID'] = hydroid
        
        
        csi = stats_dictionary['CSI']
        far = stats_dictionary['FAR']
        tpr = stats_dictionary['TPR']
        tnr = stats_dictionary['TNR']
        pnd = stats_dictionary['PND']
        
    
        dict_with_list_values = {'HydroID':[hydroid],'CSI':[csi],'FAR':[far], 'TPR':[tpr],'TNR':[tnr],'PND':[pnd],'HUC8':[huc8],'MAG':[mag],'BENCH':[bench]}
        
        dict_to_df = pd.DataFrame(dict_with_list_values,columns=['HydroID','CSI','FAR','TPR','TNR','PND','HUC8','MAG','BENCH'])
        
        concat_list = [in_mem_df, dict_to_df]

        #concatenate dataframes
        in_mem_df = pd.concat(concat_list, sort=False)
        
    #print(in_mem_df)
    return in_mem_df








if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description='Produces alpha metrics by hyrdoid per a huc.')

    parser.add_argument('-g', '--huc_gpkg',
                        help='Path to hydroid geopackage',
                        required=True)
    parser.add_argument('-a', '--agree_rast',
                        help='Path agreement raster.',
                        required=True)
    parser.add_argument('-c', '--csv',
                        help='Path to folder to hold exported csv files per huc.',
                        required=False)
    
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    huc_gpkg = args['huc_gpkg']
    agree_rast = args['agree_rast']
    #csv = args['csv']
    
#execution code
#benchmark catagories ble, nws, usgs, ifc, ras2fim, all
#fim version     parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
#    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=False,default='DEV')
#    parser.add_argument('-b','--benchmark-category',help='A benchmark category to specify. Defaults to process all categories.',required=False, default="all")


version='fim_3_0_24_14_ms'
benchmark_category = 'all'
all_test_cases = test_case.list_all_test_cases(version=version, archive='dev', benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])


    ## Loop through all test cases, build the alpha test arguments, and submit them to the process pool
error_list =[]
error_count=0
success_count=0  
for test_case_class in all_test_cases:
    
    if not os.path.exists(test_case_class.fim_dir):
        print('not')
        continue
    huc_gpkg = os.path.join(test_case_class.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    #list_all_test_cases = test_case_class.list_all_test_cases(version='fim_3_0_24_14_ms', archive='dev')
    huc_tuple=test_case_class.fim_dir.rsplit('/',1)
    huc = huc_tuple[1]


    


    print('######################################################')
    agreement_dict = test_case_class.get_current_agreements()
    for mag in agreement_dict:
        for agree in agreement_dict[mag]:
            
            mag_string = agree.split(version)
            
            mag_str = mag_string[1].split('/')
            

            magnitude_str = mag_str[1]

            agree_rast = agree

           # 'ble', 'ifc', 'nws', 'usgs', 'ras2fim'

            
            bench_string = mag_string[0].split('test_cases/')
            
            bench = bench_string[1]
            benchmark_str = bench.split('_')[0]
            

            
            try:
                stats = perform_zonal_stats(huc_gpkg,agree_rast)
                in_mem_df = assemble_hydro_alpha_for_single_huc(stats,huc,magnitude_str,benchmark_str)
                print(in_mem_df)
                success_count= success_count+1
            except:
                print('An error has occured')
                error_count=error_count +1
                error_list.append(huc_gpkg)

print(error_count)
print(success_count)
                
#"/data/previous_fim/fim_3_0_24_14_ms/12090301/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg"
# test_case_class.fim_dir = /data/previous_fim/fim_3_0_24_14_ms/12040103


print('testerino')