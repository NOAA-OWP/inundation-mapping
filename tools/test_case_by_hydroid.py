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


#####################################################
# Perform zonal stats is a funtion stored in pixel_counter.py.
# The input huc_gpkg is a single huc8 geopackage, the second input argument must be input as a dict.
# For the purposes of assembling the alpha metrics by hydroid, always use agreement_raster total area agreement tiff.
# This function is called automatically. Returns stats dict of pixel counts.
#####################################################
def perform_zonal_stats(huc_gpkg,agree_rast):
    stats = zonal_stats(huc_gpkg,{"agreement_raster":agree_rast})
    return stats


#####################################################
# Creates a pandas df containing Alpha stats by hydroid.
# Stats input is the output of zonal_stats function.
# Huc8 is the huc8 string and is passed via the directory loop during execution.
# Mag is the magnitude (100y, action, minor etc.) is passed via the directory loop.
# Bench is the benchmark source.
#####################################################
def assemble_hydro_alpha_for_single_huc(stats,huc8,mag,bench):

    in_mem_df = pd.DataFrame(columns=['HydroID','CSI','FAR','TPR','TNR','PND','HUC8','MAG','BENCH'])
    

    for dicts in stats:
        stats_dictionary = compute_stats_from_contingency_table(dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count= dicts['mp'])
        # Calls compute_stats_from_contingency_table from run_test_case.py
        
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

        in_mem_df = pd.concat(concat_list, sort=False)
        
    
    return in_mem_df


if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description='Produces alpha metrics by hyrdoid.')

    parser.add_argument('-b', '--benchmark_category',
                        help='Choice of truth data. Options are: all, ble, ifc, nws, usgs, ras2fim',
                        required=True)
    parser.add_argument('-v', '--version',
                        help='The fim version to use. Should be similar to fim_3_0_24_14_ms',
                        required=True)
    parser.add_argument('-c', '--csv',
                        help='Path to folder to hold exported csv file.',
                        required=True)
    
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    benchmark_category = args['benchmark_category']
    version = args['version']
    csv = args['csv']
    

#execution code
csv_output = pd.DataFrame(columns=['HydroID','CSI','FAR','TPR','TNR','PND','HUC8','MAG','BENCH'])
all_test_cases = test_case.list_all_test_cases(version=version, archive='dev', benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])
#This funtion, test_case_by_hydroid.py, relies on the test_case class defined in run_test_case.py.

error_list =[]
error_count=0
success_count=0  


for test_case_class in all_test_cases:

    if not os.path.exists(test_case_class.fim_dir):
        print('not')
        continue

    huc_gpkg = os.path.join(test_case_class.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    #Define the huc geopackage that contains the hydroid and geometry of each huc. 


    huc_tuple=test_case_class.fim_dir.rsplit('/',1)
    huc = huc_tuple[1]
    #define the huc8 string to be added to the record for each hydroid.


    agreement_dict = test_case_class.get_current_agreements()
    for mag in agreement_dict:
        for agree in agreement_dict[mag]:
            

            mag_string = agree.split(version)
            mag_str = mag_string[1].split('/')
            magnitude_str = mag_str[1]
            agree_rast = agree
            bench_string = mag_string[0].split('test_cases/')
            bench = bench_string[1]
            benchmark_str = bench.split('_')[0]
            #Perform string manupulation to define the magnitude and benchmark of each record as iteration progreses.


            try:
                stats = perform_zonal_stats(huc_gpkg,agree_rast)
                in_mem_df = assemble_hydro_alpha_for_single_huc(stats,huc,magnitude_str,benchmark_str)
                
                concat_df_list = [in_mem_df, csv_output]

                try:
                    csv_output = pd.concat(concat_df_list, sort=False)
                except:
                    print('An error has occured merging dataframes')
            except:
                print('An error has occured performing zonal stats')
                

csv_output.to_csv(csv, index=False, chunksize=1000)
