#!/usr/bin/env python3

# Import file management library
import os
# Import data analysis library
import pandas as pd
import argparse
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
    stats = zonal_stats(huc_gpkg,{"agreement_raster":agree_rast}, nodata_value=10)
    return stats

#####################################################
# Creates a pandas df containing Alpha stats by hydroid.
# Stats input is the output of zonal_stats function.
# Huc8 is the huc8 string and is passed via the directory loop during execution.
# Mag is the magnitude (100y, action, minor etc.) is passed via the directory loop.
# Bench is the benchmark source.
#####################################################
def assemble_hydro_alpha_for_single_huc(stats,huc8,mag,bench):

    in_mem_df = pd.DataFrame(columns=['HydroID', 'huc8', 'true_negatives_count', 'false_negatives_count', 'true_positives_count',
        'false_positives_count', 'contingency_tot_count', 'cell_area_m2', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2',
        'FN_area_km2', 'contingency_tot_area_km2', 'predPositive_area_km2', 'predNegative_area_km2', 'obsPositive_area_km2',
        'obsNegative_area_km2', 'positiveDiff_area_km2', 'CSI', 'FAR', 'TPR', 'TNR', 'PND', 'PPV', 'NPV', 'ACC', 'Bal_ACC',
        'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc',
        'predPositive_perc', 'predNegative_perc', 'obsPositive_perc', 'obsNegative_perc', 'positiveDiff_perc',
        'masked_count', 'masked_perc', 'masked_area_km2', 'MAG','BENCH'])
    
    for dicts in stats:
        stats_dictionary = compute_stats_from_contingency_table(dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count= dicts['mp'])
        # Calls compute_stats_from_contingency_table from run_test_case.py
        
        hydroid = dicts['HydroID']
        stats_dictionary['HydroID'] = hydroid
               
        
        true_negatives_count = stats_dictionary['true_negatives_count']
        false_negatives_count = stats_dictionary['false_negatives_count']
        true_positives_count = stats_dictionary['true_positives_count']
        false_positives_count = stats_dictionary['false_positives_count']
        contingency_tot_count = stats_dictionary['contingency_tot_count']
        cell_area_m2 = stats_dictionary['cell_area_m2']
        TP_area_km2 = stats_dictionary['TP_area_km2']
        FP_area_km2 = stats_dictionary['FP_area_km2']
        TN_area_km2 = stats_dictionary['TN_area_km2']
        FN_area_km2 = stats_dictionary['FN_area_km2']
        contingency_tot_area_km2 = stats_dictionary['contingency_tot_area_km2']
        predPositive_area_km2 = stats_dictionary['predPositive_area_km2']
        predNegative_area_km2 = stats_dictionary['predNegative_area_km2']
        obsPositive_area_km2 = stats_dictionary['obsPositive_area_km2']
        obsNegative_area_km2 = stats_dictionary['obsNegative_area_km2']
        positiveDiff_area_km2 = stats_dictionary['positiveDiff_area_km2']
        CSI = stats_dictionary['CSI']
        FAR = stats_dictionary['FAR']
        TPR = stats_dictionary['TPR']
        TNR = stats_dictionary['TNR']
        PND = stats_dictionary['PND']
        PPV = stats_dictionary['PPV']
        NPV = stats_dictionary['NPV']
        ACC = stats_dictionary['ACC'] 
        Bal_ACC = stats_dictionary['Bal_ACC']
        MCC = stats_dictionary['MCC']
        EQUITABLE_THREAT_SCORE = stats_dictionary['EQUITABLE_THREAT_SCORE']
        PREVALENCE = stats_dictionary['PREVALENCE']
        BIAS = stats_dictionary['BIAS']
        F1_SCORE = stats_dictionary['F1_SCORE']
        TP_perc = stats_dictionary['TP_perc']
        FP_perc = stats_dictionary['FP_perc']
        TN_perc = stats_dictionary['TN_perc']
        FN_perc = stats_dictionary['FN_perc']
        predPositive_perc = stats_dictionary['predPositive_perc'] 
        predNegative_perc = stats_dictionary['predNegative_perc']
        obsPositive_perc = stats_dictionary['obsPositive_perc']
        obsNegative_perc = stats_dictionary['obsNegative_perc']
        positiveDiff_perc = stats_dictionary['positiveDiff_perc']
        masked_count = stats_dictionary['masked_count']
        masked_perc = stats_dictionary['masked_perc']
        masked_area_km2 = stats_dictionary['masked_area_km2']
        HydroID = stats_dictionary['HydroID']


        dict_with_list_values = {'HydroID': [HydroID],'huc8':[huc8], 'true_negatives_count': [true_negatives_count], 'false_negatives_count': [false_negatives_count],
        'true_positives_count': [true_positives_count], 'false_positives_count': [false_positives_count],
        'contingency_tot_count': [contingency_tot_count], 'cell_area_m2': [cell_area_m2],
        'TP_area_km2': [TP_area_km2], 'FP_area_km2': [FP_area_km2], 'TN_area_km2': [TN_area_km2], 'FN_area_km2': [FN_area_km2],
        'contingency_tot_area_km2': [contingency_tot_area_km2], 'predPositive_area_km2': [predPositive_area_km2],
        'predNegative_area_km2': [predNegative_area_km2], 'obsPositive_area_km2': [obsPositive_area_km2],
        'obsNegative_area_km2': [obsNegative_area_km2], 'positiveDiff_area_km2': [positiveDiff_area_km2], 'CSI': [CSI],
        'FAR': [FAR], 'TPR': [TPR], 'TNR': [TNR], 'PND': [PND], 'PPV': [PPV], 'NPV': [NPV], 'ACC': [ACC],
        'Bal_ACC': [Bal_ACC], 'MCC': [MCC], 'EQUITABLE_THREAT_SCORE': [EQUITABLE_THREAT_SCORE], 'PREVALENCE': [PREVALENCE],
        'BIAS': [BIAS], 'F1_SCORE': [F1_SCORE], 'TP_perc': [TP_perc], 'FP_perc': [FP_perc], 'TN_perc': [TN_perc],
        'FN_perc': [FN_perc], 'predPositive_perc': [predPositive_perc], 'predNegative_perc': [predNegative_perc],
        'obsPositive_perc': [obsPositive_perc], 'obsNegative_perc': [obsNegative_perc], 'positiveDiff_perc': [positiveDiff_perc],
        'masked_count': [masked_count], 'masked_perc': [masked_perc], 'masked_area_km2': [masked_area_km2],'MAG':[mag],'BENCH':[bench]}
        
        
        dict_to_df = pd.DataFrame(dict_with_list_values,columns=['HydroID','huc8', 'true_negatives_count', 'false_negatives_count', 'true_positives_count',
        'false_positives_count', 'contingency_tot_count', 'cell_area_m2', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2',
        'FN_area_km2', 'contingency_tot_area_km2', 'predPositive_area_km2', 'predNegative_area_km2', 'obsPositive_area_km2',
        'obsNegative_area_km2', 'positiveDiff_area_km2', 'CSI', 'FAR', 'TPR', 'TNR', 'PND', 'PPV', 'NPV', 'ACC', 'Bal_ACC',
        'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc',
        'predPositive_perc', 'predNegative_perc', 'obsPositive_perc', 'obsNegative_perc', 'positiveDiff_perc',
        'masked_count', 'masked_perc', 'masked_area_km2', 'MAG','BENCH'])
        

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
    
    # Execution code
    csv_output = pd.DataFrame(columns=['HydroID', 'huc8', 'true_negatives_count', 'false_negatives_count', 'true_positives_count',
        'false_positives_count', 'contingency_tot_count', 'cell_area_m2', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2',
        'FN_area_km2', 'contingency_tot_area_km2', 'predPositive_area_km2', 'predNegative_area_km2', 'obsPositive_area_km2',
        'obsNegative_area_km2', 'positiveDiff_area_km2', 'CSI', 'FAR', 'TPR', 'TNR', 'PND', 'PPV', 'NPV', 'ACC', 'Bal_ACC',
        'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc',
        'predPositive_perc', 'predNegative_perc', 'obsPositive_perc', 'obsNegative_perc', 'positiveDiff_perc',
        'masked_count', 'masked_perc', 'masked_area_km2', 'MAG','BENCH'])
    # This funtion, relies on the test_case class defined in run_test_case.py to list all available test cases
    all_test_cases = test_case.list_all_test_cases(version=version, archive=True, benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])

    error_list =[]
    error_count=0
    success_count=0

    for test_case_class in all_test_cases:

        if not os.path.exists(test_case_class.fim_dir):
            print(f'{test_case_class.fim_dir} does not exist')
            continue
        else:
            print(test_case_class.test_id, end='\r')

        # Define the catchment geopackage that contains the hydroid and geometry of each catchment. 
        huc_gpkg = os.path.join(test_case_class.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')

        agreement_dict = test_case_class.get_current_agreements()
        for mag in agreement_dict:
            for agree_rast in agreement_dict[mag]:
                
                stats = perform_zonal_stats(huc_gpkg,agree_rast)
                in_mem_df = assemble_hydro_alpha_for_single_huc(stats, test_case_class.huc, mag, test_case_class.benchmark_cat)
                
                concat_df_list = [in_mem_df, csv_output]

                csv_output = pd.concat(concat_df_list, sort=False)
                
               

    csv_output = csv_output[csv_output['CSI']!= "NA" & csv_output['FAR']!= "NA" & csv_output['TPR']!= "NA" 
                    & csv_output['TNR']!= "NA" & csv_output['PND']!= "NA" & csv_output['PPV']!= "NA"
                    & csv_output['NPV']!= "NA" & csv_output['ACC']!= "NA" & csv_output['Bal_ACC']!= "NA"
                    & csv_output['MCC']!= "NA" & csv_output['EQUITABLE_THREAT_SCORE']!= "NA"
                    & csv_output['PREVALENCE']!= "NA" & csv_output['BIAS']!= "NA" & csv_output['F1_SCORE']!= "NA"
                    & csv_output['TP_perc']!= "NA" & csv_output['FP_perc']!= "NA" & csv_output['TN_perc']!= "NA"
                    & csv_output['FN_perc']!= "NA" & csv_output['predPositive_perc']!= "NA"
                    & csv_output['predNegative_perc']!= "NA" & csv_output['obsPositive_perc']!= "NA"
                    & csv_output['obsNegative_perc']!= "NA" & csv_output['positiveDiff_perc']!= "NA"  ]
    
    csv_output.to_csv(csv, index=False, chunksize=1000)



