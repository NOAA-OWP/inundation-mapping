#!/usr/bin/env python3

# Import file management library
import os
# Import data analysis library
import pandas as pd
import geopandas as gpd
import argparse
from pixel_counter import zonal_stats
from tools_shared_functions import compute_stats_from_contingency_table
from run_test_case import test_case
from shapely.validation import make_valid

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

    # in_mem_df = pd.DataFrame(columns=['HydroID', 'huc8', 'true_negatives_count', 'false_negatives_count', 'true_positives_count',
    #     'false_positives_count', 'contingency_tot_count', 'cell_area_m2', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2',
    #     'FN_area_km2', 'contingency_tot_area_km2', 'predPositive_area_km2', 'predNegative_area_km2', 'obsPositive_area_km2',
    #     'obsNegative_area_km2', 'positiveDiff_area_km2', 'CSI', 'FAR', 'TPR', 'TNR', 'PND', 'PPV', 'NPV', 'ACC', 'Bal_ACC',
    #     'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc',
    #     'predPositive_perc', 'predNegative_perc', 'obsPositive_perc', 'obsNegative_perc', 'positiveDiff_perc',
    #     'masked_count', 'masked_perc', 'masked_area_km2', 'MAG','BENCH'])

    in_mem_df = pd.DataFrame(columns=['HydroID', 'huc8','cell_area_m2',
                                    'CSI', 'FAR', 'TPR', 'TNR',  'PPV', 'NPV', 'Bal_ACC',
                                    'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE',
                                    'masked_perc', 'MAG','BENCH'])



    for dicts in stats:
        tot_pop = dicts['tn'] +dicts['fn'] + dicts['fp'] + dicts['tp']
        if tot_pop == 0:
            continue
        print('tn is')
        print(dicts['tn'])
        print('fn is')
        print(dicts['fn'])
        print('fp is')
        print(dicts['fp'])
        print('tp is')
        print(dicts['tp'])
        stats_dictionary = compute_stats_from_contingency_table(dicts['tn'], dicts['fn'], dicts['fp'], dicts['tp'], cell_area=100, masked_count= dicts['mp'])
        # Calls compute_stats_from_contingency_table from run_test_case.py
        # if 'NA' in stats_dictionary.values():
        #     continue
            

        hydroid = dicts['HydroID']
        stats_dictionary['HydroID'] = hydroid
               
        



        # true_negatives_count = round(float(stats_dictionary['true_negatives_count']),2)
        # false_negatives_count = round(float(stats_dictionary['false_negatives_count']),2)
        # true_positives_count = round(float(stats_dictionary['true_positives_count']),2)
        # false_positives_count = round(float(stats_dictionary['false_positives_count']),2)
        # contingency_tot_count = round(float(stats_dictionary['contingency_tot_count']),2)
        cell_area_m2 = round(float(stats_dictionary['cell_area_m2']),2)
        
       
        
        CSI = stats_dictionary['CSI']
        if CSI != 'NA':
            CSI = round(CSI,2)

        FAR = stats_dictionary['FAR']
        if FAR != 'NA':
            FAR = round(FAR,2)
        
        TPR = stats_dictionary['TPR']
        if TPR != 'NA':
            TPR = round(TPR,2)

        TNR = stats_dictionary['TNR']
        if TNR != 'NA':
            TNR = round(TNR,2)

        # PND = stats_dictionary['PND']
        # if PND != 'NA':
        #     PND = round(PND,2)

        PPV = stats_dictionary['PPV']
        if PPV != 'NA':
            PPV = round(PPV,2)

        NPV = stats_dictionary['NPV']
        if NPV != 'NA':
            NPV = round(NPV,2)

        # ACC = stats_dictionary['ACC']
        # if ACC != 'NA':
        #     ACC = round(ACC,2)

        Bal_ACC = stats_dictionary['Bal_ACC']
        if Bal_ACC != 'NA':
            Bal_ACC = round(Bal_ACC,2)

        MCC = stats_dictionary['MCC']
        if MCC != 'NA':
            MCC = round(MCC,2)

        EQUITABLE_THREAT_SCORE = stats_dictionary['EQUITABLE_THREAT_SCORE']
        if EQUITABLE_THREAT_SCORE != 'NA':
            EQUITABLE_THREAT_SCORE = round(EQUITABLE_THREAT_SCORE,2)

        PREVALENCE = stats_dictionary['PREVALENCE']
        if PREVALENCE != 'NA':
            PREVALENCE = round(PREVALENCE,2)

        BIAS = stats_dictionary['BIAS']
        if BIAS != 'NA':
            BIAS = round(BIAS,2)   

        F1_SCORE = stats_dictionary['F1_SCORE']
        if F1_SCORE != 'NA':
            F1_SCORE = round(F1_SCORE,2)    

        # TP_perc = stats_dictionary['TP_perc']
        # if TP_perc != 'NA':
        #     TP_perc = round(TP_perc,2) 

        # FP_perc = stats_dictionary['FP_perc']
        # if FP_perc != 'NA':
        #     FP_perc = round(FP_perc,2)
        
        # TN_perc = stats_dictionary['TN_perc']
        # if TN_perc != 'NA':
        #     TN_perc = round(TN_perc,2)

        # FN_perc = stats_dictionary['FN_perc']
        # if FN_perc != 'NA':
        #     FN_perc = round(FN_perc,2)

        # predPositive_perc = stats_dictionary['predPositive_perc']
        # if predPositive_perc != 'NA':
        #     predPositive_perc = round(predPositive_perc,2)

        # predNegative_perc = stats_dictionary['predNegative_perc']
        # if predNegative_perc != 'NA':
        #     predNegative_perc = round(predNegative_perc,2)

        # obsPositive_perc = stats_dictionary['obsPositive_perc']
        # if obsPositive_perc != 'NA':
        #     obsPositive_perc = round(obsPositive_perc,2)

        # obsNegative_perc = stats_dictionary['obsNegative_perc']
        # if obsNegative_perc != 'NA':
        #     obsNegative_perc = round(obsNegative_perc,2)

        # positiveDiff_perc = stats_dictionary['positiveDiff_perc']
        # if positiveDiff_perc != 'NA':
        #     positiveDiff_perc = round(positiveDiff_perc,2)

        # masked_count = stats_dictionary['masked_count']
        # if masked_count != 'NA':
        #     masked_count = round(masked_count,2)

        masked_perc = stats_dictionary['masked_perc']
        if masked_perc != 'NA':
            masked_perc = round(masked_perc,2)

        # masked_area_km2 = stats_dictionary['masked_area_km2']
        # if masked_area_km2 != 'NA':
        #     masked_area_km2 = round(masked_area_km2,2)

        
        HydroID = stats_dictionary['HydroID']


        dict_with_list_values = {'HydroID': [HydroID],'huc8':[huc8], 'cell_area_m2': [cell_area_m2],
        'CSI': [CSI], 'FAR': [FAR], 'TPR': [TPR], 'TNR': [TNR], 'PPV': [PPV], 'NPV': [NPV],
        'Bal_ACC': [Bal_ACC], 'MCC': [MCC], 'EQUITABLE_THREAT_SCORE': [EQUITABLE_THREAT_SCORE], 'PREVALENCE': [PREVALENCE],
        'BIAS': [BIAS], 'F1_SCORE': [F1_SCORE], 'masked_perc': [masked_perc], 'MAG':[mag],'BENCH':[bench]}
        
        


        dict_to_df = pd.DataFrame(dict_with_list_values,columns=['HydroID','huc8', 'cell_area_m2',
        'CSI', 'FAR', 'TPR', 'TNR', 'PPV', 'NPV', 'Bal_ACC',
        'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'masked_perc', 'MAG','BENCH'])
        
        #dict_to_df.round(2)
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
    parser.add_argument('-comp','--composite',
                        help='If used, composite metrics will be pulled instead',
                        required=False,default=None,action='store_true')
    ##Rob Notes
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    benchmark_category = args['benchmark_category']
    version = args['version']
    csv = args['csv']
    composite = bool(args['composite'])
    
    # Execution code
    

    csv_output = gpd.GeoDataFrame(columns=['HydroID', 'huc8', 'true_negatives_count', 'false_negatives_count', 'true_positives_count',
        'false_positives_count', 'contingency_tot_count', 'cell_area_m2', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2',
        'FN_area_km2', 'contingency_tot_area_km2', 'predPositive_area_km2', 'predNegative_area_km2', 'obsPositive_area_km2',
        'obsNegative_area_km2', 'positiveDiff_area_km2', 'CSI', 'FAR', 'TPR', 'TNR', 'PND', 'PPV', 'NPV', 'ACC', 'Bal_ACC',
        'MCC', 'EQUITABLE_THREAT_SCORE', 'PREVALENCE', 'BIAS', 'F1_SCORE', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc',
        'predPositive_perc', 'predNegative_perc', 'obsPositive_perc', 'obsNegative_perc', 'positiveDiff_perc',
        'masked_count', 'masked_perc', 'masked_area_km2', 'MAG','BENCH','geometry'], geometry = 'geometry')
    # This funtion, relies on the test_case class defined in run_test_case.py to list all available test cases
    print('listing_test_cases')
    all_test_cases = test_case.list_all_test_cases(version=version, archive=True, benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])

    error_list =[]
    error_count=0
    success_count=0
    counter = 0
    for test_case_class in all_test_cases:

        if not os.path.exists(test_case_class.fim_dir):
            print(f'{test_case_class.fim_dir} does not exist')
            continue
        else:
            print(test_case_class.test_id)
            print('thisisthecurrentinteration!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            # counter = counter + 1
            # if counter >=3:
            #     break
            #print(test_case_class.test_id, end='\r')

            # Define the catchment geopackage that contains the hydroid and geometry of each catchment. 
            huc_gpkg = os.path.join(test_case_class.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')

            agreement_dict = test_case_class.get_current_agreements(composite)
            for mag in agreement_dict:
                for agree_rast in agreement_dict[mag]:
                    print('performing_zonal_stats')
                    stats = perform_zonal_stats(huc_gpkg,agree_rast)
                    
                    print('assembling_hydroalpha_for_single_huc')
                    get_geom = gpd.read_file(huc_gpkg)
                    
                    get_geom['geometry'] = get_geom.apply(lambda row: make_valid(row.geometry), axis=1)
                    print(get_geom.crs)
                    
                    in_mem_df = assemble_hydro_alpha_for_single_huc(stats, test_case_class.huc, mag, test_case_class.benchmark_cat)
                    #get_geom = gpd.read_file(huc_gpkg)
                    #print(get_geom)
                    hydro_geom_df = get_geom[["HydroID", "geometry"]]
                    #geom_output = pd.merge(in_mem_df, hydro_geom_df, how = 'inner', on = 'HydroID')
                    

                    print('filtering_rows')
                    #in_mem_df = in_mem_df.convert_dtypes()
                    #try find and reaplace with NAN then use dropna???might be beter way to do
                    # in_mem_df = in_mem_df[(in_mem_df['CSI']!= "NA") & (in_mem_df['FAR']!= "NA") & (in_mem_df['TPR']!= "NA") 
                    # & (in_mem_df['TNR']!= "NA") & (in_mem_df['PND']!= "NA")]
                    

                    # & (in_mem_df['PPV']!= "NA")
                    # & (in_mem_df['NPV']!= "NA") & (in_mem_df['ACC']!= "NA") & (in_mem_df['Bal_ACC']!= "NA")
                    # & (in_mem_df['MCC']!= "NA") & (in_mem_df['EQUITABLE_THREAT_SCORE']!= "NA")
                    # & (in_mem_df['PREVALENCE']!= "NA") & (in_mem_df['BIAS']!= "NA") & (in_mem_df['F1_SCORE']!= "NA")
                    # & (in_mem_df['TP_perc']!= "NA") & (in_mem_df['FP_perc']!= "NA") & (in_mem_df['TN_perc']!= "NA")
                    # & (in_mem_df['FN_perc']!= "NA") & (in_mem_df['predPositive_perc']!= "NA")
                    # & (in_mem_df['predNegative_perc']!= "NA") & (in_mem_df['obsPositive_perc']!= "NA")
                    # & (in_mem_df['obsNegative_perc']!= "NA") & (in_mem_df['positiveDiff_perc']!= "NA")



                    geom_output = hydro_geom_df.merge(in_mem_df, on='HydroID', how ='inner')
                    print('merging_to_output_df')
                    print(geom_output.crs)
                    #geom_output = geom_output.convert_dtypes()
                    
                    concat_df_list = [geom_output, csv_output]
                    
                    csv_output = pd.concat(concat_df_list, sort=False)
                    print(csv_output)
                    #csv_output = csv_output[(csv_output['CSI']!= "NA")]
                    print(csv_output.crs)
                    print(type(csv_output))
                    
               


    
    print('writing_to_gpkg')
    #csv_output.to_file(csv)
    #tmp_path = "/data/temp/caleb/test_data/ble_test_geom_error.gpkg"
    csv_output.to_file(csv, driver="GPKG")

    # print('writing_to_csv')
    # tmp_path2="/data/temp/caleb/test_data/ble_test_geom_error.csv"
    # csv_output.to_csv(csv, index=False, chunksize=1000, encoding='utf-8')
    



