import argparse
import pandas as pd
import os
import re
import geopandas as gpd


#########################################################################
#Collect Hydrotables     
#Loops through huc dircectories and appends hydrotable.csv from each one.
#Drops duplicates on hydroID so that this code ignores stage changes, and only assembles one row per hydroID.
#########################################################################
def aggregate_hydro_tables(root_dir):

    aggregate_df = pd.DataFrame()
    dtype_dict_py ={'HydroID': int,'feature_id':int, 'SLOPE':float, 'AREASQKM':float, 'order_':int,'LENGTHKM':float,'LakeID':int, 'orig_ManningN':float}
    for huc_dir in os.listdir(root_dir):    
        if not re.search("^\d{6,8}$",huc_dir):        
            continue 
    
        hydroTable = pd.read_csv(os.path.join(root_dir, huc_dir, "hydroTable.csv"),dtype = dtype_dict_py)
        hydroTable = hydroTable.filter(['HydroID',"feature_id", 'SLOPE', 'AREASQKM','LENGTHKM','LakeID', 'order_', 'orig_ManningN', 'sinuosity'])
        aggregate_df = aggregate_df.append(hydroTable)        
    aggregate_df = aggregate_df.drop_duplicates(subset=['HydroID'], keep='first')
    return aggregate_df


#########################################################################
#Read Sierra Test
#Uses geopandas to injest sierra test geopackage into a geodataframe
#########################################################################
def assemble_sierra_test(geo_package_path):
    
    sierra_test_results = gpd.read_file(geo_package_path)
    return sierra_test_results
    

#########################################################################
#Read Link table
#link table necesary for maintaining uniqueness of hydroID. Used to link hydrotable to sierra test metrics.
#########################################################################
def import_link_table(link_table_path):

    link_df = pd.read_csv(link_table_path,dtype = {'location_id':str,'HydroID':int})
    link_df = link_df.dropna(subset=['location_id'])
    link_df = link_df.filter(['HydroID','location_id'])
    return link_df
    

#########################################################################
#Perform merge
#Function to merge the hydrotables to the sierra test via the link table.
# Also defines the columns/fields desired in final csv result.
#########################################################################
def perform_merge(sierra_test_results,link_df,aggregate_df):

    filter_list = ['HydroID','SLOPE','AREASQKM','LENGTHKM','LakeID','order_','sinuosity','nws_lid','location_id','HUC8','name','states','curve','mainstem','nrmse','mean_abs_y_diff_ft','mean_y_diff_ft','percent_bias','2','5','10','25','50','100','action','minor','moderate','major','geometry']
    sierra_test_merged = sierra_test_results.merge(link_df, on='location_id')
    aggregate_df_merged = aggregate_df.merge(sierra_test_merged, on='HydroID')
    aggregate_df_merged = aggregate_df_merged.filter(filter_list)    
    return aggregate_df_merged


#########################################################################
#Perform merge
#Given correct collection location of NLCD data, collects and merges into Hydroid data
#########################################################################
def aggregate_nlcd(pixel_dir,aggregate_df,run_type):
    ms_or_fr = str(run_type)
    new_df = pd.DataFrame()
    csv_data = aggregate_df
        
    for each_dir in os.listdir(pixel_dir):
        if re.search(ms_or_fr,each_dir) and re.search("pixel_counts.csv",each_dir):
            #cast name of each dir to string  
            csv_str = each_dir  
            nlcd_table = pd.read_csv(os.path.join(pixel_dir, csv_str),dtype ={'HydroID': int})
            innerj = csv_data.merge(nlcd_table, on='HydroID', how= 'inner')
            if innerj.empty:
                #steps to next loop pass if df is empty
                continue  
            if not new_df.empty:
                new_df = new_df.append(innerj)                
            else:
                new_df = innerj
     
    return new_df


#########################################################################
#Writes output
#Defines the destination of csv output
#########################################################################
def out_file_dest(aggregate_df,outfile):
    
    aggregate_df.to_csv(outfile, encoding='utf-8', index=False)


if __name__ == '__main__':
    """
    collate catchment attributes. loops through huc directories in defined fim directory. assembles needed attibutes into csv.
    
    recommended current sierra test: "/data/tools/sierra_test/official_versions/fim_3_0_24_14_ms/usgs_gages_stats.gpkg" 
    recommended current elev link table: "/data/tools/sierra_test/official_versions/fim_3_0_24_14_ms/agg_usgs_elev_table.csv"
    recommended current fim directory: "/data/previous_fim/fim_3_0_24_14_ms/"
    recommended current nlcd data set: "/data/temp/nlcd_pixel_counts_fim_3_0_26_0/"
    
    TODO
    """

    parser = argparse.ArgumentParser(description='collates catchment attributes from determined source')
    
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-s', '--sierra-test-input', help='layer containing sierra test by hydroId',required=True)
    parser.add_argument('-o','--output-csv-destination',help='location and name for output csv',required=True)
    parser.add_argument('-l','--link-elev-table',help='elev table for linking sierra tests to hydrotable on locationid and hydroid',required=True)
    parser.add_argument('-lc','--nlcd', help='data set with lulc pixel counts', required=True)
    parser.add_argument('-rt','--run-type', help='tells whether the run is ms or fr', required=True)

    args = vars(parser.parse_args())

    fim_directory = args['fim_directory']
    sierra_test_input = args['sierra_test_input']
    output_csv_destination = args['output_csv_destination']
    link_elev_table = args['link_elev_table']
    nlcd_pixel_count_dir = args['nlcd']
    run_type = args['run_type']
    
    #reads in single geopackage contianing sierra test.
    sierra_test_results = assemble_sierra_test(sierra_test_input)

    #the link table is required to prevent duplicate values when joining sierra test to hydrotables.  
    link_df = import_link_table(link_elev_table)  

    #loops through the hydrotables and collects static metrics into a df.
    aggregate_df = aggregate_hydro_tables(fim_directory)

    #merges the hydrotable df with the sierra test df via the link table.
    aggregate_df_merged = perform_merge(sierra_test_results,link_df,aggregate_df)  

    #merges in the nlcd data. This requires looping through many entries. 
    aggregate_df_merged_with_nlcd = aggregate_nlcd(nlcd_pixel_count_dir,aggregate_df,run_type) 

    #determines the output location and writes to csv.
    out_file_dest(aggregate_df_merged_with_nlcd, output_csv_destination)  
