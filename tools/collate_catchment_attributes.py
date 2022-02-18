import argparse
import pandas as pd
import os


import os
import pandas as pd
import re
import geopandas as gpd




def aggregateHydroTables(root_dir):
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


def assembleSierraTest(geoPackagePath):
    sierra_test_results = gpd.read_file(geoPackagePath)
    
    #sierra_test_results.location_id = sierra_test_results.location_id.astype(str)
    print(type(sierra_test_results.location_id.loc[0]))
    return sierra_test_results
    
def importLinkTable(link_table_path):
    link_df = pd.read_csv(link_table_path,dtype = {'location_id':str,'HydroID':int})
    link_df = link_df.dropna(subset=['location_id'])
    link_df = link_df.filter(['HydroID','location_id'])
    print(link_df)
    return link_df
    
def performMerge(sierra_test_results,link_df,aggregate_df,):
    filter_list = ['HydroID','SLOPE','AREASQKM','LENGTHKM','LakeID','order_','sinuosity','nws_lid','location_id','HUC8','name','states','curve','mainstem','nrmse','mean_abs_y_diff_ft','mean_y_diff_ft','percent_bias','2','5','10','25','50','100','action','minor','moderate','major','geometry']
    sierra_test_merged = sierra_test_results.merge(link_df, on='location_id')
    aggregate_df = aggregate_df.merge(sierra_test_merged, on='HydroID')
    aggregate_df = aggregate_df.filter(filter_list)
    
    return aggregate_df
def outFileDest(aggregate_df,outFile):
    aggregate_df.to_csv(outFile, encoding='utf-8', index=False) 
  


if __name__ == '__main__':
    """
    collate catchment attributes. loops through huc directories in defined fim directory. assembles needed attibutes into csv.
    
    recomended current sierra test: "/data/tools/sierra_test/official_versions/fim_3_0_24_14_ms/usgs_gages_stats.gpkg" 
    recomended current elev link table: "/data/tools/sierra_test/official_versions/fim_3_0_24_14_ms/agg_usgs_elev_table.csv"
    recomended current fim directory: "/data/previous_fim/fim_3_0_24_14_ms/"
    
    TODO
    """

    parser = argparse.ArgumentParser(description='collates catchment attributes from determined source')
    
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-s', '--sierra-test-input', help='layer containing sierra test by hydroId',required=True)
    parser.add_argument('-o','--output-csv-destination',help='location and name for output csv',required=True)
    parser.add_argument('-l','--link-elev-table',help='elev table for linking sierra tests to hydrotable on locationid and hydroid',required=True)
    
    args = vars(parser.parse_args())

    fim_directory = args['fim_directory']
    sierra_test_input = args['sierra_test_input']
    output_csv_destination = args['output_csv_destination']
    link_elev_table = args['link_elev_table']
    
    aggregate_df = performMerge(assembleSierraTest(sierra_test_input),importLinkTable(),aggregateHydroTables(fim_directory))
    outFileDest(aggregate_df,output_csv_destination)
    