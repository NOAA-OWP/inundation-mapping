#!/usr/bin/env python3

import pandas as pd
import os
import re
import json
import numpy as np
from glob import glob
from tqdm import tqdm
import argparse
from shared_variables import TEST_CASES_DIR,\
                             PREVIOUS_FIM_DIR,\
                             OUTPUTS_DIR,\
                             INPUTS_DIR,\
                             AHPS_BENCHMARK_CATEGORIES
from tools_shared_functions import csi,far,tpr,mcc


def Consolidate_metrics(benchmarks='all',versions='all',zones=['total_area'],metrics_output_csv=None):

    """ Consolidates metrics into single dataframe """

    if isinstance(benchmarks,str):
        benchmarks = [benchmarks]
    elif isinstance(benchmarks,list):
        pass
    else:
        benchmarks = list(benchmarks)
    
    if isinstance(versions,str):
        versions = [versions]
    elif isinstance(benchmarks,list):
        pass
    else:
        versions = list(versions)

    # loop through benchmarks
    consolidated_metrics_df = [ f for f in return_dataframe_for_benchmark_source(benchmarks,zones) ]
    
    # concat
    consolidated_metrics_df = pd.concat(consolidated_metrics_df,ignore_index=True)

    # find matching rows
    consolidated_metrics_df = find_matching_rows_by_attribute_value(consolidated_metrics_df,'version',versions)

    if metrics_output_csv is not None:
        consolidated_metrics_df.to_csv(metrics_output_csv, index=False)

    consolidated_secondary_metrics = pivot_metrics(consolidated_metrics_df)
    print(consolidated_secondary_metrics)

    return(consolidated_metrics_df,consolidated_secondary_metrics)



def pivot_metrics(consolidated_metrics_df):

    ''' Pivots metrics to provide summary of results '''

    # pivot out
    consolidated_metrics_pivot = pd.pivot_table(
                                                consolidated_metrics_df,
                                                values=['true_positives_count','false_positives_count',
                                                        'false_negatives_count','true_negatives_count'],
                                                index=['extent_config','magnitude','version'], 
                                                aggfunc=np.sum
                                               )

    # metrics to run
    metrics_functions = { 'CSI': csi , 'TPR' : tpr, 'FAR' : far, 'MCC' : mcc }
    #metrics_functions = { 'CSI': csi }
    
    def row_wise_function(df,mn,mf):
        
        ''' applies function along rows of dataframes '''

        return( 
                pd.Series(
                          mf( 
                             df['true_positives_count'],
                             df['false_positives_count'],
                             df['false_negatives_count'],
                             df['true_negatives_count']
                            ),
                          name=mn
                         )
              )

    column_generator = (  row_wise_function(consolidated_metrics_pivot,met_name,met_func) 
                          for met_name,met_func in metrics_functions.items() 
                       )

    consolidated_secondary_metrics = pd.concat(column_generator,axis=1)

    return(consolidated_secondary_metrics)


def find_matching_rows_by_attribute_value(df,attributes,matches):
    
    if (len(matches) == 1) & (matches == 'all'):
        return(df)
    else:
        df = df.loc[df.loc[:,attributes].isin(matches)]

    return(df)


def return_dataframe_for_benchmark_source(benchmarks,zones=['total_area']):

    """ returns a dataframe of results given a name for a benchmark source """

    benchmark_function_dict = { 
                                'ble' : consolidate_ble_metrics(zones)
                              }

    # if all cycle through all functions, else go through selection
    if (len(benchmarks)== 1) & ('all' in benchmarks):
        
        for f in benchmark_function_dict.values():
            yield(f)

    else:

        for b in benchmarks:
            try:
                yield( benchmark_function_dict[b] )
            except KeyError: 
                raise ValueError(f"Benchmark '{b}' not supported. "\
                                 f"Pass 'all' or one of these"\
                                 f"{list(benchmark_function_dict.keys())}")


def consolidate_ble_metrics(zones=['total_area']):

    """ consolidates ble metrics """

    # get filenames for metrics for ble
    files_to_consolidate = list()
    for zone in zones:
        file_pattern_to_glob = os.path.join(TEST_CASES_DIR,'ble_test_cases','**',f'{zone}_stats.csv')
        files_to_consolidate.extend(glob(file_pattern_to_glob, recursive=True))

    # make a metrics dataframe generator
    metrics_df_generator = metrics_data_frame_generator_from_files(files_to_consolidate)

    # concat said generator of files
    metrics_df = pd.concat(metrics_df_generator, ignore_index=True)

    # make a meta-data
    metadata_df = metadata_dataframe_from_file_names_for_ble(files_to_consolidate)

    # concat metrics and metadata dataframes
    return(   
            pd.concat( (metadata_df,metrics_df), axis=1) 
          )


def metrics_data_frame_generator_from_files(file_names):

    """ Reads in metrics dataframes from filenames """

    for f in file_names:
        yield pd.read_csv(f,index_col=0).T.reset_index(drop=True)


def metadata_dataframe_from_file_names_for_ble(file_names):
    
    """ Makes a dataframe for meta-data """

    file_name_index_dict = { 
                            'benchmark_source' : parse_benchmark_source(file_names),
                            'version' : parse_version_name(file_names),
                            'huc' : parse_huc(file_names),
                            'magnitude' : parse_magnitude(file_names),
                            'extent_config' : parse_eval_metadata(file_names,'model'),
                            'calibrated' : parse_eval_metadata(file_names,'calibrated')
                           }

    return( 
            pd.DataFrame(file_name_index_dict)
          )


def parse_benchmark_source(file_names):

    """ Parses benchmark source """

    for f in file_names:
        yield f.split('/')[3].split('_')[0]


def parse_version_name(file_names):

    """ Parses version name """

    for f in file_names:
        yield f.split('/')[6]


def parse_magnitude(file_names):

    """ Parses magnitude """

    for f in file_names:
        yield f.split('/')[7]


def parse_huc(file_names):

    """ parses huc """

    for f in file_names:
        yield f.split('/')[4].split('_')[0]


def parse_eval_metadata(file_names,metadata_field):
   
    """ parsing eval metadata json files """

    for f in file_names:
        root_dir = os.path.abspath(os.sep)
        dir_of_metadata = f.split('/')[0:7]
        eval_metadata_filepath = os.path.join(root_dir,*dir_of_metadata,'eval_metadata.json')
        
        # read eval data, if no file write None 
        try:
            with open(eval_metadata_filepath) as fObj:
                eval_metadata = json.load(fObj)
        except FileNotFoundError:
            yield None
        else:
            yield eval_metadata[metadata_field]


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-b','--benchmarks',help='Allowed benchmarks', required=False, default='all', nargs="+")
    parser.add_argument('-v','--versions',help='Allowed versions', required=False, default='all', nargs="+")
    parser.add_argument('-z','--zones',help='Allowed zones', required=False, default='total_area', nargs="+")
    parser.add_argument('-o','--metrics-output-csv',help='File path to outputs csv', required=False, default=None)

    args = vars(parser.parse_args())

    Consolidate_metrics(**args)
