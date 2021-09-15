#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import os
from glob import iglob

def Reset_mannings(hydrofabric_dir,mannings_value,overwrite_files=False):

    src_table_filePaths, hydro_table_filePaths = make_file_paths_for_tables(hydrofabric_dir)
    
    for srcFP,hydFP in zip(src_table_filePaths,hydro_table_filePaths):

        src_table = load_src_table(srcFP)
        hydro_table = load_hydro_table(hydFP)
        
        src_table, hydro_table = reset_mannings_for_a_processing_unit(src_table,hydro_table,mannings_value)
        
        if overwrite_files:
            src_table.to_csv(srcFP,index=False)
            hydro_table.to_csv(hydFP,index=False)

        #yield(src_table, hydro_table)


def load_hydro_table(hydro_table_filePath):

    hydro_table = pd.read_csv( hydro_table_filePath, 
                               dtype= { 'HydroID' : str,
                                        'feature_id' : str,
                                        'stage' : float,
                                        'discharge_cms': float,
                                        'HUC' : str,
                                        'LakeID' : str 
                                       }
                             )

    return(hydro_table)


def load_src_table(src_table_filePath):

    src_table = pd.read_csv( src_table_filePath, 
                             dtype= { 'HydroID' : str,
                                      'feature_id' : str,
                                      'stage' : float,
                                      'discharge_cms': float,
                                      'HUC' : str,
                                      'LakeID' : str 
                                     }
                           )

    return(src_table)


def make_file_paths_for_tables(hydrofabric_dir):

    src_table_filePath_to_glob = os.path.join(hydrofabric_dir,'**','src_full_crosswalked*.csv')
    hydro_table_filePath_to_glob = os.path.join(hydrofabric_dir,'**','hydroTable*.csv')

    src_table_filePaths = iglob(src_table_filePath_to_glob,recursive=True)
    hydro_table_filePaths = iglob(hydro_table_filePath_to_glob,recursive=True)

    return(src_table_filePaths,hydro_table_filePaths)


def reset_mannings_for_a_processing_unit(src_table,hydro_table,mannings_value):

    src_table = override_mannings(src_table,mannings_value)

    src_table = calculate_discharge(src_table)

    hydro_table["discharge_cms"] = src_table["Discharge (m3s-1)"]

    return(src_table,hydro_table)


def override_mannings(table,mannings_value,mannings_attribute="ManningN"):

    table[mannings_attribute] = mannings_value

    return(table)


def calculate_discharge(src_table):

    src_table['Discharge (m3s-1)'] = src_table['WetArea (m2)']* \
                                     pow(src_table['HydraulicRadius (m)'],2.0/3)* \
                                     pow(src_table['SLOPE'],0.5)/src_table['ManningN']
    
    return(src_table)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Overwrites mannings n values and recomputes discharge values for SRCs and Hydro-Tables')
    parser.add_argument('-y','--hydrofabric-dir', help='Hydrofabric directory', required=True)
    parser.add_argument('-n','--mannings-value', help='Mannings N value to use', required=True, type=float)
    parser.add_argument('-o','--overwrite-files', help='Overwrites original files if used', required=False, default=False,action='store_true')
    
    
    args = vars(parser.parse_args())

    Reset_mannings(**args)
