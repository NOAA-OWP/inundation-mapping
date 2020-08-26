#!/usr/bin/env python3

import pandas as pd
import os
import argparse

def calculate_objective_function(dir,outfile,mannings):

    # 100 year Obj Function
    dir_100= os.path.join(dir,'100yr')
    stats_100yr = pd.read_csv(os.path.join(dir_100,'stats_summary.csv'),index_col=0)
    dev_branch = os.path.basename(dir)
    objective_function_100 = float(stats_100yr.iloc[:,[-1]].loc[['FP_area_km2', 'FN_area_km2']].sum())
    fim_2_3_3_objective_function_100 = float(stats_100yr.loc[['FP_area_km2', 'FN_area_km2'],'fim_2_3_3'].sum())
    fim_1_0_0_objective_function_100 = float(stats_100yr.loc[['FP_area_km2', 'FN_area_km2'],'fim_1_0_0'].sum())

    # 500 year Obj Function
    dir_500= os.path.join(dir,'500yr')
    stats_500yr = pd.read_csv(os.path.join(dir_500,'stats_summary.csv'),index_col=0)
    objective_function_500 = float(stats_500yr.iloc[:,[-1]].loc[['FP_area_km2', 'FN_area_km2']].sum())
    fim_2_3_3_objective_function_500 = float(stats_500yr.loc[['FP_area_km2', 'FN_area_km2'],'fim_2_3_3'].sum())
    fim_1_0_0_objective_function_500 = float(stats_500yr.loc[['FP_area_km2', 'FN_area_km2'],'fim_1_0_0'].sum())

    dictionary={}
    for cnt,value in enumerate(mannings.split(",")):
        streamorder = cnt+1
        dictionary[str(streamorder)] = value


    dev_obj_func = objective_function_100+objective_function_500
    fim_2_3_3_obj_func = fim_2_3_3_objective_function_100+fim_2_3_3_objective_function_500
    fim_1_0_0_obj_func = fim_1_0_0_objective_function_100+fim_1_0_0_objective_function_500

    with open(outfile, "w+") as f:
        f.write(str(dev_branch) + ' Objective Function: ' + str(dev_obj_func) + '\n' + 'Based on mannings parameter values:' + '\n' +
        str(dictionary) + '\n' +
        'fim_2_3_3 Objective Function: ' + str(fim_2_3_3_obj_func) + '\n' +
        'fim_1_0_0 Objective Function: ' + str(fim_1_0_0_obj_func))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Calculate Objective function for single BLE')
    parser.add_argument('-d','--dir', help='directory of stat table', required=True,type=str)
    parser.add_argument('-o','--outfile', help='objective funtion output file', required=True,type=str)
    parser.add_argument('-p','--mannings', help='mannings parameter set', required=True,type=str)

    args = vars(parser.parse_args())

    dir = args['dir']
    outfile = args['outfile']
    mannings = args['mannings']

    calculate_objective_function(dir,outfile,mannings)
