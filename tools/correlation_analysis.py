import numpy as np
import pandas as pd
import statistics
from scipy import stats
import scipy
import matplotlib.pyplot as plt
import os
import re
import geopandas as gpd
import argparse
import shutil


#############################################
#Creates needed folders based off of input parent directory. 
#Outputs contain string of paths for those output directories.
#############################################
def create_needed_folders(parent_dir, separate_y_n):
    path_huc4 = os.path.join(parent_dir, "huc_4_groups")
    path_outputs = os.path.join(parent_dir, "analysis_outputs")

    if separate_y_n == "yes":
        if os.path.exists(path_huc4):
            shutil.rmtree(path_huc4,ignore_errors=True)
        if not os.path.exists(path_huc4):
            os.makedirs(path_huc4)

        if os.path.exists(path_outputs):
            shutil.rmtree(path_outputs,ignore_errors=True)
        if not os.path.exists(path_outputs):
            os.makedirs(path_outputs) 
        return path_huc4, path_outputs
    
    if separate_y_n == "no":
        if os.path.exists(path_outputs):
            shutil.rmtree(path_outputs,ignore_errors=True)
        if not os.path.exists(path_outputs):
            os.makedirs(path_outputs) 
        return path_huc4, path_outputs


#############################################
#Reads in the csv and returns a df.
#Rigid format will only succeed on output from collate_catchment_attributes.py
#############################################
def read_in_and_filter_dataframe(input_csv):
    df = pd.read_csv(input_csv,dtype ={'HUC8':str})
    return df   
    
        
#############################################
#Correlation matrix is main method for determining sigle variable correlation
#Will output to folder designated in create_needed_folders
#############################################
def get_correlation_matricies(input_df,path_outputs):
    
    csv_df = input_df
    csv_df = csv_df.dropna(axis=0)
    correlation_matrix = csv_df.corr()
    correlation_matrix.to_csv(os.path.join(path_outputs, "correlation_matricies"), index=False)
    
    return correlation_matrix


#############################################
#Takes all rows from collate output, and assembles rows with same huc4 into individual csv's
#############################################
def separate_into_huc4(csv_df,out_folder):    
    huc4_list = []
    col_list = csv_df.columns
    
    for index, row in csv_df.iterrows():        #make list of all unique huc4's
        huc8 = row['HUC8']
        huc4 = huc8[0:4]
        if huc4 not in huc4_list:
            huc4_list.append(huc4)
        
    for item in huc4_list:                      #make dataframe for each unique huc4
        df_name = item + "_" + "huc4_df"
        df_rows = pd.DataFrame(columns = col_list)
        for index, row in csv_df.iterrows():
            huc8 = row['HUC8']
            huc4 = huc8[0:4]
            
            if huc4 == item:            
                df_rows = df_rows.append(row)
        pathstring = out_folder +"/" + str(df_name)+".csv"
        print(pathstring)
        df_rows.to_csv(pathstring, index=True)


#############################################
#takes each csv in huc4 folder, runs correlation analysis, creates histogram of all returned correlations
#############################################
def bin_error_huc4(huc_4_groups,out_folder, variable_choice):
    p_kt_list = []
    kt_list = []
    root_dir = huc_4_groups
    for huc4 in os.listdir(root_dir):
        if not huc4.startswith('.'):
            csv_data = pd.read_csv(os.path.join(root_dir, huc4))
            csv_data.dropna(axis=0)
            
            if len(csv_data) > 3:
                nmrse_list = csv_data['nrmse'].tolist()
                var_list = csv_data[variable_choice].tolist()

                corrkt = stats.kendalltau(var_list, nmrse_list)             #run kendall tau on each huc4, add correlation to list. 
                p_kt_list.append(corrkt[1])
                kt_list.append(corrkt[0])
    fig = plt.figure(figsize =(10, 7))
 
    plt.hist(kt_list, bins = [-1,-.9,-.8,-.7,-.6,-.5,-.4,-.3,-.2,-.1,0, .1, .2, .3,.4,.5,.6,.7,.8,.9,1])
    plt.title("Kendall Tau Histogram, grouped by HUC4 error vs "+variable_choice)
       
    path = out_folder + '/huc4_histograms/'     
    if not os.path.exists(path):    
        print(path)
        os.makedirs(path)
    fig.savefig(out_folder + '/huc4_histograms/' + variable_choice + '.png')



if __name__ == '__main__':
    """
    correlation analysis.py takes output from collate tool and performs single variable analysis
    current recommended input: "/data/temp/caleb/master_data/ms_all_gauges_nlcd.csv"
    
    command to run using Caleb's file directories:
    python correlation_analysis.py -p "/data/temp/caleb" -in "/data/temp/caleb/master_data/ms_all_gauges_nlcd.csv" -var "lulc_2" -sep "yes"

    Each time the code runs with separate option set to yes, the histogram output will be deleted. Be sure to grab that file before running again. 

    TODO
    """

    parser = argparse.ArgumentParser(description='performs single variable analysis on collated attributes')
    
    parser.add_argument('-in','--input-csv',help='csv containing collated variables.',required=True)
    parser.add_argument('-p', '--parent-dir', help='path to where user wants output and working directories to be created',required=True)
    parser.add_argument('-var', '--variable-choice', help='choose which variable to test in huc4 histograms, must be spelled the same way as in csv',required=True)
    parser.add_argument('-sep', '--seperate-y-n', help='perform separation by huc4 yes or no',required=True)

    args = vars(parser.parse_args())

    print("hi there") 

    input_csv = args['input_csv']
    parent_dir = args['parent_dir']
    variable_choice = args['variable_choice']
    seperate_y_n = args['seperate_y_n']
        

path_list = create_needed_folders(parent_dir)
path_huc4 = path_list[0]
path_outputs = path_list[1]

input_df = read_in_and_filter_dataframe(input_csv)
get_correlation_matricies(input_df,path_outputs)

if seperate_y_n == "yes":
    separate_into_huc4(input_df,path_huc4)

bin_error_huc4(path_huc4,path_outputs, variable_choice)

