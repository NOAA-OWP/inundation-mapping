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


def get_correlation_matricies(input_csv,out_folder):
    csv_df = pd.read_csv(input_csv,dtype = {'HUC8':str})
    csv_df = csv_df.dropna(axis=0)
    correlation_matrix = csv_df.corr()
    correlation_matrix.to_csv(os.path.join(out_folder, input_csv), index=False)
    return csv_df

def get_scatter_plots(csv_df, out_folder):
    for col in csv_df.columns:
        col_str = str(col)
        col_list = col_str + "_list"
        col_list = csv_df[col_str].tolist()

    for col in csv_df.columns:
        if col != "nrmse_list":
            fig, ax = plt.subplots()
            ax.scatter(col_list, nrmse_list)
            plt.title(col_str + "scatter")
            plt.xlabel(col_str)
            plt.ylabel("nrmse")
            fig.savefig(out_folder + '/scatter_plots/' + col_str + '.png')

def separate_into_huc4(csv_df,out_folder):
    
    huc4_list = []
    col_list = csv_df.columns

    #make list of all unique huc4's
    for index, row in csv_df.iterrows():
        huc8 = row['HUC8']
        huc4 = huc8[0:4]
        if huc4 not in huc4_list:
            huc4_list.append(huc4)
        
        
    #make dataframe for each unique huc4

    for item in huc4_list:
        df_name = item + "_" + "huc4_df"
        df_rows = pd.DataFrame(columns = col_list)
        for index, row in ms_filter_gauges.iterrows():
            huc8 = row['HUC8']
            huc4 = huc8[0:4]
            
            if huc4 == item:            
                df_rows = df_rows.append(row)
        df_rows.to_csv(out_folder +'/huc4_folder/'+ str(df_name)+".csv", index=True)

def bin_error_huc4(out_folder,variable_choice):
    p_kt_list = []
    kt_list = []
    root_dir = out_folder +'/huc4_folder'
    for huc4 in os.listdir(root_dir):
        if not huc4.startswith('.'):
            
            #run kendall tau on each huc4, add correlation to list. 
            
            csv_data = pd.read_csv(os.path.join(root_dir, huc4))
            csv_data.dropna(axis=0)
            
            if len(csv_data) > 3:
                nmrse_list = csv_data['nrmse'].tolist()
                lulc1_list = csv_data['lulc_2'].tolist()

                corrkt = stats.kendalltau(lulc1_list, nmrse_list)
                p_kt_list.append(corrkt[1])
                kt_list.append(corrkt[0])
    fig = plt.figure(figsize =(10, 7))
 
    plt.hist(kt_list, bins = [-1,-.9,-.8,-.7,-.6,-.5,-.4,-.3,-.2,-.1,0, .1, .2, .3,.4,.5,.6,.7,.8,.9,1])
    plt.title("Kendall Tau Histogram, grouped by HUC4 error vs "+variable_choice)
    
    # show plot
    fig.savefig(out_folder + '/huc4_histograms/' + variable_choice + '.png')


if __name__ == '__main__':
    """
    correlation analysis.py takes output from collat tool and performs single variable analysis
    
    recommended input: "/data/temp/caleb/master_data/ms_filter_gauges_nlcd.csv"
    
    TODO
    """

    parser = argparse.ArgumentParser(description='performs single variable analysis on collated attributes')
    
    parser.add_argument('-in','--input-csv',help='Csv containing collated variables.',required=True)
    parser.add_argument('-out', '--out-folder', help='folder to hold output graphs and charts',required=True)
    parser.add_argument('-var', '--variable-choice', help='choose which variable to test in huc4 histograms',required=True)
    parser.add_argument('-sep', '--seperate-y-n', help='perform separation by huc4 yes or no',required=True)

    args = vars(parser.parse_args())

    input_csv = args['input_csv']
    out_folder = args['out_folder']
    variable_choice = args['variable_choice']
    seperate_y_n = args['seperate_y_n']
        

csv_df = get_correlation_matricies(input_csv,out_folder)
get_scatter_plots(csv_df, out_folder)
if seperate_y_n = "yes":
    separate_into_huc4(csv_df,out_folder)
    
bin_error_huc4(out_folder,variable_choice)