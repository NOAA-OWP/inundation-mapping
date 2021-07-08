#!/usr/bin/env python3

import os
import sys
import pandas as pd
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from functools import reduce
from multiprocessing import Pool
from os.path import isfile, join, dirname, isdir
import shutil
import warnings
from pathlib import Path
sns.set_theme(style="whitegrid")
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Identify the SRC bankfull stage values using the NWM 1.5yr flows

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    nwm_flow_dir : str
        Directory containing NWM recurrence flows files.
    number_of_jobs : str
        Number of jobs.
    plots : str
        Flag to create SRC plots for all hydroids (True/False)
"""

def nwm_1_5_bankfull_lookup(args):

    src_full_filename           = args[0]
    src_modify_filename         = args[1]
    nwm_flow_dir                = args[2]
    huc                         = args[3]
    src_plot_option             = args[4]
    huc_output_dir              = args[5]

    # Read the src_full_crosswalked.csv
    #print('Processing: ' + str(huc))
    log_file.write('Calculating: ' + str(huc) + '\n')
    df_src = pd.read_csv(src_full_filename,dtype={'HydroID': str})

    # NWM recurr intervals
    recurr_1_5_yr_filename = join(nwm_flow_dir,'recurr_1_5_cms.csv')
    df_nwm15 = pd.read_csv(recurr_1_5_yr_filename)
    df_nwm15 = df_nwm15.rename(columns={'discharge':'discharge_1_5'})

    # Combine the nwm 1.5yr flows into the SRC via feature_id
    df_src = df_src.merge(df_nwm15,how='left',on='feature_id')

    # Check if there are any missing data in the discharge_1_5
    check_null = df_src['discharge_1_5'].isnull().sum()
    if check_null > 0:
        log_file.write('Missing feature_id in crosswalk for' + str(huc) + ' --> missing entries= ' + str(check_null) + '\n')

    # Locate the closest SRC discharge value to the NWM 1.5yr flow
    df_src['Q_1_5_find'] = (df_src['discharge_1_5'] - df_src['Discharge (m3s-1)']).abs()
    df_1_5 = df_src[['Stage','HydroID','Volume (m3)','HydraulicRadius (m)','Q_1_5_find']]
    df_1_5 = df_1_5[df_1_5['Stage'] > 0.0] # Ensure bankfull stage is greater than stage=0
    df_1_5 = df_1_5.loc[df_1_5.groupby('HydroID')['Q_1_5_find'].idxmin()].reset_index(drop=True)
    df_1_5 = df_1_5.rename(columns={'Stage':'Stage_1_5','Volume (m3)':'Volume_bankfull','HydraulicRadius (m)':'HRadius_bankfull'}) # rename volume to use later for channel portion calc
    df_src = df_src.merge(df_1_5[['Stage_1_5','HydroID','Volume_bankfull','HRadius_bankfull']],how='left',on='HydroID')
    df_src.drop(['Q_1_5_find'], axis=1, inplace=True)

    # Calculate the channel portion of bankfull Volume
    df_src['chann_volume_ratio'] = df_src['Volume_bankfull'] / (df_src['Volume (m3)']+.01) # adding 0.01 to avoid dividing by 0 at stage=0
    #df_src['chann_volume_ratio'] = df_src['chann_volume_ratio'].clip_upper(1.0)
    df_src['chann_volume_ratio'].where(df_src['chann_volume_ratio'] <= 1.0, 1.0, inplace=True)
    #df_src.drop(['Volume_bankfull'], axis=1, inplace=True)

    # Calculate the channel portion of bankfull Hydraulic Radius
    df_src['chann_hradius_ratio'] = df_src['HRadius_bankfull'] / (df_src['HydraulicRadius (m)']+.01) # adding 0.01 to avoid dividing by 0 at stage=0
    df_src['chann_hradius_ratio'].where(df_src['chann_hradius_ratio'] <= 1.0, 1.0, inplace=True)
    #df_src.drop(['HRadius_bankfull'], axis=1, inplace=True)

    # Create a new column to identify channel/floodplain via the bankfull stage value
    df_src.loc[df_src['Stage'] <= df_src['Stage_1_5'], 'channel_fplain_1_5'] = 'channel'
    df_src.loc[df_src['Stage'] > df_src['Stage_1_5'], 'channel_fplain_1_5'] = 'floodplain'

    # Output new SRC with bankfull column
    df_src.to_csv(src_modify_filename,index=False)
    log_file.write('Completed: ' + str(huc) + '\n')

    # plot rating curves
    if src_plot_option == 'True':
        if isdir(huc_output_dir) == False:
            os.mkdir(huc_output_dir)
        generate_src_plot(df_src, huc_output_dir)


def generate_src_plot(df_src, plt_out_dir):

    ## create list of unique hydroids
    hydroids = df_src.HydroID.unique().tolist()
    #hydroids = [17820017]

    for hydroid in hydroids:
        print("Creating SRC plot: " + str(hydroid))
        plot_df = df_src.loc[df_src['HydroID'] == hydroid]

        f, ax = plt.subplots(figsize=(6.5, 6.5))
        ax.set_title(str(hydroid))
        sns.despine(f, left=True, bottom=True)
        #sns.scatterplot(x='Discharge (m3s-1)', y='Stage', data=plot_df, ax=ax)
        #sns.lineplot(x='Discharge (m3s-1)', y='Stage_1_5', data=plot_df, color='green', ax=ax)
        #plt.fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_1_5'],alpha=0.5)
        #plt.text(plot_df['Discharge (m3s-1)'].median(), plot_df['Stage_1_5'].median(), "NWM 1.5yr: " + str(plot_df['Stage_1_5'].median()))
        sns.scatterplot(x='chann_volume_ratio', y='Stage', data=plot_df, ax=ax, label="chann_volume_ratio", s=38)
        sns.scatterplot(x='chann_hradius_ratio', y='Stage', data=plot_df, ax=ax, label="chann_hradius_ratio", s=12)
        ax.legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '.png',dpi=175, bbox_inches='tight')
        plt.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Identify bankfull stage for each hydroid synthetic rating curve')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-flows','--nwm-flow-dir',help='NWM recurrence flows dir',required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-plots','--src-plot-option',help='Optional (True or False): use this flag to create src plots for all hydroids. WARNING - long runtime',required=False,default='False',type=str)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    nwm_flow_dir = args['nwm_flow_dir']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    procs_list = []

    # Open log file
    print('Writing progress to log file here: ' + str(join(fim_dir,'bankfull_detect.log')))
    sys.__stdout__ = sys.stdout
    log_file = open(join(fim_dir,'bankfull_detect.log'),"w")
    sys.stdout = log_file

    huc_list  = os.listdir(fim_dir)
    for huc in huc_list:
        if huc != 'logs' and huc[-3:] != 'log':
            src_full_filename = join(fim_dir,huc,'src_full_crosswalked.csv')
            src_modify_filename = join(fim_dir,huc,'src_full_crosswalked_bankfull.csv')
            huc_output_dir = join(fim_dir,huc,'src_plots')

            if isfile(src_full_filename):
                procs_list.append([src_full_filename, src_modify_filename, nwm_flow_dir, huc, src_plot_option, huc_output_dir])
            else:
                log_file.write(str(huc) + ' --> can not find the src_full_crosswalked.csv in the fim output dir: ' + str(join(fim_dir,huc)) + '\n')


    # Initiate multiprocessing
    print(f"Identifying bankfull thresholds for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        pool.map(nwm_1_5_bankfull_lookup, procs_list)

    # Close log file
    sys.stdout = sys.__stdout__
    log_file.close()
