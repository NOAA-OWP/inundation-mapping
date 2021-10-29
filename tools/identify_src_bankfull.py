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
import datetime as dt
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

def src_bankfull_lookup(args):

    src_full_filename           = args[0]
    src_modify_filename         = args[1]
    df_bflows                   = args[2]
    huc                         = args[3]
    src_plot_option             = args[4]
    huc_output_dir              = args[5]

    ## Read the src_full_crosswalked.csv
    #print('Processing: ' + str(huc))
    log_text = 'Calculating: ' + str(huc) + '\n'
    df_src = pd.read_csv(src_full_filename,dtype={'HydroID': int,'feature_id': int})

    ## NWM recurr rename discharge var
    df_bflows = df_bflows.rename(columns={'discharge':'discharge_1_5'})

    ## Combine the nwm 1.5yr flows into the SRC via feature_id
    df_src = df_src.merge(df_bflows,how='left',on='feature_id')

    ## Check if there are any missing data, negative or zero flow values in the discharge_1_5
    check_null = df_src['discharge_1_5'].isnull().sum()
    if check_null > 0:
        log_text += 'Missing feature_id in crosswalk for huc: ' + str(huc) + ' --> these featureids will be ignored in bankfull calcs (~' + str(check_null/84) +  ' features) \n'
        ## Fill missing/nan nwm discharge_1_5 values with -999 to handle later
        df_src['discharge_1_5'] = df_src['discharge_1_5'].fillna(-999)
    negative_flows = len(df_src.loc[(df_src.discharge_1_5 <= 0) & (df_src.discharge_1_5 != -999)])
    if negative_flows > 0:
        log_text += 'HUC: ' + str(huc) + ' --> Negative or zero flow values found (likely lakeid loc)\n'

    ## Define the channel geometry variable names to use from the src
    hradius_var = 'HydraulicRadius (m)'
    volume_var = 'Volume (m3)'

    ## Locate the closest SRC discharge value to the NWM 1.5yr flow
    df_src['Q_1_5_find'] = (df_src['discharge_1_5'] - df_src['Discharge (m3s-1)']).abs()

    ## Check for any missing/null entries in the input SRC
    if df_src['Q_1_5_find'].isnull().values.any(): # there may be null values for lake or coastal flow lines (need to set a value to do groupby idxmin below)
        log_text += 'HUC: ' + str(huc) + ' --> Null values found in "Q_1_5_find" calc. These will be filled with 999999 () \n'
        ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
        df_src['Q_1_5_find'] = df_src['Q_1_5_find'].fillna(999999)
    if df_src['HydroID'].isnull().values.any():
        log_text += 'HUC: ' + str(huc) + ' --> Null values found in "HydroID"... \n'

    df_1_5 = df_src[['Stage','HydroID',volume_var,hradius_var,'Q_1_5_find']] # create new subset df to perform the Q_1_5 lookup
    df_1_5 = df_1_5[df_1_5['Stage'] > 0.0] # Ensure bankfull stage is greater than stage=0
    df_1_5.reset_index(drop=True, inplace=True)
    df_1_5 = df_1_5.loc[df_1_5.groupby('HydroID')['Q_1_5_find'].idxmin()].reset_index(drop=True) # find the index of the Q_1_5_find (closest matching flow)
    df_1_5 = df_1_5.rename(columns={'Stage':'Stage_1_5',volume_var:'Volume_bankfull',hradius_var:'HRadius_bankfull'}) # rename volume to use later for channel portion calc
    df_src = df_src.merge(df_1_5[['Stage_1_5','HydroID','Volume_bankfull','HRadius_bankfull']],how='left',on='HydroID')
    df_src.drop(['Q_1_5_find'], axis=1, inplace=True)

    ## Calculate the channel portion of bankfull Volume
    df_src['chann_volume_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
    df_src['chann_volume_ratio'].where(df_src['Stage'] == 0, df_src['Volume_bankfull'] / (df_src[volume_var]),inplace=True)
    #df_src['chann_volume_ratio'] = df_src['chann_volume_ratio'].clip_upper(1.0)
    df_src['chann_volume_ratio'].where(df_src['chann_volume_ratio'] <= 1.0, 1.0, inplace=True) # set > 1.0 ratio values to 1.0 (these are within the channel)
    df_src['chann_volume_ratio'].where(df_src['discharge_1_5'] > 0.0, 0.0, inplace=True) # if the discharge_1_5 value <= 0 then set channel ratio to 0 (will use global overbank manning n)
    #df_src.drop(['Volume_bankfull'], axis=1, inplace=True)

    ## Calculate the channel portion of bankfull Hydraulic Radius
    df_src['chann_hradius_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
    df_src['chann_hradius_ratio'].where(df_src['Stage'] == 0, df_src['HRadius_bankfull'] / (df_src[hradius_var]),inplace=True)
    #df_src['chann_hradius_ratio'] = df_src['HRadius_bankfull'] / (df_src[hradius_var]+.0001) # old adding 0.01 to avoid dividing by 0 at stage=0
    df_src['chann_hradius_ratio'].where(df_src['chann_hradius_ratio'] <= 1.0, 1.0, inplace=True) # set > 1.0 ratio values to 1.0 (these are within the channel)
    df_src['chann_hradius_ratio'].where(df_src['discharge_1_5'] > 0.0, 0.0, inplace=True) # if the discharge_1_5 value <= 0 then set channel ratio to 0 (will use global overbank manning n)
    #df_src.drop(['HRadius_bankfull'], axis=1, inplace=True)

    ## mask bankfull variables when the 1.5yr flow value is <= 0
    df_src['Stage_1_5'].mask(df_src['discharge_1_5'] <= 0.0,inplace=True)

    ## Create a new column to identify channel/floodplain via the bankfull stage value
    df_src.loc[df_src['Stage'] <= df_src['Stage_1_5'], 'channel_fplain_1_5'] = 'channel'
    df_src.loc[df_src['Stage'] > df_src['Stage_1_5'], 'channel_fplain_1_5'] = 'floodplain'
    df_src['channel_fplain_1_5'] = df_src['channel_fplain_1_5'].fillna('channel')

    ## Output new SRC with bankfull column
    df_src.to_csv(src_modify_filename,index=False)
    log_text += 'Completed: ' + str(huc)

    ## plot rating curves (optional arg)
    if src_plot_option == 'True':
        if isdir(huc_output_dir) == False:
            os.mkdir(huc_output_dir)
        generate_src_plot(df_src, huc_output_dir)

    return(log_text)

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

def multi_process(src_bankfull_lookup, procs_list):
    ## Initiate multiprocessing
    print(f"Identifying bankfull stage for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        map_output = pool.map(src_bankfull_lookup, procs_list)
    log_file.writelines(["%s\n" % item  for item in map_output])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Identify bankfull stage for each hydroid synthetic rating curve')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-flows','--bankfull-flow-input',help='NWM recurrence flows dir',required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-plots','--src-plot-option',help='Optional (True or False): use this flag to create src plots for all hydroids. WARNING - long runtime',required=False,default='False',type=str)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bankfull_flow_filepath = args['bankfull_flow_input']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    procs_list = []

    ## Print message to user and initiate run clock
    print('Writing progress to log file here: ' + str(join(fim_dir,'bankfull_detect.log')))
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Check that the bankfull flow filepath exists and read to dataframe
    if not isfile(bankfull_flow_filepath):
        print('!!! Can not find the input recurr flow file: ' + str(bankfull_flow_filepath))
    else:
        df_bflows = pd.read_csv(bankfull_flow_filepath,dtype={'feature_id': int})
        huc_list  = os.listdir(fim_dir)
        huc_pass_list = []
        for huc in huc_list:
            if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
                src_barc_full_filename = join(fim_dir,huc,'src_full_crosswalked_BARC.csv')
                src_orig_full_filename = join(fim_dir,huc,'src_full_crosswalked.csv')
                src_modify_filename = join(fim_dir,huc,'src_full_crosswalked_bankfull.csv')
                huc_output_dir = join(fim_dir,huc,'src_plots')
                ## check if BARC modified src_full_crosswalked_BARC.csv exists otherwise use the orginial src_full_crosswalked.csv
                if isfile(src_barc_full_filename):
                    print(str(huc))
                    huc_pass_list.append(str(huc) + " --> src_full_crosswalked_BARC.csv")
                    procs_list.append([src_barc_full_filename, src_modify_filename, df_bflows, huc, src_plot_option, huc_output_dir])
                elif isfile(src_orig_full_filename):
                    print(str(huc))
                    huc_pass_list.append(str(huc) + " --> src_full_crosswalked.csv")
                    procs_list.append([src_orig_full_filename, src_modify_filename, df_bflows, huc, src_plot_option, huc_output_dir])
                else:
                    print(str(huc) + 'WARNING --> can not find the SRC crosswalked csv file in the fim output dir: ' + str(join(fim_dir,huc)) + '\n')

        ## initiate log file
        print(f"Identifying bankfull stage for {len(procs_list)} hucs using {number_of_jobs} jobs")
        sys.__stdout__ = sys.stdout
        log_file = open(join(fim_dir,'logs','log_bankfull_indentify.log'),"w")
        sys.stdout = log_file
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.writelines(["%s\n" % item  for item in huc_pass_list])
        log_file.write('#########################################################\n\n')

        ## Pass huc procs_list to multiprocessing function
        multi_process(src_bankfull_lookup, procs_list)

        ## Record run time and close log file
        end_time = dt.datetime.now()
        log_file.write('END TIME: ' + str(end_time) + '\n')
        tot_run_time = end_time - begin_time
        log_file.write(str(tot_run_time))
        sys.stdout = sys.__stdout__
        log_file.close()
