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
    Vary the Manning's n values for in-channel vs. floodplain

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    channel_ratio_src_column : str
        SRC attribute containing the channel vs. floodplain attribute
    mann_n_table : str
        Path to a csv file containing Manning's n values by feature_id
    hydrotable_suffix : str
        Suffix to append to the new hydroTable csv file
    number_of_jobs : str
        Number of jobs.
    src_plot_option : str
        Optional (True or False): use this flag to crate src plots for all hydroids
"""

def variable_mannings_calc(args):

    in_src_bankfull_filename    = args[0]
    channel_ratio_src_column    = args[1]
    mann_n_table                = args[2]
    huc                         = args[3]
    out_src_bankfull_filename   = args[4]
    htable_filename             = args[5]
    new_htable_filename         = args[6]
    src_plot_option             = args[7]
    huc_output_dir              = args[8]

    # Read the src_full_crosswalked.csv
    print('Processingg: ' + str(huc))
    df_src = pd.read_csv(in_src_bankfull_filename,dtype={'feature_id': str})

    # Check that the channel ratio column the user specified exists in the def
    if channel_ratio_src_column not in df_src.columns:
        print(out_src_bankfull_filename + ' does not contain the specified channel ratio column: ' + channel_ratio_src_column)
        print(df_src.columns)

    # Read the Manning's n csv (must contain feature_id, channel mannings, floodplain mannings)
    df_mann = pd.read_csv(mann_n_table,dtype={'feature_id': str})

    # Merge (crosswalk) the df of Manning's n with the SRC df (using the channel/fplain delination in the channel_ratio_src_column)
    df_src = df_src.merge(df_mann,  how='left', on='feature_id')
    check_null = df_src['channel_n'].isnull().sum() + df_src['overbank_n'].isnull().sum()
    if check_null > 0:
        print('Null feature_ids in crosswalk for huc: ' + str(huc) + ' --> missing entries= ' + str(check_null/84))

    # Calculate composite Manning's n using the channel geometry ratio attribute given by user (e.g. chann_hradius_ratio or chann_vol_ratio)
    df_src['comp_ManningN'] = (df_src[channel_ratio_src_column]*df_src['channel_n']) + ((1.0 - df_src[channel_ratio_src_column])*df_src['overbank_n'])
    print('Done calculating composite Manning n (' + channel_ratio_src_column + '): ' + str(huc))

    # Check if there are any missing data in the composite ManningN column
    check_null = df_src['comp_ManningN'].isnull().sum()
    if check_null > 0:
        print('!!!!Missing values in the var_ManningN merge' + str(huc) + ' --> missing entries= ' + str(check_null))

    # Check which channel geometry parameters exist in the src_df (use bathy adjusted vars by default) --> this is needed to handle differences btw BARC & no-BARC outputs
    if 'HydraulicRadius (m)_bathy_adj' in df_src:
        hydr_radius = 'HydraulicRadius (m)_bathy_adj'
    else:
        hydr_radius = 'HydraulicRadius (m)'

    if 'WetArea (m2)_bathy_adj' in df_src:
        wet_area = 'WetArea (m2)_bathy_adj'
    else:
        wet_area = 'WetArea (m2)'

    ## Calculate Q using Manning's equation
    #df_src.rename(columns={'Discharge (m3s-1)'}, inplace=True) # rename the previous Discharge column
    df_src['Discharge (m3s-1)_varMann'] = df_src[wet_area]* \
    pow(df_src[hydr_radius],2.0/3)* \
    pow(df_src['SLOPE'],0.5)/df_src['comp_ManningN']

    ## Set Q values to 0 and -999 for specified criteria
    df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] == 0,0,inplace=True)
    if 'Thalweg_burn_elev' in df_src:
        df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] == df_src['Thalweg_burn_elev'],0,inplace=True)
        df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] < df_src['Thalweg_burn_elev'],-999,inplace=True)

    # Output new SRC with bankfull column
    df_src.to_csv(out_src_bankfull_filename,index=False)

    # Output new hydroTable with new discharge_1_5
    df_src_trim = df_src[['HydroID','Stage','Discharge (m3s-1)_varMann','comp_ManningN']]
    df_src_trim = df_src_trim.rename(columns={'Stage':'stage','Discharge (m3s-1)_varMann': 'discharge_cms','comp_ManningN':'ManningN'})
    df_htable = pd.read_csv(htable_filename,dtype={'HUC': str})
    df_htable.drop(['discharge_cms','ManningN'], axis=1, inplace=True) # drop the original discharge column to be replaced with updated version
    df_htable = df_htable.merge(df_src_trim, how='left', left_on=['HydroID','stage'], right_on=['HydroID','stage'])
    #extra_cols = ['HydraulicRadius (m)','WetArea (m2)','SLOPE','ManningN']
    #if df_htable.columns.isin(cols).all()):
    #    df_htable = df_htable[['HydroID','feature_id','stage','discharge_cms','HydraulicRadius (m)','WetArea (m2)','SLOPE','ManningN','HUC','LakeID']]
    #else:
    #    df_htable = df_htable[['HydroID','feature_id','stage','discharge_cms','HUC','LakeID']] # set column order for hydroTable output
    df_htable.to_csv(new_htable_filename,index=False)
    print('Output new files: ' + str(huc))

    # plot rating curves
    if src_plot_option == 'True':
        if isdir(huc_output_dir) == False:
            os.mkdir(huc_output_dir)
        generate_src_plot(df_src, huc_output_dir)


def generate_src_plot(df_src, plt_out_dir):

    ## create list of unique hydroids
    hydroids = df_src.HydroID.unique().tolist()

    ## plot each hydroid SRC in the huc
    for hydroid in hydroids:
        print("Creating SRC plot: " + str(hydroid))
        plot_df = df_src.loc[df_src['HydroID'] == hydroid]

        f, ax = plt.subplots(figsize=(6.5, 6.5))
        ax.set_title(str(hydroid))
        sns.despine(f, left=True, bottom=True)
        sns.scatterplot(x='Discharge (m3s-1)', y='Stage', data=plot_df, label="Orig SRC", ax=ax, color='blue')
        sns.scatterplot(x='Discharge (m3s-1)_varMann', y='Stage', data=plot_df, label="SRC w/ vMann", ax=ax, color='orange')
        sns.lineplot(x='Discharge (m3s-1)', y='Stage_1_5', data=plot_df, color='green', ax=ax)
        plt.fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_1_5'],alpha=0.5)
        plt.text(plot_df['Discharge (m3s-1)'].median(), plot_df['Stage_1_5'].median(), "NWM 1.5yr: " + str(plot_df['Stage_1_5'].median()))
        ax.legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '.png',dpi=175, bbox_inches='tight')
        plt.close()

#    for hydroid in hydroids:
#        print("Creating SRC plot: " + str(hydroid))
#        plot_df = df_src.loc[df_src['HydroID'] == hydroid]
#
#        f, ax = plt.subplots(figsize=(6.5, 6.5))
#        ax.set_title(str(hydroid))
#        sns.despine(f, left=True, bottom=True)
#        sns.scatterplot(x='comp_ManningN', y='Stage', data=plot_df, label="Orig SRC", ax=ax, color='blue')
#        #sns.scatterplot(x='Discharge (m3s-1)_varMann', y='Stage', data=plot_df, label="SRC w/ vMann", ax=ax, color='orange')
#        sns.lineplot(x='comp_ManningN', y='Stage_1_5', data=plot_df, color='green', ax=ax)
#        plt.fill_between(plot_df['comp_ManningN'], plot_df['Stage_1_5'],alpha=0.5)
#        plt.text(plot_df['comp_ManningN'].median(), plot_df['Stage_1_5'].median(), "NWM 1.5yr: " + str(plot_df['Stage_1_5'].median()))
#        ax.legend()
#        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '.png',dpi=175, bbox_inches='tight')
#        plt.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Vary the Manning's n values for in-channel vs. floodplain (recalculate Manning's eq for Discharge)")
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-bc','--channel-ratio-src-column',help='SRC attribute containing the channel vs. overbank geometry ratio (for composite calc)',required=False,type=str,default='chann_hradius_ratio')
    parser.add_argument('-mann','--mann-n-table',help="Path to a csv file containing Manning's n values by featureid",required=True,type=str)
    parser.add_argument('-suff','--hydrotable-suffix',help="Suffix to append to the new hydroTable csv file (e.g. '_global_0-6_0-11')",required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-plots','--src-plot-option',help='Optional (True or False): use this flag to create src plots for all hydroids. WARNING - long runtime',required=False,default='False',type=str)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    channel_ratio_src_column = args['channel_ratio_src_column']
    mann_n_table = args['mann_n_table']
    hydrotable_suffix = args['hydrotable_suffix']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    procs_list = []

    # check that manning's roughness table contains proper columns
    df_mann_check = pd.read_csv(mann_n_table)
    if 'channel_n' not in df_mann_check.columns or 'overbank_n' not in df_mann_check.columns or 'feature_id' not in df_mann_check.columns:
        print('Missing required data column ("feature_id","channel_n", and/or "overbank_n")!!! --> ' + mann_n_table)
    else:
        # Loop through hucs in the fim_dir and create list of variables to feed to multiprocessing
        huc_list  = os.listdir(fim_dir)
        for huc in huc_list:
            if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
                in_src_bankfull_filename = join(fim_dir,huc,'src_full_crosswalked_bankfull.csv')
                out_src_bankfull_filename = join(fim_dir,huc,'src_full_crosswalked_vmann.csv')
                htable_filename = join(fim_dir,huc,'hydroTable.csv')
                new_htable_filename = join(fim_dir,huc,'hydroTable' + hydrotable_suffix + '.csv')
                huc_plot_output_dir = join(fim_dir,huc,'src_plots')

                if isfile(in_src_bankfull_filename):
                    procs_list.append([in_src_bankfull_filename, channel_ratio_src_column, mann_n_table, huc, out_src_bankfull_filename, htable_filename, new_htable_filename, src_plot_option, huc_plot_output_dir])
                else:
                    print(str(huc) + ' --> can not find the src_full_crosswalked_bankfull.csv in the fim output dir: ' + str(join(fim_dir,huc)))

        # Initiate multiprocessing
        print(f"Applying variable Manning's n to SRC calcs for {len(procs_list)} hucs using {number_of_jobs} jobs")
        with Pool(processes=number_of_jobs) as pool:
            pool.map(variable_mannings_calc, procs_list)
