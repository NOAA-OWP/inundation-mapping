#!/usr/bin/env python3
import datetime as dt
import re
import os
import sys
import pandas as pd
import numpy as np
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import shutil
import traceback
import warnings
from functools import reduce
from multiprocessing import Pool
from os.path import isfile, join, dirname, isdir
from pathlib import Path
from tqdm import tqdm

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
    file_suffix : str
        Suffix to append to the output log file
    number_of_jobs : str
        Number of jobs.
    src_plot_option : str
        Optional (True or False): use this flag to crate src plots for all hydroids
"""


def variable_mannings_calc(args):
    in_src_bankfull_filename = args[0]
    channel_ratio_src_column = args[1]
    df_mann = args[2]
    huc = args[3]
    branch_id = args[4]
    htable_filename = args[5]
    output_suffix = args[6]
    src_plot_option = args[7]
    huc_output_dir = args[8]

    ## Read the src_full_crosswalked.csv
    print('Calculating variable roughness: ' + str(huc) + '  branch id: ' + str(branch_id))
    log_text = (
        'Calculating variable roughness: ' + str(huc) + '  branch id: ' + str(branch_id) + '\n'
    )
    df_src = pd.read_csv(in_src_bankfull_filename, dtype={'feature_id': 'int64'})

    ## Check that the channel ratio column the user specified exists in the def
    if channel_ratio_src_column not in df_src.columns:
        print(
            'WARNING --> '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + in_src_bankfull_filename
            + ' does not contain the specified channel ratio column: '
            + channel_ratio_src_column
        )
        print('Skipping --> ' + str(huc) + '  branch id: ' + str(branch_id))
        log_text += (
            'WARNING --> '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + in_src_bankfull_filename
            + ' does not contain the specified channel ratio column: '
            + channel_ratio_src_column
            + '\n'
        )
    else:
        try:
            if 'comp_ManningN' in df_src.columns:
                df_src.drop(
                    [
                        'channel_n',
                        'overbank_n',
                        'comp_ManningN',
                        'vmann_on',
                        'Discharge (m3s-1)_varMann',
                    ],
                    axis=1,
                    inplace=True,
                )  # drop these cols (in case vmann was previously performed)

            ## Merge (crosswalk) the df of Manning's n with the SRC df (using the channel/fplain delination in the channel_ratio_src_column)
            df_src = df_src.merge(df_mann, how='left', on='feature_id')
            check_null = df_src['channel_n'].isnull().sum() + df_src['overbank_n'].isnull().sum()
            if check_null > 0:
                log_text += (
                    str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' --> '
                    + 'Null feature_ids found in crosswalk btw roughness dataframe and src dataframe'
                    + ' --> missing entries= '
                    + str(check_null / 84)
                    + '\n'
                )

            ## Calculate composite Manning's n using the channel geometry ratio attribute given by user (e.g. chann_hradius_ratio or chann_vol_ratio)
            df_src['comp_ManningN'] = (df_src[channel_ratio_src_column] * df_src['channel_n']) + (
                (1.0 - df_src[channel_ratio_src_column]) * df_src['overbank_n']
            )
            # print('Done calculating composite Manning n (' + channel_ratio_src_column + '): ' + str(huc))

            ## Check if there are any missing data in the composite ManningN column
            check_null_comp = df_src['comp_ManningN'].isnull().sum()
            if check_null_comp > 0:
                log_text += (
                    str(huc)
                    + '  branch id: '
                    + str(branch_id)
                    + ' --> '
                    + 'Missing values in the comp_ManningN calculation'
                    + ' --> missing entries= '
                    + str(check_null_comp / 84)
                    + '\n'
                )
            df_src['vmann_on'] = np.where(
                df_src['comp_ManningN'].isnull(), False, True
            )  # create field to identify where vmann is applied (True=yes; False=no)

            ## Define the channel geometry variable names to use from the src
            hydr_radius = 'HydraulicRadius (m)'
            wet_area = 'WetArea (m2)'

            ## Calculate Q using Manning's equation
            # df_src.rename(columns={'Discharge (m3s-1)'}, inplace=True) # rename the previous Discharge column
            df_src['Discharge (m3s-1)_varMann'] = (
                df_src[wet_area]
                * pow(df_src[hydr_radius], 2.0 / 3)
                * pow(df_src['SLOPE'], 0.5)
                / df_src['comp_ManningN']
            )

            ## Set Q values to 0 and -999 for specified criteria (thalweg notch check happens in BARC)
            # df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] == 0,0,inplace=True)
            # if 'Thalweg_burn_elev' in df_src:
            #     df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] == df_src['Thalweg_burn_elev'],0,inplace=True)
            #     df_src['Discharge (m3s-1)_varMann'].mask(df_src['Stage'] < df_src['Thalweg_burn_elev'],-999,inplace=True)

            ## Use the default discharge column when vmann is not being applied
            df_src['Discharge (m3s-1)_varMann'] = np.where(
                df_src['vmann_on'] == False,
                df_src['Discharge (m3s-1)'],
                df_src['Discharge (m3s-1)_varMann'],
            )  # reset the discharge value back to the original if vmann=false
            df_src['comp_ManningN'] = np.where(
                df_src['vmann_on'] == False, df_src['ManningN'], df_src['comp_ManningN']
            )  # reset the ManningN value back to the original if vmann=false

            ## Output new SRC with bankfull column
            df_src.to_csv(in_src_bankfull_filename, index=False)

            ## Output new hydroTable with updated discharge and ManningN column
            df_src_trim = df_src[
                ['HydroID', 'Stage', 'vmann_on', 'comp_ManningN', 'Discharge (m3s-1)_varMann']
            ]
            df_src_trim = df_src_trim.rename(
                columns={
                    'Stage': 'stage',
                    'comp_ManningN': 'vmann_ManningN',
                    'Discharge (m3s-1)_varMann': 'vmann_discharge_cms',
                }
            )
            df_src_trim['ManningN'] = df_src_trim[
                'vmann_ManningN'
            ]  # create a copy of vmann modified ManningN (used to track future changes)
            df_src_trim['discharge_cms'] = df_src_trim[
                'vmann_discharge_cms'
            ]  # create a copy of vmann modified discharge (used to track future changes)
            df_htable = pd.read_csv(htable_filename, dtype={'HUC': str})

            ## Check if BARC ran
            # if not set(['orig_discharge_cms']).issubset(df_htable.columns):
            #     df_htable.rename(columns={'discharge_cms':'orig_discharge_cms'},inplace=True)
            #     df_htable.rename(columns={'ManningN':'orig_ManningN'},inplace=True)
            # else:

            ## drop the previously modified discharge column to be replaced with updated version
            df_htable.drop(
                ['vmann_on', 'discharge_cms', 'ManningN', 'vmann_discharge_cms', 'vmann_ManningN'],
                axis=1,
                inplace=True,
            )
            df_htable = df_htable.merge(
                df_src_trim, how='left', left_on=['HydroID', 'stage'], right_on=['HydroID', 'stage']
            )

            df_htable['vmann_on'] = np.where(
                df_htable['LakeID'] > 0, False, df_htable['vmann_on']
            )  # reset the ManningN value back to the original if vmann=false

            ## Output new hydroTable csv
            if output_suffix != "":
                htable_filename = os.path.splitext(htable_filename)[0] + output_suffix + '.csv'
            df_htable.to_csv(htable_filename, index=False)

            log_text += 'Completed: ' + str(huc)

            ## plot rating curves
            if src_plot_option:
                if isdir(huc_output_dir) == False:
                    os.mkdir(huc_output_dir)
                generate_src_plot(df_src, huc_output_dir)
        except Exception as ex:
            summary = traceback.StackSummary.extract(traceback.walk_stack(None))
            print(str(huc) + '  branch id: ' + str(branch_id) + " failed for some reason")
            print(f"*** {ex}")
            print(''.join(summary.format()))
            log_text += (
                'ERROR --> '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + " failed (details: "
                + (f"*** {ex}")
                + (''.join(summary.format()))
                + '\n'
            )

    return log_text


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
        sns.scatterplot(
            x='Discharge (m3s-1)', y='Stage', data=plot_df, label="Orig SRC", ax=ax, color='blue'
        )
        sns.scatterplot(
            x='Discharge (m3s-1)_varMann',
            y='Stage',
            data=plot_df,
            label="SRC w/ vMann",
            ax=ax,
            color='orange',
        )
        sns.lineplot(x='Discharge (m3s-1)', y='Stage_bankfull', data=plot_df, color='green', ax=ax)
        plt.fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_bankfull'], alpha=0.5)
        plt.text(
            plot_df['Discharge (m3s-1)'].median(),
            plot_df['Stage_bankfull'].median(),
            "NWM Bankfull Approx: " + str(plot_df['Stage_bankfull'].median()),
        )
        ax.legend()
        plt.savefig(
            plt_out_dir + os.sep + str(hydroid) + '_vmann.png', dpi=175, bbox_inches='tight'
        )
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


def multi_process(variable_mannings_calc, procs_list, log_file, verbose):
    ## Initiate multiprocessing
    print(
        f"Applying variable Manning's n to SRC calcs for {len(procs_list)} hucs using {number_of_jobs} jobs"
    )
    with Pool(processes=number_of_jobs) as pool:
        if verbose:
            map_output = tqdm(pool.imap(variable_mannings_calc, procs_list), total=len(procs_list))
            tuple(map_output)  # fetch the lazy results
        else:
            map_output = pool.map(variable_mannings_calc, procs_list)
    log_file.writelines(["%s\n" % item for item in map_output])


def run_prep(
    fim_dir,
    channel_ratio_src_column,
    mann_n_table,
    output_suffix,
    number_of_jobs,
    verbose,
    src_plot_option,
):
    procs_list = []

    print(
        'Writing progress to log file here: '
        + str(join(fim_dir, 'log_composite_n' + output_suffix + '.log'))
    )
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## initiate log file
    log_file = open(join(fim_dir, 'logs', 'log_composite_n' + output_suffix + '.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    ## Check that the input fim_dir exists
    assert os.path.isdir(fim_dir), 'ERROR: could not find the input fim_dir location: ' + str(
        fim_dir
    )
    ## Check that the manning's roughness input filepath exists and then read to dataframe
    assert os.path.isfile(mann_n_table), 'Can not find the input roughness/feature_id file: ' + str(
        mann_n_table
    )

    ## Read the Manning's n csv (ensure that it contains feature_id, channel mannings, floodplain mannings)
    print('Importing the Manning roughness data file: ' + mann_n_table)
    df_mann = pd.read_csv(mann_n_table, dtype={'feature_id': 'int64'})
    if (
        'channel_n' not in df_mann.columns
        or 'overbank_n' not in df_mann.columns
        or 'feature_id' not in df_mann.columns
    ):
        print(
            'Missing required data column ("feature_id","channel_n", and/or "overbank_n")!!! --> '
            + df_mann
        )
    else:
        print('Running the variable_mannings_calc function...')

        ## Loop through hucs in the fim_dir and create list of variables to feed to multiprocessing
        huc_list = os.listdir(fim_dir)
        for huc in huc_list:
            # if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
            if re.match('\d{8}', huc):
                huc_branches_dir = os.path.join(fim_dir, huc, 'branches')
                for branch_id in os.listdir(huc_branches_dir):
                    branch_dir = os.path.join(huc_branches_dir, branch_id)
                    in_src_bankfull_filename = join(
                        branch_dir, 'src_full_crosswalked_' + branch_id + '.csv'
                    )
                    htable_filename = join(branch_dir, 'hydroTable_' + branch_id + '.csv')
                    huc_plot_output_dir = join(branch_dir, 'src_plots')

                    if isfile(in_src_bankfull_filename) and isfile(htable_filename):
                        procs_list.append(
                            [
                                in_src_bankfull_filename,
                                channel_ratio_src_column,
                                df_mann,
                                huc,
                                branch_id,
                                htable_filename,
                                output_suffix,
                                src_plot_option,
                                huc_plot_output_dir,
                            ]
                        )
                    else:
                        print(
                            'HUC: '
                            + str(huc)
                            + '  branch id: '
                            + str(branch_id)
                            + '\nWARNING --> can not find required file (src_full_crosswalked_bankfull_*.csv or hydroTable_*.csv) in the fim output dir: '
                            + str(branch_dir)
                            + ' - skipping this branch!!!\n'
                        )
                        log_file.write(
                            'HUC: '
                            + str(huc)
                            + '  branch id: '
                            + str(branch_id)
                            + '\nWARNING --> can not find required file (src_full_crosswalked_bankfull_*.csv or hydroTable_*.csv) in the fim output dir: '
                            + str(branch_dir)
                            + ' - skipping this branch!!!\n'
                        )

        ## Pass huc procs_list to multiprocessing function
        multi_process(variable_mannings_calc, procs_list, log_file, verbose)

        ## Record run time and close log file
        end_time = dt.datetime.now()
        log_file.write('END TIME: ' + str(end_time) + '\n')
        tot_run_time = end_time - begin_time
        log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
        log_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Vary the Manning's n values for in-channel vs. floodplain (recalculate Manning's eq for Discharge)"
    )
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-bc',
        '--channel-ratio-src-column',
        help='SRC attribute containing the channel vs. overbank geometry ratio (for composite calc)',
        required=False,
        type=str,
        default='chann_hradius_ratio',
    )
    parser.add_argument(
        '-mann',
        '--mann-n-table',
        help="Path to a csv file containing Manning's n values by featureid",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-suff',
        '--output-suffix',
        help="Suffix to append to the output log file (e.g. '_global_06_011')",
        default="",
        required=False,
        type=str,
    )
    parser.add_argument(
        '-j', '--number-of-jobs', help='number of workers', required=False, default=1, type=int
    )
    parser.add_argument(
        '-vb',
        '--verbose',
        help='Optional verbose progress bar',
        required=False,
        default=None,
        action='store_true',
    )
    parser.add_argument(
        '-plots',
        '--src-plot-option',
        help='OPTIONAL flag: use this flag to create src plots for all hydroids. WARNING - long runtime',
        default=False,
        required=False,
        action='store_true',
    )

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    channel_ratio_src_column = args['channel_ratio_src_column']
    mann_n_table = args['mann_n_table']
    output_suffix = args['output_suffix']
    number_of_jobs = args['number_of_jobs']
    verbose = bool(args['verbose'])
    src_plot_option = args['src_plot_option']

    run_prep(
        fim_dir,
        channel_ratio_src_column,
        mann_n_table,
        output_suffix,
        number_of_jobs,
        verbose,
        src_plot_option,
    )
