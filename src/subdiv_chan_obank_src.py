#!/usr/bin/env python3
import argparse
import datetime as dt
import multiprocessing
import os
import re
import shutil
import sys
import traceback
import warnings
from functools import reduce
from multiprocessing import Pool
from os.path import dirname, isdir, isfile, join
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm


sns.set_theme(style="whitegrid")
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Compute channel geomety and Manning's equation using subdivision method (separate in-channel vs. overbank)
    Also apply unique Manning's n-values for channel and overbank using a user provided feature_id csv

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    mann_n_table : str
        Path to a csv file containing Manning's n values by feature_id
        (must contain variables "feature_id", "channel_n", "overbank_n")
    file_suffix : str
        Optional: Suffix to append to the output log file
    number_of_jobs : str
        Number of jobs.
    src_plot_option : str
        Optional (True or False): use this flag to crate src plots for all hydroids (long run time)
"""


def variable_mannings_calc(args):
    in_src_bankfull_filename = args[0]
    df_mann = args[1]
    huc = args[2]
    branch_id = args[3]
    htable_filename = args[4]
    output_suffix = args[5]
    src_plot_option = args[6]
    huc_output_dir = args[7]

    ## Read the src_full_crosswalked.csv
    log_text = 'Calculating modified SRC: ' + str(huc) + '  branch id: ' + str(branch_id) + '\n'
    try:
        df_src_orig = pd.read_csv(in_src_bankfull_filename, dtype={'feature_id': 'int64'})

        ## Check that the channel ratio column the user specified exists in the def
        if 'Stage_bankfull' not in df_src_orig.columns:
            print(
                'WARNING --> '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + in_src_bankfull_filename
                + ' does not contain the specified bankfull column: '
                + 'Stage_bankfull'
            )
            print('Skipping --> ' + str(huc) + '  branch id: ' + str(branch_id))
            log_text += (
                'WARNING --> '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + in_src_bankfull_filename
                + ' does not contain the specified bankfull column: '
                + 'Stage_bankfull'
                + '\n'
            )
        else:
            df_src_orig = df_src_orig.drop(
                [
                    'channel_n',
                    'overbank_n',
                    'subdiv_applied',
                    'Discharge (m3s-1)_subdiv',
                    'Volume_chan (m3)',
                    'Volume_obank (m3)',
                    'BedArea_chan (m2)',
                    'BedArea_obank (m2)',
                    'WettedPerimeter_chan (m)',
                    'WettedPerimeter_obank (m)',
                ],
                axis=1,
                errors='ignore',
            )  # drop these cols (in case vmann was previously performed)

            ## Calculate subdiv geometry variables
            print('Calculating subdiv variables for SRC: ' + str(huc) + '  branch id: ' + str(branch_id))
            log_text = (
                'Calculating subdiv variables for SRC: ' + str(huc) + '  branch id: ' + str(branch_id) + '\n'
            )
            df_src = subdiv_geometry(df_src_orig)

            ## Merge (crosswalk) the df of Manning's n with the SRC df
            ##   (using the channel/fplain delination in the 'Stage_bankfull')
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

            ## Check if there are any missing data in the 'Stage_bankfull' column
            ##   (these are locations where subdiv will not be applied)
            df_src['subdiv_applied'] = np.where(
                df_src['Stage_bankfull'].isnull(), False, True
            )  # create field to identify where vmann is applied (True=yes; False=no)

            ## Calculate Manning's equation discharge for channel, overbank, and total
            df_src = subdiv_mannings_eq(df_src)

            ## Use the default discharge column when vmann is not being applied
            df_src['Discharge (m3s-1)_subdiv'] = np.where(
                df_src['subdiv_applied'] == False,
                df_src['Discharge (m3s-1)'],
                df_src['Discharge (m3s-1)_subdiv'],
            )  # reset the discharge value back to the original if vmann=false

            ## Output new SRC with bankfull column
            df_src.to_csv(in_src_bankfull_filename, index=False)

            ## Output new hydroTable with updated discharge and ManningN column
            df_src_trim = df_src[
                [
                    'HydroID',
                    'Stage',
                    'Bathymetry_source',
                    'subdiv_applied',
                    'channel_n',
                    'overbank_n',
                    'Discharge (m3s-1)_subdiv',
                ]
            ]
            df_src_trim = df_src_trim.rename(
                columns={'Stage': 'stage', 'Discharge (m3s-1)_subdiv': 'subdiv_discharge_cms'}
            )
            df_src_trim['discharge_cms'] = df_src_trim[
                'subdiv_discharge_cms'
            ]  # create a copy of vmann modified discharge (used to track future changes)
            df_htable = pd.read_csv(
                htable_filename,
                dtype={'HUC': str, 'last_updated': object, 'submitter': object, 'obs_source': object},
            )

            ## drop the previously modified discharge column to be replaced with updated version
            df_htable = df_htable.drop(
                [
                    'subdiv_applied',
                    'discharge_cms',
                    'overbank_n',
                    'channel_n',
                    'subdiv_discharge_cms',
                    'Bathymetry_source',
                ],
                axis=1,
                errors='ignore',
            )
            df_htable = df_htable.merge(
                df_src_trim, how='left', left_on=['HydroID', 'stage'], right_on=['HydroID', 'stage']
            )

            ## Output new hydroTable csv
            if output_suffix != "":
                htable_filename = os.path.splitext(htable_filename)[0] + output_suffix + '.csv'
            df_htable.to_csv(htable_filename, index=False)

            log_text += 'Completed: ' + str(huc)

            ## plot rating curves
            if src_plot_option:
                if isdir(huc_output_dir) is False:
                    os.mkdir(huc_output_dir)
                generate_src_plot(df_src, huc_output_dir)
    except Exception as ex:
        summary = traceback.StackSummary.extract(traceback.walk_stack(None))
        print(
            'WARNING: ' + str(huc) + '  branch id: ' + str(branch_id) + " subdivision failed for some reason"
        )
        # print(f"*** {ex}")
        # print(''.join(summary.format()))
        log_text += (
            'ERROR --> '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + " subdivision failed (details: "
            + (f"*** {ex}")
            + (''.join(summary.format()))
            + '\n'
        )

    return log_text


def subdiv_geometry(df_src):
    ## Calculate in-channel volume & bed area
    df_src['Volume_chan (m3)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'],
        df_src['Volume (m3)'],
        (
            df_src['Volume_bankfull']
            + ((df_src['Stage'] - df_src['Stage_bankfull']) * df_src['SurfArea_bankfull'])
        ),
    )
    df_src['BedArea_chan (m2)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'], df_src['BedArea (m2)'], df_src['BedArea_bankfull']
    )
    df_src['WettedPerimeter_chan (m)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'],
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000),
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000)
        + ((df_src['Stage'] - df_src['Stage_bankfull']) * 2),
    )

    ## Calculate overbank volume & bed area
    df_src['Volume_obank (m3)'] = np.where(
        df_src['Stage'] > df_src['Stage_bankfull'], (df_src['Volume (m3)'] - df_src['Volume_chan (m3)']), 0.0
    )
    df_src['BedArea_obank (m2)'] = np.where(
        df_src['Stage'] > df_src['Stage_bankfull'],
        (df_src['BedArea (m2)'] - df_src['BedArea_chan (m2)']),
        0.0,
    )
    df_src['WettedPerimeter_obank (m)'] = df_src['BedArea_obank (m2)'] / df_src['LENGTHKM'] / 1000
    return df_src


def subdiv_mannings_eq(df_src):
    ## Calculate discharge (channel) using Manning's equation
    df_src = df_src.drop(
        ['WetArea_chan (m2)', 'HydraulicRadius_chan (m)', 'Discharge_chan (m3s-1)', 'Velocity_chan (m/s)'],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_chan (m2)'] = df_src['Volume_chan (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_chan (m)'] = df_src['WetArea_chan (m2)'] / df_src['WettedPerimeter_chan (m)']
    df_src['HydraulicRadius_chan (m)'].fillna(0, inplace=True)
    df_src['Discharge_chan (m3s-1)'] = (
        df_src['WetArea_chan (m2)']
        * pow(df_src['HydraulicRadius_chan (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['channel_n']
    )
    df_src['Velocity_chan (m/s)'] = df_src['Discharge_chan (m3s-1)'] / df_src['WetArea_chan (m2)']
    df_src['Velocity_chan (m/s)'].fillna(0, inplace=True)

    ## Calculate discharge (overbank) using Manning's equation
    df_src = df_src.drop(
        [
            'WetArea_obank (m2)',
            'HydraulicRadius_obank (m)',
            'Discharge_obank (m3s-1)',
            'Velocity_obank (m/s)',
        ],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_obank (m2)'] = df_src['Volume_obank (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_obank (m)'] = df_src['WetArea_obank (m2)'] / df_src['WettedPerimeter_obank (m)']
    df_src = df_src.replace([np.inf, -np.inf], np.nan)  # need to replace inf instances (divide by 0)
    df_src['HydraulicRadius_obank (m)'].fillna(0, inplace=True)
    df_src['Discharge_obank (m3s-1)'] = (
        df_src['WetArea_obank (m2)']
        * pow(df_src['HydraulicRadius_obank (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['overbank_n']
    )
    df_src['Velocity_obank (m/s)'] = df_src['Discharge_obank (m3s-1)'] / df_src['WetArea_obank (m2)']
    df_src['Velocity_obank (m/s)'].fillna(0, inplace=True)

    ## Calcuate the total of the subdivided discharge (channel + overbank)
    df_src = df_src.drop(
        ['Discharge (m3s-1)_subdiv'], axis=1, errors='ignore'
    )  # drop these cols (in case subdiv was previously performed)
    df_src['Discharge (m3s-1)_subdiv'] = df_src['Discharge_chan (m3s-1)'] + df_src['Discharge_obank (m3s-1)']
    df_src.loc[df_src['Stage'] == 0, ['Discharge (m3s-1)_subdiv']] = 0
    return df_src


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
        sns.scatterplot(
            x='Discharge (m3s-1)_subdiv',
            y='Stage',
            data=plot_df,
            label="SRC w/ Subdiv",
            ax=ax,
            color='orange',
        )
        sns.scatterplot(
            x='Discharge_chan (m3s-1)',
            y='Stage',
            data=plot_df,
            label="SRC Channel",
            ax=ax,
            color='green',
            s=8,
        )
        sns.scatterplot(
            x='Discharge_obank (m3s-1)',
            y='Stage',
            data=plot_df,
            label="SRC Overbank",
            ax=ax,
            color='purple',
            s=8,
        )
        sns.lineplot(x='Discharge (m3s-1)', y='Stage_bankfull', data=plot_df, color='green', ax=ax)
        plt.fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_bankfull'], alpha=0.5)
        plt.text(
            plot_df['Discharge (m3s-1)'].median(),
            plot_df['Stage_bankfull'].median(),
            "NWM Bankfull Approx: " + str(plot_df['Stage_bankfull'].median()),
        )
        ax.legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '_vmann.png', dpi=175, bbox_inches='tight')
        plt.close()


def multi_process(variable_mannings_calc, procs_list, log_file, number_of_jobs, verbose):
    ## Initiate multiprocessing
    available_cores = multiprocessing.cpu_count()
    if number_of_jobs > available_cores:
        number_of_jobs = available_cores - 2
        print(
            "Provided job number exceeds the number of available cores. "
            + str(number_of_jobs)
            + " max jobs will be used instead."
        )

    print(
        "Computing subdivided SRC and applying variable Manning's n to channel/overbank for "
        f"{len(procs_list)} branches using {number_of_jobs} jobs"
    )
    with Pool(processes=number_of_jobs) as pool:
        if verbose:
            map_output = tqdm(pool.imap(variable_mannings_calc, procs_list), total=len(procs_list))
            tuple(map_output)  # fetch the lazy results
        else:
            map_output = pool.map(variable_mannings_calc, procs_list)
    log_file.writelines(["%s\n" % item for item in map_output])


def run_prep(
    fim_dir, mann_n_table, output_suffix, number_of_jobs, verbose, src_plot_option, process_huc=None
):
    procs_list = []

    print(f"Writing progress to log file here: {fim_dir}/logs/subdiv_src_{output_suffix}.log")
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## initiate log file
    log_file = open(join(fim_dir, 'logs', 'subdiv_src_' + output_suffix + '.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    ## Check that the input fim_dir exists
    assert os.path.isdir(fim_dir), 'ERROR: could not find the input fim_dir location: ' + str(fim_dir)
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
            'Missing required data column ("feature_id","channel_n", and/or "overbank_n")!!! --> ' + df_mann
        )
    else:
        print('Running the variable_mannings_calc function...')

        ## Loop through hucs in the fim_dir and create list of variables to feed to multiprocessing
        huc_list = [d for d in os.listdir(fim_dir) if re.match(r'^\d{8}$', d)]
        huc_list.sort()  # sort huc_list for helping track progress in future print statments
        for huc in huc_list:
            # if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
            if process_huc is None or huc in process_huc:
                if re.match(r'\d{8}', huc):
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
                                + '\nWARNING --> can not find required file (src_full_crosswalked_bankfull_*.csv '
                                + 'or hydroTable_*.csv) in the fim output dir: '
                                + str(branch_dir)
                                + ' - skipping this branch!!!\n'
                            )
                            log_file.write(
                                'HUC: '
                                + str(huc)
                                + '  branch id: '
                                + str(branch_id)
                                + '\nWARNING --> can not find required file (src_full_crosswalked_bankfull_*.csv '
                                + 'or hydroTable_*.csv) in the fim output dir: '
                                + str(branch_dir)
                                + ' - skipping this branch!!!\n'
                            )

        ## Pass huc procs_list to multiprocessing function
        multi_process(variable_mannings_calc, procs_list, log_file, number_of_jobs, verbose)

        ## Record run time and close log file
        end_time = dt.datetime.now()
        log_file.write('END TIME: ' + str(end_time) + '\n')
        tot_run_time = end_time - begin_time
        log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
        log_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Subdivide the default SRC to compute a seperate channel component and overbank"
        "component. Impliment user provided Manning's n values for in-channel vs. overbank flow. "
        "Recalculate Manning's eq for discharge"
    )
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
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
        '-j',
        '--number-of-jobs',
        help='OPTIONAL: number of workers (default=8)',
        required=False,
        default=8,
        type=int,
    )
    parser.add_argument(
        '-vb',
        '--verbose',
        help='OPTIONAL: verbose progress bar',
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
    mann_n_table = args['mann_n_table']
    output_suffix = args['output_suffix']
    number_of_jobs = args['number_of_jobs']
    verbose = bool(args['verbose'])
    src_plot_option = args['src_plot_option']

    run_prep(fim_dir, mann_n_table, output_suffix, number_of_jobs, verbose, src_plot_option)
