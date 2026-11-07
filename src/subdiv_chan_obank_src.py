#!/usr/bin/env python3
import argparse
import datetime as dt
import logging

# import multiprocessing
import os
import traceback
from multiprocessing import Pool
from os.path import isdir, isfile, join

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm

from src.utils.shared_functions import setup_file_logger

sns.set_theme(style="whitegrid")

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
    branch_jobs : str
        Number of branch jobs.
    src_plot_option : str
        Optional (True or False): use this flag to crate src plots for all hydroids (long run time)
"""

# Unpacks the dictionary in each list items. Only needed as we are usign imap and dictionaries
def _run_variable_mannings_calc(args):
    return variable_mannings_calc(**args)

# TODO: Nov 2026: We will build up a list of log messages in this child MP worker.
# We will do this until we upgrade the pool where we can have each worker write to the log(s)
# instead of collecting it as a return from each worker
def variable_mannings_calc(in_src_bankfull_filename, df_mann, huc,
                           branch_id, htable_filename, output_suffix,
                           src_plot_option, huc_output_dir):

    '''
    Return:
        log_text: a list of strings for the log files.
        has_error: When true, it means shut down the code and stop downstream code.
    '''

    ## Read the src_full_crosswalked.csv
    log_text = [f"Calculating modified SRC: {huc} - branch id: {str(branch_id)}"]
    has_error = False

    try:
        log_text.append(".......................")

        df_src_orig = pd.read_csv(in_src_bankfull_filename, dtype={'feature_id': 'int64'})

        ## Check that the channel ratio column the user specified exists in the def
        if 'Stage_bankfull' not in df_src_orig.columns:
            log_text.append('Skipping --> ' + str(huc) + '  branch id: ' + str(branch_id))

            log_text.append(
                'WARNING --> '
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' ' + in_src_bankfull_filename
                + ' does not contain the specified bankfull column: '
                + 'Stage_bankfull'
            )
            return log_text, False
        
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
        log_text.append(
            'Calculating subdiv variables for SRC: ' + str(huc) + '  branch id: ' + str(branch_id) + '\n'
        )
        df_src = subdiv_geometry(df_src_orig)

        ## Merge (crosswalk) the df of Manning's n with the SRC df
        ##   (using the channel/fplain delination in the 'Stage_bankfull')
        df_src = df_src.merge(df_mann, how='left', on='feature_id')
        check_null = df_src['channel_n'].isnull().sum() + df_src['overbank_n'].isnull().sum()
        if check_null > 0:
            msg = (
                "WARNING:"
                + str(huc)
                + '  branch id: '
                + str(branch_id)
                + ' --> '
                + 'Null feature_ids found in crosswalk btw roughness dataframe and src dataframe (these will be set to default n values)'
                + ' --> missing entries= '
                + str(check_null / 84)
            )
            log_text.append(msg)
            # TODO: Nov 2026: Should this return and stop processing?

        ## Set default channel and overbank n values
        default_channel_n = 0.06
        default_overbank_n = 0.12
        ## Fill in the missing values with the default n values
        df_src['channel_n'] = df_src['channel_n'].fillna(default_channel_n)
        df_src['overbank_n'] = df_src['overbank_n'].fillna(default_overbank_n)

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
        log_text.append(f"{in_src_bankfull_filename} updated")

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
        log_text.append(f"{htable_filename} updated")

        ## plot rating curves
        if src_plot_option:
            if isdir(huc_output_dir) is False:
                os.mkdir(huc_output_dir)
            generate_src_plot(df_src, huc_output_dir)

        log_text.append(f'Completed: {huc} - branch id: {str(branch_id)}')

    except Exception:
        err_msg = (
            'ERROR --> '
            + str(huc)
            + '  branch id: '
            + str(branch_id)
            + " subdivision failed"
        )
        log_text.append(err_msg)
        log_text.append(traceback.format_exc())
        has_error = True
        # do not re-raise

    return log_text, has_error


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
    df_src = df_src['HydraulicRadius_chan (m)'].fillna(0)
    df_src['Discharge_chan (m3s-1)'] = (
        df_src['WetArea_chan (m2)']
        * pow(df_src['HydraulicRadius_chan (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['channel_n']
    )
    df_src['Velocity_chan (m/s)'] = df_src['Discharge_chan (m3s-1)'] / df_src['WetArea_chan (m2)']
    df_src = df_src['Velocity_chan (m/s)'].fillna(0)

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
    df_src = df_src['HydraulicRadius_obank (m)'].fillna(0)
    df_src['Discharge_obank (m3s-1)'] = (
        df_src['WetArea_obank (m2)']
        * pow(df_src['HydraulicRadius_obank (m)'], 2.0 / 3)
        * pow(df_src['SLOPE'], 0.5)
        / df_src['overbank_n']
    )
    df_src['Velocity_obank (m/s)'] = df_src['Discharge_obank (m3s-1)'] / df_src['WetArea_obank (m2)']
    df_src = df_src['Velocity_obank (m/s)'].fillna(0)

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


# TODO: Nov 2026: Do this until we upgrade the pool where we can have each worker write to the log(s)
# instead of collecting it as a return from each worker
def multi_process(procs_list, branch_jobs, verbose):
    logging.debug(
        "Computing subdivided SRC and applying variable Manning's n to channel/overbank for "
        f"{len(procs_list)} branches using {branch_jobs} jobs"
    )

    # TODO: Nov 2026: This should be cleaned up or use the shared function version
    map_output = None
    with Pool(processes=branch_jobs) as pool:
        if verbose:
            map_output = tqdm(pool.imap(_run_variable_mannings_calc, procs_list), total=len(procs_list))
            # tuple(map_output)  # fetch the lazy results
        else:
            map_output = pool.imap(_run_variable_mannings_calc, procs_list)

    in_error = False
    # TODO: Nov 2026: This is temp as with this MP setup, child workers can not write to logs
    # so we will send back log text and an error bool so we can shut down the downstream tools if it fails.
    # Once we do, we can use logging.info, .debug, .warning, etc
    for log_text_list, has_error in map_output:
        if has_error:
            for item in log_text_list:
                logging.error(item)  # Note.. error type
            in_error = True
        else:
            for item in log_text_list:
                logging.debug(item)  # Note.. debug (log file only, not screen) type

    return in_error


def run_prep(huc_dir, mann_n_table, output_suffix, branch_jobs, verbose, src_plot_option):
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## initiate log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    log_file_path = setup_file_logger(log_dir, f"subdiv_src_{output_suffix}")
    logging.info("Start subdiv_chan_obank_src")
    print(f"Logs will be saved to {log_file_path}")    
    logging.debug('START TIME: ' + str(begin_time))
    logging.debug('#########################################################')    

    ## Check that the input fim_dir exists
    # assert os.path.isdir(fim_dir), 'ERROR: could not find the input fim_dir location: ' + str(fim_dir)
    ## Check that the manning's roughness input filepath exists and then read to dataframe
    assert os.path.isfile(mann_n_table), 'Can not find the input roughness/feature_id file: ' + str(
        mann_n_table
    )

    ## Read the Manning's n csv (ensure that it contains feature_id, channel mannings, floodplain mannings)
    logging.debug('Importing the Manning roughness data file: ' + mann_n_table)

    in_error = False
    try:
        df_mann = pd.read_csv(mann_n_table, dtype={'feature_id': 'int64'})
        if (
            'channel_n' not in df_mann.columns
            or 'overbank_n' not in df_mann.columns
            or 'feature_id' not in df_mann.columns
        ):
            logging.warning(
                'Missing required data column ("feature_id","channel_n", and/or "overbank_n")!!! --> ' + df_mann
            )
        else:
            logging.debug('Running the variable_mannings_calc function...')
            huc = os.path.basename(os.path.normpath(huc_dir))
            huc_branches_dir = os.path.join(huc_dir, 'branches')
            procs_list = []            
            for branch_id in os.listdir(huc_branches_dir):
                branch_dir = os.path.join(huc_branches_dir, branch_id)
                in_src_bankfull_filename = join(branch_dir, 'src_full_crosswalked_' + branch_id + '.csv')
                htable_filename = join(branch_dir, 'hydroTable_' + branch_id + '.csv')
                huc_plot_output_dir = join(branch_dir, 'src_plots')

                if isfile(in_src_bankfull_filename) and isfile(htable_filename):
                    args = {
                        "in_src_bankfull_filename" : in_src_bankfull_filename,
                        "df_mann": df_mann,
                        "huc": huc,
                        'branch_id': branch_id,
                        "htable_filename": htable_filename,
                        "output_suffix": output_suffix,
                        "src_plot_option": src_plot_option,
                        "huc_plot_output_dir": huc_plot_output_dir
                    }
                    procs_list.append(args)
                else:
                    msg = (
                        'HUC: '
                        + str(huc)
                        + '  branch id: '
                        + str(branch_id)
                        + '\nWARNING --> can not find required file (src_full_crosswalked_bankfull_*.csv '
                        + 'or hydroTable_*.csv) in the fim output dir: '
                        + str(branch_dir)
                        + ' - skipping this branch!!!'
                    )
                    logging.warning(msg)

            ## Pass huc procs_list to multiprocessing function
            in_error = multi_process(procs_list, branch_jobs, verbose)

    except Exception as ex:
        logging.critical(f"Critical Error while processing subdiv_chan_obank_src for {huc_dir}")
        logging.critical(traceback.format_exc())
        logging.info("")
        raise ex
    finally:
        logging.info("End of processing subdiv_chan_obank_src")
        if in_error:
            print("Aborting....")
            raise Exception("Aborting....")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Subdivide the default SRC to compute a seperate channel component and overbank"
        "component. Impliment user provided Manning's n values for in-channel vs. overbank flow. "
        "Recalculate Manning's eq for discharge"
    )
    parser.add_argument('-huc_dir', '--huc_dir', help='FIM output dir', required=True, type=str)
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
        '-jb',
        '--branch_jobs',
        help='OPTIONAL: number of branches workers (default=8)',
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
    run_prep(**args)
