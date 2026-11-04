#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import sys
import traceback
import shutil

from multiprocessing import Pool
from os.path import  isdir, isfile, join

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from tqdm import tqdm

from src.utils.shared_functions import setup_file_logger


sns.set_theme(style="whitegrid")

# Unpacks the dictionary in each list items
def _run_bankfull_lookup(args):
    return src_bankfull_lookup(**args)


# TODO: Nov 2026: We will build up a list of log messages in this child MP worker.
# We will do this until we upgrade the pool where we can have each worker write to the log(s)
# instead of collecting it as a return from each worker
def src_bankfull_lookup(src_orig_full_filename, df_bflows, huc, branch_id, src_plot_option, huc_src_plots_output_dir):

    """
        Identify the SRC bankfull stage values using the NWM bankfull estimated flows

        Parameters
        ----------
        fim_dir : str
            Directory containing FIM output folders.
        bankfull_flow_dir : str
            Directory containing "bankfull" flows files (e.g. NWM bankfull estimated recurr).
        branch_jobs : str
            Number of branch jobs.
        plots : str
            Optional: Flag to create SRC plots for all hydroids (True/False)
    """
    
    log_text = []

    ## Read the src_full_crosswalked.csv
    print(".......................")
    log_text.append(".......................")
    msg = f"Calculating: {huc} - branch id: {str(branch_id)}"
    print(msg)    
    log_text.append(msg)
    has_error = False

    try:
        df_src = pd.read_csv(src_orig_full_filename, dtype={'HydroID': int, 'feature_id': int})
        if 'bankfull_flow' in df_src.columns:
            log_text.append(
                'WARNING: HUC: '
                + huc
                + '  branch id: '
                + str(branch_id)
                + ' --> bankfull_flow column already exists in the src_full_crosswalked.csv file. '
                'Please be sure to run the "update_htable_src.py to clean up the src_full_crosswalked.csv'
            )
            print(
                f'WARNING bankfull_flow column already exists in the src_full_crosswalked.csv file for {huc}.'
                ' Please be sure to run the "update_htable_src.py to clean up the src_full_crosswalked.csv'
            )
            return log_text, False

        ## NWM recurr rename discharge var
        df_bflows = df_bflows.rename(columns={'discharge': 'bankfull_flow'})

        ## Combine the nwm bankfull estimated flows into the SRC via feature_id
        df_src = df_src.merge(df_bflows, how='left', on='feature_id')

        ## Check if there are any missing data, negative or zero flow values in the bankfull_flow
        check_null = df_src['bankfull_flow'].isnull().sum()

        if check_null > 0:

            err_msg = (
                'WARNING: Missing feature_id in crosswalk for huc: '
                + huc
                + '  branch id: '
                + str(branch_id)
                + ' --> these featureids will be ignored in bankfull calcs (~'
                + str(check_null / 84)
                + ' features)'
            )
            print(err_msg)
            log_text.append(err_msg)
            ## Fill missing/nan nwm bankfull_flow values with -999 to handle later
            df_src['bankfull_flow'] = df_src['bankfull_flow'].fillna(-999)

        negative_flows = len(df_src.loc[(df_src.bankfull_flow <= 0) & (df_src.bankfull_flow != -999)])

        if negative_flows > 0:
            log_text.append(
                'INFO: HUC: '
                + huc
                + '  branch id: '
                + str(branch_id)
                + ' --> Negative or zero flow values found in the input bankfull flows csv (posible lakeid loc)'
            )

        ## Define the channel geometry variable names to use from the src
        hradius_var = 'HydraulicRadius (m)'
        volume_var = 'Volume (m3)'
        surface_area_var = 'SurfaceArea (m2)'
        bedarea_var = 'BedArea (m2)'

        ## Locate the closest SRC discharge value to the NWM bankfull estimated flow
        df_src['Q_bfull_find'] = (df_src['bankfull_flow'] - df_src['Discharge (m3s-1)']).abs()

        ## Check for any missing/null entries in the input SRC
        # There may be null values for lake or coastal flow lines
        # (need to set a value to do groupby idxmin below)
        if df_src['Q_bfull_find'].isnull().values.any():
            err_msg = (
                'WARNING: HUC: '
                + huc
                + '  branch id: '
                + str(branch_id)
                + ' --> Null values found in "Q_bfull_find" calc. These will be filled with 999999 ()'
            )
            print(err_msg)
            log_text.append(err_msg)

            ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            df_src['Q_bfull_find'] = df_src['Q_bfull_find'].fillna(999999)
        if df_src['HydroID'].isnull().values.any():
            err_msg = (
                'WARNING: HUC: '
                + huc
                + '  branch id: '
                + str(branch_id)
                + ' --> Null values found in "HydroID"...'
            )
            print(err_msg)
            log_text.append(err_msg)

        df_bankfull_calc = df_src[
            ['Stage', 'HydroID', bedarea_var, volume_var, hradius_var, surface_area_var, 'Q_bfull_find']
        ]  # create new subset df to perform the Q_1_5 lookup
        df_bankfull_calc = df_bankfull_calc[
            df_bankfull_calc['Stage'] > 0.0
        ]  # Ensure bankfull stage is greater than stage=0
        df_bankfull_calc = df_bankfull_calc.reset_index(drop=True)
        # find the index of the Q_bfull_find (closest matching flow)
        df_bankfull_calc = df_bankfull_calc.loc[
            df_bankfull_calc.groupby('HydroID')['Q_bfull_find'].idxmin()
        ].reset_index(drop=True)
        # rename volume to use later for channel portion calc
        df_bankfull_calc = df_bankfull_calc.rename(
            columns={
                'Stage': 'Stage_bankfull',
                bedarea_var: 'BedArea_bankfull',
                volume_var: 'Volume_bankfull',
                hradius_var: 'HRadius_bankfull',
                surface_area_var: 'SurfArea_bankfull',
            }
        )
        df_src = df_src.merge(
            df_bankfull_calc[
                [
                    'Stage_bankfull',
                    'HydroID',
                    'BedArea_bankfull',
                    'Volume_bankfull',
                    'HRadius_bankfull',
                    'SurfArea_bankfull',
                ]
            ],
            how='left',
            on='HydroID',
        )
        df_src = df_src.drop(['Q_bfull_find'], axis=1)

        ## The bankfull ratio variables below were previously used for the composite variable roughness routine
        ##      (not currently implimented)

        ## NOTE: if this gets reimplemented.. get rid of the inplace=True. Those need to be removed system wide.

        # ## Calculate the channel portion of bankfull Volume
        # df_src['chann_volume_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
        # df_src['chann_volume_ratio'].where(
        #     df_src['Stage'] == 0, df_src['Volume_bankfull'] / (df_src[volume_var]), inplace=True
        # )
        # #df_src['chann_volume_ratio'] = df_src['chann_volume_ratio'].clip_upper(1.0)
        # # set > 1.0 ratio values to 1.0 (these are within the channel)
        # df_src['chann_volume_ratio'].where(df_src['chann_volume_ratio'] <= 1.0, 1.0, inplace=True)
        # # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
        # df_src['chann_volume_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True)
        # #df_src = df_src.drop(['Volume_bankfull'], axis=1)

        # ## Calculate the channel portion of bankfull Hydraulic Radius
        # df_src['chann_hradius_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
        # df_src['chann_hradius_ratio'].where(
        #     df_src['Stage'] == 0, df_src['HRadius_bankfull'] / (df_src[hradius_var]), inplace=True
        # )
        # # old adding 0.01 to avoid dividing by 0 at stage=0
        # #df_src['chann_hradius_ratio'] = df_src['HRadius_bankfull'] / (df_src[hradius_var]+.0001)
        # # set > 1.0 ratio values to 1.0 (these are within the channel)
        # df_src['chann_hradius_ratio'].where(df_src['chann_hradius_ratio'] <= 1.0, 1.0, inplace=True)
        # # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
        # df_src['chann_hradius_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True)
        # #df_src = df_src.drop(['HRadius_bankfull'], axis=1)

        # ## Calculate the channel portion of bankfull Surface Area
        # df_src['chann_surfarea_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
        # df_src['chann_surfarea_ratio'].where(
        #     df_src['Stage'] == 0, df_src['SurfArea_bankfull'] / (df_src[surface_area_var]), inplace=True
        # )
        # # set > 1.0 ratio values to 1.0 (these are within the channel)
        # df_src['chann_surfarea_ratio'].where(df_src['chann_surfarea_ratio'] <= 1.0, 1.0, inplace=True)
        # # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
        # df_src['chann_surfarea_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True)
        # #df_src = df_src.drop(['HRadius_bankfull'], axis=1)

        ## mask bankfull variables when the bankfull estimated flow value is <= 0
        df_src['Stage_bankfull'] = df_src['Stage_bankfull'].mask(df_src['bankfull_flow'] <= 0.0)

        ## Create a new column to identify channel/floodplain via the bankfull stage value
        df_src.loc[df_src['Stage'] <= df_src['Stage_bankfull'], 'bankfull_proxy'] = 'channel'
        df_src.loc[df_src['Stage'] > df_src['Stage_bankfull'], 'bankfull_proxy'] = 'floodplain'
        df_src['bankfull_proxy'] = df_src['bankfull_proxy'].fillna('channel')

        ## Output new SRC with bankfull column
        df_src.to_csv(src_orig_full_filename, index=False)
        log_text.append(f"{src_orig_full_filename} updated")

        ## plot rating curves (optional arg)
        if src_plot_option:
            if isdir(huc_src_plots_output_dir) is False:
                os.mkdir(huc_src_plots_output_dir)
            generate_src_plot(df_src, huc_src_plots_output_dir)

    except Exception:
        # summary = traceback.StackSummary.extract(traceback.walk_stack(None))
        # print(str(huc) + '  branch id: ' + str(branch_id) + " failed for some reason")
        # print(f"*** {ex}")
        # print(''.join(summary.format()))
        err_msg = f'ERROR --> HUC: {huc} - branch id: {str(branch_id)}'
        log_text.append(err_msg)
        log_text.append(traceback.format_exc())
        has_error = True

        # this goes back to calibrate_rating_curve.sh which rolls up to its parent "tee"
        # Then it can be scanned in the error system based on solely the "tee" file
        print(err_msg)
        print(traceback.format_exc())

        # do not re-raise

    return log_text, has_error


def generate_src_plot(df_src, plt_out_dir):
    ## create list of unique hydroids
    hydroids = df_src.HydroID.unique().tolist()
    # hydroids = [17820017]

    for hydroid in hydroids:
        print("Creating SRC plot: " + str(hydroid))
        plot_df = df_src.loc[df_src['HydroID'] == hydroid]

        fig, axes = plt.subplots(1, 2, figsize=(12, 6))
        fig.suptitle(str(hydroid))
        axes[0].set_title('Rating Curve w/ Bankfull')
        axes[1].set_title('Channel Volume vs. HRadius Ratio')
        sns.despine(fig, left=True, bottom=True)
        sns.scatterplot(x='Discharge (m3s-1)', y='Stage', data=plot_df, ax=axes[0])
        sns.lineplot(x='Discharge (m3s-1)', y='Stage_bankfull', data=plot_df, color='green', ax=axes[0])
        axes[0].fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_bankfull'], alpha=0.5)
        axes[0].text(
            plot_df['Discharge (m3s-1)'].median(),
            plot_df['Stage_bankfull'].median(),
            "Bankfull Proxy Stage: " + str(plot_df['Stage_bankfull'].median()),
        )
        sns.scatterplot(
            x='chann_volume_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_volume_ratio", s=38
        )
        sns.scatterplot(
            x='chann_hradius_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_hradius_ratio", s=12
        )
        sns.scatterplot(
            x='chann_surfarea_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_surfarea_ratio", s=12
        )
        axes[1].legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '_bankfull.png', dpi=100, bbox_inches='tight')
        plt.close()


def multi_process(procs_list, branch_jobs, verbose):

    print(f"Identifying bankfull stage for {len(procs_list)} branches using {branch_jobs} jobs")

    # TODO: Nov 2026: This should be cleaned up or use the shared function version
    map_output = None
    with Pool(processes=branch_jobs) as pool:
        # progress_bar = tqdm(total=len(procs_list[0]))
        if verbose:
            map_output = tqdm(pool.imap(_run_bankfull_lookup, procs_list), total=len(procs_list))
            # tuple(map_output)  # fetch the lazy results
        else:
            map_output = pool.map(_run_bankfull_lookup, procs_list)

    in_error = False
    # TODO: Nov 2026: Do this until we upgrade the pool where we can have each worker write to the log(s)
    # instead of collecting it as a return from each worker
    for log_text_list, has_error in map_output:
        if has_error:
            for item in log_text_list:
                logging.error(item)  # Note.. error type
            in_error = True
        else:
            for item in log_text_list:
                logging.info(item)  # Note.. info type

    return in_error

def run_prep(huc_dir, bankfull_flow_filepath, branch_jobs, verbose, src_plot_option):

    ## Check that the bankfull flow filepath exists and read to dataframe
    # Note: This does work in the fim_pipeline chain as it throws an exception that is caught by the .sh files
    # which rollup to the huc_{huc num}_unit.log file for future scanning.
    assert os.path.isfile(bankfull_flow_filepath), 'ERROR: Can not find the input bankfull flow file: ' + str(
        bankfull_flow_filepath
    )

    ## initiate log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = setup_file_logger(log_dir, "identify_src_bankfull")

    overall_start_time = dt.datetime.now(dt.timezone.utc)
    logging.info("Start Identify Src Bankfull")
    logging.info(f"START TIME: {overall_start_time.strftime("%m/%d/%Y %H:%M:%S")}")
    logging.info("")
    print(f"Logs will be saved to {log_file_path}")    
    in_error = False
    try:
        df_bflows = pd.read_csv(bankfull_flow_filepath, dtype={'feature_id': int})

        # get hucnumber
        huc = os.path.basename(os.path.normpath(huc_dir))
        huc_branches_dir = os.path.join(huc_dir, 'branches')
        procs_list = []
        for branch_id in os.listdir(huc_branches_dir):
            branch_dir = os.path.join(huc_branches_dir, branch_id)

            huc_src_plots_output_dir = join(branch_dir, 'src_plots')
            src_orig_full_filename = join(branch_dir, 'src_full_crosswalked_' + branch_id + '.csv')

            ## Check if BARC modified src_full_crosswalked_BARC.csv exists otherwise use
            #   orginial src_full_crosswalked.csv
            if isfile(src_orig_full_filename):
                args = {
                        'src_orig_full_filename': src_orig_full_filename,
                        'df_bflows': df_bflows,
                        'huc': huc,
                        'branch_id': branch_id,
                        'src_plot_option': src_plot_option,
                        'huc_src_plots_output_dir': huc_src_plots_output_dir,
                    }
                procs_list.append(args)
            else:
                msg = (f"HUC: {str(huc)}  branch id: {str(branch_id)}: "
                "WARNING --> can not find the SRC crosswalked csv file in the fim output dir:" 
                f" {str(branch_dir)}  - skipping this branch!!!")
                logging.warning(msg)

        logging.info
        ## Pass huc procs_list to multiprocessing function
        in_error = multi_process(procs_list, branch_jobs, verbose)
    
    except Exception as ex:
        logging.critical(f"Critical Error while processing bankfull for {huc_dir}")
        logging.critical(traceback.format_exc())
        logging.info("")
        raise ex
    finally:
        logging.info(f"End of processing identify_src_bankful")
        if in_error:
            print("Aborting....")
            raise Exception("Aborting....")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Identify bankfull stage for each hydroid synthetic rating curve'
    )
    parser.add_argument('-huc_dir', '--huc_dir', help='huc run directory', required=True, type=str)
    parser.add_argument(
        '-flows',
        '--bankfull-flow-filepath',
        help='NWM recurrence flows dir (flow units in CMS!!!)',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-jb',
        '--branch-jobs',
        help='OPTIONAL: number of branches job (default=8)',
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
        help='OPTIONAL flag: use this flag to create src plots for all hydroids (helpful for evaluating). '
        'WARNING - long runtime',
        default=False,
        required=False,
        action='store_true',
    )

    args = vars(parser.parse_args())
    run_prep(**args)
