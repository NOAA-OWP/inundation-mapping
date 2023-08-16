#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
from multiprocessing import Pool
from os import environ
from os.path import dirname, isdir, isfile, join

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


sns.set_theme(style="whitegrid")
# from utils.shared_functions import mem_profile

"""
    Estimate feature_id missing bathymetry in the raw channel geometry using input bankfull regression geometry

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    bankfull_geom : str
        Input regression dataset w/ bankfull geometry by featureid (topwidth & xsec area)
    number_of_jobs : int
        Number of jobs.
    src_plot_option : str
        Optional (True or False): use this flag to crate src plots for all hydroids
"""

# sa_ratio_flag = 10
# thal_stg_limit = 3
# bankful_xs_ratio_flag = 10
# bathy_xsarea_flag = 1
# thal_hyd_radius_flag = 10
# ignore_streamorder = 10
sa_ratio_flag = float(
    environ['surf_area_thalweg_ratio_flag']
)  # 10x --> Flag: Surface area ratio value to identify possible thalweg notch "jump" (SA x+1 / SA x)
thal_stg_limit = float(
    environ['thalweg_stg_search_max_limit']
)  # 3m --> Threshold: Stage value limit below which to look for the surface area ratio flag (only flag thalweg notch below this threshold)
bankful_xs_ratio_flag = float(
    environ['bankful_xs_area_ratio_flag']
)  # 10x --> Flag: Identify bogus BARC adjusted values where the regression bankfull XS Area/SRC bankfull area is > threshold (topwidth crosswalk issues or bad bankfull regression data points??)
bathy_xsarea_flag = float(
    environ['bathy_xs_area_chg_flag']
)  # 1x --> Flag: Cross section area limit to cap the amount of bathy XS area added to the SRC. Limits the bathy_calc_xs_area/ BANKFULL_XSEC_AREA to the specified threshold
thal_hyd_radius_flag = float(
    environ['thalweg_hyd_radius_flag']
)  # 10x --> Flag: Idenitify possible erroneous BARC-adjusted hydraulic radius values. BARC discharge values greater than the specified threshold and within the thal_stg_limit are set to 0
ignore_streamorder = int(
    environ['ignore_streamorders']
)  # 10 --> Do not perform BARC for streamorders >= provided value


def bathy_rc_lookup(args):
    input_src_fileName = args[0]
    df_bfull_geom = args[1]
    output_bathy_fileName = args[2]
    output_bathy_streamorder_fileName = args[3]
    output_bathy_thalweg_fileName = args[4]
    output_bathy_xs_lookup_fileName = args[5]
    input_htable_fileName = args[6]
    out_src_filename = args[7]
    huc = args[8]
    src_plot_option = args[9]
    huc_plot_output_dir = args[10]

    log_text = 'Calculating: ' + str(huc) + '\n'

    ## Read in the default src_full_crosswalked.csv
    input_src_base = pd.read_csv(input_src_fileName, dtype={'feature_id': int})

    ## Rename input bankfull_geom data columns for consistant referencing
    df_bfull_geom = df_bfull_geom.rename(
        columns={
            'COMID': 'feature_id',
            'BANKFULL_WIDTH': 'BANKFULL_WIDTH (m)',
            'BANKFULL_XSEC_AREA': 'BANKFULL_XSEC_AREA (m2)',
        }
    )
    df_bfull_geom = df_bfull_geom.rename(
        columns={
            'BANKFULL_TOPWIDTH_q': 'BANKFULL_WIDTH (m)',
            'BANKFULL_XSEC_AREA_q': 'BANKFULL_XSEC_AREA (m2)',
        }
    )
    ## Merge input_bathy and modified_src_base df using feature_id/COMID attributes
    modified_src_base = input_src_base.merge(
        df_bfull_geom.loc[:, ['feature_id', 'BANKFULL_WIDTH (m)', 'BANKFULL_XSEC_AREA (m2)']],
        how='left',
        on='feature_id',
    )

    ## Check that the merge process returned matching feature_id entries
    if modified_src_base['BANKFULL_WIDTH (m)'].count() == 0:
        log_text += (
            'WARNING: No matching feature_id found between input bathy data and src_base --> No bathy calculations added to SRC for huc '
            + str(huc)
            + '\n'
        )
    else:
        ## Use SurfaceArea variable to identify thalweg-restricted stage values for each hydroid
        ## Calculate the interrow SurfaceArea ratio n/(n-1)
        modified_src_base['SA_div_flag'] = modified_src_base['SurfaceArea (m2)'].div(
            modified_src_base['SurfaceArea (m2)'].shift(1)
        )
        ## Mask SA_div_flag when Stage = 0 or when the SA_div_flag value (n / n-1) is > threshold value (i.e. 10x)
        modified_src_base['SA_div_flag'].mask(
            (modified_src_base['Stage'] == 0)
            | (modified_src_base['SA_div_flag'] < sa_ratio_flag)
            | (modified_src_base['SurfaceArea (m2)'] == 0),
            inplace=True,
        )
        ## Create new df to filter and groupby HydroID
        find_thalweg_notch = modified_src_base[
            ['HydroID', 'Stage', 'SurfaceArea (m2)', 'SA_div_flag']
        ]
        find_thalweg_notch = find_thalweg_notch[
            find_thalweg_notch['Stage'] < thal_stg_limit
        ]  # assuming thalweg burn-in is less than 3 meters
        find_thalweg_notch = find_thalweg_notch[find_thalweg_notch['SA_div_flag'].notnull()]
        find_thalweg_notch = find_thalweg_notch.loc[
            find_thalweg_notch.groupby('HydroID')['Stage'].idxmax()
        ].reset_index(drop=True)
        ## Assign thalweg_burn_elev variable to the stage value found in previous step
        find_thalweg_notch['Thalweg_burn_elev'] = find_thalweg_notch['Stage']
        ## Merge the Thalweg_burn_elev value back into the modified SRC --> this is used to mask the discharge after Manning's equation
        modified_src_base = modified_src_base.merge(
            find_thalweg_notch.loc[:, ['HydroID', 'Thalweg_burn_elev']], how='left', on='HydroID'
        )

        ## Calculate bankfull vs top width difference for each feature_id
        modified_src_base['Top Width Diff (m)'] = (
            modified_src_base['TopWidth (m)'] - modified_src_base['BANKFULL_WIDTH (m)']
        ).abs()
        ## Calculate XS Area field (Channel Volume / Stream Length)
        modified_src_base['XS Area (m2)'] = modified_src_base['Volume (m3)'] / (
            modified_src_base['LENGTHKM'] * 1000
        )

        ## Groupby HydroID and find min of Top Width Diff (m)
        output_bathy = modified_src_base[
            [
                'feature_id',
                'HydroID',
                'order_',
                'Stage',
                'SurfaceArea (m2)',
                'Thalweg_burn_elev',
                'BANKFULL_WIDTH (m)',
                'TopWidth (m)',
                'XS Area (m2)',
                'BANKFULL_XSEC_AREA (m2)',
                'Top Width Diff (m)',
            ]
        ]
        ## filter out stage = 0 rows in SRC (assuming geom at stage 0 is not a valid channel geom)
        output_bathy = output_bathy[output_bathy['Stage'] > 0]
        ## filter SRC rows identified as Thalweg burned
        output_bathy['Top Width Diff (m)'].mask(
            output_bathy['Stage'] <= output_bathy['Thalweg_burn_elev'], inplace=True
        )
        ## ignore hydroid/featureid that did not have a valid Bankfull lookup (areas outside CONUS - i.e. Canada)
        output_bathy = output_bathy[output_bathy['BANKFULL_XSEC_AREA (m2)'].notnull()]
        ## ignore SRC entries with 0 surface area --> handles input SRC artifacts/errors in Great Lakes region
        output_bathy = output_bathy[output_bathy['SurfaceArea (m2)'] > 0]
        ## find index of minimum top width difference --> this will be used as the SRC "bankfull" row for future calcs
        output_bathy = output_bathy.loc[
            output_bathy.groupby('HydroID')['Top Width Diff (m)'].idxmin()
        ].reset_index(drop=True)
        log_text += (
            'Average: bankfull width crosswalk difference (m): '
            + str(output_bathy['Top Width Diff (m)'].mean())
        ) + '\n'
        log_text += (
            'Minimum: bankfull width crosswalk difference (m): '
            + str(output_bathy['Top Width Diff (m)'].min())
        ) + '\n'
        log_text += (
            'Maximum: bankfull width crosswalk difference (m): '
            + str(output_bathy['Top Width Diff (m)'].max())
        ) + '\n'
        log_text += (
            'STD: bankfull width crosswalk difference (m): '
            + str(output_bathy['Top Width Diff (m)'].std())
        ) + '\n'

        ## Calculate XS Area difference between SRC and Bankfull database
        output_bathy['XS Area Diff (m2)'] = (
            output_bathy['BANKFULL_XSEC_AREA (m2)'] - output_bathy['XS Area (m2)']
        )
        output_bathy['XS Bankfull Area Ratio'] = (
            output_bathy['BANKFULL_XSEC_AREA (m2)'] / output_bathy['XS Area (m2)']
        ).round(2)
        ## masking negative XS Area Diff and XS Area = 0
        output_bathy['XS Bankfull Area Ratio'].mask(
            (output_bathy['XS Area Diff (m2)'] < 0) | (output_bathy['XS Area (m2)'] == 0),
            inplace=True,
        )
        ## masking negative XS Area Diff and XS Area = 0
        output_bathy['XS Area Diff (m2)'].mask(
            (output_bathy['XS Area Diff (m2)'] < 0) | (output_bathy['XS Area (m2)'] == 0),
            inplace=True,
        )
        ## remove bogus values where bankfull area ratio > threshold --> 10x (topwidth crosswalk issues or bad bankfull regression data points??)
        output_bathy['XS Area Diff (m2)'].mask(
            output_bathy['XS Bankfull Area Ratio'] > bankful_xs_ratio_flag, inplace=True
        )
        ## remove bogus values where bankfull area ratio > threshold --> 10x (topwidth crosswalk issues or bad bankfull regression data points??)
        output_bathy['XS Bankfull Area Ratio'].mask(
            output_bathy['XS Bankfull Area Ratio'] > bankful_xs_ratio_flag, inplace=True
        )
        ## Print XS Area Diff statistics
        log_text += (
            'Average: bankfull XS Area crosswalk difference (m2): '
            + str(output_bathy['XS Area Diff (m2)'].mean())
        ) + '\n'
        log_text += (
            'Minimum: bankfull XS Area crosswalk difference (m2): '
            + str(output_bathy['XS Area Diff (m2)'].min())
        ) + '\n'
        log_text += (
            'Maximum: bankfull XS Area crosswalk difference (m2): '
            + str(output_bathy['XS Area Diff (m2)'].max())
        ) + '\n'
        log_text += (
            'STD: bankfull XS Area crosswalk difference (m2): '
            + str(output_bathy['XS Area Diff (m2)'].std())
        ) + '\n'

        ## Bin XS Bankfull Area Ratio by stream order
        stream_order_bathy_ratio = output_bathy[
            ['order_', 'Stage', 'XS Bankfull Area Ratio']
        ].copy()
        ## mask stage values when XS Bankfull Area Ratio is null (need to filter to calculate the median for valid values below)
        stream_order_bathy_ratio['Stage'].mask(
            stream_order_bathy_ratio['XS Bankfull Area Ratio'].isnull(), inplace=True
        )
        stream_order_bathy_ratio = stream_order_bathy_ratio.groupby('order_').agg(
            count=('XS Bankfull Area Ratio', 'count'),
            mean_xs_area_ratio=('XS Bankfull Area Ratio', 'mean'),
            median_stage_bankfull=('Stage', 'median'),
        )
        ## fill XS Bankfull Area Ratio and Stage values if no values were found in the grouby calcs
        stream_order_bathy_ratio = (
            stream_order_bathy_ratio.ffill() + stream_order_bathy_ratio.bfill()
        ) / 2
        ## fill first and last stream order values if needed
        stream_order_bathy_ratio = stream_order_bathy_ratio.bfill().ffill()
        ## Get count_total tally of the total number of stream order hydroids in the HUC (not filtering anything out)
        stream_order_bathy_ratio_count = output_bathy.groupby('order_').agg(
            count_total=('Stage', 'count')
        )
        stream_order_bathy_ratio = stream_order_bathy_ratio.merge(
            stream_order_bathy_ratio_count, how='left', on='order_'
        )
        ## Fill any remaining null values: mean_xs_area_ratio --> 1 median_stage_bankfull --> 0
        stream_order_bathy_ratio['mean_xs_area_ratio'].mask(
            stream_order_bathy_ratio['mean_xs_area_ratio'].isnull(), 1, inplace=True
        )
        stream_order_bathy_ratio['median_stage_bankfull'].mask(
            stream_order_bathy_ratio['median_stage_bankfull'].isnull(), 0, inplace=True
        )

        ## Combine SRC df and df of XS Area for each hydroid and matching stage and order from bins above
        output_bathy = output_bathy.merge(stream_order_bathy_ratio, how='left', on='order_')
        modified_src_base = modified_src_base.merge(
            stream_order_bathy_ratio, how='left', on='order_'
        )

        ## Calculate stage vs median_stage_bankfull difference for bankfull lookup
        modified_src_base['lookup_stage_diff'] = (
            modified_src_base[['median_stage_bankfull', 'Thalweg_burn_elev']].max(axis=1)
            - modified_src_base['Stage']
        ).abs()

        ## If median_stage_bankfull is null then set lookup_stage_diff to 999 at stage 0 (handles errors for channels outside CONUS)
        modified_src_base['lookup_stage_diff'].mask(
            (modified_src_base['Stage'] == 0)
            & (modified_src_base['median_stage_bankfull'].isnull()),
            999,
            inplace=True,
        )

        ## Groupby HydroID again and find min of lookup_stage_diff
        xs_area_hydroid_lookup = modified_src_base[
            [
                'HydroID',
                'BANKFULL_XSEC_AREA (m2)',
                'XS Area (m2)',
                'Stage',
                'Thalweg_burn_elev',
                'median_stage_bankfull',
                'lookup_stage_diff',
                'mean_xs_area_ratio',
            ]
        ]
        xs_area_hydroid_lookup = xs_area_hydroid_lookup.loc[
            xs_area_hydroid_lookup.groupby('HydroID')['lookup_stage_diff'].idxmin()
        ].reset_index(drop=True)

        ## Calculate bathy adjusted XS Area ('XS Area (m2)' mutliplied by mean_xs_area_ratio)
        xs_area_hydroid_lookup['bathy_calc_xs_area'] = (
            xs_area_hydroid_lookup['XS Area (m2)'] * xs_area_hydroid_lookup['mean_xs_area_ratio']
        ) - xs_area_hydroid_lookup['XS Area (m2)']

        ## Calculate the ratio btw the lookup SRC XS_Area and the Bankfull_XSEC_AREA --> use this as a flag for potentially bad XS data
        xs_area_hydroid_lookup['bankfull_XS_ratio_flag'] = (
            xs_area_hydroid_lookup['bathy_calc_xs_area']
            / xs_area_hydroid_lookup['BANKFULL_XSEC_AREA (m2)']
        )
        ## Set bath_cal_xs_area to 0 if the bankfull_XS_ratio_flag is > threshold --> 5x (assuming too large of difference to be a reliable bankfull calculation)
        xs_area_hydroid_lookup['bathy_calc_xs_area'].mask(
            xs_area_hydroid_lookup['bankfull_XS_ratio_flag'] > bathy_xsarea_flag,
            xs_area_hydroid_lookup['BANKFULL_XSEC_AREA (m2)'],
            inplace=True,
        )
        xs_area_hydroid_lookup['barc_on'] = np.where(
            xs_area_hydroid_lookup['bathy_calc_xs_area'].isnull(), False, True
        )  # field to identify where vmann is on/off
        xs_area_hydroid_lookup['bathy_calc_xs_area'].mask(
            xs_area_hydroid_lookup['bankfull_XS_ratio_flag'].isnull(), 0, inplace=True
        )

        ## Merge bathy_calc_xs_area to the modified_src_base
        modified_src_base = modified_src_base.merge(
            xs_area_hydroid_lookup.loc[:, ['HydroID', 'bathy_calc_xs_area', 'barc_on']],
            how='left',
            on='HydroID',
        )

        ## Mask/null the bathy calculated area for streamorders that the user wants to ignore (set bathy_cals_xs_area = 0 for streamorder = 10)
        modified_src_base['bathy_calc_xs_area'].mask(
            modified_src_base['order_'] >= ignore_streamorder, 0.0, inplace=True
        )

        ## Calculate new bathy adjusted channel geometry variables
        modified_src_base = modified_src_base.rename(
            columns={
                'Discharge (m3s-1)': 'orig_Discharge (m3s-1)',
                'XS Area (m2)': 'orig_XS Area (m2)',
                'Volume (m3)': 'orig_Volume (m3)',
                'WetArea (m2)': 'orig_WetArea (m2)',
                'HydraulicRadius (m)': 'orig_HydraulicRadius (m)',
            }
        )
        modified_src_base['XS Area (m2)'] = (
            modified_src_base['orig_XS Area (m2)'] + modified_src_base['bathy_calc_xs_area']
        )
        modified_src_base['Volume (m3)'] = (
            modified_src_base['XS Area (m2)'] * modified_src_base['LENGTHKM'] * 1000
        )
        modified_src_base['WetArea (m2)'] = (
            modified_src_base['Volume (m3)'] / modified_src_base['LENGTHKM'] / 1000
        )
        modified_src_base['HydraulicRadius (m)'] = (
            modified_src_base['WetArea (m2)'] / modified_src_base['WettedPerimeter (m)']
        )
        modified_src_base['HydraulicRadius (m)'].fillna(0, inplace=True)
        ## mask out negative top width differences (avoid thalweg burn notch)
        modified_src_base['HydraulicRadius (m)'].mask(
            (modified_src_base['HydraulicRadius (m)'] > thal_hyd_radius_flag)
            & (modified_src_base['Stage'] < thal_stg_limit),
            0,
            inplace=True,
        )

        ## Calculate Q using Manning's equation
        modified_src_base['Discharge (m3s-1)'] = (
            modified_src_base['WetArea (m2)']
            * pow(modified_src_base['HydraulicRadius (m)'], 2.0 / 3)
            * pow(modified_src_base['SLOPE'], 0.5)
            / modified_src_base['ManningN']
        )
        ## mask discharge values for stage = 0 rows in SRC (replace with 0) --> do we need SRC to start at 0??
        modified_src_base['Discharge (m3s-1)'].mask(
            modified_src_base['Stage'] == 0, 0, inplace=True
        )
        modified_src_base['Discharge (m3s-1)'].mask(
            modified_src_base['Stage'] == modified_src_base['Thalweg_burn_elev'], 0, inplace=True
        )
        modified_src_base['Discharge (m3s-1)'].mask(
            modified_src_base['Stage'] < modified_src_base['Thalweg_burn_elev'], -999, inplace=True
        )

        ## Organize bathy calc output variables for csv
        output_bathy = output_bathy[
            [
                'HydroID',
                'order_',
                'Stage',
                'SurfaceArea (m2)',
                'TopWidth (m)',
                'BANKFULL_WIDTH (m)',
                'Top Width Diff (m)',
                'XS Area (m2)',
                'BANKFULL_XSEC_AREA (m2)',
                'XS Area Diff (m2)',
                'XS Bankfull Area Ratio',
                'count',
                'median_stage_bankfull',
                'mean_xs_area_ratio',
            ]
        ]

        ## Export bathy/bankful calculation tables for easy viewing
        output_bathy.to_csv(output_bathy_fileName, index=False)
        stream_order_bathy_ratio.to_csv(output_bathy_streamorder_fileName, index=True)
        find_thalweg_notch.to_csv(output_bathy_thalweg_fileName, index=True)
        xs_area_hydroid_lookup.to_csv(output_bathy_xs_lookup_fileName, index=True)

        ## Output new src_full_crosswalked
        modified_src_base.to_csv(out_src_filename, index=False)
        ## Update the hydroTable
        modified_hydro_table = modified_src_base.loc[
            :,
            [
                'HydroID',
                'Stage',
                'barc_on',
                'Volume (m3)',
                'WetArea (m2)',
                'HydraulicRadius (m)',
                'Discharge (m3s-1)',
            ],
        ]
        modified_hydro_table.rename(
            columns={'Stage': 'stage', 'Discharge (m3s-1)': 'discharge_cms'}, inplace=True
        )
        df_htable = pd.read_csv(input_htable_fileName, dtype={'HUC': str})
        df_htable.drop(
            ['barc_on'], axis=1, inplace=True
        )  # drop the default "barc_on" variable from add_crosswalk.py
        if not set(
            [
                'orig_discharge_cms',
                'orig_Volume (m3)',
                'orig_WetArea (m2)',
                'orig_HydraulicRadius (m)',
            ]
        ).issubset(
            df_htable.columns
        ):  # check if "orig_" attributes do NOT already exist (likely generated from previous BARC run)
            df_htable.rename(
                columns={
                    'discharge_cms': 'orig_discharge_cms',
                    'Volume (m3)': 'orig_Volume (m3)',
                    'WetArea (m2)': 'orig_WetArea (m2)',
                    'HydraulicRadius (m)': 'orig_HydraulicRadius (m)',
                },
                inplace=True,
            )
        else:
            df_htable.drop(
                ['discharge_cms', 'Volume (m3)', 'WetArea (m2)', 'HydraulicRadius (m)'],
                axis=1,
                inplace=True,
            )  # drop the previously modified columns - to be replaced with updated version
        df_htable = df_htable.merge(
            modified_hydro_table,
            how='left',
            left_on=['HydroID', 'stage'],
            right_on=['HydroID', 'stage'],
        )
        df_htable.to_csv(input_htable_fileName, index=False)
        log_text += ('Output new hydroTable and src_full_crosswalked: ') + '\n'
        log_text += ('Completed Bathy Calculations: ') + str(huc) + '\n#################\n'

        ## plot rating curves (optional arg)
        if src_plot_option == 'True':
            if isdir(huc_plot_output_dir) == False:
                os.mkdir(huc_plot_output_dir)
            generate_src_plot(df_htable, huc_plot_output_dir)

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
            x='orig_discharge_cms', y='stage', data=plot_df, label="Orig SRC", ax=ax, color='blue'
        )
        sns.scatterplot(
            x='discharge_cms', y='stage', data=plot_df, label="SRC w/ BARC", ax=ax, color='orange'
        )
        # sns.lineplot(x='discharge_1_5', y='Stage_1_5', data=plot_df, color='green', ax=ax)
        # plt.fill_between(plot_df['discharge_1_5'], plot_df['Stage_1_5'],alpha=0.5)
        # plt.text(plot_df['discharge_1_5'].median(), plot_df['Stage_1_5'].median(), "NWM 1.5yr: " + str(plot_df['Stage_1_5'].median()))
        ax.legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '_barc.png', dpi=175, bbox_inches='tight')
        plt.close()


def multi_process(bathy_rc_lookup, procs_list):
    print(f"Applying bathy adjustment calcs for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        map_output = pool.map(bathy_rc_lookup, procs_list)
        # log_file.write(str(map_output))
    log_file.writelines(["%s\n" % item for item in map_output])


if __name__ == '__main__':
    # output_src,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName
    parser = argparse.ArgumentParser(
        description="Estimate the unaccounted for channel bathymetry using a regression-based estimate of channel XSec Area"
    )
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-bfull_geom',
        '--bankfull-xsec-input',
        help='Regression dataset w/ bankfull geometry by featureid (topwidth & xsec area)',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-j', '--number-of-jobs', help='number of workers', required=False, default=1, type=int
    )
    parser.add_argument(
        '-plots',
        '--src-plot-option',
        help='Optional (True or False): use this flag to create src plots for all hydroids. WARNING - long runtime',
        required=False,
        default='False',
        type=str,
    )

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bankfull_regres_filepath = args['bankfull_xsec_input']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    procs_list = []

    print('STARTING Bathy Adjusted Rating Curve routine...')
    ## Check that the input bankfull geom filepath exists and then read it to dataframe
    if not isfile(bankfull_regres_filepath):
        print(
            '!!! Can not find the input bankfull geometry regression file: '
            + str(bankfull_regres_filepath)
        )
    else:
        ## Read the Manning's n csv (ensure that it contains feature_id, channel mannings, floodplain mannings)
        print('Importing the bankfull regression data file: ' + bankfull_regres_filepath)
        df_bfull_geom = pd.read_csv(bankfull_regres_filepath, dtype={'COMID': int})
        if 'COMID' not in df_bfull_geom.columns and 'feature_id' not in df_bfull_geom.columns:
            print('Missing required data column ("feature_id" or "COMID")!!! --> ' + df_bfull_geom)
        else:
            print('Running Bathy Adjust Rating Curve (BARC)...')

            ## Print message to user and initiate run clock
            print('Writing progress to log file here: ' + str(join(fim_dir, 'log_BARC.log')))
            print('This may take a few minutes...')
            ## Create a time var to log run time
            begin_time = dt.datetime.now()

            ## Loop through hucs in the fim_dir and create list of variables to feed to multiprocessing
            huc_list = os.listdir(fim_dir)
            huc_pass_list = []
            for huc in huc_list:
                if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
                    # output_src,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName
                    in_src_filename = join(fim_dir, huc, 'src_full_crosswalked.csv')
                    out_src_filename = join(fim_dir, huc, 'src_full_crosswalked_BARC.csv')
                    htable_filename = join(fim_dir, huc, 'hydroTable.csv')
                    output_bath_filename = join(fim_dir, huc, 'bathy_crosswalk_calcs.csv')
                    output_bathy_thalweg_fileName = join(fim_dir, huc, 'bathy_thalweg_flag.csv')
                    output_bathy_streamorder_fileName = join(
                        fim_dir, huc, 'bathy_stream_order_calcs.csv'
                    )
                    output_bathy_thalweg_fileName = join(fim_dir, huc, 'bathy_thalweg_flag.csv')
                    output_bathy_xs_lookup_fileName = join(
                        fim_dir, huc, 'bathy_xs_area_hydroid_lookup.csv'
                    )
                    huc_plot_output_dir = join(fim_dir, huc, 'src_plots')

                    if isfile(in_src_filename):
                        print(str(huc))
                        huc_pass_list.append(str(huc))
                        procs_list.append(
                            [
                                in_src_filename,
                                df_bfull_geom,
                                output_bath_filename,
                                output_bathy_streamorder_fileName,
                                output_bathy_thalweg_fileName,
                                output_bathy_xs_lookup_fileName,
                                htable_filename,
                                out_src_filename,
                                huc,
                                src_plot_option,
                                huc_plot_output_dir,
                            ]
                        )
                    else:
                        print(
                            str(huc)
                            + ' --> can not find the src_full_crosswalked.csv in the fim output dir: '
                            + str(join(fim_dir, huc))
                        )

            ## initiate log file
            print(
                f"Applying bathy adjustment calcs for {len(procs_list)} hucs using {number_of_jobs} jobs..."
            )
            sys.__stdout__ = sys.stdout
            log_file = open(join(fim_dir, 'logs', 'log_barc.log'), "w")
            sys.stdout = log_file
            log_file.write('START TIME: ' + str(begin_time) + '\n')
            log_file.writelines(["%s\n" % item for item in huc_pass_list])

            ## Write env variables to log files
            log_file.write(
                'sa_ratio_flag = '
                + str(sa_ratio_flag)
                + ' --> Flag: Surface area ratio value to identify possible thalweg notch "jump" (SA x+1 / SA x)'
                + '\n'
            )
            log_file.write(
                'thal_stg_limit = '
                + str(thal_stg_limit)
                + ' --> Threshold: Stage value limit below which to look for the surface area ratio flag (only flag thalweg notch below this threshold)'
                + '\n'
            )
            log_file.write(
                'bankful_xs_ratio_flag = '
                + str(bankful_xs_ratio_flag)
                + ' --> Flag: Identify bogus BARC adjusted values where the regression bankfull XS Area/SRC bankfull area is > threshold (topwidth crosswalk issues or bad bankfull regression data points??)'
                + '\n'
            )
            log_file.write(
                'bathy_xsarea_flag = '
                + str(bathy_xsarea_flag)
                + ' --> Flag: Cross section area limit to cap the amount of bathy XS area added to the SRC. Limits the bathy_calc_xs_area/ BANKFULL_XSEC_AREA to the specified threshold'
                + '\n'
            )
            log_file.write(
                'thal_hyd_radius_flag = '
                + str(thal_hyd_radius_flag)
                + ' --> Flag: Idenitify possible erroneous BARC-adjusted hydraulic radius values. BARC discharge values greater than the specified threshold and within the thal_stg_limit are set to 0'
                + '\n'
            )
            log_file.write(
                'ignore_streamorder = '
                + str(ignore_streamorder)
                + ' --> Do not perform BARC for streamorders >= provided value'
                + '\n'
            )
            log_file.write('#########################################################\n\n')

            ## Pass huc procs_list to multiprocessing function
            multi_process(bathy_rc_lookup, procs_list)

            ## Record run time and close log file
            end_time = dt.datetime.now()
            log_file.write('END TIME: ' + str(end_time) + '\n')
            tot_run_time = end_time - begin_time
            log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
            sys.stdout = sys.__stdout__
            log_file.close()
