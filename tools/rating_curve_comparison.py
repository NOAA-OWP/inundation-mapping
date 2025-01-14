#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import re
import shutil
import sys
import time
import traceback
import warnings
from functools import reduce
from multiprocessing import Pool
from os.path import isfile, join
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import seaborn as sns
from rasterio import features as riofeatures
from rasterio import plot as rioplot
from shapely.geometry import Polygon


gpd.options.io_engine = "pyogrio"


warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Plot Rating Curves and Compare to USGS Gages

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    output_dir : str
        Directory containing rating curve plots and tables.
    usgs_gages_filename : str
        File name of USGS rating curves.
    nwm_flow_dir : str
        Directory containing NWM recurrence flows files.
    number_of_jobs : str
        Number of jobs.
    stat_groups : str
        string of columns to group eval metrics.
"""


def check_file_age(file):
    '''
    Checks if file exists, determines the file age, and recommends
    updating if older than 1 month.

    Returns
    -------
    None.

    '''
    file = Path(file)
    if file.is_file():
        modification_time = file.stat().st_mtime
        current_time = time.time()
        file_age_days = (current_time - modification_time) / 86400
        if file_age_days > 30:
            check = (
                f'{file.name} is {int(file_age_days)} days old, consider updating.\n'
                'Update with rating_curve_get_usgs_curves.py'
            )
        else:
            check = f'{file.name} is {int(file_age_days)} days old.'

        return check


# recurr_intervals = ['recurr_1_5_cms.csv','recurr_5_0_cms.csv','recurr_10_0_cms.csv']


def generate_rating_curve_metrics(args):
    elev_table_filename = args[0]
    branches_folder = args[1]
    usgs_gages_filename = args[2]
    usgs_recurr_stats_filename = args[3]
    nwm_recurr_data_filename = args[4]
    rc_comparison_plot_filename = args[5]
    nwm_flow_dir = args[6]
    catfim_flows_filename = args[7]
    huc = args[8]
    alt_plot = args[9]
    single_plot = args[10]

    logging.info("Generating rating curve metrics for huc: " + str(huc))
    elev_table = pd.read_csv(
        elev_table_filename,
        dtype={'location_id': object, 'feature_id': object, 'HydroID': object, 'levpa_id': object},
    )

    # Filter out null and non-integer location_id entries (the crosswalk steps tries to fill AHPS only sites with the nws_lid)
    elev_table = elev_table.dropna(subset=['location_id'])
    elev_table = elev_table[elev_table['location_id'].apply(lambda x: str(x).isdigit())]

    # Read in the USGS gages rating curve database csv
    usgs_gages = pd.read_csv(usgs_gages_filename, dtype={'location_id': object, 'feature_id': object})

    # Aggregate FIM4 hydroTables
    if not elev_table.empty:
        hydrotable = pd.DataFrame()
        for branch in elev_table.levpa_id.unique():
            branch_elev_table = elev_table.loc[elev_table.levpa_id == branch].copy()
            # branch_elev_table = elev_table.loc[(elev_table.levpa_id == branch) & (elev_table.location_id.notnull())].copy()
            branch_hydrotable = pd.read_csv(
                join(branches_folder, str(branch), f'hydroTable_{branch}.csv'),
                dtype={
                    'HydroID': object,
                    'feature_id': object,
                    'obs_source': object,
                    'last_updated': object,
                    'submitter': object,
                },
            )
            # Only pull SRC for hydroids that are in this branch
            branch_hydrotable = branch_hydrotable.loc[
                branch_hydrotable.HydroID.isin(branch_elev_table.HydroID)
            ]
            branch_hydrotable = branch_hydrotable.drop(columns=['order_'])
            # Join SRC with elevation data
            branch_elev_table = branch_elev_table.rename(columns={'feature_id': 'fim_feature_id'})
            branch_hydrotable = branch_hydrotable.merge(branch_elev_table, on="HydroID")
            # Append to full rating curve dataframe
            if hydrotable.empty:
                hydrotable = branch_hydrotable
            else:
                hydrotable = pd.concat([hydrotable, branch_hydrotable])

        # Join rating curves with elevation data
        # elev_table = elev_table.rename(columns={'feature_id':'fim_feature_id'})
        # hydrotable = hydrotable.merge(elev_table, on="HydroID")
        if 'location_id' in hydrotable.columns:
            relevant_gages = list(hydrotable.location_id.unique())
        else:
            relevant_gages = []
        usgs_gages = usgs_gages[usgs_gages['location_id'].isin(relevant_gages)]
        usgs_gages = usgs_gages.reset_index(drop=True)

        if len(usgs_gages) > 0:
            # Adjust rating curve to elevation
            hydrotable['elevation_ft'] = (
                hydrotable.stage + hydrotable.dem_adj_elevation
            ) * 3.28084  # convert from m to ft
            # hydrotable['raw_elevation_ft'] = (hydrotable.stage + hydrotable.dem_elevation) * 3.28084 # convert from m to ft
            hydrotable['discharge_cfs'] = hydrotable.discharge_cms * 35.3147
            usgs_gages = usgs_gages.rename(
                columns={"flow": "discharge_cfs", "elevation_navd88": "elevation_ft"}
            )

            hydrotable['source'] = "FIM"
            usgs_gages['source'] = "USGS"
            limited_hydrotable = hydrotable.filter(
                items=[
                    'location_id',
                    'elevation_ft',
                    'discharge_cfs',
                    'source',
                    'HydroID',
                    'levpa_id',
                    'dem_adj_elevation',
                ]
            )
            select_usgs_gages = usgs_gages.filter(
                items=['location_id', 'elevation_ft', 'discharge_cfs', 'source']
            )
            if (
                'default_discharge_cms' in hydrotable.columns
            ):  # check if both "FIM" and "FIM_default" SRCs are available
                hydrotable['default_discharge_cfs'] = hydrotable.default_discharge_cms * 35.3147
                limited_hydrotable_default = hydrotable.filter(
                    items=[
                        'location_id',
                        'elevation_ft',
                        'default_discharge_cfs',
                        'HydroID',
                        'levpa_id',
                        'dem_adj_elevation',
                    ]
                )
                limited_hydrotable_default['discharge_cfs'] = limited_hydrotable_default.default_discharge_cfs
                limited_hydrotable_default['source'] = "FIM_default"
                rating_curves = pd.concat([limited_hydrotable, select_usgs_gages])
                rating_curves = pd.concat([rating_curves, limited_hydrotable_default])
            else:
                rating_curves = pd.concat([limited_hydrotable, select_usgs_gages])

            # Add stream order
            stream_orders = hydrotable.filter(items=['location_id', 'order_']).drop_duplicates()
            rating_curves = rating_curves.merge(stream_orders, on='location_id')
            rating_curves['order_'].fillna(0, inplace=True)
            rating_curves['order_'] = rating_curves['order_'].astype('int')

            # NWM recurr intervals
            recurr_intervals = ("2", "5", "10", "25", "50")
            recurr_dfs = []
            for interval in recurr_intervals:
                recurr_file = join(nwm_flow_dir, 'nwm3_17C_recurr_{}_0_cms.csv'.format(interval))
                df = pd.read_csv(recurr_file, dtype={'feature_id': str})
                # Update column names
                df = df.rename(columns={"discharge": interval})
                recurr_dfs.append(df)

            # Merge NWM recurr intervals into a single layer
            nwm_recurr_intervals_all = reduce(
                lambda x, y: pd.merge(x, y, on='feature_id', how='outer'), recurr_dfs
            )
            nwm_recurr_intervals_all = pd.melt(
                nwm_recurr_intervals_all,
                id_vars=['feature_id'],
                value_vars=recurr_intervals,
                var_name='recurr_interval',
                value_name='discharge_cms',
            )

            # Append catfim data (already set up in format similar to nwm_recurr_intervals_all)
            cat_fim = pd.read_csv(catfim_flows_filename, dtype={'feature_id': str})
            nwm_recurr_intervals_all = pd.concat([nwm_recurr_intervals_all, cat_fim])

            # Convert discharge to cfs and filter
            nwm_recurr_intervals_all['discharge_cfs'] = nwm_recurr_intervals_all.discharge_cms * 35.3147
            nwm_recurr_intervals_all = nwm_recurr_intervals_all.filter(
                items=['discharge_cfs', 'recurr_interval', 'feature_id']
            ).drop_duplicates()

            # Identify unique gages
            usgs_crosswalk = hydrotable.filter(items=['location_id', 'feature_id']).drop_duplicates()
            usgs_crosswalk = usgs_crosswalk.dropna(subset=['location_id'])

            nwm_recurr_data_table = pd.DataFrame()
            # usgs_recurr_data = pd.DataFrame()

            # Interpolate USGS/FIM elevation at each gage
            for index, gage in usgs_crosswalk.iterrows():
                # Interpolate USGS elevation at NWM recurrence intervals
                usgs_rc = rating_curves.loc[
                    (rating_curves.location_id == gage.location_id) & (rating_curves.source == "USGS")
                ]

                if len(usgs_rc) < 1:
                    logging.info(
                        f"missing USGS rating curve data for usgs station {gage.location_id} in huc {huc}"
                    )
                    continue

                str_order = np.unique(usgs_rc.order_).item()
                feature_id = str(gage.feature_id)

                usgs_pred_elev, feature_index = get_recurr_intervals(
                    usgs_rc, usgs_crosswalk, nwm_recurr_intervals_all
                )

                # Handle sites missing data
                if len(usgs_pred_elev) < 1:
                    logging.info(
                        f"WARNING: missing USGS elevation data for usgs station {gage.location_id} in huc {huc}"
                    )
                    continue

                # Clean up data
                usgs_pred_elev['location_id'] = gage.location_id
                usgs_pred_elev = usgs_pred_elev.filter(
                    items=['location_id', 'recurr_interval', 'discharge_cfs', 'pred_elev']
                )
                usgs_pred_elev = usgs_pred_elev.rename(columns={"pred_elev": "USGS"})

                # Interpolate FIM elevation at NWM recurrence intervals
                fim_rc = rating_curves.loc[
                    (rating_curves.location_id == gage.location_id) & (rating_curves.source == "FIM")
                ]

                if len(fim_rc) < 1:
                    logging.info(
                        f"missing FIM rating curve data for usgs station {gage.location_id} in huc {huc}"
                    )
                    continue

                if feature_index is not None:
                    fim_pred_elev, feature_index = get_recurr_intervals(
                        fim_rc, usgs_crosswalk, nwm_recurr_intervals_all, feature_index
                    )

                # Handle sites missing data
                if len(fim_pred_elev) < 1:
                    logging.info(
                        f"WARNING: missing FIM elevation data for usgs station {gage.location_id} in huc {huc}"
                    )
                    continue

                # Clean up data
                fim_pred_elev = fim_pred_elev.rename(columns={"pred_elev": "FIM"})
                fim_pred_elev = fim_pred_elev.filter(items=['recurr_interval', 'discharge_cfs', 'FIM'])
                usgs_pred_elev = usgs_pred_elev.merge(fim_pred_elev, on=['recurr_interval', 'discharge_cfs'])

                # Add attributes
                usgs_pred_elev['HUC'] = huc
                usgs_pred_elev['HUC4'] = huc[0:4]
                usgs_pred_elev['str_order'] = str_order
                usgs_pred_elev['feature_id'] = feature_id

                # Melt dataframe
                usgs_pred_elev = pd.melt(
                    usgs_pred_elev,
                    id_vars=[
                        'location_id',
                        'feature_id',
                        'recurr_interval',
                        'discharge_cfs',
                        'HUC',
                        'HUC4',
                        'str_order',
                    ],
                    value_vars=['USGS', 'FIM'],
                    var_name="source",
                    value_name='elevation_ft',
                )
                nwm_recurr_data_table = pd.concat([nwm_recurr_data_table, usgs_pred_elev])

                # Interpolate FIM elevation at USGS observations
                # fim_rc = fim_rc.merge(usgs_crosswalk, on="location_id")
                # usgs_rc = usgs_rc.rename(columns={"elevation_ft": "USGS"})
                #
                # # Sort stage in ascending order
                # usgs_rc = usgs_rc.sort_values('USGS',ascending=True)
                #
                # # Interpolate FIM elevation at USGS observations
                # usgs_rc['FIM'] = np.interp(
                #     usgs_rc.discharge_cfs.values,
                #     fim_rc['discharge_cfs'],
                #     fim_rc['elevation_ft'],
                #     left=np.nan,
                #     right=np.nan,
                # )
                # usgs_rc = usgs_rc[usgs_rc['FIM'].notna()]
                # usgs_rc = usgs_rc.drop(columns=["source"])
                #
                # # Melt dataframe
                # usgs_rc = pd.melt(
                #     usgs_rc,
                #     id_vars=['location_id', 'discharge_cfs', 'str_order'],
                #     value_vars=['USGS', 'FIM'],
                #     var_name="source",
                #     value_name='elevation_ft',
                # )
                #
                # if not usgs_rc.empty:
                #     usgs_recurr_data = pd.concat([usgs_recurr_data, usgs_rc])

            # Generate stats for all sites in huc
            # if not usgs_recurr_data.empty:
            #     usgs_recurr_stats_table = calculate_rc_stats_elev(usgs_recurr_data)
            #     usgs_recurr_stats_table.to_csv(usgs_recurr_stats_filename,index=False)

            # # Generate plots (not currently being used)
            # fim_elev_at_USGS_rc_plot_filename = join(
            #     dirname(rc_comparison_plot_filename), 'FIM_elevations_at_USGS_rc_' + str(huc) + '.png'
            # )
            # generate_facet_plot(usgs_recurr_data, fim_elev_at_USGS_rc_plot_filename)

            if not nwm_recurr_data_table.empty:
                nwm_recurr_data_table.discharge_cfs = np.round(nwm_recurr_data_table.discharge_cfs, 2)
                nwm_recurr_data_table.elevation_ft = np.round(nwm_recurr_data_table.elevation_ft, 2)
                nwm_recurr_data_table.to_csv(nwm_recurr_data_filename, index=False)
            if 'location_id' not in nwm_recurr_data_table.columns:
                logging.info(
                    f"WARNING: nwm_recurr_data_table is missing location_id column for gage {relevant_gages} "
                    f"in huc {huc}"
                )

            # plot rating curves
            if alt_plot:
                generate_rc_and_rem_plots(
                    rating_curves, rc_comparison_plot_filename, nwm_recurr_data_table, branches_folder
                )
            elif single_plot:
                generate_single_plot(rating_curves, rc_comparison_plot_filename, nwm_recurr_data_table)
            else:
                generate_facet_plot(rating_curves, rc_comparison_plot_filename, nwm_recurr_data_table)
        else:
            logging.info(f"no USGS data for gage(s): {relevant_gages} in huc {huc}")
    else:
        logging.info(f"no valid USGS gages found in huc {huc} (note: may be ahps sites without UGSG gages)")


def aggregate_metrics(output_dir, procs_list, stat_groups):
    # Default stat group to location_id
    if stat_groups is None:
        stat_groups = ['location_id']

    # agg_usgs_interp_elev_stats = join(output_dir,'agg_usgs_interp_elev_stats.csv')
    agg_nwm_recurr_flow_elev = join(output_dir, 'agg_nwm_recurr_flow_elevations.csv')
    agg_nwm_recurr_flow_elev_stats = join(
        output_dir, f"agg_nwm_recurr_flow_elev_stats_{'_'.join(stat_groups)}.csv"
    )

    # if os.path.isfile(agg_usgs_interp_elev_stats):
    #     os.remove(agg_usgs_interp_elev_stats)
    if os.path.isfile(agg_nwm_recurr_flow_elev):
        os.remove(agg_nwm_recurr_flow_elev)
    if os.path.isfile(agg_nwm_recurr_flow_elev_stats):
        os.remove(agg_nwm_recurr_flow_elev_stats)

    for huc in procs_list:
        # if os.path.isfile(huc[3]):
        #     usgs_recurr_stats = pd.read_csv(huc[3])
        #
        #     # Write/append usgs_recurr_stats
        #     if os.path.isfile(agg_usgs_interp_elev_stats):
        #         usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False, mode='a',header=False)
        #     else:
        #         usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False)
        if os.path.isfile(huc[4]):
            nwm_recurr_data = pd.read_csv(huc[4], dtype={'location_id': str, 'feature_id': str})

            # Write/append nwm_recurr_data
            if os.path.isfile(agg_nwm_recurr_flow_elev):
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev, index=False, mode='a', header=False)
            else:
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev, index=False)

    agg_stats = pd.read_csv(agg_nwm_recurr_flow_elev, dtype={'location_id': str, 'feature_id': str})

    agg_recurr_stats_table = calculate_rc_stats_elev(agg_stats, stat_groups)

    agg_recurr_stats_table.to_csv(agg_nwm_recurr_flow_elev_stats, index=False)

    return agg_recurr_stats_table


def generate_single_plot(rc, plot_filename, recurr_data_table):
    tmp_rc = rc.copy()

    # Filter FIM elevation based on USGS data
    for gage in rc.location_id.unique():
        rc = rc[rc.location_id == gage]

        plot_filename_splitext = os.path.splitext(plot_filename)
        gage_plot_filename = plot_filename_splitext[0] + '_' + gage + plot_filename_splitext[1]

        # print(recurr_data_table.head)
        try:
            min_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.min()
            max_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.max()
            min_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.min()
            max_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.max()
            ri100 = recurr_data_table[
                (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
            ].discharge_cfs.max()

            rc = rc.drop(
                rc[
                    (rc.location_id == gage)
                    & (rc.source == 'FIM')
                    & (
                        ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                        & (rc.discharge_cfs > max_q)
                    )
                ].index
            )
            rc = rc.drop(
                rc[
                    (rc.location_id == gage)
                    & (rc.source == 'FIM')
                    & (rc.elevation_ft < min_elev - 2)
                    & (rc.discharge_cfs < min_q)
                ].index
            )

            if 'default_discharge_cfs' in rc.columns:  # Plot both "FIM" and "FIM_default" rating curves
                rc = rc.drop(
                    rc[
                        (rc.location_id == gage)
                        & (rc.source == 'FIM_default')
                        & (
                            ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                            & (rc.discharge_cfs > max_q)
                        )
                    ].index
                )
                rc = rc.drop(
                    rc[
                        (rc.location_id == gage)
                        & (rc.source == 'FIM_default')
                        & (rc.elevation_ft < min_elev - 2)
                    ].index
                )
        except Exception as ex:
            summary = traceback.StackSummary.extract(traceback.walk_stack(None))
            logging.info("WARNING: rating curve dataframe not processed correctly...")
            logging.info(f'Summary: {summary} \n Exception: \n {repr(ex)}')

        rc = rc.rename(columns={"location_id": "USGS Gage"})

        # split out branch 0 FIM data
        rc['source_branch'] = np.where(
            (rc.source == 'FIM') & (rc.levpa_id == '0'),
            'FIM_b0',
            np.where((rc.source == 'FIM_default') & (rc.levpa_id == '0'), 'FIM_default_b0', rc.source),
        )
        # rc['source_branch'] = np.where(
        #     (rc.source == 'FIM_default') & (rc.levpa_id == '0'), 'FIM_default_b0', rc.source
        # )

        ## Generate rating curve plots
        num_plots = len(rc["USGS Gage"].unique())
        if num_plots > 3:
            columns = num_plots // 3
        else:
            columns = 1

        sns.set(style="ticks")

        # Plot both "FIM" and "FIM_default" rating curves
        if '0' in rc.levpa_id.values:  # checks to see if branch zero data exists in the rating curve df
            hue_order = (
                ['USGS', 'FIM', 'FIM_default', 'FIM_b0', 'FIM_default_b0']
                if 'default_discharge_cfs' in rc.columns
                else ['USGS', 'FIM', 'FIM_b0']
            )
            kw = (
                {
                    'color': ['blue', 'green', 'orange', 'green', 'orange'],
                    'linestyle': ["-", "-", "-", "--", "--"],
                }
                if 'default_discharge_cfs' in rc.columns
                else {'color': ['blue', 'green', 'green'], 'linestyle': ["-", "-", "--"]}
            )
        else:
            hue_order = (
                ['USGS', 'FIM', 'FIM_default'] if 'default_discharge_cfs' in rc.columns else ['USGS', 'FIM']
            )
            kw = (
                {'color': ['blue', 'green', 'orange'], 'linestyle': ["-", "-", "-"]}
                if 'default_discharge_cfs' in rc.columns
                else {'color': ['blue', 'green'], 'linestyle': ["-", "_"]}
            )
        # Facet Grid
        g = sns.FacetGrid(
            rc,
            col="USGS Gage",
            hue="source_branch",
            hue_order=hue_order,
            sharex=False,
            sharey=False,
            col_wrap=columns,
            height=3.5,
            aspect=1.65,
            hue_kws=kw,
        )
        g.map(plt.plot, "discharge_cfs", "elevation_ft", linewidth=2, alpha=0.8)
        g.set_axis_labels(x_var="Discharge (cfs)", y_var="Elevation (ft)")

        ## Plot recurrence intervals
        axes = g.axes_dict
        for gage in axes:
            ax = axes[gage]
            plt.sca(ax)
            try:
                recurr_data = recurr_data_table[
                    (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
                ].filter(items=['recurr_interval', 'discharge_cfs'])
                for i, r in recurr_data.iterrows():
                    if not r.recurr_interval.isnumeric():
                        continue  # skip catfim flows
                    label = 'NWM 17C\nRecurrence' if r.recurr_interval == '2' else None  # only label 2 yr
                    plt.axvline(
                        x=r.discharge_cfs, c='purple', linewidth=0.5, label=label
                    )  # plot recurrence intervals
                    plt.text(
                        r.discharge_cfs,
                        ax.get_ylim()[1] - (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.03,
                        r.recurr_interval,
                        size='small',
                        c='purple',
                    )
            except Exception as ex:
                summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                logging.info("WARNING: Could not plot recurrence intervals...")
                logging.info(f'Summary: {summary} \n Exception: \n {repr(ex)}')

        # Adjust the arrangement of the plots
        g.fig.tight_layout(w_pad=1)
        g.add_legend()

        plt.savefig(gage_plot_filename)
        plt.close()

        rc = tmp_rc


def generate_facet_plot(rc, plot_filename, recurr_data_table):
    # Filter FIM elevation based on USGS data
    gage_max_q = {}
    for gage in rc.location_id.unique():
        # print(recurr_data_table.head)
        try:
            min_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.min()
            max_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.max()
            min_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.min()
            max_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.max()
            gage_max_q[gage] = max_q
            ri100 = recurr_data_table[
                (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
            ].discharge_cfs.max()

            rc = rc.drop(
                rc[
                    (rc.location_id == gage)
                    & (rc.source == 'FIM')
                    & (
                        ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                        & (rc.discharge_cfs > max_q)
                    )
                ].index
            )
            rc = rc.drop(
                rc[
                    (rc.location_id == gage)
                    & (rc.source == 'FIM')
                    & (rc.elevation_ft < min_elev - 2)
                    & (rc.discharge_cfs < min_q)
                ].index
            )

            if 'default_discharge_cfs' in rc.columns:  # Plot both "FIM" and "FIM_default" rating curves
                rc = rc.drop(
                    rc[
                        (rc.location_id == gage)
                        & (rc.source == 'FIM_default')
                        & (
                            ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                            & (rc.discharge_cfs > max_q)
                        )
                    ].index
                )
                rc = rc.drop(
                    rc[
                        (rc.location_id == gage)
                        & (rc.source == 'FIM_default')
                        & (rc.elevation_ft < min_elev - 2)
                    ].index
                )
        except Exception as ex:
            summary = traceback.StackSummary.extract(traceback.walk_stack(None))
            logging.info("WARNING: rating curve dataframe not processed correctly...")
            logging.info(f'Summary: {summary} \n Exception: \n {repr(ex)}')

    rc = rc.rename(columns={"location_id": "USGS Gage"})

    # split out branch 0 FIM data
    rc['source_branch'] = np.where(
        (rc.source == 'FIM') & (rc.levpa_id == '0'),
        'FIM_b0',
        np.where((rc.source == 'FIM_default') & (rc.levpa_id == '0'), 'FIM_default_b0', rc.source),
    )
    # rc['source_branch'] = np.where(
    #     (rc.source == 'FIM_default') & (rc.levpa_id == '0'), 'FIM_default_b0', rc.source
    # )

    ## Generate rating curve plots
    num_plots = len(rc["USGS Gage"].unique())
    if num_plots > 3:
        columns = num_plots // 3
    else:
        columns = 1

    sns.set(style="ticks")

    # Plot both "FIM" and "FIM_default" rating curves
    if '0' in rc.levpa_id.values:  # checks to see if branch zero data exists in the rating curve df
        hue_order = (
            ['USGS', 'FIM', 'FIM_default', 'FIM_b0', 'FIM_default_b0']
            if 'default_discharge_cfs' in rc.columns
            else ['USGS', 'FIM', 'FIM_b0']
        )
        kw = (
            {
                'color': ['blue', 'green', 'orange', 'green', 'orange'],
                'linestyle': ["-", "-", "-", "--", "--"],
            }
            if 'default_discharge_cfs' in rc.columns
            else {'color': ['blue', 'green', 'green'], 'linestyle': ["-", "-", "--"]}
        )
    else:
        hue_order = (
            ['USGS', 'FIM', 'FIM_default'] if 'default_discharge_cfs' in rc.columns else ['USGS', 'FIM']
        )
        kw = (
            {'color': ['blue', 'green', 'orange'], 'linestyle': ["-", "-", "-"]}
            if 'default_discharge_cfs' in rc.columns
            else {'color': ['blue', 'green'], 'linestyle': ["-", "_"]}
        )
    # Facet Grid
    g = sns.FacetGrid(
        rc,
        col="USGS Gage",
        hue="source_branch",
        hue_order=hue_order,
        sharex=False,
        sharey=False,
        col_wrap=columns,
        height=3.5,
        aspect=1.65,
        hue_kws=kw,
    )
    g.map(plt.plot, "discharge_cfs", "elevation_ft", linewidth=2, alpha=0.8)
    g.set_axis_labels(x_var="Discharge (cfs)", y_var="Elevation (ft)")

    ## Plot recurrence intervals
    axes = g.axes_dict
    recurr_data_max = {}
    for gage in axes:
        ax = axes[gage]
        plt.sca(ax)
        try:
            recurr_data = recurr_data_table[
                (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
            ].filter(items=['recurr_interval', 'discharge_cfs'])
            recurr_q_max = recurr_data['discharge_cfs'].max()
            recurr_data_max[gage] = recurr_q_max
            for i, r in recurr_data.iterrows():
                if not r.recurr_interval.isnumeric():
                    continue  # skip catfim flows
                label = 'NWM 17C\nRecurrence' if r.recurr_interval == '2' else None  # only label 2 yr
                plt.axvline(
                    x=r.discharge_cfs, c='purple', linewidth=0.5, label=label
                )  # plot recurrence intervals
                plt.text(
                    r.discharge_cfs,
                    ax.get_ylim()[1] - (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.03,
                    r.recurr_interval,
                    size='small',
                    c='purple',
                )
        except Exception as ex:
            summary = traceback.StackSummary.extract(traceback.walk_stack(None))
            logging.info("WARNING: Could not plot recurrence intervals...")
            logging.info(f'Summary: {summary} \n Exception: \n {repr(ex)}')

    padding = 0.05
    for gage in g.axes_dict:
        ax = g.axes_dict[gage]
        max_q = gage_max_q.get(gage, None)
        recurr_q_max = recurr_data_max.get(gage, None)
        if max_q is not None and not np.isnan(max_q):
            if max_q > recurr_q_max:    
                max_x = max_q
            else:
                max_x = recurr_q_max + (0.001 * recurr_q_max) # To make sure vertical lines are displayed in the plot
        # For gages without USGS rating curve data
        else:
            max_x = rc.discharge_cfs.max()
        padding_value = max_x * padding
        ax.set_xlim(0 - padding_value ,max_x)
        
    # Adjust the arrangement of the plots
    g.fig.tight_layout(w_pad=1)
    g.add_legend()

    plt.savefig(plot_filename)
    plt.close()


def generate_rc_and_rem_plots(rc, plot_filename, recurr_data_table, branches_folder):
    ## Set up figure
    num_plots = len(rc["location_id"].unique())
    fig = plt.figure(figsize=(6, 2.4 * num_plots))
    gs = fig.add_gridspec(num_plots, 2, width_ratios=[2, 3])
    ax = gs.subplots()
    if ax.ndim == 1:  # hucs with only one plot will only have one-dimensional axes;
        ax = np.expand_dims(ax, axis=0)  # the axes manipulations below require 2 dimensions
    plt.tight_layout(w_pad=1)

    # Create a dictionary with location_id as keys and branch id as values
    gage_branch_dict = rc.groupby('location_id')['levpa_id'].first().to_dict()

    for i, gage in enumerate(gage_branch_dict):
        #####################################################################################################
        # Filter FIM elevation based on USGS data

        min_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.min()
        max_elev = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].elevation_ft.max()
        min_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.min()
        max_q = rc.loc[(rc.location_id == gage) & (rc.source == 'USGS')].discharge_cfs.max()
        ri100 = recurr_data_table[
            (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
        ].discharge_cfs.max()

        rc = rc.drop(
            rc[
                (rc.location_id == gage)
                & (rc.source == 'FIM')
                & (
                    ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                    & (rc.discharge_cfs > max_q)
                )
            ].index
        )
        rc = rc.drop(
            rc[
                (rc.location_id == gage)
                & (rc.source == 'FIM')
                & (rc.elevation_ft < min_elev - 2)
                & (rc.discharge_cfs < min_q)
            ].index
        )

        if 'default_discharge_cfs' in rc.columns:  # Plot both "FIM" and "FIM_default" rating curves
            rc = rc.drop(
                rc[
                    (rc.location_id == gage)
                    & (rc.source == 'FIM_default')
                    & (
                        ((rc.elevation_ft > (max_elev + 2)) | (rc.discharge_cfs > ri100))
                        & (rc.discharge_cfs > max_q)
                    )
                ].index
            )
            rc = rc.drop(
                rc[
                    (rc.location_id == gage) & (rc.source == 'FIM_default') & (rc.elevation_ft < min_elev - 2)
                ].index
            )

        ######################################################################################################
        ## Read in reaches, catchment raster, and rem raster
        branch = gage_branch_dict[gage]
        if os.path.isfile(
            os.path.join(
                branches_folder,
                branch,
                f'demDerived_reaches_split_filtered_addedAttributes_crosswalked_{branch}.gpkg',
            )
        ):
            reaches = gpd.read_file(
                os.path.join(
                    branches_folder,
                    branch,
                    f'demDerived_reaches_split_filtered_addedAttributes_crosswalked_{branch}.gpkg',
                )
            )
            reach = reaches[reaches.HydroID == hydroid]
        with rasterio.open(
            os.path.join(
                branches_folder, branch, f'gw_catchments_reaches_filtered_addedAttributes_{branch}.tif'
            )
        ) as catch_rast:
            catchments = catch_rast.read()
        with rasterio.open(os.path.join(branches_folder, branch, f'rem_zeroed_masked_{branch}.tif')) as rem:
            rem_transform = rem.transform
            rem_extent = rioplot.plotting_extent(rem)
            rem_sub25 = rem.read()
            # Set all pixels above the SRC calculation height to nan
            rem_sub25[np.where(rem_sub25 > 25.3)] = -9999.0
            rem_sub25[np.where(rem_sub25 == -9999.0)] = np.nan

        # Plot the rating curve
        ax[i, 1].plot(
            "discharge_cfs",
            "elevation_ft",
            data=rc[(rc.source == 'USGS') & (rc.location_id == gage)],
            linewidth=2,
            alpha=0.8,
            label='USGS',
        )
        ax[i, 1].plot(
            "discharge_cfs",
            "elevation_ft",
            data=rc[(rc.source == 'FIM') & (rc.location_id == gage)],
            linewidth=2,
            alpha=0.8,
            label='FIM',
        )
        if 'default_discharge_cfs' in rc.columns:
            ax[i, 1].plot(
                "default_discharge_cfs",
                "elevation_ft",
                data=rc[(rc.source == 'FIM_default') & (rc.location_id == gage)],
                linewidth=2,
                alpha=0.8,
                label='FIM_default',
            )

        # Plot the recurrence intervals
        recurr_data = recurr_data_table[
            (recurr_data_table.location_id == gage) & (recurr_data_table.source == 'FIM')
        ].filter(items=['recurr_interval', 'discharge_cfs'])
        for _, r in recurr_data.iterrows():
            if not r.recurr_interval.isnumeric():
                continue  # skip catfim flows
            label = 'NWM 17C\nRecurrence' if r.recurr_interval == '2' else None  # only label 2 yr
            ax[i, 1].axvline(
                x=r.discharge_cfs, c='purple', linewidth=0.5, label=label
            )  # plot recurrence intervals
            ax[i, 1].text(
                r.discharge_cfs,
                ax[i, 1].get_ylim()[1] - (ax[i, 1].get_ylim()[1] - ax[i, 1].get_ylim()[0]) * 0.06,
                r.recurr_interval,
                size='small',
                c='purple',
            )

        # Get the hydroid
        hydroid = rc[rc.location_id == gage].HydroID.unique()[0]
        if not hydroid:
            logging.info(f'Gage {gage} in HUC {branch} has no HydroID')
            continue

        # Filter the reaches and REM by the hydroid
        catchment_rem = rem_sub25.copy()
        catchment_rem[np.where(catchments != int(hydroid))] = np.nan

        # Convert raster to WSE feet and limit to upper bound of rating curve
        dem_adj_elevation = rc[rc.location_id == gage].dem_adj_elevation.unique()[0]
        catchment_rem = (catchment_rem + dem_adj_elevation) * 3.28084
        max_elev = rc[(rc.source == 'FIM') & (rc.location_id == gage)].elevation_ft.max()
        catchment_rem[np.where(catchment_rem > max_elev)] = (
            np.nan
        )  # <-- Comment out this line to get the full raster that is
        # used in rating curve creation
        # Create polygon for perimeter/area stats
        catchment_rem_1s = catchment_rem.copy()
        catchment_rem_1s[np.where(~np.isnan(catchment_rem_1s))] = 1
        features = riofeatures.shapes(
            catchment_rem_1s, mask=~np.isnan(catchment_rem), transform=rem_transform, connectivity=8
        )
        del catchment_rem_1s
        features = [f for f in features]
        geom = [Polygon(f[0]['coordinates'][0]) for f in features]
        poly = gpd.GeoDataFrame({'geometry': geom})
        # These lines are calculating perimeter/area stats and can be removed if there is
        # a separate process set up later that calculates these for all hydroids within a catchment.
        # poly['perimeter'] = poly.length
        # poly['area'] = poly.area
        # # poly['perimeter_area_ratio'] = poly.length/poly.area
        # poly['perimeter_area_ratio_sqrt'] = poly.length/(poly.area**.5)
        bounds = poly.total_bounds
        bounds = ((bounds[0] - 20, bounds[2] + 20), (bounds[1] - 20, bounds[3] + 20))

        # REM plot
        if 'reach' in locals():
            reach.plot(ax=ax[i, 0], color='#999999', linewidth=0.9)
        im = ax[i, 0].imshow(
            rasterio.plot.reshape_as_image(catchment_rem),
            cmap='gnuplot',
            extent=rem_extent,
            interpolation='none',
        )
        plt.colorbar(im, ax=ax[i, 0], location='left')
        ax[i, 0].set_xbound(bounds[0])
        ax[i, 0].set_ybound(bounds[1])
        ax[i, 0].set_xticks([])
        ax[i, 0].set_yticks([])
        ax[i, 0].set_title(gage)

    del catchments, rem_sub25, catchment_rem
    ax[0, 1].legend()
    plt.savefig(plot_filename, dpi=200)
    plt.close()


# def get_recurr_intervals_fim(site_rc, usgs_crosswalk, nwm_recurr_intervals, feature_index):
#     usgs_site = site_rc.merge(usgs_crosswalk, on="location_id")
#     nwm_ids = len(usgs_site.feature_id.drop_duplicates())

#     if nwm_ids > 0:
#         try:
#             nwm_recurr_intervals = nwm_recurr_intervals.copy().loc[
#                 nwm_recurr_intervals.feature_id == usgs_site.feature_id.drop_duplicates().loc[feature_index]
#             ]
#             nwm_recurr_intervals['pred_elev'] = np.interp(
#                 nwm_recurr_intervals.discharge_cfs.values,
#                 usgs_site['discharge_cfs'],
#                 usgs_site['elevation_ft'],
#                 left=np.nan,
#                 right=np.nan,
#             )

#             return nwm_recurr_intervals
#         except Exception as ex:
#             summary = traceback.StackSummary.extract(traceback.walk_stack(None))
#             # logging.info("WARNING: get_recurr_intervals failed for some reason....")
#             # logging.info(f"*** {ex}")
#             # logging.info(''.join(summary.format()))
#             print(summary, repr(ex))
#             return []

#     else:
#         return []


def get_recurr_intervals(site_rc, usgs_crosswalk, nwm_recurr_intervals, feature_index=None):
    usgs_site = site_rc.merge(usgs_crosswalk, on="location_id")
    nwm_ids = len(usgs_site.feature_id.drop_duplicates())


    if nwm_ids > 0:
        try:
            if feature_index is None:
                min_discharge = site_rc.loc[(site_rc.source == 'USGS')].discharge_cfs.min()
                max_discharge = site_rc.loc[(site_rc.source == 'USGS')].discharge_cfs.max()
                discharge_range = max_discharge - min_discharge
                filtered = nwm_recurr_intervals.copy().loc[
                    nwm_recurr_intervals.feature_id == usgs_site.feature_id.drop_duplicates().iloc[0]
                ]
                min_q_recurr = filtered.discharge_cfs.min()
                max_q_recurr = filtered.discharge_cfs.max()
                spread_q = max_q_recurr - min_q_recurr
                ratio = spread_q / discharge_range
                # If there is only one feature_id for each location_id or the ratio is large enough
                if nwm_ids == 1 or ratio > 0.1:
                    feature_index = 0
                # If there is more one feature_id for each location_id and the ratio is not large enough
                else:
                    feature_index = 1
            nwm_recurr_intervals = nwm_recurr_intervals.copy().loc[
                nwm_recurr_intervals.feature_id == usgs_site.feature_id.drop_duplicates().iloc[feature_index]
                ]
            nwm_recurr_intervals['pred_elev'] = np.interp(
                nwm_recurr_intervals.discharge_cfs.values,
                usgs_site['discharge_cfs'],
                usgs_site['elevation_ft'],
                left=np.nan,
                right=np.nan,
            )

            return nwm_recurr_intervals, feature_index
        except Exception as ex:
            summary = traceback.StackSummary.extract(traceback.walk_stack(None))
            print(summary, repr(ex))
            return [], None

    else:
        return [], None


def calculate_rc_stats_elev(rc, stat_groups=None):
    usgs_elev = "USGS"
    src_elev = "FIM"

    # Collect any extra columns not associated with melt
    col_index = list(rc.columns)
    pivot_vars = ['source', 'elevation_ft']
    col_index = [col for col in col_index if col not in pivot_vars]

    # Unmelt elevation/source
    rc_unmelt = (
        rc.set_index(col_index)
        .pivot(columns="source")['elevation_ft']
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Calculate variables for NRMSE
    rc_unmelt["yhat_minus_y"] = rc_unmelt[src_elev] - rc_unmelt[usgs_elev]
    rc_unmelt["yhat_minus_y_squared"] = rc_unmelt["yhat_minus_y"] ** 2

    # Calculate metrics by group
    station_rc = rc_unmelt.groupby(stat_groups)

    # Calculate variables for NRMSE
    sum_y_diff = (
        station_rc.apply(lambda x: x["yhat_minus_y_squared"].sum())
        .reset_index(stat_groups, drop=False)
        .rename({0: "sum_y_diff"}, axis=1)
    )

    # Determine number of events that are modeled
    n = (
        station_rc.apply(lambda x: x[usgs_elev].count())
        .reset_index(stat_groups, drop=False)
        .rename({0: "n"}, axis=1)
    )

    # Determine the maximum/minimum USGS elevation
    y_max = (
        station_rc.apply(lambda x: x[usgs_elev].max())
        .reset_index(stat_groups, drop=False)
        .rename({0: "y_max"}, axis=1)
    )
    y_min = (
        station_rc.apply(lambda x: x[usgs_elev].min())
        .reset_index(stat_groups, drop=False)
        .rename({0: "y_min"}, axis=1)
    )

    # Collect variables for NRMSE
    nrmse_table = reduce(
        lambda x, y: pd.merge(x, y, on=stat_groups, how='outer'), [sum_y_diff, n, y_max, y_min]
    )
    nrmse_table_group = nrmse_table.groupby(stat_groups)

    # Calculate nrmse
    def NRMSE(x):
        if x['n'].values == 1:  # when n==1, NRME equation will return an `inf`
            return x['sum_y_diff'] ** 0.5
        else:
            return ((x['sum_y_diff'] / x['n']) ** 0.5) / (x['y_max'] - x['y_min'])

    nrmse = nrmse_table_group.apply(NRMSE).reset_index(stat_groups, drop=False).rename({0: "nrmse"}, axis=1)

    # Calculate Mean Absolute Depth Difference
    mean_abs_y_diff = (
        station_rc.apply(lambda x: (abs(x["yhat_minus_y"]).mean()))
        .reset_index(stat_groups, drop=False)
        .rename({0: "mean_abs_y_diff_ft"}, axis=1)
    )

    # Calculate Mean Depth Difference (non-absolute value)
    mean_y_diff = (
        station_rc.apply(lambda x: (x["yhat_minus_y"].mean()))
        .reset_index(stat_groups, drop=False)
        .rename({0: "mean_y_diff_ft"}, axis=1)
    )

    # Calculate Percent Bias
    percent_bias = (
        station_rc.apply(lambda x: 100 * (x["yhat_minus_y"].sum() / x[usgs_elev].sum()))
        .reset_index(stat_groups, drop=False)
        .rename({0: "percent_bias"}, axis=1)
    )

    rc_stat_table = reduce(
        lambda x, y: pd.merge(x, y, on=stat_groups, how='outer'),
        [n, nrmse, mean_abs_y_diff, mean_y_diff, percent_bias],
    )

    return rc_stat_table


def create_static_gpkg(output_dir, output_gpkg, agg_recurr_stats_table, gages_gpkg_filepath):
    '''
    Merges the output dataframe from aggregate_metrics() with the usgs gages GIS data
    '''
    # Load in the usgs_gages geopackage
    usgs_gages = gpd.read_file(gages_gpkg_filepath, engine='fiona')
    # Merge the stats for all of the recurrance intervals/thresholds
    usgs_gages = usgs_gages.merge(agg_recurr_stats_table, on='location_id')
    # Load in the rating curves file
    agg_nwm_recurr_flow_elev = join(output_dir, 'agg_nwm_recurr_flow_elevations.csv')
    agg_stats = pd.read_csv(agg_nwm_recurr_flow_elev, dtype={'location_id': str, 'feature_id': str})
    diff_table = calculate_rc_diff(agg_stats)
    # Merge recurrence interval difference table with points layer
    usgs_gages = usgs_gages.merge(diff_table, on='location_id')
    usgs_gages = usgs_gages.round(decimals=2)

    # Write to file
    usgs_gages.to_file(join(output_dir, output_gpkg), driver='GPKG', index=False, engine='fiona')

    # Create figure
    usgs_gages.replace(np.inf, np.nan, inplace=True)  # replace inf with nan for plotting
    fig, ax = plt.subplots(2, 2, figsize=(18, 10))

    # Bin data
    max_bin = usgs_gages['mean_abs_y_diff_ft'].max()
    bins = (0, 1, 3, 6, 9, max_bin if max_bin > 12 else 12)
    usgs_gages['mean_abs_y_diff_ft'] = pd.cut(usgs_gages['mean_abs_y_diff_ft'], bins=bins)

    max_bin = usgs_gages['mean_y_diff_ft'].max()
    min_bin = usgs_gages['mean_y_diff_ft'].min()
    bins = (min_bin if min_bin < -12 else -12, -9, -6, -3, -1, 0, 1, 3, 6, 9, max_bin if max_bin > 12 else 12)
    usgs_gages['mean_y_diff_ft'] = pd.cut(usgs_gages['mean_y_diff_ft'], bins=bins)

    # Create subplots
    sns.histplot(ax=ax[0, 0], y='nrmse', data=usgs_gages, binwidth=0.2, binrange=(0, 10))
    sns.countplot(ax=ax[1, 0], y='mean_abs_y_diff_ft', data=usgs_gages)
    sns.countplot(ax=ax[1, 1], y='mean_y_diff_ft', data=usgs_gages)
    sns.boxplot(
        ax=ax[0, 1],
        data=usgs_gages[['2', '5', '10', '25', '50', '100', 'action', 'minor', 'moderate', 'major']],
    )
    ax[0, 1].set(ylim=(-12, 12))

    fig.tight_layout()
    fig.savefig(join(output_dir, f'{output_gpkg}_summary_plots.png'.replace('.gpkg', '')))

    return


def calculate_rc_diff(rc):
    usgs_elev = "USGS"
    src_elev = "FIM"

    # Collect any extra columns not associated with melt
    col_index = list(rc.columns)
    pivot_vars = ['source', 'elevation_ft']
    col_index = [col for col in col_index if col not in pivot_vars]

    # Unmelt elevation/source
    rc_unmelt = (
        rc.set_index(col_index)
        .pivot(columns="source")['elevation_ft']
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Calculate water surface elevation difference at recurrence intervals
    rc_unmelt["yhat_minus_y"] = rc_unmelt[src_elev] - rc_unmelt[usgs_elev]
    # Remove duplicate location_id-recurr_interval pairs and pivot
    rc_unmelt = rc_unmelt.set_index(['location_id', 'recurr_interval'], verify_integrity=False)
    rc_unmelt = (
        rc_unmelt[~rc_unmelt.index.duplicated(keep='first')]
        .reset_index()
        .pivot(index='location_id', columns='recurr_interval', values='yhat_minus_y')
    )
    # Reorder columns
    rc_unmelt = rc_unmelt[['2', '5', '10', '25', '50', '100', 'action', 'minor', 'moderate', 'major']]

    return rc_unmelt


def evaluate_results(sierra_results=[], labels=[], save_location=''):
    '''
    Compares multiple Sierra Test results using a boxplot.

    Parameters
    ------------
    sierra_results : list
        List of GeoDataFrames with sierra test results.
    labels : list
        List of strings that will be used as labels for sierra_results.
        Length must be equal to sierra_results.
    save_location : str
        Path to save output boxplot figure.

    Example
    ------------
    from rating_curve_comparison import evaluate_results
    import geopandas as gpd

    sierra_1 = gpd.read_file("/data/path/to/fim_3_X_ms.gpkg")
    sierra_new = gpd.read_file("/data/path/to/fim_experiment.gpkg")

    evaluate_results([sierra_1, sierra_new], ["fim_3_X_ms", "fim_calibrate_SRC"], "/path/to/output.png")
    '''

    assert len(sierra_results) == len(labels), "Each Sierra Test results must also have a label"

    # Define recurrence intervals to plot
    recurr_intervals = ("2", "5", "10", "25", "50", "100", "action", "minor", "moderate", "major")

    # Assign labels to the input sierra test result dataframes
    for df, label in zip(sierra_results, labels):
        df['_version'] = label

    # Combine all dataframes into one
    all_results = sierra_results[0]
    all_results = pd.concat([all_results, sierra_results[1:]])

    # Melt results for boxplotting
    all_results_melted = all_results.melt(
        id_vars=["location_id", '_version'],
        value_vars=recurr_intervals,
        var_name='recurr_interval',
        value_name='error_ft',
    )

    # Plot all results in a comparison boxplot
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.boxplot(
        x='recurr_interval', y='error_ft', hue='_version', data=all_results_melted, ax=ax, fliersize=3
    )
    ax.set(ylim=(-30, 30))
    ax.grid()
    ax.legend(bbox_to_anchor=(1, 1), loc='upper right', title='FIM Version')
    ax.set_title('Sierra Test Results Comparison')
    plt.savefig(save_location)


if __name__ == '__main__':

    """
    Sample Usage:
    python3 /foss_fim/tools/rating_curve_comparison.py
        -fim_dir data/previous_fim/hand_4_5_8_0/
        -output_dir data/fim_performance/hand_4_5_8_0/rating_curve_comparison/
        -gages /data/inputs/usgs_gages/usgs_rating_curves.csv
        -flows /data/inputs/rating_curve/nwm_recur_flows/
        -catfim /data/inputs/usgs_gages/catfim_flows_cms.csv
        -j 40
    """

    parser = argparse.ArgumentParser(
        description='generate rating curve plots and tables for FIM and USGS gages'
    )
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-output_dir', '--output-dir', help='rating curves output folder', required=True, type=str
    )
    parser.add_argument('-gages', '--usgs-gages-filename', help='USGS rating curves', required=True, type=str)
    parser.add_argument('-flows', '--nwm-flow-dir', help='NWM recurrence flows dir', required=True, type=str)

    # TODO Sep 2024: catfim_flows_cms.csv should be renamed as it which made no sense.
    #    It has nothing to do with catfim
    parser.add_argument(
        '-catfim', '--catfim-flows-filename', help='Categorical FIM flows file', required=True, type=str
    )
    parser.add_argument(
        '-j', '--number-of-jobs', help='number of workers', required=False, default=1, type=int
    )
    parser.add_argument(
        '-group', '--stat-groups', help='column(s) to group stats', required=False, type=str, nargs='+'
    )
    parser.add_argument(
        '-pnts',
        '--stat-gages',
        help='takes 2 arguments: 1) file path of input usgs_gages.gpkg and 2) output GPKG name to write USGS '
        'gages with joined stats',
        required=False,
        type=str,
        nargs=2,
    )
    parser.add_argument(
        '-alt',
        '--alt-plot',
        help='Generate rating curve plots with REM maps',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-eval',
        '--evaluate-results',
        help='Create a boxplot comparison of multiple input Sierra Test results. '
        'Expects 2 arguments: 1) path to the Sierra Test results for comparison and 2) the corresponding '
        'label for the boxplot.',
        required=False,
        nargs=2,
        action='append',
    )
    parser.add_argument('-s', '--single-plot', help='Create single plots', action='store_true')
    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    output_dir = args['output_dir']
    usgs_gages_filename = args['usgs_gages_filename']
    nwm_flow_dir = args['nwm_flow_dir']
    catfim_flows_filename = args['catfim_flows_filename']
    number_of_jobs = args['number_of_jobs']
    stat_groups = args['stat_groups']
    alt_plot = args['alt_plot']
    eval = args['evaluate_results']
    single_plot = args['single_plot']
    if args['stat_gages']:
        gages_gpkg_filepath = args['stat_gages'][0]
        stat_gages = args['stat_gages'][1]
        assert os.path.exists(
            gages_gpkg_filepath
        ), f"{gages_gpkg_filepath} does not exist. Please specify a full path to a USGS geopackage (.gpkg)"
    else:
        stat_gages = None

    start_time = dt.datetime.now()
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    log_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    print("==========================================================================")
    # Open log file
    # Create log output
    level = (
        logging.INFO
    )  # using WARNING level to avoid benign? info messages ("Failed to auto identify EPSG: 7")
    format = '  %(message)s'
    log_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    os.makedirs(output_dir, exist_ok=True)
    handlers = [
        logging.FileHandler(os.path.join(output_dir, f'rating_curve_comparison_{log_dt_string}.log')),
        logging.StreamHandler(),
    ]
    logging.basicConfig(level=level, format=format, handlers=handlers)

    logging.info(".. (Sierra Test) / rating curve comparison")
    logging.info(f".. Started: {dt_string} \n")

    try:
        # Make sure that location_id is the only -group when using -pnts
        assert not stat_gages or (
            stat_gages and (not stat_groups or stat_groups == ['location_id'])
        ), "location_id is the only acceptable stat_groups argument when producting an output GPKG"
        # Make sure that the -pnts flag is used with the -eval flag
        assert not eval or (eval and stat_gages), "You must use the -pnts flag with the -eval flag"
        procs_list = []

        plots_dir = join(output_dir, 'plots')
        os.makedirs(plots_dir, exist_ok=True)
        tables_dir = join(output_dir, 'tables')
        os.makedirs(tables_dir, exist_ok=True)

        # Check age of gages csv and recommend updating if older than 30 days.
        print(check_file_age(usgs_gages_filename))

        merged_elev_table = []
        huc_list = [huc for huc in os.listdir(fim_dir) if re.search(r"^\d{6,8}$", huc)]
        for huc in huc_list:
            elev_table_filename = join(fim_dir, huc, 'usgs_elev_table.csv')
            branches_folder = join(fim_dir, huc, 'branches')
            usgs_recurr_stats_filename = join(tables_dir, f"usgs_interpolated_elevation_stats_{huc}.csv")
            nwm_recurr_data_filename = join(tables_dir, f"nwm_recurrence_flow_elevations_{huc}.csv")
            rc_comparison_plot_filename = join(plots_dir, f"FIM-USGS_rating_curve_comparison_{huc}.png")

            if isfile(elev_table_filename):
                procs_list.append(
                    [
                        elev_table_filename,
                        branches_folder,
                        usgs_gages_filename,
                        usgs_recurr_stats_filename,
                        nwm_recurr_data_filename,
                        rc_comparison_plot_filename,
                        nwm_flow_dir,
                        catfim_flows_filename,
                        huc,
                        alt_plot,
                        single_plot,
                    ]
                )
                # Aggregate all of the individual huc elev_tables into one aggregate
                #      for accessing all data in one csv
                read_elev_table = pd.read_csv(
                    elev_table_filename,
                    dtype={'location_id': str, 'HydroID': str, 'huc': str, 'feature_id': int},
                )
                read_elev_table['huc'] = huc
                merged_elev_table.append(read_elev_table)

        # Output a concatenated elev_table to_csv
        if merged_elev_table:
            logging.info("Creating aggregate elev table csv")
            concat_elev_table = pd.concat(merged_elev_table)
            concat_elev_table['thal_burn_depth_meters'] = (
                concat_elev_table['dem_elevation'] - concat_elev_table['dem_adj_elevation']
            )
            concat_elev_table.to_csv(join(output_dir, 'agg_usgs_elev_table.csv'), index=False)

        # Initiate multiprocessing
        logging.info(
            f"Generating rating curve metrics for {len(procs_list)} hucs using {number_of_jobs} jobs"
        )
        with Pool(processes=number_of_jobs) as pool:
            pool.map(generate_rating_curve_metrics, procs_list)

        # Create point layer of usgs gages with joined stats attributes
        if stat_gages:
            logging.info("Creating usgs gages GPKG with joined rating curve summary stats")
            agg_recurr_stats_table = aggregate_metrics(output_dir, procs_list, ['location_id'])
            create_static_gpkg(output_dir, stat_gages, agg_recurr_stats_table, gages_gpkg_filepath)
            del agg_recurr_stats_table  # memory cleanup
        else:  # if not producing GIS layer, just aggregate metrics
            logging.info(f"Aggregating rating curve metrics for {len(procs_list)} hucs")
            aggregate_metrics(output_dir, procs_list, stat_groups)

        logging.info('Delete intermediate tables')
        shutil.rmtree(tables_dir, ignore_errors=True)

        # Compare current sierra test results to previous tests
        if eval:
            # Transpose comparison sierra results
            sierra_test_paths, sierra_test_labels = np.array(eval).T.tolist()
            # Add current sierra test to lists
            sierra_test_paths = sierra_test_paths + [join(output_dir, stat_gages)]
            sierra_test_labels = sierra_test_labels + [stat_gages.replace('.gpkg', '')]
            # Read in all sierra test results
            sierra_test_dfs = [gpd.read_file(i) for i in sierra_test_paths]
            # Feed results into evaluation function
            evaluate_results(
                sierra_test_dfs, sierra_test_labels, join(output_dir, 'Sierra_Test_Eval_boxplot.png')
            )
    except Exception:
        logging.info("-- Exception")
        print("")
        logging.info(traceback.format_exc())

    end_time = dt.datetime.now()
    time_duration = end_time - start_time
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print("==========================================================================")
    logging.info(f".. Ended: {dt_string} \n")
    logging.info(f".. Duration: {str(time_duration).split('.')[0]}")
