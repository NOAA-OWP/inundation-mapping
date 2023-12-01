#!/usr/bin/env python3

import argparse
import glob
import os
import re
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from natsort import natsorted
from tools_shared_functions import aggregate_wbd_hucs, get_metadata
from tools_shared_variables import BAD_SITES, DISCARD_AHPS_QUERY

from utils.shared_variables import VIZ_PROJECTION


# Get variables from .env file.
load_dotenv()
WBD_LAYER = os.getenv("WBD_LAYER")
API_BASE_URL = os.getenv("API_BASE_URL")


#########################################################################
# Create boxplot
#########################################################################
def boxplot(
    dataframe,
    x_field,
    x_order,
    y_field,
    hue_field,
    ordered_hue,
    title_text,
    fim_configuration,
    textbox_str=False,
    simplify_legend=False,
    dest_file=False,
):
    '''
    Create boxplots.

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis
    x_order : List
        Order to arrange the x-axis.
    y_field : STR
        Field to use for the y-axis
    hue_field : STR
        Field to use for hue (typically FIM version)
    title_text : STR
        Text for plot title.
    fim_configuration: STR
        Configuration of FIM (FR or MS or Composite).
    simplify_legend : BOOL, optional
        If True, it will simplify legend to FIM 1, FIM 2, FIM 3.
        The default is False.
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False
        no plot is saved to disk. The default is False.

    Returns
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    # initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    # Use seaborn to plot the boxplot
    axes = sns.boxplot(
        x=x_field,
        y=y_field,
        order=x_order,
        hue=hue_field,
        hue_order=ordered_hue,
        data=dataframe,
        palette='bright',
    )
    # set title of plot
    axes.set_title(f'{title_text} ({y_field})', fontsize=20, weight='bold')
    # Set yticks and background horizontal line.
    axes.set(ylim=(0.0, 1.0), yticks=np.arange(0, 1.1, 0.1))
    for index, ytick in enumerate(axes.get_yticks()):
        plt.axhline(y=ytick, color='black', linestyle='--', linewidth=1, alpha=0.1)
    # Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field}', fontsize='xx-large', weight='bold')
    axes.set_xlabel('', fontsize=0, weight='bold')
    # Set sizes of ticks and legend.
    axes.tick_params(labelsize='xx-large')
    axes.legend(markerscale=2, fontsize=20, loc='lower left')

    # If simple legend desired
    if simplify_legend:
        # trim labels to FIM 1, FIM 2, and the FIM 3 version
        handles, org_labels = axes.get_legend_handles_labels()
        label_dict = {}
        for label in org_labels:
            if 'fim_1' in label:
                label_dict[label] = 'FIM 1'
            elif 'fim_2' in label:
                label_dict[label] = 'FIM 2' + ' ' + fim_configuration.lower()
            elif 'fim_3' in label and len(label) < 20:
                label_dict[label] = (
                    re.split('_fr|_ms|_comp', label)[0].replace('_', '.').replace('fim.', 'FIM ')
                    + ' '
                    + fim_configuration.lower()
                )
                if label.endswith('_c'):
                    label_dict[label] = label_dict[label] + ' c'
            elif 'fim_4' in label and len(label) < 20:
                label_dict[label] = label.replace('_', '.').replace('fim.', 'FIM ')
            else:
                label_dict[label] = label
        # Define simplified labels as a list.
        new_labels = [label_dict[label] for label in org_labels]
        # Define legend location. FAR needs to be in different location than CSI/POD.
        if y_field in ['FAR', 'PND', 'EQUITABLE_THREAT_SCORE']:
            legend_location = 'upper right'
        else:
            legend_location = 'lower left'
        # rename legend labels to the simplified labels.
        axes.legend(
            handles,
            new_labels,
            markerscale=2,
            fontsize=14,
            loc=legend_location,
            ncol=int(np.ceil(len(new_labels) / 7)),
        )
    # Print textbox if supplied
    if textbox_str:
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        axes.text(
            0.01,
            0.99,
            textbox_str,
            transform=axes.transAxes,
            fontsize=14,
            verticalalignment='top',
            bbox=box_props,
        )

    # If figure to be saved to disk, then do so, otherwise return figure
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig


#########################################################################
# Create scatter plot
#########################################################################
def scatterplot(dataframe, x_field, y_field, title_text, stats_text=False, annotate=False, dest_file=False):
    '''
    Create boxplots.

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis (Assumes FIM 2)
    y_field : STR
        Field to use for the y-axis (Assumes FIM 3)
    title_text : STR
        Text for plot title.
    stats_text : STR or BOOL
        Text for stats to place on chart. Default is false (no stats printed)
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False
        no plot is saved to disk. The default is False.

    Returnsy
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    # initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))

    # Use seaborn to plot the boxplot
    axes = sns.scatterplot(data=dataframe, x=x_field, y=y_field, color='black', s=150)

    # Set xticks and yticks and background horizontal line.
    axes.set(ylim=(0.0, 1.0), yticks=np.arange(0, 1.1, 0.1))
    axes.set(xlim=(0.0, 1.0), xticks=np.arange(0, 1.1, 0.1))
    axes.grid(visible=True, which='major', axis='both')

    # Set sizes of ticks and legend.
    axes.tick_params(labelsize='xx-large')

    # Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field.replace("_", " ")}', fontsize='xx-large', weight='bold')
    axes.set_xlabel(f'{x_field.replace("_", " ")}', fontsize='xx-large', weight='bold')

    # Plot diagonal line
    diag_range = [0, 1]
    axes.plot(diag_range, diag_range, color='gray', transform=axes.transAxes)

    # set title of plot
    axes.set_title(f'{title_text}', fontsize=20, weight='bold')

    if annotate:
        # Set text for labels
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        textbox_str = 'Target Better'
        axes.text(
            0.3,
            0.6,
            textbox_str,
            transform=axes.transAxes,
            fontsize=32,
            color='gray',
            fontweight='bold',
            verticalalignment='top',
            bbox=box_props,
            rotation=35,
            rotation_mode='anchor',
        )
        textbox_str = 'Baseline Better'
        axes.text(
            0.5,
            0.2,
            textbox_str,
            transform=axes.transAxes,
            fontsize=32,
            color='gray',
            fontweight='bold',
            verticalalignment='top',
            bbox=box_props,
            rotation=35,
            rotation_mode='anchor',
        )

    if stats_text:
        # Add statistics textbox
        axes.text(
            0.01,
            0.80,
            stats_text,
            transform=axes.transAxes,
            fontsize=24,
            verticalalignment='top',
            bbox=box_props,
        )

    # If figure to be saved to disk, then do so, otherwise return fig
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig


#########################################################################
# Create barplot
#########################################################################
def barplot(
    dataframe,
    x_field,
    x_order,
    y_field,
    hue_field,
    ordered_hue,
    title_text,
    fim_configuration,
    textbox_str=False,
    simplify_legend=False,
    display_values=False,
    dest_file=False,
):
    '''
    Create barplots.

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis
    x_order : List
        Order to arrange the x-axis.
    y_field : STR
        Field to use for the y-axis
    hue_field : STR
        Field to use for hue (typically FIM version)
    title_text : STR
        Text for plot title.
    fim_configuration: STR
        Configuration of FIM (FR or MS or Composite).
    simplify_legend : BOOL, optional
        If True, it will simplify legend to FIM 1, FIM 2, FIM 3.
        Default is False.
    display_values : BOOL, optional
        If True, Y values will be displayed above bars.
        Default is False.
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False
        no plot is saved to disk. Default is False.

    Returns
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    # initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    # Use seaborn to plot the boxplot
    axes = sns.barplot(
        x=x_field,
        y=y_field,
        order=x_order,
        hue=hue_field,
        hue_order=ordered_hue,
        data=dataframe,
        palette='bright',
    )
    # set title of plot
    axes.set_title(f'{title_text}', fontsize=20, weight='bold')
    # Set yticks and background horizontal line.
    axes.set(ylim=(0.0, 1.0), yticks=np.arange(0, 1.1, 0.1))
    for index, ytick in enumerate(axes.get_yticks()):
        plt.axhline(y=ytick, color='black', linestyle='--', linewidth=1, alpha=0.1)
    # Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field.upper()}', fontsize='xx-large', weight='bold')
    axes.set_xlabel('', fontsize=0, weight='bold')
    # Set sizes of ticks and legend.
    axes.tick_params(labelsize='xx-large')
    axes.legend(markerscale=2, fontsize=20, loc='upper right')
    # If simple legend desired
    if simplify_legend:
        # trim labels to FIM 1, FIM 2, FIM 3
        handles, org_labels = axes.get_legend_handles_labels()
        label_dict = {}
        for label in org_labels:
            if 'fim_1' in label:
                label_dict[label] = 'FIM 1'
            elif 'fim_2' in label:
                label_dict[label] = 'FIM 2' + ' ' + fim_configuration.lower()
            elif 'fim_3' in label and len(label) < 20:
                label_dict[label] = (
                    re.split('_fr|_ms|_comp', label)[0].replace('_', '.').replace('fim.', 'FIM ')
                    + ' '
                    + fim_configuration.lower()
                )
                if label.endswith('_c'):
                    label_dict[label] = label_dict[label] + ' c'
            elif 'fim_4' in label and len(label) < 20:
                label_dict[label] = label.replace('_', '.').replace('fim.', 'FIM ')
            else:
                label_dict[label] = label
        # Define simplified labels as a list.
        new_labels = [label_dict[label] for label in org_labels]
        # rename legend labels to the simplified labels.
        axes.legend(
            handles,
            new_labels,
            markerscale=2,
            fontsize=14,
            loc='upper right',
            ncol=int(np.ceil(len(new_labels) / 7)),
        )
    # Add Textbox
    if textbox_str:
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        axes.text(
            0.01,
            0.99,
            textbox_str,
            transform=axes.transAxes,
            fontsize=18,
            verticalalignment='top',
            bbox=box_props,
        )

    # Display Y values above bars
    if display_values:
        # Add values of bars directly above bar.
        for patch in axes.patches:
            value = round(patch.get_height(), 3)
            axes.text(
                patch.get_x() + patch.get_width() / 2.0,
                patch.get_height(),
                '{:1.3f}'.format(value),
                ha="center",
                fontsize=18,
            )

    # If figure to be saved to disk, then do so, otherwise return fig
    if dest_file:
        fig.savefig(dest_file)
        plt.close(fig)
    else:
        return fig


#######################################################################
# Filter dataframe generated from csv file from run_test_case aggregation
########################################################################
def filter_dataframe(dataframe, unique_field):
    '''

    This script will filter out the sites (or hucs) which are not consistently
    found for all versions for a given magnitude. For example, an AHPS
    lid site must have output for all 3 versions (fim1, fim2, fim3) for
    a given magnitude (eg action) otherwise that lid is filtered out.
    Likewise for a BLE a huc must have output for all 3 versions
    (fim1, fim2, fim3) for a given magnitude (eg 100yr) otherwise it is
    filtered out.

    Parameters
    ----------
    dataframe : Pandas DataFrame
        Containing the input metrics originating from synthesize_test_cases
    unique_field : STR
        base resolution for each benchmark source: 'nws'/'usgs' (nws_lid)
        ble (huc).

    Returns
    -------
    final_filtered_dataframe : Pandas Dataframe
        Filtered dataframe that contains only common sites (lids or hucs) between versions for each magnitude.
        For example, for AHPS all sites which were run for each version for a given magnitude will be kept
            or for ble, all hucs which ran for all versions for a given magnitude.
    unique_sites: DICT
        The sites that were included in the dataframe for each magnitude.

    '''

    # Get lists of sites for each magnitude/version
    unique_sites = dataframe.groupby(['magnitude', 'version'])[unique_field].agg('unique')
    # Get unique magnitudes
    magnitudes = dataframe.magnitude.unique()
    # Create new dataframe to hold metrics for the common sites as well as the actual lists of common sites.
    final_filtered_dataframe = pd.DataFrame()
    all_unique_sites = {}
    # Cycle through each magnitude
    for magnitude in magnitudes:
        # Compile a list of sets containing unique lids pertaining to each threshold.
        # List contains 3 unique sets [{fim1:unique lids},{fim2: unique lids},{fim3: unique lids}]
        sites_per_magnitude = [set(a) for a in unique_sites[magnitude]]
        # Intersect the sets to get the common lids per threshold then convert to list.
        common_sites_per_magnitude = list(set.intersection(*sites_per_magnitude))
        # Write common sites to dataframe
        all_unique_sites[magnitude] = common_sites_per_magnitude
        # Query filtered dataframe and only include data associated with the common sites for that magnitude
        filtered_common_sites = dataframe.query(
            f'magnitude == "{magnitude}" & {unique_field} in @common_sites_per_magnitude'
        )
        # Append the data for each magnitude to a final dataframe that will contain data for all common sites
        #   for all magnitudes.
        final_filtered_dataframe = pd.concat(
            [final_filtered_dataframe, filtered_common_sites], ignore_index=True
        )

    return final_filtered_dataframe, all_unique_sites


##############################################################################
##############################################################################
# Main function to analyze metric csv.
##############################################################################
def eval_plots(
    metrics_csv,
    workspace,
    versions=[],
    stats=['CSI', 'FAR', 'TPR', 'PND', 'MCC', 'EQUITABLE_THREAT_SCORE'],
    spatial=False,
    fim_1_ms=False,
    site_barplots=False,
):
    '''
    Creates plots and summary statistics using metrics compiled from
    synthesize_test_cases. Required inputs are metrics_csv and workspace.
    Outputs include:
        aggregate_<benchmark source>_<configuration>.csv: this csv
            contains the aggregated total statistics (i.e. CSI, FAR, POD)
            using the summed area_sq_km fields
        <benchmark source>_<configuration>_common_sites.csv: this csv
            contains the unique sites (e.g usgs/nws: nws_lid; ble: huc08)
            considered for aggregation/plots for each magnitude. The selected
            sites occur in all versions analyzed. For example, if FIM 1,
            FIM 2, FIM 3.0.0.3 were versions analyzed, the common sites
            would be those that had data for ALL versions. This
            analysis is then redone for each magnitude. As such, the number
            of sites may vary with magnitude. The number of sites for each
            magnitude is annotated on generated plots.
        <benchmark source>_<configuration>_analyzed_data.csv: this is the
            dataset used to create plots and aggregate statistics. It is
            a subset of the input metrics file and consists of the common
            sites.
        csi_aggr_<benchmark source>_<configuration>.png: bar plot of the
            aggregated CSI scores. Number of common sites is annotated
            (see list of sites listed in *_*_common_sites.csv).
        csi_<benchmark source>_<configuration>.png: box plot of CSI scores
            (sites weighted equally). Number of common sites is annotated
            (see list of sites listed in *_*_common_sites.csv).
        far_<benchmark source>_<configuration>*.png: box plot of FAR scores
            (sites weighted equally). Number of common sites is annotated
            (see list of sites listed in *_*_common_sites.csv).
        tpr_<benchmark source>_<configuration>*.png: box plot of TPR/POD
            scores (sites weighted equally). Number of common sites is
            annotated (see list of sites listed in *_*_common_sites.csv).
        csi_scatter_<magnitude>_<configuration>*.png: scatter plot comparing
            two versions for a given magnitude. This is only generated if
            there are exactly two versions analyzed.
        csi_scatter_<magnitude>_<configuration>_data.csv: data used to create the
            csi_scatter_plot
        Optional: 'individual' directory with subfolders for each site in analysis. In these
            site subdirectories are the following files:
                csi_<site_name>_<benchmark_source>_<configuration>.png: A barplot
                    of CSI for each version for all magnitudes for the site.
        Optional (if spatial argument supplied):
            fim_performance_points.shp -- A shapefile of ahps points with
            metrics contained in attribute table.
            fim_performance_polys.shp -- A shapefile of huc8 polygons with
            metrics contained in attribute table.



    Parameters
    ----------
    metrics_csv : STRING
        Path to csv produced as part of synthesize_test_cases containing
        all metrics across all versions.
    workspace : STRING
        Path to the output workspace. Subdirectories will be created
        reflecting the evaluation datasets.
    versions: LIST
        A list of versions to be aggregated/plotted. Uses the "startswith"
        approach. Versions should be supplied in the order they are to
        be plotted. For example: ['fim_', 'fb']; This will evaluate all
        versions that start with fim_ (e.g. fim_1, fim_2, fim_3) and any
        feature branch that starts with "fb". To esbalish version order,
        the fim versions are naturally sorted and then fb versions
        (naturally sorted) are appended. These versions are also used to
        filter the input metric csv as only these versions are retained
        for analysis.
    stats: LIST
        A list of statistics to be plotted. Must be identical to column
        field in metrics_csv. CSI, POD, TPR are currently calculated, if
        additional statistics are desired formulas would need to be coded.
    spatial : BOOL, optional
        Creates spatial datasets of the base unit (ble: huc polygon, ahps: point)
        with metrics contained in attribute tables. The geospatial data is
        either supplied in the .env file (WBD Huc layer) or from WRDS (ahps).
        The outputs are consistent with requirements set forth by the vizualization team.
        Additionally, there is a commented out section where if the user
        passes the extent files generated during creation of nws/usgs ahps
        preprocessing, the actual maps and flows used for evaluation are
        appended to the ahps shapefile output.
    fim_1_ms: BOOL
        Default is false. If True then fim_1 rows are duplicated with
        extent_config set to MS. This allows for FIM 1 to be included
        in MS plots/stats (helpful for nws/usgs ahps comparisons).
    site_barplots: BOOL
        Default is false. If True then barplots for each individual site are
        created. An 'individual' directory with subdirectories of each site
        are created and the plot is located in each site subdirectory.

    Returns
    -------
    all_datasets : DICT
        Dictionary containing all datasets generated.
        Keys: (benchmark_source, extent_config),
        Values: (filtered dataframe, common sites)

    '''

    # Import metrics csv as DataFrame and initialize all_datasets dictionary
    csv_df = pd.read_csv(metrics_csv, dtype={'huc': str})

    # fim_1_ms flag enables FIM 1 to be shown on MS plots/stats
    if fim_1_ms:
        # Query FIM 1 rows based on version beginning with "fim_1"
        fim_1_rows = csv_df.query('version.str.startswith("fim_1")').copy()
        # Set extent configuration to MS (instead of FR)
        fim_1_rows['extent_config'] = 'MS'
        # Append duplicate FIM 1 rows to original dataframe
        csv_df = pd.concat([csv_df, fim_1_rows], ignore_index=True)

    # If versions are supplied then filter out
    if versions:
        # Filter out versions based on supplied version list
        metrics = csv_df.query("version == @versions")
    else:
        metrics = csv_df

    # Group by benchmark source
    benchmark_by_source = metrics.groupby(['benchmark_source', 'extent_config'])

    ''' Iterate through benchmark_by_source. Pre-filter metrics dataframe
    as needed (e.g. usgs/nws filter query). Then further filtering to
    discard all hucs/nws_lid that are not present across all analyzed
    versions for a given magnitude. The final filtered dataset is written
    to a dictionary with the key (benchmark source, extent config)
    and values (filtered dataframe, common sites). '''

    all_datasets = {}
    for (benchmark_source, extent_configuration), benchmark_metrics in benchmark_by_source:
        '''If source is usgs/nws define the base resolution and query
        (use alternate query if passed). Append filtered datasets to
        all_datasets dictionary.'''

        if benchmark_source in ['usgs', 'nws']:
            # Set the base processing unit for the ahps runs.
            base_resolution = 'nws_lid'

            # Filter the dataset based on query (IMPORTED FROM TOOLS_SHARED_VARIABLES.py)
            ahps_metrics = benchmark_metrics.query(DISCARD_AHPS_QUERY, engine='python')

            # Filter out all instances where the base_resolution doesn't exist across all desired
            #   fim versions for a given magnitude
            all_datasets[(benchmark_source, extent_configuration)] = filter_dataframe(
                ahps_metrics, base_resolution
            )

        # If source is 'ble', set base_resolution and append ble dataset to all_datasets dictionary
        elif benchmark_source in ['ble', 'ifc', 'ras2fim']:
            # Set the base processing unit for ble runs
            base_resolution = 'huc'

            # Filter out all instances where base_resolution doesn't exist across all desired fim versions
            #   for a given magnitude
            all_datasets[(benchmark_source, extent_configuration)] = filter_dataframe(
                benchmark_metrics, base_resolution
            )

    # For each dataset in all_datasets, generate plots and aggregate statistics
    for (dataset_name, configuration), (dataset, sites) in all_datasets.items():
        # Define and create the output workspace as a subfolder within the supplied workspace
        output_workspace = Path(workspace) / dataset_name / configuration.lower()
        output_workspace.mkdir(parents=True, exist_ok=True)

        # Write out the filtered dataset and common sites to file
        dataset.to_csv(
            output_workspace / (f'{dataset_name}_{configuration.lower()}_analyzed_data.csv'), index=False
        )
        sites_pd = pd.DataFrame.from_dict(sites, orient='index').transpose()
        sites_pd.to_csv(
            output_workspace / (f'{dataset_name}_{configuration.lower()}_common_sites.csv'), index=False
        )

        # Set the order of the magnitudes and define base resolution
        if dataset_name == 'ble':
            magnitude_order = ['100yr', '500yr']
            base_resolution = 'huc'
        elif dataset_name == 'ifc':
            magnitude_order = ['2yr', '5yr', '10yr', '25yr', '50yr', '100yr', '200yr', '500yr']
            base_resolution = 'huc'
        elif dataset_name == 'ras2fim':
            magnitude_order = ['2yr', '5yr', '10yr', '25yr', '50yr', '100yr']
            base_resolution = 'huc'
        elif dataset_name in ['usgs', 'nws']:
            magnitude_order = ['action', 'minor', 'moderate', 'major']
            base_resolution = 'nws_lid'

        # Calculate aggregated metrics based on total_sq_km fields
        dataset_sums = dataset.groupby(['version', 'magnitude'])[
            ['TP_area_km2', 'FP_area_km2', 'FN_area_km2']
        ].sum()
        dataset_sums['csi'] = dataset_sums['TP_area_km2'] / (
            dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2'] + dataset_sums['FN_area_km2']
        )
        dataset_sums['far'] = dataset_sums['FP_area_km2'] / (
            dataset_sums['TP_area_km2'] + dataset_sums['FP_area_km2']
        )
        dataset_sums['pod'] = dataset_sums['TP_area_km2'] / (
            dataset_sums['TP_area_km2'] + dataset_sums['FN_area_km2']
        )
        dataset_sums = dataset_sums.reset_index()

        # Write aggregated metrics to file
        dataset_sums.to_csv(
            output_workspace / f'aggregate_{dataset_name}_{configuration.lower()}.csv', index=False
        )

        ## This section naturally orders analyzed versions which defines the hue order for the generated plots
        # Get all versions in dataset
        all_versions = list(dataset.version.unique())
        version_order = []

        # If versions are not specified then use all available versions and assign to versions_list
        if not versions:
            versions_list = all_versions
        # If versions are supplied assign to versions_list
        else:
            versions_list = versions
        # For each version supplied by the user
        for version in versions_list:
            # Select all the versions that start with the supplied version.
            selected_versions = [
                sel_version for sel_version in all_versions if sel_version.startswith(version)
            ]
            # Naturally sort selected_versions
            selected_versions = natsorted(selected_versions)
            # Populate version order based on the sorted subsets.
            version_order.extend(selected_versions)

        # Define textbox which will contain the counts of each magnitude
        textbox = []
        for magnitude in sites:
            count = len(sites[magnitude])
            line_text = f'{magnitude.title()} Sites = {count}'
            textbox.append(line_text)
        textbox = '\n'.join(textbox)

        # Create aggregate barplot
        aggregate_file = output_workspace / (f'csi_aggr_{dataset_name}_{configuration.lower()}.png')
        barplot(
            dataframe=dataset_sums,
            x_field='magnitude',
            x_order=magnitude_order,
            y_field='csi',
            hue_field='version',
            ordered_hue=version_order,
            title_text=f'Aggregate {dataset_name.upper()} FIM Scores',
            fim_configuration=configuration,
            textbox_str=textbox,
            simplify_legend=True,
            dest_file=aggregate_file,
        )

        # If enabled, write out barplots of CSI for individual sites.
        if site_barplots:
            individual_dirs = output_workspace / 'individual'
            individual_dirs.mkdir(parents=True, exist_ok=True)
            subset = dataset.groupby(base_resolution)
            for site_name, site_data in subset:
                site_file = (
                    individual_dirs / f'csi_{str(site_name)}_{dataset_name}_{configuration.lower()}.png'
                )
                barplot(
                    dataframe=site_data,
                    x_field='magnitude',
                    x_order=magnitude_order,
                    y_field='CSI',
                    hue_field='version',
                    ordered_hue=version_order,
                    title_text=f'{str(site_name).upper()} FIM Scores',
                    fim_configuration=configuration,
                    textbox_str=False,
                    simplify_legend=True,
                    dest_file=site_file,
                )

        # Create box plots for each metric in supplied stats
        for stat in stats:
            output_file = output_workspace / (f'{stat.lower()}_{dataset_name}_{configuration.lower()}.png')
            boxplot(
                dataframe=dataset,
                x_field='magnitude',
                x_order=magnitude_order,
                y_field=stat,
                hue_field='version',
                ordered_hue=version_order,
                title_text=f'{dataset_name.upper()} FIM Sites',
                fim_configuration=configuration,
                textbox_str=textbox,
                simplify_legend=True,
                dest_file=output_file,
            )

        # Get the last 2 versions from the version order for scatter plot
        if len(version_order) == 2:
            x_version, y_version = version_order
            for magnitude in magnitude_order:
                # Scatterplot comparison between last 2 versions
                x_csi = dataset.query(f'version == "{x_version}" & magnitude == "{magnitude}"')[
                    [base_resolution, 'CSI']
                ]
                y_csi = dataset.query(f'version == "{y_version}" & magnitude == "{magnitude}"')[
                    [base_resolution, 'CSI']
                ]
                plotdf = pd.merge(
                    x_csi, y_csi, on=base_resolution, suffixes=(f"_{x_version}", f"_{y_version}")
                )
                # Define arguments for scatterplot function
                title_text = f'CSI {magnitude}'
                dest_file = output_workspace / f'csi_scatter_{magnitude}_{configuration.lower()}.png'
                scatterplot(
                    dataframe=plotdf,
                    x_field=f'CSI_{x_version}',
                    y_field=f'CSI_{y_version}',
                    title_text=title_text,
                    annotate=False,
                    dest_file=dest_file,
                )
                # Write out dataframe used to create scatter plots
                plotdf['Diff (C-B)'] = plotdf[f'CSI_{y_version}'] - plotdf[f'CSI_{x_version}']
                plotdf.to_csv(
                    output_workspace / f'csi_scatter_{magnitude}_{configuration.lower()}_data.csv',
                    index=False,
                )

    #######################################################################
    # Create spatial layers with threshold and mapping information
    ########################################################################
    if spatial:
        ###############################################################
        # This section will join ahps metrics to a spatial point layer
        ###############################################################
        if (all_datasets.get(('nws', 'MS')) and all_datasets.get(('usgs', 'MS'))) or (
            all_datasets.get(('nws', 'COMP')) and all_datasets.get(('usgs', 'COMP'))
        ):  # export composite metrics to shp
            # Get point data for ahps sites
            # Get metrics for usgs and nws benchmark sources
            try:
                usgs_dataset, sites = all_datasets.get(('usgs', 'MS'))
                nws_dataset, sites = all_datasets.get(('nws', 'MS'))
            except TypeError:  # Composite metrics
                usgs_dataset, sites = all_datasets.get(('usgs', 'COMP'))
                nws_dataset, sites = all_datasets.get(('nws', 'COMP'))
            # Append usgs/nws dataframes and filter unnecessary columns and rename remaining.
            all_ahps_datasets = pd.concat([usgs_dataset, nws_dataset])
            all_ahps_datasets = all_ahps_datasets.filter(
                [
                    'huc',
                    'nws_lid',
                    'version',
                    'magnitude',
                    'TP_area_km2',
                    'FP_area_km2',
                    'TN_area_km2',
                    'FN_area_km2',
                    'CSI',
                    'FAR',
                    'TPR',
                    'PND',
                    'MCC',
                    'EQUITABLE_THREAT_SCORE',
                    'benchmark_source',
                ]
            )
            all_ahps_datasets.rename(columns={'benchmark_source': 'source'}, inplace=True)

            # Get spatial data from WRDS
            # Get metadata from WRDS API
            select_by = 'nws_lid'
            selector = list(all_ahps_datasets.nws_lid.unique())
            metadata_url = f'{API_BASE_URL}/metadata'
            metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector)
            # Create geospatial data from WRDS output
            dictionary, gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)
            # Trim out unecessary columns and rename remaining columns
            gdf = gdf.filter(
                [
                    'identifiers_nws_lid',
                    'nws_data_name',
                    'identifiers_nwm_feature_id',
                    'nws_data_wfo',
                    'nws_data_state',
                    'nws_data_county',
                    'geometry',
                ]
            )
            gdf.rename(
                columns={
                    'identifiers_nws_lid': 'nws_lid',
                    'nws_data_name': 'lid_name',
                    'identifiers_nwm_feature_id': 'feature_id',
                    'nws_data_wfo': 'wfo',
                    'nws_data_state': 'state',
                    'nws_data_county': 'county',
                    'HUC8': 'huc8',
                },
                inplace=True,
            )

            # Join spatial data to metric data
            gdf['nws_lid'] = gdf['nws_lid'].str.lower()
            joined = gdf.merge(all_ahps_datasets, on='nws_lid')
            # Project to VIZ projection and write to file
            joined = joined.to_crs(VIZ_PROJECTION)
            joined.to_file(Path(workspace) / 'fim_performance_points.shp')
        else:
            print(
                'NWS/USGS MS datasets not analyzed, no spatial data created.\n'
                'To produce spatial data analyze a MS version.'
            )

        ################################################################
        # This section joins ble (FR) metrics to a spatial layer of HUCs.
        ################################################################
        if (
            all_datasets.get(('ble', 'FR'))
            and all_datasets.get(('ifc', 'FR'))
            and all_datasets.get(('ras2fim', 'FR'))
        ) or (
            all_datasets.get(('ble', 'COMP'))
            and all_datasets.get(('ifc', 'COMP'))
            and all_datasets.get(('ras2fim', 'COMP'))
        ):
            # Select BLE, FR dataset.
            try:
                ble_dataset, sites = all_datasets.get(('ble', 'FR'))
                ifc_dataset, sites = all_datasets.get(('ifc', 'FR'))
                ras2fim_dataset, sites = all_datasets.get(('ras2fim', 'FR'))
            except TypeError:
                ble_dataset, sites = all_datasets.get(('ble', 'COMP'))
                ifc_dataset, sites = all_datasets.get(('ifc', 'COMP'))
                ras2fim_dataset, sites = all_datasets.get(('ras2fim', 'COMP'))

            huc_datasets = pd.concat([ble_dataset, ifc_dataset, ras2fim_dataset])
            # Read in HUC spatial layer
            wbd_gdf = gpd.read_file(Path(WBD_LAYER), layer='WBDHU8')
            # Join metrics to HUC spatial layer
            wbd_with_metrics = wbd_gdf.merge(huc_datasets, how='inner', left_on='HUC8', right_on='huc')
            # Filter out unnecessary columns
            wbd_with_metrics = wbd_with_metrics.filter(
                [
                    'version',
                    'magnitude',
                    'huc',
                    'TP_area_km2',
                    'FP_area_km2',
                    'TN_area_km2',
                    'FN_area_km2',
                    'CSI',
                    'FAR',
                    'TPR',
                    'PND',
                    'MCC',
                    'EQUITABLE_THREAT_SCORE',
                    'benchmark_source',
                    'geometry',
                ]
            )
            wbd_with_metrics.rename(columns={'benchmark_source': 'source'}, inplace=True)
            # Project to VIZ projection
            wbd_with_metrics = wbd_with_metrics.to_crs(VIZ_PROJECTION)
            # Write out to file
            wbd_with_metrics.to_file(Path(workspace) / 'fim_performance_polys.shp')
        else:
            print(
                'BLE/IFC/RAS2FIM FR datasets not analyzed, no spatial data created.\n'
                'To produce spatial data analyze a FR version'
            )


def convert_shapes_to_csv(workspace):
    # Convert any geopackage in the root level of output_mapping_dir to CSV and rename.
    shape_list = glob.glob(os.path.join(workspace, '*.shp'))
    for shape in shape_list:
        gdf = gpd.read_file(shape)
        parent_directory = os.path.split(shape)[0]
        file_name = shape.replace('.shp', '.csv')
        csv_output_path = os.path.join(parent_directory, file_name)
        gdf.to_csv(csv_output_path)


#######################################################################
if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Plot and aggregate statistics for benchmark datasets (BLE/AHPS libraries)'
    )
    parser.add_argument(
        '-m', '--metrics_csv', help='Metrics csv created from synthesize test cases.', required=True
    )
    parser.add_argument('-w', '--workspace', help='Output workspace', required=True)
    parser.add_argument(
        '-v',
        '--versions',
        help='List of versions to be plotted/aggregated. Versions are filtered using the "startswith" '
        'approach. For example, ["fim_","fb1"] would retain all versions that began with "fim_" '
        '(e.g. fim_1..., fim_2..., fim_3...) as well as any feature branch that began with "fb". '
        'An other example ["fim_3","fb"] would result in all fim_3 versions being plotted along with the fb.',
        nargs='+',
        default=[],
    )
    parser.add_argument(
        '-s',
        '--stats',
        help='List of statistics (abbrev to 3 letters) to be plotted/aggregated',
        nargs='+',
        default=['CSI', 'TPR', 'FAR', 'PND', 'MCC', 'EQUITABLE_THREAT_SCORE'],
        required=False,
    )
    parser.add_argument(
        '-sp',
        '--spatial',
        help='If enabled, creates spatial layers with metrics populated in attribute table.',
        action='store_true',
        required=False,
    )
    parser.add_argument(
        '-f',
        '--fim_1_ms',
        help='If enabled fim_1 rows will be duplicated and extent config assigned "ms" so that '
        'fim_1 can be shown on mainstems plots/stats',
        action='store_true',
        required=False,
    )
    parser.add_argument(
        '-i',
        '--site_plots',
        help='If enabled individual barplots for each site are created.',
        action='store_true',
        required=False,
    )

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())

    # Finalize Variables
    m = args['metrics_csv']
    w = args['workspace']
    v = args['versions']
    s = args['stats']
    sp = args['spatial']
    f = args['fim_1_ms']
    i = args['site_plots']

    # Run eval_plots function
    print('The following AHPS sites are considered "BAD_SITES":  ' + ', '.join(BAD_SITES))
    print('The following query is used to filter AHPS:  ' + DISCARD_AHPS_QUERY)
    eval_plots(metrics_csv=m, workspace=w, versions=v, stats=s, spatial=sp, fim_1_ms=f, site_barplots=i)

    # Convert output shapefiles to CSV
    print("Converting to CSVs...")
    convert_shapes_to_csv(w)
