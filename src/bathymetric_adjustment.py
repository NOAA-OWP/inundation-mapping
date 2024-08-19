#!/usr/bin/env python3

import datetime as dt
import os
import re
import sys
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from os.path import join

import geopandas as gpd
import pandas as pd

from utils.shared_functions import progress_bar_handler


def correct_rating_for_bathymetry(fim_dir, huc, bathy_file, verbose):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input bathy_file.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.
        bathy_file : str
            Path to bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        verbose : bool
            Verbose printing.

        Returns
        ----------
        log_text : str

    """

    log_text = f'Calculating bathymetry adjustment: {huc}\n'

    # Load wbd and use it as a mask to pull the bathymetry data
    fim_huc_dir = join(fim_dir, huc)
    wbd8_clp = gpd.read_file(join(fim_huc_dir, 'wbd8_clp.gpkg'), engine="pyogrio", use_arrow=True)
    bathy_data = gpd.read_file(bathy_file, mask=wbd8_clp, engine="fiona")
    bathy_data = bathy_data.rename(columns={'ID': 'feature_id'})

    # Get src_full from each branch
    src_all_branches = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
        if os.path.isfile(src_full):
            src_all_branches.append(src_full)

    # Update src parameters with bathymetric data
    for src in src_all_branches:
        src_df = pd.read_csv(src)
        if 'Bathymetry_source' in src_df.columns:
            src_df = src_df.drop(columns='Bathymetry_source')
        branch = re.search(r'branches/(\d{10}|0)/', src).group()[9:-1]
        log_text += f'  Branch: {branch}\n'

        if bathy_data.empty:
            log_text += '  There were no bathymetry feature_ids for this branch'
            src_df['Bathymetry_source'] = [""] * len(src_df)
            src_df.to_csv(src, index=False)
            return log_text

        # Merge in missing bathy data and fill Nans
        try:
            src_df = src_df.merge(
                bathy_data[
                    ['feature_id', 'missing_xs_area_m2', 'missing_wet_perimeter_m', 'Bathymetry_source']
                ],
                on='feature_id',
                how='left',
                validate='many_to_one',
            )
        # If there's more than one feature_id in the bathy data, just take the mean
        except pd.errors.MergeError:
            reconciled_bathy_data = bathy_data.groupby('feature_id')[
                ['missing_xs_area_m2', 'missing_wet_perimeter_m']
            ].mean()
            reconciled_bathy_data['Bathymetry_source'] = bathy_data.groupby('feature_id')[
                'Bathymetry_source'
            ].first()
            src_df = src_df.merge(reconciled_bathy_data, on='feature_id', how='left', validate='many_to_one')
        # Exit if there are no recalculations to be made
        if ~src_df['Bathymetry_source'].any(axis=None):
            log_text += '    No matching feature_ids in this branch\n'
            continue

        src_df['missing_xs_area_m2'] = src_df['missing_xs_area_m2'].fillna(0.0)
        src_df['missing_wet_perimeter_m'] = src_df['missing_wet_perimeter_m'].fillna(0.0)
        # Add missing hydraulic geometry into base parameters
        src_df['Volume (m3)'] = src_df['Volume (m3)'] + (
            src_df['missing_xs_area_m2'] * (src_df['LENGTHKM'] * 1000)
        )
        src_df['BedArea (m2)'] = src_df['BedArea (m2)'] + (
            src_df['missing_wet_perimeter_m'] * (src_df['LENGTHKM'] * 1000)
        )
        # Recalc discharge with adjusted geometries
        src_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter_m']
        src_df['WetArea (m2)'] = src_df['WetArea (m2)'] + src_df['missing_xs_area_m2']
        src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
        src_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)'].fillna(0)
        src_df['Discharge (m3s-1)'] = (
            src_df['WetArea (m2)']
            * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
            * pow(src_df['SLOPE'], 0.5)
            / src_df['ManningN']
        )
        # Force zero stage to have zero discharge
        src_df.loc[src_df['Stage'] == 0, ['Discharge (m3s-1)']] = 0
        # Calculate number of adjusted HydroIDs
        count = len(src_df.loc[(src_df['Stage'] == 0) & (src_df['Bathymetry_source'] == 'USACE eHydro')])

        # Write src back to file
        src_df.to_csv(src, index=False)
        log_text += f'    Successfully recalculated {count} HydroIDs\n'
    return log_text


def multi_process_hucs(fim_dir, bathy_file, wbd_buffer, wbd, output_suffix, number_of_jobs, verbose):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids in the input bathy_file.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        bathy_file : str
            Path to bathymetric adjustment geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        wbd_buffer : int
            Distance in meters to buffer wbd dataset when searching for relevant HUCs.
        wbd : str
            Path to wbd input data, e.g.
            "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
        output_suffix : str
            Output filename suffix.
        number_of_jobs : int
            Number of CPU cores to parallelize HUC processing.
        verbose : bool
            Verbose printing.

    """

    # Set up log file
    print(
        'Writing progress to log file here: '
        + str(join(fim_dir, 'logs', 'bathymetric_adjustment' + output_suffix + '.log'))
    )
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Initiate log file
    log_file = open(join(fim_dir, 'logs', 'bathymetric_adjustment' + output_suffix + '.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    # Exit program if the bathymetric data doesn't exist
    if not os.path.exists(bathy_file):
        statement = f'The input bathymetry file {bathy_file} does not exist. Exiting...'
        log_file.write(statement)
        print(statement)
        sys.exit(0)

    # Find applicable HUCs to apply bathymetric adjustment
    # NOTE: This block can be removed if we have estimated bathymetry data for
    # the whole domain later.
    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]
    bathy_gdf = gpd.read_file(bathy_file, engine="pyogrio", use_arrow=True)
    buffered_bathy = bathy_gdf.geometry.buffer(wbd_buffer)  # We buffer the bathymetric data to get adjacent
    wbd = gpd.read_file(
        wbd, mask=buffered_bathy, engine="fiona"
    )  # HUCs that could also have bathymetric reaches included
    hucs_with_bathy = wbd.HUC8.to_list()
    hucs = [h for h in fim_hucs if h in hucs_with_bathy]
    log_file.write(f"Identified {len(hucs)} HUCs that have bathymetric data: {hucs}\n")
    print(f"Identified {len(hucs)} HUCs that have bathymetric data\n")

    # Set up multiprocessor
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
        executor_dict = {}
        for huc in hucs:
            arg_keeper = {'fim_dir': fim_dir, 'huc': huc, 'bathy_file': bathy_file, 'verbose': verbose}
            future = executor.submit(correct_rating_for_bathymetry, **arg_keeper)
            executor_dict[future] = huc

        # Send the executor to the progress bar and wait for all tasks to finish
        progress_bar_handler(executor_dict, f"Running BARC on {len(hucs)} HUCs")
        # Get the returned logs and write to the log file
        for future in executor_dict.keys():
            try:
                log_file.write(future.result())
            except Exception as ex:
                print(f"ERROR: {executor_dict[future]} BARC failed for some reason")
                log_file.write(f"ERROR --> {executor_dict[future]} BARC failed (details: *** {ex} )\n")
                traceback.print_exc(file=log_file)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    log_file.close()


if __name__ == '__main__':
    """
    Parameters
    ----------
    fim_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/bathymetric_adjustment.log.
    bathy_file : str
        Path to bathymetric adjustment geopackage, e.g.
        "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
    wbd_buffer : int
        Distance in meters to buffer wbd dataset when searching for relevant HUCs.
    wbd : str
        Path to wbd input data, e.g.
        "/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg".
    output_suffix : str
        Optional. Output filename suffix. Defaults to no suffix.
    number_of_jobs : int
        Optional. Number of CPU cores to parallelize HUC processing. Defaults to 8.
    verbose : bool
        Optional flag for enabling verbose printing.

    Sample Usage
    ----------
    python3 /foss_fim/src/bathymetric_adjustment.py -fim_dir /outputs/fim_run_dir -bathy /data/inputs/bathymetry/bathymetry_adjustment_data.gpkg
        -buffer 5000 -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg -j $jobLimit

    """

    parser = ArgumentParser(description="Bathymetric Adjustment")
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-bathy',
        '--bathy_file',
        help="Path to geopackage with preprocessed bathymetic data",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-buffer',
        '--wbd-buffer',
        help="Buffer to apply to bathymetry data to find applicable HUCs",
        required=True,
        type=int,
    )
    parser.add_argument(
        '-wbd',
        '--wbd',
        help="Buffer to apply to bathymetry data to find applicable HUCs",
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

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bathy_file = args['bathy_file']
    wbd_buffer = int(args['wbd_buffer'])
    wbd = args['wbd']
    output_suffix = args['output_suffix']
    number_of_jobs = args['number_of_jobs']
    verbose = bool(args['verbose'])

    multi_process_hucs(fim_dir, bathy_file, wbd_buffer, wbd, output_suffix, number_of_jobs, verbose)
