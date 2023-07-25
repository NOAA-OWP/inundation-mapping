#!/usr/bin/env python3

import sys, os, traceback, re
from os.path import join
import pandas as pd
import geopandas as gpd
import datetime as dt
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
sys.path.append('/foss_fim/tools')
from synthesize_test_cases import progress_bar_handler


def correct_rating_for_bathymetry(fim_dir, huc, bathy_file, verbose):

    log_text = f'Calculating bathymetry adjustment: {huc}\n'

    # Load wbd and use it as a mask to pull the bathymetry data
    fim_huc_dir = join(fim_dir, huc)
    wbd8_clp = gpd.read_file(join(fim_huc_dir, 'wbd8_clp.gpkg'))
#    bathy_data = gpd.read_file(bathy_file, mask=wbd_clp)
    ##### TEMP TEST DATA #############
    bathy_data = pd.DataFrame.from_dict(
        {'feature_id':[3786927, 11050844, 11050846, 3824135, 3824131, 3821271, 3821269],
#        {'feature_id':[3786927, 11050844, 11050846, 3824135, 3824131, 3821271, 3821269, 3821269], # testing duplicate feature ids
        'missing_wet_perimeter_m_m2':[1448.25, 2155.88, 1057.73, 1569.73, 2262.68, 1292.2, 1180.27],
        'missing_wet_perimeter':[0.59, 1.54, 1.31, 1.23, 1.44, 0.68, 0.73],
        'Bathymetry_source':['USACE eHydro']*7})
    #################################

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
        branch = re.search('[\d[1]|\d{10}]', src).group()
        log_text += f'  Branch: {branch}\n'

        if bathy_data.empty:
            log_text += '  There were no bathymetry feature_ids for this branch'
            src_df['Bathymetry_source'] = [""]* len(src_df)
            src_df.to_csv(src, index=False)
            return log_text
    
        # Merge in missing bathy data and fill Nans
        try:
            src_df = src_df.merge(bathy_data, on='feature_id', how='left', validate='many_to_one')
        # If there's more than one feature_id in the bathy data, just take the mean
        except pd.errors.MergeError:
            reconciled_bathy_data = bathy_data.groupby('feature_id').mean()
            reconciled_bathy_data['Bathymetry_source'] = bathy_data.groupby('feature_id').first()['Bathymetry_source']
            src_df = src_df.merge(reconciled_bathy_data, on='feature_id', how='left', validate='many_to_one')
        src_df['missing_wet_perimeter_m_m2'] = src_df['missing_wet_perimeter_m_m2'].fillna(0.0)
        src_df['missing_wet_perimeter'] = src_df['missing_wet_perimeter'].fillna(0.0)
        # Add missing hydraulic geometry into base parameters
        src_df['Volume (m3)'] = src_df['Volume (m3)'] + (src_df['missing_wet_perimeter_m_m2'] * (src_df['LENGTHKM'] * 1000))
        src_df['BedArea (m2)'] = src_df['BedArea (m2)'] + (src_df['missing_wet_perimeter'] * (src_df['LENGTHKM'] * 1000))
        # Recalc discharge with adjusted geometries
        src_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)'] + src_df['missing_wet_perimeter']
        src_df['WetArea (m2)'] = src_df['WetArea (m2)'] + src_df['missing_wet_perimeter_m_m2']
        src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)']/src_df['WettedPerimeter (m)']
        src_df['HydraulicRadius (m)'].fillna(0, inplace=True)
        src_df['Discharge (m3s-1)'] = src_df['WetArea (m2)']* \
            pow(src_df['HydraulicRadius (m)'],2.0/3)* \
            pow(src_df['SLOPE'],0.5)/src_df['ManningN']
        # Force zero stage to have zero discharge
        src_df.loc[src_df['Stage']==0,['Discharge (m3s-1)']] = 0
        # Calculate number of adjusted HydroIDs
        count = len(src_df.loc[(src_df['Stage']==0) & (src_df['Bathymetry_source'] == 'USACE eHydro')])

        # Write src back to file
        src_df.to_csv(src, index=False)
        log_text += f'  Successfully recalculated {count} HydroIDs\n'
    return log_text


def multi_process_hucs(fim_dir, bathy_file, output_suffix, number_of_jobs, verbose, src_plot_option):

    # Set up log file
    print('Writing progress to log file here: ' + str(join(fim_dir,'logs','bathymetric_adjustment' + output_suffix + '.log')))
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## initiate log file
    log_file = open(join(fim_dir,'logs','bathymetric_adjustment' + output_suffix + '.log'),"w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    # Set up multiprocessor
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:

        # Loop through all test cases, build the alpha test arguments, and submit them to the process pool
        executor_dict = {}
        hucs = [h for h in os.listdir(fim_dir) if re.match('\d{8}', h)]
        for huc in hucs:
            
            arg_keeper = { 
                        'fim_dir': fim_dir,
                        'huc': huc,
                        'bathy_file': bathy_file,
                        'verbose': verbose,
                        }
            future = executor.submit(correct_rating_for_bathymetry, **arg_keeper)
            executor_dict[future] = huc

        # Send the executor to the progress bar and wait for all tasks to finish
        progress_bar_handler(executor_dict, True, f"Running BARC on {len(hucs)} HUCs")
        # Get the returned logs and write to the log file
        for future in executor_dict.keys():
            try:
                log_file.write(future.result())
            except Exception as ex:
                print(f"WARNING: {executor_dict[future]} BARC failed for some reason")
                log_file.write( f"ERROR --> {executor_dict[future]} BARC failed (details: *** {ex} )\n")
                traceback.print_exc(file=log_file)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    log_file.close()

if __name__ == '__main__':

    parser = ArgumentParser(description="Bathymetric Adjustment")
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-bathy','--bathy_file',help="Path to geopackage with preprocessed bathymetic data",required=True,type=str)
    parser.add_argument('-suff','--output-suffix',help="Suffix to append to the output log file (e.g. '_global_06_011')",default="",required=False,type=str)
    parser.add_argument('-j','--number-of-jobs',help='OPTIONAL: number of workers (default=8)',required=False,default=8,type=int)
    parser.add_argument('-vb','--verbose',help='OPTIONAL: verbose progress bar',required=False,default=None,action='store_true')
    parser.add_argument('-plots','--src-plot-option',help='OPTIONAL flag: use this flag to create src plots for all hydroids. WARNING - long runtime',default=False,required=False, action='store_true')

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bathy_file = args['bathy_file']
    output_suffix = args['output_suffix']
    number_of_jobs = args['number_of_jobs']
    verbose = bool(args['verbose'])
    src_plot_option = bool(args['src_plot_option'])

    multi_process_hucs(fim_dir, bathy_file, output_suffix, number_of_jobs, verbose, src_plot_option)