import argparse
import os
import traceback
from multiprocessing import Pool

import pandas as pd
from pixel_counter import zonal_stats


def queue_zonal_stats(fim_run_dir, raster_path_dict, output_dir, job_number):
    """
    This function sets up multiprocessing of the process_zonal_stats() function.

    """

    # Make output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    fim_version = os.path.split(fim_run_dir)[1]  # Parse FIM Version

    fim_run_dir_list = os.listdir(fim_run_dir)  # List all HUCs in FIM run dir

    # Define variables to pass into process_zonal_stats()
    procs_list = []
    for huc in fim_run_dir_list:
        vector = os.path.join(
            fim_run_dir, huc, 'demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg'
        )
        csv = os.path.join(output_dir, fim_version + '_' + huc + '_pixel_counts.csv')
        print(csv)
        procs_list.append([vector, csv, raster_path_dict])

    # Initiate multiprocessing
    with Pool(processes=job_number) as pool:
        pool.map(process_zonal_stats, procs_list)


def process_zonal_stats(args):
    """
    This function calls zonal_stats() in multiprocessing mode.

    """
    # Extract variables from args
    vector = args[0]
    csv = args[1]
    raster = args[2]

    # Do the zonal stats
    try:
        stats = zonal_stats(vector, raster)  # Call function

        # Export CSV
        df = pd.DataFrame(stats)
        df.to_csv(csv)
        print("Finished writing: " + csv)

    # Save traceback to error file if error is encountered.
    except Exception:
        error_file = csv.replace('.csv', '_error.txt')
        with open(error_file, 'w+') as f:
            traceback.print_exc(file=f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes pixel counts for raster classes within a vector area.'
    )
    parser.add_argument('-d', '--fim-run-dir', help='Path to vector file.', required=True)
    parser.add_argument(
        '-n',
        '--nlcd',
        help='Path to National Land Cover Database raster file.',
        required=False,
        default="",
    )
    parser.add_argument(
        '-l', '--levees', help='Path to levees raster file.', required=False, default=""
    )
    parser.add_argument('-b', '--bridges', help='Path to bridges file.', required=False, default="")
    parser.add_argument(
        '-o',
        '--output-dir',
        help='Path to output directory where CSV files will be written.',
        required=False,
        default="",
    )
    parser.add_argument(
        '-j', '--job-number', help='Number of jobs to use.', required=False, default=""
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    nlcd = args['nlcd']
    levees = args['levees']
    bridges = args['bridges']

    raster_path_dict = {'nlcd': nlcd, 'levees': levees, 'bridges': bridges}

    args = vars(parser.parse_args())

    raster_path_dict = {'nlcd': args['nlcd']}

    queue_zonal_stats(
        args['fim_run_dir'], raster_path_dict, args['output_dir'], int(args['job_number'])
    )
