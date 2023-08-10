#!/usr/bin/env python3

import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Checks final FIM outputs to identify missing HUCs'
    )
    parser.add_argument('-i', '--huc-list-dir', help='list of HUCs to run', required=True)
    parser.add_argument('-o', '--output-folder', help='directory of HUCs completed', required=True)

    args = vars(parser.parse_args())

    huc_list_dir = args['huc_list_dir']
    output_folder = args['output_folder']

    if not os.path.isfile(huc_list_dir):
        huc_list = huc_list_dir.split()
    else:
        with open(huc_list_dir) as f:
            huc_list = f.read().splitlines()

    output_huc_list = os.listdir(output_folder)

    if 'logs' in output_huc_list:
        output_huc_list.remove('logs')

    if 'aggregate_fim_outputs' in output_huc_list:
        output_huc_list.remove('aggregate_fim_outputs')

    missing_hucs = list(set(huc_list) - set(output_huc_list))

    if len(missing_hucs) > 0:
        print(f"MISSING {len(missing_hucs)} HUCS from outputs: {missing_hucs}")
