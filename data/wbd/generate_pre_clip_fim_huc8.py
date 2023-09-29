#!/usr/bin/env python3

import argparse
import os

from dotenv import load_dotenv


# from clip_vectors_to_wbd import subset_vector_layers


# # TODO
# # Update denylist after pulling the preclipped .wbd into the container
# load_dotenv('/foss_fim/src/bash_variables.env')
# input_WBD_gdb = os.getenv('input_WBD_gdb')
# pre_clip_huc_dir= os.getenv('pre_clip_huc_dir')


def pre_clip_hucs_from_wbd(wbd_file, outputs_dir, log_file):
    print(wbd_file, outputs_dir, log_file)

    # subset_vector_layers()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script is a wrapper of clip_vectors_to_wbd.py which will generate a '
        '<HUC>_wbd8_clp.gkg file per huc and place it within the output directory specified as the '
        'outputs_dir argument.',
        usage='''
            generate_pre_clipped_wbd.py
                /data/inputs/wbd/WBD_National.gpkg
                /data/inputs/pre_clipped_hucs_date
                -l log.txt
        ''',
    )
    parser.add_argument('wbd_file', help='.wbd file to clip into individual HUC.gpkg files.')
    parser.add_argument('outputs_dir', help='Directory to output all of the HUC level .gpkg files.')
    parser.add_argument(
        '-l', '--log_file', help='Optional argument to write stdout to a log file', default=None
    )

    args = vars(parser.parse_args())
    wbd_file = args['wbd_file']
    outputs_dir = args['outputs_dir']
    log_file = args['log_file']

    pre_clip_hucs_from_wbd(wbd_file, outputs_dir, log_file)
