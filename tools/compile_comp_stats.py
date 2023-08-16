#!/usr/bin/env python3

import argparse
import os
from glob import glob, iglob

import numpy as np
import pandas as pd


########################################################
'''
Feb 15, 2023 - This file may be deprecated. At a minimum, it needs
   a significant review and/or upgrade.
'''

########################################################


def Compile_comp_stats(hydrofabric_dirs):
    all_minutes = []
    all_models = []
    all_hucs = []

    file_iterator = get_log_files(hydrofabric_dirs)

    for entry in file_iterator:
        model, log = entry
        log_file = open(log, 'r')

        huc8code = log.split('/')[-1][0:8]

        for line in log_file:
            if 'wall clock' in line:
                time_string = line.strip().split(' ')[-1]

                time_string_split = time_string.split(':')

                if len(time_string_split) == 2:
                    minutes, seconds = time_string_split

                    total_minutes = float(minutes) + float(seconds) / 60

                if len(time_string_split) == 3:
                    hours, minutes, seconds = time_string_split

                    total_minutes = float(hours) * 60 + float(minutes) + float(seconds) / 60

                all_minutes.append(total_minutes)
                all_models.append(model)
                all_hucs.append(huc8code)

    df = pd.DataFrame({'Minutes': all_minutes, 'Model': all_models, 'HUC': all_hucs})

    total_per_huc = df.pivot_table(values='Minutes', index=['Model', 'HUC'], aggfunc=sum)

    print(
        total_per_huc.pivot_table(
            values='Minutes', index='Model', aggfunc=[np.mean, np.median, np.sum]
        )
    )


def get_log_files(hydrofabric_dirs):
    for hydrofabric_dir in hydrofabric_dirs:
        log_dir = os.path.join(hydrofabric_dir, 'logs')

        if os.path.join(log_dir, 'branch'):
            model = 'GMS'
        if '_MS' in log_dir:
            model = 'MS'
        if '_FR' in log_dir:
            model = 'FR'

        for fn in iglob(os.path.join(log_dir, '**', '[0-9]*.log'), recursive=True):
            yield (model, fn)


if __name__ == '__main__':
    ########################################################
    '''
    Feb 15, 2023 - This file may be deprecated. At a minimum, it needs
    a significant review and/or upgrade.
    '''

    ########################################################

    # parse arguments
    parser = argparse.ArgumentParser(description='Get Comp Stats')
    parser.add_argument(
        '-y',
        '--hydrofabric_dirs',
        help='Directory path to FIM hydrofabric by processing unit',
        required=True,
        nargs='+',
    )

    Compile_comp_stats(**vars(parser.parse_args()))
