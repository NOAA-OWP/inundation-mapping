#!/usr/bin/env python3

import argparse
from glob import iglob
from os.path import join

import numpy as np
import pandas as pd


# desired output for branches
# dataframe columns: HUC, branch_id, exit status, ,time, ram,

########################################################
'''
Feb 15, 2023 - This file may be deprecated. At a minimum, it needs
   a significant review and/or upgrade.
'''

########################################################


def compile_summary(gms_output_dir, ouput=None):
    unit_summary = join(gms_output_dir, logs, 'summary_gms_unit.log')
    branch_summary = join(gms_output_dir, logs, 'summary_gms_branch.log')

    unit_summary = pd.read_csv(unit_summary, sep='\t')
    branch_summary = pd.read_csv(branch_summary, sep='\t')


if __name__ == '__main__':
    ########################################################
    '''
    Feb 15, 2023 - This file may be deprecated. At a minimum, it needs
    a significant review and/or upgrade.
    '''

    ########################################################

    parser = argparse.ArgumentParser(description='Create stream network level paths')
    parser.add_argument('-d', '--gms-output-dir', help='Input stream network', required=True)
    parser.add_argument('-o', '--output', help='Input stream network', required=True)

    args = vars(parser.parse_args())

    compile_summary(**args)
