#!/usr/bin/env python3

import argparse
import pandas as pd
import numpy as np
from glob import iglob
from os.path import join

# desired output for branches
# dataframe columns: HUC, branch_id, exit status, ,time, ram, 

def compile_summary(gms_output_dir,ouput=None):

    unit_summary = join(gms_output_dir,logs, 'summary_gms_unit.log')
    branch_summary = join(gms_output_dir,logs, 'summary_gms_branch.log')

    unit_summary = pd.read_csv(unit_summary,sep='\t')
    branch_summary = pd.read_csv(branch_summary,sep='\t')




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create stream network level paths')
    parser.add_argument('-d','--gms-output-dir', help='Input stream network', required=True)
    parser.add_argument('-o','--output', help='Input stream network', required=True)
    
    args = vars(parser.parse_args())

    compile_summary(**args)

