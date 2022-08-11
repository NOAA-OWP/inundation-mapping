#!/usr/bin/env python3

# Removes branches with Exit status: 61 following split_flows.py.
# Removes both the branch folder and the reference in gms_inputs.csv

import os
import argparse
import pandas as pd
import shutil

def remove_error_branches(logfile, gms_inputs):
    if os.path.isfile(logfile):

        errors_df = pd.read_csv(logfile, sep=':', header=None)
        gms_inputs_df = pd.read_csv(gms_inputs, header=None)

        for i, row in errors_df.iterrows():
            error_code = row[2]

            if error_code == 61:
                dirname, basename = os.path.split(row[0])

                splitext = str.split(os.path.splitext(basename)[0], '_')

                huc = splitext[0]
                branch = splitext[3]

                output_dir = os.path.split(os.path.split(dirname)[0])[0]
                branch_dir = os.path.join(output_dir, huc, 'branches', branch)
                if os.path.exists(branch_dir):
                    shutil.rmtree(branch_dir)

                if int(branch) in gms_inputs_df.loc[:,1].values:
                    gms_inputs_df = gms_inputs_df.drop(index=gms_inputs_df[gms_inputs_df.loc[:,1]==int(branch)].index[0])

        # Overwrite gms_inputs.csv with error branches removed
        gms_inputs_df.to_csv(gms_inputs, header=False, index=False)


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Remove branches with Exit status: 61')
    parser.add_argument('-f','--logfile', help='non_zero_exit_codes.log', required=True)
    parser.add_argument('-g','--gms-inputs', help='gms_inputs.csv', required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    remove_error_branches(args['logfile'], args['gms_inputs'])

    # remove_error_branches('/data/outputs/dev-prune-error-branches/branch_errors/non_zero_exit_codes.log',
    #                       '/data/outputs/dev-prune-error-branches/gms_inputs.csv')