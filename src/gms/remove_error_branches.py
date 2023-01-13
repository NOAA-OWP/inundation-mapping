#!/usr/bin/env python3

# Removes branches with Exit status: 61 following split_flows.py.
# Removes both the branch folder and the reference in gms_inputs.csv

import os
import argparse
import pandas as pd
import shutil

def remove_error_branches(logfile, gms_inputs):
    if os.path.isfile(logfile):
        try:
            errors_df = pd.read_csv(logfile, sep=':', header=None)
        except pd.errors.EmptyDataError:
            print('\nLog file is empty. Skipping this HUC.\n')
            return

        gms_inputs_df = pd.read_csv(gms_inputs, header=None, dtype={0:str,1:str})

        # Make copy of gms_inputs.csv
        gms_inputs_copy = os.path.splitext(gms_inputs)[0] + '_original.csv'
        if not os.path.isfile(gms_inputs_copy):
            gms_inputs_df.to_csv(gms_inputs_copy, header=False, index=False)
            
        gms_inputs_removed = os.path.splitext(gms_inputs)[0] + '_removed.csv'
        if not os.path.isfile(gms_inputs_removed):
            error_branches = None
        else:
            error_branches = pd.read_csv(gms_inputs_removed, header=None, dtype=str)

        first_occurrence = []
        for i, row in errors_df.iterrows():
            error_code = row[2]

            if error_code == 61:
                dirname, basename = os.path.split(row[0])

                filename = os.path.splitext(basename)[0]

                print(f"Removing {filename}")

                split = str.split(filename, '_')

                huc = split[0]
                branch = split[3]

                if huc not in first_occurrence:
                    # Ignore previous removals for this HUC
                    if error_branches is not None:
                        error_branches = error_branches[error_branches[0] != huc]

                    first_occurrence.append(huc)

                output_dir = os.path.split(os.path.split(dirname)[0])[0]
                branch_dir = os.path.join(output_dir, huc, 'branches', branch)
                if os.path.exists(branch_dir):
                    shutil.rmtree(branch_dir)

                # Remove bad branch from DataFrame
                if branch in gms_inputs_df.loc[:,1].values:
                    gms_inputs_df = gms_inputs_df.drop(index=gms_inputs_df[gms_inputs_df.loc[:,1]==branch].index[0])

                tmp_df = pd.DataFrame([huc, branch]).T
                if error_branches is None:
                    error_branches = tmp_df
                else:
                    error_branches = pd.concat([error_branches, tmp_df])

        # Save list of removed branches
        if error_branches is not None and len(error_branches) > 0:
            pd.DataFrame(error_branches).to_csv(gms_inputs_removed, header=False, index=False)

            # Overwrite gms_inputs.csv with error branches removed
            gms_inputs_df.to_csv(gms_inputs, header=False, index=False)

            print('\nDone removing error branches\n')

        else:
            print('\nDone -- no branches to remove')

    else:
        print('\nNo log file found\n')


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Remove branches with Exit status: 61')
    parser.add_argument('-f','--logfile', help='Location of non_zero_exit_codes.log', required=True)
    parser.add_argument('-g','--gms-inputs', help='Location of gms_inputs.csv', required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    remove_error_branches(args['logfile'], args['gms_inputs'])