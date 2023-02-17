#!/usr/bin/env python3

import pandas as pd
import argparse

def filter_inputs_by_huc(fim_inputs, hucs, fim_outputs):

    try:
        with open(hucs[0]) as hf:
            hucsList = set([str(h).rstrip() for h in hf])
    except FileNotFoundError:
        hucsList = set(hucs)
    
    fim_inputs = pd.read_csv(fim_inputs,header=None,dtype=str)
    fim_inputs_mask = fim_inputs.loc[:,0].isin(hucsList)
    fim_inputs = fim_inputs.loc[fim_inputs_mask,:]

    assert len(fim_inputs) > 0, "Filtered FIM list is empty"

    fim_inputs.to_csv(fim_outputs, index=False, header=False)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts the elevation of the thalweg to the lateral zonal minimum.')
    parser.add_argument('-g','--fim-inputs',help='Raster of elevation.',required=True)
    parser.add_argument('-u','--hucs',help='Raster of elevation.',required=True,nargs='+')
    parser.add_argument('-o','--fim-outputs',help='Raster of elevation.',required=True)

    args = vars(parser.parse_args())

    filter_inputs_by_huc(**args)
