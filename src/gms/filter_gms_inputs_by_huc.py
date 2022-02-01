#!/usr/bin/env python3

import pandas as pd
import argparse

def filter_gms_inputs_by_huc(gms_inputs,hucs,gms_outputs):

    try:
        with open(hucs[0]) as hf:
            hucsList = set([str(h).rstrip() for h in hf])
    except FileNotFoundError:
        hucsList = set(hucs)
    
    gms_inputs = pd.read_csv(gms_inputs,header=None,dtype=str)
    gms_inputs_mask = gms_inputs.loc[:,0].isin(hucsList)
    gms_inputs = gms_inputs.loc[gms_inputs_mask,:]

    assert len(gms_inputs) > 0, "Filtered GMS list is empty"

    gms_inputs.to_csv(gms_outputs,index=False,header=False)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts the elevation of the thalweg to the lateral zonal minimum.')
    parser.add_argument('-g','--gms-inputs',help='Raster of elevation.',required=True)
    parser.add_argument('-u','--hucs',help='Raster of elevation.',required=True,nargs='+')
    parser.add_argument('-o','--gms-outputs',help='Raster of elevation.',required=True)

    args = vars(parser.parse_args())

    filter_gms_inputs_by_huc(**args)
