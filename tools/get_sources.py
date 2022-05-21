#!/usr/bin/env python3

from pygeohydro import WBD
import py3dep
import argparse
import csv
import pandas as pd


def get_sources_by_hucs(hucs,output_file=None):

    # Parse HUCs from hucs.
    if isinstance(hucs,list):
        if len(hucs) == 1:
            try:
                with open(hucs[0]) as csv_file:  # Does not have to be CSV format.
                    hucs = [i[0] for i in csv.reader(csv_file)]
            except FileNotFoundError:
                hucs = hucs
        else:
                hucs = hucs
    elif isinstance(hucs,str):
        try:
            with open(hucs) as csv_file:  # Does not have to be CSV format.
                hucs = [i[0] for i in csv.reader(csv_file)]
        except FileNotFoundError:
            hucs = list(hucs)
        
    huc_length = [ len(h) for h in hucs ]
    huc_length = set(huc_length)

    if len(huc_length) > 1:
        raise ValueError("Pass equivalent length HUCs")

    huc_length = list(huc_length)[0]

    wbd = WBD(f'huc{huc_length}')
    wbd_df = wbd.byids(f'huc{huc_length}',hucs)

    bounds = [g.bounds for g in wbd_df.geometry]

    source_polygons = [py3dep.query_3dep_sources(b,crs=wbd_df.crs) for b in bounds]

    source_polygons = pd.concat(source_polygons).reset_index(drop=True)
    source_polygons = source_polygons.explode()
    source_polygons = source_polygons.reset_index(drop=True)

    print(source_polygons)
    breakpoint()

    if output_file is not None:
        source_polygons.to_file(output_file,index=False,driver='GPKG')

    return(source_polygons)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Gets polygons of sources')
    parser.add_argument('-u','--hucs',help='HUCs consistent size',required=True,nargs='+')
    parser.add_argument('-o','--output-file',help='Output file name',required=False,default=None)

    args = vars(parser.parse_args())

    get_sources_by_hucs(**args)

