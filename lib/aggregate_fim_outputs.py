#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import json
import rasterio
from rasterio.merge import merge
from utils.shared_variables import PREP_PROJECTION


def aggregate_fim_outputs(fim_out_dir):

    print ("aggregating outputs to HUC6 scale")
    drop_folders = ['logs']
    huc_list = [huc for huc in os.listdir(fim_out_dir) if huc not in drop_folders]
    huc6_list = [str(huc[0:6]) for huc in os.listdir(fim_out_dir) if huc not in drop_folders]
    huc6_list = list(set(huc6_list))

    for huc in huc_list:

        os.makedirs(os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6])), exist_ok=True)

        # merge hydrotable
        aggregate_hydrotable = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6]),'hydroTable.csv')
        hydrotable = pd.read_csv(os.path.join(fim_out_dir,huc,'hydroTable.csv'))

        # write out hydrotable
        if os.path.isfile(aggregate_hydrotable):
            hydrotable.to_csv(aggregate_hydrotable,index=False, mode='a',header=False)
        else:
            hydrotable.to_csv(aggregate_hydrotable,index=False)

        del hydrotable

        # merge src
        aggregate_src = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6]),'src.json')
        src = open(os.path.join(fim_out_dir,huc,'src.json'))
        src = json.load(src)

        # write out src
        if os.path.isfile(aggregate_src):
            with open(aggregate_src, 'a') as outfile:
                    json.dump(src, outfile)
        else:
            with open(aggregate_src, 'w') as outfile:
                    json.dump(src, outfile)

        del src

    for huc6 in huc6_list:
        huc6_dir = os.path.join(fim_out_dir,'aggregate_fim_outputs',huc6)

        huc6_filter = [path.startswith(huc6) for path in huc_list]
        subset_huc6_list = [i for (i, v) in zip(huc_list, huc6_filter) if v]

        # aggregate and mosaic rem
        rem_list = [os.path.join(fim_out_dir,huc,'rem_zeroed_masked.tif') for huc in subset_huc6_list]

        rem_files_to_mosaic = []

        for rem in rem_list:
            rem_src = rasterio.open(rem)
            rem_files_to_mosaic.append(rem_src)

        mosaic, out_trans = merge(rem_files_to_mosaic)
        out_meta = rem_src.meta.copy()
        out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION})

        rem_mosaic = os.path.join(huc6_dir,'rem_zeroed_masked.tif')
        with rasterio.open(rem_mosaic, "w", **out_meta) as dest:
            dest.write(mosaic)

        del rem_files_to_mosaic,rem_src,out_meta,mosaic

        # aggregate and mosaic catchments
        catchment_list = [os.path.join(fim_out_dir,huc,'gw_catchments_reaches_filtered_addedAttributes.tif') for huc in subset_huc6_list]

        cat_files_to_mosaic = []

        for cat in catchment_list:
            cat_src = rasterio.open(cat)
            cat_files_to_mosaic.append(cat_src)

        mosaic, out_trans = merge(cat_files_to_mosaic)
        out_meta = cat_src.meta.copy()
        out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION})

        catchment_mosaic = os.path.join(huc6_dir,'gw_catchments_reaches_filtered_addedAttributes.tif')
        with rasterio.open(catchment_mosaic, "w", **out_meta) as dest:
            dest.write(mosaic)

        del cat_files_to_mosaic,cat_src,out_meta,mosaic


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregate layers buy HUC6')
    parser.add_argument('-d','--fim-outputs-directory', help='FIM outputs directory', required=True)


    args = vars(parser.parse_args())

    fim_outputs_directory = args['fim_outputs_directory']

    aggregate_fim_outputs(fim_outputs_directory)
