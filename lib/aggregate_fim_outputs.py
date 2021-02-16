#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import json
import rasterio
from rasterio.merge import merge
import shutil
import csv
from utils.shared_variables import PREP_PROJECTION


def aggregate_fim_outputs(fim_out_dir):

    print ("aggregating outputs to HUC6 scale")

    drop_folders = ['logs']
    huc_list = [huc for huc in os.listdir(fim_out_dir) if huc not in drop_folders]
    huc6_list = [str(huc[0:6]) for huc in os.listdir(fim_out_dir) if huc not in drop_folders]
    huc6_list = list(set(huc6_list))

    for huc in huc_list:

        os.makedirs(os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6])), exist_ok=True)

        # original file paths
        hydrotable_filename = os.path.join(fim_out_dir,huc,'hydroTable.csv')
        src_filename = os.path.join(fim_out_dir,huc,'src.json')

        # aggregate file name paths
        aggregate_hydrotable = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6]),'hydroTable.csv')
        aggregate_src = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc[0:6]),f'rating_curves_{huc[0:6]}.json')

        if len(huc)> 6:

            # open hydrotable
            hydrotable = pd.read_csv(hydrotable_filename)

            # write/append aggregate hydrotable
            if os.path.isfile(aggregate_hydrotable):
                hydrotable.to_csv(aggregate_hydrotable,index=False, mode='a',header=False)
            else:
                hydrotable.to_csv(aggregate_hydrotable,index=False)

            del hydrotable

            # open src
            src = open(src_filename)
            src = json.load(src)

            # write/append aggregate src
            if os.path.isfile(aggregate_src):

                with open(aggregate_src, "r+") as file:
                    data = json.load(file)
                    data.update(src)

                with open(aggregate_src, 'w') as outfile:
                    json.dump(data, outfile)
            else:
                with open(aggregate_src, 'w') as outfile:
                        json.dump(src, outfile)

            del src

        else:
            shutil.copy(hydrotable_filename, aggregate_hydrotable)
            shutil.copy(src_filename, aggregate_src)

    for huc6 in huc6_list:

        ## add feature_id to aggregate src
        aggregate_hydrotable = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc6),'hydroTable.csv')
        aggregate_src = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc6),f'rating_curves_{huc6}.json')

        # Open aggregate src for writing feature_ids to
        src_data = {}
        with open(aggregate_src) as jsonf:
            src_data = json.load(jsonf)

        with open(aggregate_hydrotable) as csvf:
            csvReader = csv.DictReader(csvf)

            for row in csvReader:
                if row['HydroID'].lstrip('0') in src_data and 'nwm_feature_id' not in src_data[row['HydroID'].lstrip('0')]:
                    src_data[row['HydroID'].lstrip('0')]['nwm_feature_id'] = row['feature_id']

        # Write src_data to JSON file
        with open(aggregate_src, 'w') as jsonf:
            json.dump(src_data, jsonf)

        ## aggregate rasters
        huc6_dir = os.path.join(fim_out_dir,'aggregate_fim_outputs',huc6)

        # aggregate file paths
        rem_mosaic = os.path.join(huc6_dir,f'hand_grid_{huc6}.tif')
        catchment_mosaic = os.path.join(huc6_dir,f'catchments_{huc6}.tif')

        if huc6 not in huc_list:

            huc6_filter = [path.startswith(huc6) for path in huc_list]
            subset_huc6_list = [i for (i, v) in zip(huc_list, huc6_filter) if v]

            # aggregate and mosaic rem
            rem_list = [os.path.join(fim_out_dir,huc,'rem_zeroed_masked.tif') for huc in subset_huc6_list]

            if len(rem_list) > 1:

                rem_files_to_mosaic = []

                for rem in rem_list:

                    rem_src = rasterio.open(rem)
                    rem_files_to_mosaic.append(rem_src)

                mosaic, out_trans = merge(rem_files_to_mosaic)
                out_meta = rem_src.meta.copy()
                out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION,'compress': 'lzw'})

                with rasterio.open(rem_mosaic, "w", **out_meta, tiled=True, blockxsize=256, blockysize=256, BIGTIFF='YES') as dest:
                    dest.write(mosaic)

                del rem_files_to_mosaic,rem_src,out_meta,mosaic

            elif len(rem_list)==1:

                shutil.copy(rem_list[0], rem_mosaic)

            # aggregate and mosaic catchments
            catchment_list = [os.path.join(fim_out_dir,huc,'gw_catchments_reaches_filtered_addedAttributes.tif') for huc in subset_huc6_list]

            if len(catchment_list) > 1:

                cat_files_to_mosaic = []

                for cat in catchment_list:
                    cat_src = rasterio.open(cat)
                    cat_files_to_mosaic.append(cat_src)

                mosaic, out_trans = merge(cat_files_to_mosaic)
                out_meta = cat_src.meta.copy()

                out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION,'compress': 'lzw'})

                with rasterio.open(catchment_mosaic, "w", **out_meta, tiled=True, blockxsize=256, blockysize=256, BIGTIFF='YES') as dest:
                    dest.write(mosaic)

                del cat_files_to_mosaic,cat_src,out_meta,mosaic

            elif len(catchment_list)==1:

                shutil.copy(catchment_list[0], catchment_mosaic)

        else:
            # original file paths
            rem_filename = os.path.join(fim_out_dir,huc6,'rem_zeroed_masked.tif')
            catchment_filename = os.path.join(fim_out_dir,huc6,'gw_catchments_reaches_filtered_addedAttributes.tif')

            shutil.copy(rem_filename, rem_mosaic)
            shutil.copy(catchment_filename, catchment_mosaic)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregate layers buy HUC6')
    parser.add_argument('-d','--fim-outputs-directory', help='FIM outputs directory', required=True)


    args = vars(parser.parse_args())

    fim_outputs_directory = args['fim_outputs_directory']

    aggregate_fim_outputs(fim_outputs_directory)
