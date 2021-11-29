#!/usr/bin/env python3

import os
import argparse
from multiprocessing import Pool
import pandas as pd
import json
import rasterio
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
import shutil
import csv
from utils.shared_variables import PREP_PROJECTION,VIZ_PROJECTION


def aggregate_fim_outputs(args):

    fim_out_dir   = args[0]
    huc6          = args[1]
    huc_list      = args[2]

    print(f"aggregating {huc6}")

    huc6_dir = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc6))
    os.makedirs(huc6_dir, exist_ok=True)

    # aggregate file name paths
    aggregate_hydrotable = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc6),'hydroTable.csv')
    aggregate_src = os.path.join(fim_out_dir,'aggregate_fim_outputs',str(huc6),f'rating_curves_{huc6}.json')

    for huc in huc_list:

        # original file paths
        hydrotable_filename = os.path.join(fim_out_dir,huc,'hydroTable.csv')
        src_filename = os.path.join(fim_out_dir,huc,'src.json')

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

    ## add feature_id to aggregate src
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
    # aggregate file paths
    rem_mosaic = os.path.join(huc6_dir,f'hand_grid_{huc6}_prepprj.tif')
    catchment_mosaic = os.path.join(huc6_dir,f'catchments_{huc6}_prepprj.tif')

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

            with rasterio.open(rem_mosaic, "w", **out_meta, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dest:
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

            with rasterio.open(catchment_mosaic, "w", **out_meta, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dest:
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

    ## reproject rasters
    reproject_raster(rem_mosaic,VIZ_PROJECTION)
    os.remove(rem_mosaic)

    reproject_raster(catchment_mosaic,VIZ_PROJECTION)
    os.remove(catchment_mosaic)


def reproject_raster(raster_name,reprojection):

    with rasterio.open(raster_name) as src:
        transform, width, height = calculate_default_transform(
            src.crs, reprojection, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': reprojection,
            'transform': transform,
            'width': width,
            'height': height,
            'compress': 'lzw'
        })

        raster_proj_rename = os.path.split(raster_name)[1].replace('_prepprj.tif', '.tif')
        raster_proj_dir = os.path.join(os.path.dirname(raster_name), raster_proj_rename)

        with rasterio.open(raster_proj_dir, 'w', **kwargs, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dst:
            # for i in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=reprojection,
                resampling=Resampling.nearest)
    del src, dst


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregate layers buy HUC6')
    parser.add_argument('-d','--fim-outputs-directory', help='FIM outputs directory', required=True)
    parser.add_argument('-j','--number-of-jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)


    args = vars(parser.parse_args())

    fim_outputs_directory = args['fim_outputs_directory']
    number_of_jobs = int(args['number_of_jobs'])

    drop_folders = ['logs']
    huc_list = [huc for huc in os.listdir(fim_outputs_directory) if huc not in drop_folders]
    huc6_list = [str(huc[0:6]) for huc in os.listdir(fim_outputs_directory) if huc not in drop_folders]
    huc6_list = list(set(huc6_list))

    procs_list = []

    for huc6 in huc6_list:

        limited_huc_list = [huc for huc in huc_list if huc.startswith(huc6)]

        procs_list.append([fim_outputs_directory,huc6,limited_huc_list])

    print(f"aggregating {len(huc_list)} hucs to HUC6 scale using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        pool.map(aggregate_fim_outputs, procs_list)
