#!/usr/bin/env python3

from os import listdir, remove
from os.path import split, dirname
from pathlib import Path
import argparse
from multiprocessing import Pool
import pandas as pd
import geopandas as gpd
import json
import rasterio as rio
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

    huc6_dir = Path(fim_out_dir / 'aggregate_fim_outputs' / f"{huc6}")
    huc6_dir.mkdir(parents=True,exist_ok=True)

    # Aggregate catchments
    aggregate_catchments = Path(fim_out_dir / 'aggregate_fim_outputs' / str(huc6) / f'catchments_{huc6}.gpkg')
    aggregate_streams = Path(fim_out_dir / 'aggregate_fim_outputs' / str(huc6) / f'streams_{huc6}.gpkg')
    
    # Aggregate file name paths
    aggregate_hydrotable = Path(fim_out_dir / 'aggregate_fim_outputs' / str(huc6) / 'hydroTable.csv')
    aggregate_src = Path(fim_out_dir / 'aggregate_fim_outputs' / str(huc6) / f'rating_curves_{huc6}.json')

    for huc in huc_list:

        # Original file paths
        hydrotable_filename = Path(fim_out_dir / huc / 'hydroTable.csv')
        src_filename = Path(fim_out_dir / huc / 'src.json')
        streams_filename = Path(fim_out_dir / huc / 'demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg')
        catchments_filename = Path(fim_out_dir / huc / 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')

        if len(huc)> 6:

            hydrotable = pd.read_csv(hydrotable_filename)
            '''
            # Filter hydrotable to subset list of variables
            filtered_vars = ['HydroID','feature_id','order_','HUC','LakeID','barc_on','vmann_on','adjust_src_on','stage','discharge_cms']
            for var in filtered_vars:
                if var not in hydrotable_read.columns:
                    hydrotable_read[var] = pd.NA
            hydrotable = hydrotable_read[filtered_vars]
            if hydrotable.isnull().all().all():
                print('WARNING!! hydroTable does not contain any valid values - please check HUC --> ' + str(huc))
            '''
            #hydrotable = hydrotable_read.reindex(sorted(hydrotable_read.columns), axis=1)
            if hydrotable.isnull().all().all():
                print('WARNING!! hydroTable does not contain any valid values - please check HUC --> ' + str(huc))

            # Write/append aggregate hydrotable
            if aggregate_hydrotable.exists():
                #hydrotable.to_csv(aggregate_hydrotable,index=False, mode='a',header=False)
                hydrotable_concat = pd.read_csv(aggregate_hydrotable)
                if not set(hydrotable_concat.columns) == set(hydrotable.columns):
                    print('WARNING!! HUC6: ' + str(huc6) + ' found a mismatch in column dimensions/names during concatenation...')
                hydrotable = pd.concat([hydrotable_concat, hydrotable], axis=0)
            hydrotable.to_csv(aggregate_hydrotable,index=False)

            del hydrotable

            # Open src
            src = open(src_filename)
            src = json.load(src)

            # Write/append aggregate src
            if aggregate_src.exists():

                with open(aggregate_src, "r+") as file:
                    data = json.load(file)
                    data.update(src)

                with open(aggregate_src, 'w') as outfile:
                    json.dump(data, outfile)
            else:
                with open(aggregate_src, 'w') as outfile:
                        json.dump(src, outfile)

            del src

            # Open streams
            streams = gpd.read_file(streams_filename)

            # Write/append aggregate hydrotable
            if aggregate_streams.exists():
                streams.to_file(aggregate_streams,index=False, mode='a',header=False,driver='GPKG')
            else:
                streams.to_file(aggregate_streams,index=False,driver='GPKG')

            del streams

            # Open catchments
            catchments = gpd.read_file(catchments_filename)
            if 'src_calibrated' not in catchments.columns:
                catchments['src_calibrated'] = pd.NA

            # Write/append aggregate catchments
            if aggregate_catchments.exists():
                catchments.to_file(aggregate_catchments,index=False, mode='a',header=False,driver='GPKG')
            else:
                catchments.to_file(aggregate_catchments,index=False,driver='GPKG')

            del catchments

        else:
            shutil.copy(hydrotable_filename, aggregate_hydrotable)
            shutil.copy(src_filename, aggregate_src)

    ## Add feature_id to aggregate src
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

    ## Aggregate rasters
    # Aggregate file paths
    rem_mosaic = Path(huc6_dir / f'hand_grid_{huc6}_prepprj.tif')
    catchment_mosaic = Path(huc6_dir / f'catchments_{huc6}_prepprj.tif')

    if huc6 not in huc_list:

        huc6_filter = [path.startswith(huc6) for path in huc_list]
        subset_huc6_list = [i for (i, v) in zip(huc_list, huc6_filter) if v]

        # Aggregate and mosaic rem
        rem_list = [str(Path(fim_out_dir / huc / 'rem_zeroed_masked.tif')) for huc in subset_huc6_list]

        if len(rem_list) > 1:

            rem_files_to_mosaic = []

            for rem in rem_list:

                rem_src = rio.open(rem)
                rem_files_to_mosaic.append(rem_src)

            mosaic, out_trans = merge(rem_files_to_mosaic)
            out_meta = rem_src.meta.copy()
            out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION,'compress': 'lzw'})

            with rio.open(rem_mosaic, "w", **out_meta, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dest:
                dest.write(mosaic)

            del rem_files_to_mosaic,rem_src,out_meta,mosaic

        elif len(rem_list)==1:

            shutil.copy(rem_list[0], rem_mosaic)

        # Aggregate and mosaic catchments
        catchment_list = [str(Path(fim_out_dir / huc / 'gw_catchments_reaches_filtered_addedAttributes.tif')) for huc in subset_huc6_list]

        if len(catchment_list) > 1:

            cat_files_to_mosaic = []

            for cat in catchment_list:
                cat_src = rio.open(cat)
                cat_files_to_mosaic.append(cat_src)

            mosaic, out_trans = merge(cat_files_to_mosaic)
            out_meta = cat_src.meta.copy()

            out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "dtype": str(mosaic.dtype), "transform": out_trans,"crs": PREP_PROJECTION,'compress': 'lzw'})

            with rio.open(catchment_mosaic, "w", **out_meta, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dest:
                dest.write(mosaic)

            del cat_files_to_mosaic,cat_src,out_meta,mosaic

        elif len(catchment_list)==1:

            shutil.copy(catchment_list[0], catchment_mosaic)

    else:
        # Original file paths
        rem_filename = Path(fim_out_dir / huc6 / 'rem_zeroed_masked.tif')
        catchment_filename = Path(fim_out_dir / huc6 / 'gw_catchments_reaches_filtered_addedAttributes.tif')

        shutil.copy(rem_filename, rem_mosaic)
        shutil.copy(catchment_filename, catchment_mosaic)

    ## Reproject rasters
    reproject_raster(rem_mosaic,VIZ_PROJECTION)
    remove(rem_mosaic)

    reproject_raster(catchment_mosaic,VIZ_PROJECTION)
    remove(catchment_mosaic)


def reproject_raster(raster_name,reprojection):

    with rio.open(raster_name) as src:
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

        raster_proj_rename = split(raster_name)[1].replace('_prepprj.tif', '.tif')
        raster_proj_dir = Path(dirname(raster_name), raster_proj_rename)

        with rio.open(raster_proj_dir, 'w', **kwargs, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES') as dst:
            reproject(
                source=rio.band(src, 1),
                destination=rio.band(dst, 1),
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

    fim_outputs_directory = Path(args['fim_outputs_directory'])
    number_of_jobs = int(args['number_of_jobs'])

    drop_folders = ['logs']
    huc_list = [huc for huc in listdir(fim_outputs_directory) if huc not in drop_folders]
    huc6_list = [str(huc[0:6]) for huc in listdir(fim_outputs_directory) if huc not in drop_folders]
    huc6_list = list(set(huc6_list))

    procs_list = []

    for huc6 in huc6_list:

        limited_huc_list = [huc for huc in huc_list if huc.startswith(huc6)]

        procs_list.append([fim_outputs_directory,huc6,limited_huc_list])

    print(f"aggregating {len(huc_list)} hucs to HUC6 scale using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        pool.map(aggregate_fim_outputs, procs_list)
    print('Aggregation finished')
