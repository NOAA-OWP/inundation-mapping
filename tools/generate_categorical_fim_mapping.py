#!/usr/bin/env python3

import argparse
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait

import geopandas as gpd
import pandas as pd
import rasterio
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation
from rasterio.features import shapes
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from utils.shared_functions import getDriver
from utils.shared_variables import PREP_PROJECTION, VIZ_PROJECTION


def generate_categorical_fim(
    fim_run_dir, source_flow_dir, output_catfim_dir, job_number_huc, job_number_inundate, depthtif, log_file
):
    source_flow_dir_list = os.listdir(source_flow_dir)
    output_flow_dir_list = os.listdir(fim_run_dir)

    # Log missing hucs
    missing_hucs = list(set(source_flow_dir_list) - set(output_flow_dir_list))
    missing_hucs = [huc for huc in missing_hucs if "." not in huc]
    if len(missing_hucs) > 0:
        f = open(log_file, 'a+')
        f.write(f"Missing hucs from output directory: {', '.join(missing_hucs)}\n")
        f.close()

    # Loop through matching huc directories in the source_flow directory
    matching_hucs = list(set(output_flow_dir_list) & set(source_flow_dir_list))

    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in matching_hucs:
            if "." in huc:
                continue

            # Get list of AHPS site directories
            ahps_site_dir = os.path.join(source_flow_dir, huc)
            ahps_site_dir_list = os.listdir(ahps_site_dir)

            # Map path to huc directory inside out output_catfim_dir
            cat_fim_huc_dir = os.path.join(output_catfim_dir, huc)
            if not os.path.exists(cat_fim_huc_dir):
                os.mkdir(cat_fim_huc_dir)

            # Loop through AHPS sites
            for ahps_site in ahps_site_dir_list:
                # map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj)
                ahps_site_parent = os.path.join(ahps_site_dir, ahps_site)
                thresholds_dir_list = os.listdir(ahps_site_parent)

                # Map parent directory for all inundation output filesoutput files.
                cat_fim_huc_ahps_dir = os.path.join(cat_fim_huc_dir, ahps_site)
                if not os.path.exists(cat_fim_huc_ahps_dir):
                    os.mkdir(cat_fim_huc_ahps_dir)

                # Loop through thresholds/magnitudes and define inundation output files paths
                for magnitude in thresholds_dir_list:
                    if "." in magnitude:
                        continue
                    magnitude_flows_csv = os.path.join(
                        ahps_site_parent,
                        magnitude,
                        'ahps_' + ahps_site + '_huc_' + huc + '_flows_' + magnitude + '.csv',
                    )
                    if os.path.exists(magnitude_flows_csv):
                        output_extent_grid = os.path.join(
                            cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_extent.tif'
                        )
                        try:
                            executor.submit(
                                run_inundation,
                                magnitude_flows_csv,
                                huc,
                                output_extent_grid,
                                ahps_site,
                                magnitude,
                                log_file,
                                fim_run_dir,
                                job_number_inundate,
                            )
                        except Exception:
                            traceback.print_exc()
                            sys.exit(1)


def run_inundation(
    magnitude_flows_csv,
    huc,
    output_extent_grid,
    ahps_site,
    magnitude,
    log_file,
    fim_run_dir,
    job_number_inundate,
):
    huc_dir = os.path.join(fim_run_dir, huc)
    try:
        print("Running Inundate_gms for " + huc)
        map_file = Inundate_gms(
            hydrofabric_dir=fim_run_dir,
            forecast=magnitude_flows_csv,
            num_workers=job_number_inundate,
            hucs=huc,
            inundation_raster=output_extent_grid,
            inundation_polygon=None,
            depths_raster=None,
            verbose=False,
            log_file=None,
            output_fileNames=None,
        )
        print("Mosaicking for " + huc)
        Mosaic_inundation(
            map_file,
            mosaic_attribute='inundation_rasters',
            mosaic_output=output_extent_grid,
            mask=os.path.join(huc_dir, 'wbd.gpkg'),
            unit_attribute_name='huc8',
            nodata=-9999,
            workers=1,
            remove_inputs=False,
            subset=None,
            verbose=False,
        )

    except Exception:
        # Log errors and their tracebacks
        f = open(log_file, 'a+')
        f.write(f"{output_extent_grid} - inundation error: {traceback.format_exc()}\n")
        f.close()

    # Inundation.py appends the huc code to the supplied output_extent_grid.
    # Modify output_extent_grid to match inundation.py saved filename.
    # Search for this file, if it didn't create, send message to log file.
    base_file_path, extension = os.path.splitext(output_extent_grid)
    saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)
    if not os.path.exists(saved_extent_grid_filename):
        with open(log_file, 'a+') as f:
            f.write('FAILURE_huc_{}:{}:{} map failed to create\n'.format(huc, ahps_site, magnitude))


def post_process_huc_level(
    job_number_tif, ahps_dir_list, huc_dir, attributes_dir, gpkg_dir, fim_version, huc
):
    # Loop through ahps sites
    for ahps_lid in ahps_dir_list:
        tifs_to_reformat_list = []
        ahps_lid_dir = os.path.join(huc_dir, ahps_lid)

        # Append desired filenames to list.
        tif_list = os.listdir(ahps_lid_dir)
        for tif in tif_list:
            if 'extent.tif' in tif:
                tifs_to_reformat_list.append(os.path.join(ahps_lid_dir, tif))

        # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
        nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

        print(f"Reformatting TIFs {ahps_lid} for {huc_dir}")
        with ProcessPoolExecutor(max_workers=job_number_tif) as executor:
            for tif_to_process in tifs_to_reformat_list:
                if not os.path.exists(tif_to_process):
                    continue
                try:
                    magnitude = os.path.split(tif_to_process)[1].split('_')[1]
                    try:
                        interval_stage = float(
                            (os.path.split(tif_to_process)[1].split('_')[2])
                            .replace('p', '.')
                            .replace("ft", "")
                        )
                        if interval_stage == 'extent':
                            interval_stage = None
                    except ValueError:
                        interval_stage = None
                    executor.submit(
                        reformat_inundation_maps,
                        ahps_lid,
                        tif_to_process,
                        gpkg_dir,
                        fim_version,
                        huc,
                        magnitude,
                        nws_lid_attributes_filename,
                        interval_stage,
                    )
                except Exception as ex:
                    print(f"*** {ex}")
                    traceback.print_exc()


def post_process_cat_fim_for_viz(
    job_number_huc, job_number_tif, output_catfim_dir, attributes_dir, log_file="", fim_version=""
):
    print("In post processing...")
    # Create workspace
    gpkg_dir = os.path.join(output_catfim_dir, 'gpkg')
    if not os.path.exists(gpkg_dir):
        os.mkdir(gpkg_dir)

    # Find the FIM version
    merged_layer = os.path.join(output_catfim_dir, 'catfim_library.gpkg')
    if not os.path.exists(merged_layer):  # prevents appending to existing output
        huc_ahps_dir_list = os.listdir(output_catfim_dir)
        skip_list = ['errors', 'logs', 'gpkg', 'missing_files.txt', 'messages', merged_layer]

        # Loop through all categories
        print("Building list of TIFs to reformat...")
        with ProcessPoolExecutor(max_workers=job_number_huc) as huc_exector:
            for huc in huc_ahps_dir_list:
                if huc in skip_list:
                    continue
                huc_dir = os.path.join(output_catfim_dir, huc)
                try:
                    ahps_dir_list = os.listdir(huc_dir)
                except NotADirectoryError:
                    continue
                # If there's no mapping for a HUC, delete the HUC directory.
                if ahps_dir_list == []:
                    os.rmdir(huc_dir)
                    continue

                huc_exector.submit(
                    post_process_huc_level,
                    job_number_tif,
                    ahps_dir_list,
                    huc_dir,
                    attributes_dir,
                    gpkg_dir,
                    fim_version,
                    huc,
                )

        # Merge all layers
        print(f"Merging {len(os.listdir(gpkg_dir))} layers...")
        for layer in os.listdir(gpkg_dir):
            # Open dissolved extent layers
            diss_extent_filename = os.path.join(gpkg_dir, layer)
            diss_extent = gpd.read_file(diss_extent_filename)
            diss_extent['viz'] = 'yes'

            # Write/append aggregate diss_extent
            print(f"Merging layer: {layer}")
            if os.path.isfile(merged_layer):
                diss_extent.to_file(merged_layer, driver=getDriver(merged_layer), index=False, mode='a')
            else:
                diss_extent.to_file(merged_layer, driver=getDriver(merged_layer), index=False)
            del diss_extent

            # shutil.rmtree(gpkg_dir)  # TODO

    else:
        print(f"{merged_layer} already exists.")


def reformat_inundation_maps(
    ahps_lid,
    extent_grid,
    gpkg_dir,
    fim_version,
    huc,
    magnitude,
    nws_lid_attributes_filename,
    interval_stage=None,
):
    try:
        # Convert raster to to shapes
        with rasterio.open(extent_grid) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = (
            {'properties': {'extent': 1}, 'geometry': s}
            for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
        )

        # Convert list of shapes to polygon
        extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=PREP_PROJECTION)
        # Dissolve polygons
        extent_poly_diss = extent_poly.dissolve(by='extent')

        # Update attributes
        extent_poly_diss = extent_poly_diss.reset_index(drop=True)
        extent_poly_diss['ahps_lid'] = ahps_lid
        extent_poly_diss['magnitude'] = magnitude
        extent_poly_diss['version'] = fim_version
        extent_poly_diss['huc'] = huc
        extent_poly_diss['interval_stage'] = interval_stage
        # Project to Web Mercator
        extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

        # Join attributes
        nws_lid_attributes_table = pd.read_csv(nws_lid_attributes_filename, dtype={'huc': str})
        nws_lid_attributes_table = nws_lid_attributes_table.loc[
            (nws_lid_attributes_table.magnitude == magnitude) & (nws_lid_attributes_table.nws_lid == ahps_lid)
        ]
        extent_poly_diss = extent_poly_diss.merge(
            nws_lid_attributes_table,
            left_on=['ahps_lid', 'magnitude', 'huc'],
            right_on=['nws_lid', 'magnitude', 'huc'],
        )
        extent_poly_diss = extent_poly_diss.drop(columns='nws_lid')
        # Save dissolved multipolygon
        handle = os.path.split(extent_grid)[1].replace('.tif', '')
        diss_extent_filename = os.path.join(gpkg_dir, f"{handle}_{huc}_dissolved.gpkg")
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        if not extent_poly_diss.empty:
            extent_poly_diss.to_file(
                diss_extent_filename, driver=getDriver(diss_extent_filename), index=False
            )

    except Exception:
        pass
        # Log and clean out the gdb so it's not merged in later


#        try:
#            print(e)
##            f = open(log_file, 'a+')
##            f.write(str(diss_extent_filename) + " - dissolve error: " + str(e))
##            f.close()
#        except:
#            pass


def manage_catfim_mapping(
    fim_run_dir,
    source_flow_dir,
    output_catfim_dir,
    attributes_dir,
    job_number_huc,
    job_number_inundate,
    overwrite,
    depthtif,
):
    # Create output directory
    if not os.path.exists(output_catfim_dir):
        os.mkdir(output_catfim_dir)

    # Create log directory
    log_dir = os.path.join(output_catfim_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    # Create error log path
    log_file = os.path.join(log_dir, 'errors.log')

    job_number_tif = job_number_inundate

    print("Generating Categorical FIM")
    generate_categorical_fim(
        fim_run_dir,
        source_flow_dir,
        output_catfim_dir,
        job_number_huc,
        job_number_inundate,
        depthtif,
        log_file,
    )

    print("Aggregating Categorical FIM")
    # Get fim_version.
    fim_version = (
        os.path.basename(os.path.normpath(fim_run_dir))
        .replace('fim_', '')
        .replace('_ms_c', '')
        .replace('_', '.')
    )
    post_process_cat_fim_for_viz(
        job_number_huc, job_number_tif, output_catfim_dir, attributes_dir, log_file, fim_version
    )


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    parser.add_argument(
        '-r', '--fim-run-dir', help='Name of directory containing outputs of fim_run.sh', required=True
    )
    parser.add_argument(
        '-s',
        '--source-flow-dir',
        help='Path to directory containing flow CSVs to use to generate categorical FIM.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-o',
        '--output-catfim-dir',
        help='Path to directory where categorical FIM outputs will be written.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-j',
        '--number-of-jobs',
        help='Number of processes to use. Default is 1.',
        required=False,
        default="1",
        type=int,
    )
    parser.add_argument(
        '-depthtif',
        '--write-depth-tiff',
        help='Using this option will write depth TIFFs.',
        required=False,
        action='store_true',
    )

    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    source_flow_dir = args['source_flow_dir']
    output_catfim_dir = args['output_catfim_dir']
    number_of_jobs = int(args['number_of_jobs'])
    depthtif = args['write_depth_tiff']

    manage_catfim_mapping(fim_run_dir, source_flow_dir, output_catfim_dir, number_of_jobs, depthtif)
