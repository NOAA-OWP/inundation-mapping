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

import utils.fim_logger as fl
from utils.shared_functions import getDriver
from utils.shared_variables import ALASKA_CRS, PREP_PROJECTION, VIZ_PROJECTION

# will become global once initiallized
FLOG = fl.FIM_logger()
MP_LOG =  fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


# This is not part of an MP process, but needs to have FLOG carried over so this file can see it
def prepare_for_inundation(
    fim_run_dir, output_flows_dir, output_catfim_dir, job_number_huc, job_number_inundate, depthtif, log_output_file
):
    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)
    
    source_flow_huc_dir_list = [x for x in os.listdir(output_flows_dir) if os.path.isdir(os.path.join(output_flows_dir, x))
                                and x[0] in ['0', '1', '2']]
        
    fim_source_huc_dir_list = [x for x in os.listdir(fim_run_dir) if os.path.isdir(os.path.join(fim_run_dir, x))
                               and x[0] in ['0', '1', '2']]

    # Log missing hucs
    missing_hucs = list(set(source_flow_huc_dir_list) - set(fim_source_huc_dir_list))
    missing_hucs = [huc for huc in missing_hucs if "." not in huc]

    # Loop through matching huc directories in the source_flow directory
    matching_hucs = list(set(fim_source_huc_dir_list) & set(source_flow_huc_dir_list))

    child_log_file_prefix = "MP_run_ind"
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in matching_hucs:
            if "." in huc:
                continue

            # Get list of AHPS site directories
            ahps_site_dir = os.path.join(output_flows_dir, huc)
            
            # ahps_site_dir_list = os.listdir(ahps_site_dir)
            ahps_site_dir_list = [x for x in os.listdir(ahps_site_dir) if os.path.isdir(os.path.join(ahps_site_dir, x))]            

            # Map path to huc directory inside out output_catfim_dir
            cat_fim_huc_dir = os.path.join(output_catfim_dir, huc)
            if not os.path.exists(cat_fim_huc_dir):
                os.mkdir(cat_fim_huc_dir)

            # Loop through AHPS sites
            for ahps_site in ahps_site_dir_list:
                # map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj)
                ahps_site_parent = os.path.join(ahps_site_dir, ahps_site)
                
                # thresholds_dir_list = os.listdir(ahps_site_parent)
                thresholds_dir_list = [x for x in os.listdir(ahps_site_parent) if os.path.isdir(os.path.join(ahps_site_parent, x))]            

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
                                fim_run_dir,
                                job_number_inundate,
                                str(FLOG.LOG_FILE_PATH),
                                child_log_file_prefix                                
                            )
                        except Exception:
                            FLOG.critical("An error occured while attempting inundation"
                                         f" for {huc} -- {ahps_site} -- {magnitude}")
                            FLOG.critical(traceback.print_exc())
                            sys.exit(1)

    # rolls up logs from child MP processes into this parent_log_output_file
    MP_LOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix )
    return


# This is part of an MP Pool
def run_inundation(
    magnitude_flows_csv,
    huc,
    output_extent_grid,
    ahps_site,
    magnitude,
    fim_run_dir,
    job_number_inundate,
    parent_log_output_file,
    parent_log_file_prefix
):
    
    # Note: parent_log_file_prefix is "MP_run_ind", meaning all logs created by this function start
    #  with the phrase "MP_run_ind"
    #  They will be rolled up into the parent_log_output_file
    
    # This is setting up logging for this function to go up to the parent
    log_folder = os.path.dirname(parent_log_output_file)    
    MP_LOG.MP_Log_setup(parent_log_file_prefix, log_folder)
    
    huc_dir = os.path.join(fim_run_dir, huc)
    try:
        MP_LOG.lprint(f"Running Inundate_gms for {huc} : {ahps_site} : {magnitude}")
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
        MP_LOG.lprint(f"Mosaicking for {huc} : {ahps_site} : {magnitude}")
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
        MP_LOG.lprint(f"Mosaicking complete for {huc} : {ahps_site} : {magnitude}")

    except Exception:
        # Log errors and their tracebacks
        MP_LOG.error(f"Exception: running inundation for {huc}" + traceback.format_exc())

    # Inundation.py appends the huc code to the supplied output_extent_grid.
    # Modify output_extent_grid to match inundation.py saved filename.
    # Search for this file, if it didn't create, send message to log file.
    base_file_path, extension = os.path.splitext(output_extent_grid)
    saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)
    if not os.path.exists(saved_extent_grid_filename):
        MP_LOG.error('FAILURE_huc_{}:{}:{} map failed to create\n'.format(huc, ahps_site, magnitude))
    return


# This is part of an MP Pool
def post_process_huc_level(output_catfim_dir, job_number_tif, ahps_dir_list, huc_dir, gpkg_dir, fim_version, huc,
                           parent_log_output_file, parent_log_file_prefix
):
  
    #Note: parent_log_file_prefix is "MP_post_process_{huc}", meaning all logs created by this function start
    #  with the phrase "MP_post_process_{huc}". This one rollups up to the master catfim log
    # This is setting up logging for this function to go up to the parent
    log_folder = os.path.dirname(parent_log_output_file)
    MP_LOG.MP_Log_setup(parent_log_file_prefix, log_folder)    
    
    MP_LOG.lprint(f">> we made it to post_process_huc_level for huc {huc}")
    MP_LOG.trace("the ahps_dir_list is ...")
    MP_LOG.trace(ahps_dir_list)
    # Loop through ahps sites
    
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')    
    
    for ahps_lid in ahps_dir_list:
        print()
        MP_LOG.lprint("start iterater for ahps_dir_list")
        MP_LOG.trace(f">> ahps list is {ahps_lid}")
        tifs_to_reformat_list = []
        ahps_lid_dir = os.path.join(huc_dir, ahps_lid)
        MP_LOG.lprint(f">> ahps_lid_dir is {ahps_lid_dir}")

        # Append desired filenames to list.
        tif_list = [x for x in os.listdir(ahps_lid_dir) if os.path.isdir(os.path.join(ahps_lid_dir, x))
                    and x.endwith(".tif")]
        #tif_list = os.listdir(ahps_lid_dir)
        for tif in tif_list:
            MP_LOG.trace(f"tif is at {tif}")
            if 'extent.tif' in tif:
                tifs_to_reformat_list.append(os.path.join(ahps_lid_dir, tif))

        # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
        nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

        if len(tifs_to_reformat_list) == 0:
            MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {ahps_lid_dir}")
            continue

        child_log_file_prefix = "MP_reformat_tifs_{huc}"
        # print(f"Reformatting TIFs {ahps_lid} for {huc_dir}") ## TEMP DEBUG ADD BACK IN MAYBE AFTER DEBUGGING?
        with ProcessPoolExecutor(max_workers=job_number_tif) as executor:
            for tif_to_process in tifs_to_reformat_list:
                #if not os.path.exists(tif_to_process):
                #    continue
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
                        MP_LOG.error(f"Value Error for {huc} - {ahps_lid} - magnitude {magnitude} at {ahps_lid_dir}")
                        MP_LOG.error(traceback.print_exc())
                        
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
                        parent_log_output_file,
                        child_log_file_prefix
                    )
                except Exception:
                    MP_LOG.error(f"An ind reformat map error occured for {huc} - {ahps_lid} - magnitude {magnitude}")
                    MP_LOG.error(traceback.print_exc())
        # rolls up logs from child MP processes into this parent_log_output_file
        MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix )
                   
    return

# This is not part of an MP process, but does need FLOG carried into it so it can use FLOG directly
def post_process_cat_fim_for_viz(output_catfim_dir, job_number_huc, job_number_inundate, fim_version, log_output_file):
    
    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)
    
    FLOG.lprint("Beginning post processing...")
    gpkg_dir = os.path.join(output_catfim_dir, 'gpkg')
    if not os.path.exists(gpkg_dir):
        os.mkdir(gpkg_dir)

    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')

    # Find the FIM version
    merged_layer = os.path.join(output_mapping_dir, 'catfim_library.gpkg')

    if not os.path.exists(merged_layer):  # prevents appending to existing output
        #huc_ahps_dir_list = os.listdir(output_mapping_dir)
        huc_ahps_dir_list = [x for x in os.listdir(output_mapping_dir) if os.path.isdir(os.path.join(output_mapping_dir, x))
                    and x[0] in ['0', '1', '2']]        
        
        FLOG.trace(">>>>>>>>>>>>>>>>>>>>>>>")
        FLOG.trace("Here is the list of huc mapping ahps dirs...")
        FLOG.trace(huc_ahps_dir_list)
        
        FLOG.lprint("")
        
        # skip_list = ['errors', 'logs', 'gpkg', 'missing_files.txt', 'messages', merged_layer]

        # Loop through all categories
        FLOG.lprint("Building list of TIFs to reformat...")
        child_log_file_prefix = "MP_post_process_{huc}"
        with ProcessPoolExecutor(max_workers=job_number_huc) as huc_exector:

            for huc in huc_ahps_dir_list:
                FLOG.lprint(f"huc in process pools start is {huc}")
                #if huc in skip_list:
                #    continue

                huc_dir = os.path.join(output_catfim_dir, huc)

                try:
                    ahps_dir_list = [x for x in os.listdir(huc_dir) if os.path.isdir(os.path.join(huc_dir, x))]
                    # ahps_dir_list = os.listdir(huc_dir)
                except NotADirectoryError:
                    FLOG.warning(f"{huc_dir} directory missing. Continuing on")
                    continue

                # If there's no mapping for a HUC, delete the HUC directory.

                if ahps_dir_list == []:
                    # Temp DEBUG
                    # os.rmdir(huc_dir)
                    FLOG.warning(f"no mapping for {huc}")
                    continue

                huc_exector.submit(
                    post_process_huc_level,
                    output_catfim_dir,
                    job_number_inundate,
                    ahps_dir_list,
                    huc_dir,
                    gpkg_dir,
                    fim_version,
                    huc,
                    str(FLOG.LOG_FILE_PATH),
                    child_log_file_prefix
                )
        # rolls up logs from child MP processes into this parent_log_output_file
        MP_LOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix )

        FLOG.trace(">>>>>>>>>>>>>>>>>>>>>>>")
        
        # Merge all layers
        gpkg_files = [x for x in os.listdir(gpkg_dir) if x.endswith('.gpkg')]
        FLOG.lprint(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}")        
        for layer in gpkg_files:
            # Open dissolved extent layers
            diss_extent_filename = os.path.join(gpkg_dir, layer)
            diss_extent = gpd.read_file(diss_extent_filename, engine='fiona')
            diss_extent['viz'] = 'yes'

            # Write/append aggregate diss_extent
            FLOG.lprint(f"Merging layer: {layer}")
            if os.path.isfile(merged_layer):
                diss_extent.to_file(merged_layer, driver=getDriver(merged_layer), index=False, mode='a')
            else:
                diss_extent.to_file(merged_layer, driver=getDriver(merged_layer), index=False)
            del diss_extent

            # shutil.rmtree(gpkg_dir)  # TODO  (hold for now, 
            #   re-add later. It leaves a huge amt of disk space)

    else:
        FLOG.warning(f"{merged_layer} already exists.")
    return

# This is part of an MP pool
def reformat_inundation_maps(
    ahps_lid,
    extent_grid,
    gpkg_dir,
    fim_version,
    huc,
    magnitude,
    nws_lid_attributes_filename,
    interval_stage,
    parent_log_output_file,
    parent_log_file_prefix
):
    
	# Note: parent_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this function start
    #  with the phrase "MP_reformat_tifs_{huc}". This will rollup to the master catfim logs
    
    # This is setting up logging for this function to go up to the parent
    log_folder = os.path.dirname(parent_log_output_file)    
    MP_LOG.MP_Log_setup(parent_log_file_prefix, log_folder)    
    
    try:
        MP_LOG.trace(f'{huc} : {ahps_lid} Inside reformat_inundation_maps...')
        # Convert raster to to shapes
        with rasterio.open(extent_grid) as src:
            image = src.read(1)
            mask = image > 0

        # print(f'{ahps_lid} Converted raster into shapes, now to aggregate shapes...')

        # Aggregate shapes
        results = (
            {'properties': {'extent': 1}, 'geometry': s}
            for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
        )

        # Convert list of shapes to polygon
        # extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=PREP_PROJECTION) # Previous code
        extent_poly = gpd.GeoDataFrame.from_features(list(results))  # Update to accomodate AK projection
        extent_poly = extent_poly.set_crs(src.crs)  # Update to accomodate AK projection

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
        
        MP_LOG.lprint(f"Inside reformat_inundation_maps - about to save {diss_extent_filename}")
        if not extent_poly_diss.empty:
            extent_poly_diss.to_file(
                diss_extent_filename, driver=getDriver(diss_extent_filename), index=False
            )
            MP_LOG.lprint(f"Inside reformat_inundation_maps - Saved {diss_extent_filename}")

    except Exception:
        MP_LOG.error(f"Inside reformat_inundation_maps - exception thrown {diss_extent_filename}")
        MP_LOG.error(traceback.format_exc())
        pass
        # Log and clean out the gdb so it's not merged in later


#        try:
#            print(e)
##            f = open(log_file, 'a+')
##            f.write(str(diss_extent_filename) + " - dissolve error: " + str(e))
##            f.close()
#        except:
#            pass
    return


# This is not part of an MP progress and simply needs the
# pointer of FLOG carried over here so it can use it directly.
def manage_catfim_mapping(
    fim_run_dir,
    output_flows_dir,
    output_catfim_dir,
    job_number_huc,
    job_number_inundate,
    depthtif,
    log_output_file
):
    
    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)
        
    # Create output directory
    if not os.path.exists(output_catfim_dir):
        os.mkdir(output_catfim_dir)

    FLOG.lprint("Generating Categorical FIM")
    prepare_for_inundation(
        fim_run_dir,
        output_flows_dir,
        output_catfim_dir,
        job_number_huc,
        job_number_inundate,
        depthtif,
        str(FLOG.LOG_FILE_PATH),
    )

    FLOG.lprint("Aggregating Categorical FIM")
    # Get fim_version.
    fim_version = (
        os.path.basename(os.path.normpath(fim_run_dir))
        .replace('fim_', '')
        .replace('_ms_c', '')
        .replace('_', '.')
    )
    post_process_cat_fim_for_viz(
        output_catfim_dir, job_number_huc, job_number_inundate, fim_version, str(FLOG.LOG_FILE_PATH)
    )
    
    return


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
    log_output_file = os.path.join(output_catfim_dir, "logs", "gen_cat_mapping.log")

    manage_catfim_mapping(fim_run_dir, source_flow_dir, output_catfim_dir, number_of_jobs, 1, depthtif, log_output_file)
