#!/usr/bin/env python3

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

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
MP_LOG = fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


# This is not part of an MP process, but needs to have FLOG carried over so this file can see it
def prepare_for_inundation(
    fim_run_dir,
    output_flows_dir,
    output_mapping_dir,
    job_number_huc,
    job_number_inundate,
    depthtif,
    log_output_file,
):
    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    source_flow_huc_dir_list = [
        x
        for x in os.listdir(output_flows_dir)
        if os.path.isdir(os.path.join(output_flows_dir, x)) and x[0] in ['0', '1', '2']
    ]
    fim_source_huc_dir_list = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    ]
    # Log missing hucs
    missing_hucs = list(set(source_flow_huc_dir_list) - set(fim_source_huc_dir_list))
    missing_hucs = [huc for huc in missing_hucs if "." not in huc]
    # Loop through matching huc directories in the source_flow directory
    matching_hucs = list(set(fim_source_huc_dir_list) & set(source_flow_huc_dir_list))

    child_log_file_prefix = "MP_run_ind"
    ctr = 0
    num_hucs = len(matching_hucs)
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in matching_hucs:
            if "." in huc:
                continue

            # Get list of AHPS site directories
            huc_flows_dir = os.path.join(output_flows_dir, huc)

            # ahps_site_dir_list = os.listdir(ahps_site_dir)
            ahps_site_dir_list = [
                x for x in os.listdir(huc_flows_dir) if os.path.isdir(os.path.join(huc_flows_dir, x))
            ]

            # Map path to huc directory inside the mapping directory
            huc_mapping_dir = os.path.join(output_mapping_dir, huc)
            if not os.path.exists(huc_mapping_dir):
                os.mkdir(huc_mapping_dir)

            FLOG.lprint(f">> Running inundation and mosiacking for {huc} ({ctr+1} of {num_hucs})")

            # Loop through AHPS sites
            for ahps_site in ahps_site_dir_list:
                # map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj)
                ahps_site_parent = os.path.join(huc_flows_dir, ahps_site)

                # thresholds_dir_list = os.listdir(ahps_site_parent)
                thresholds_dir_list = [
                    x
                    for x in os.listdir(ahps_site_parent)
                    if os.path.isdir(os.path.join(ahps_site_parent, x))
                ]

                # Map parent directory for all inundation output files output files.
                huc_site_mapping_dir = os.path.join(huc_mapping_dir, ahps_site)
                if not os.path.exists(huc_site_mapping_dir):
                    os.mkdir(huc_site_mapping_dir)

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
                            huc_site_mapping_dir, ahps_site + '_' + magnitude + '_extent.tif'
                        )
                        FLOG.trace(f"Begin inundation against {magnitude_flows_csv}")
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
                                log_output_file,
                                child_log_file_prefix,
                            )

                        except Exception:
                            FLOG.critical(
                                "An error occured while attempting inundation"
                                f" for {huc} -- {ahps_site} -- {magnitude}"
                            )
                            FLOG.critical(traceback.format_exc())
                            MP_LOG.merge_log_files(log_output_file, child_log_file_prefix)
                            sys.exit(1)

    # end of ProcessPoolExecutor

    # rolls up logs from child MP processes into this parent_log_output_file
    MP_LOG.merge_log_files(log_output_file, child_log_file_prefix)
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
    parent_log_file_prefix,
):
    # Note: parent_log_file_prefix is "MP_run_ind", meaning all logs created by this function start
    #  with the phrase "MP_run_ind"
    #  They will be rolled up into the parent_log_output_file
    # This is setting up logging for this function to go up to the parent
    MP_LOG.MP_Log_setup(parent_log_output_file, parent_log_file_prefix)

    huc_dir = os.path.join(fim_run_dir, huc)
    try:
        MP_LOG.lprint(f"... Running Inundate_gms and mosiacking for {huc} : {ahps_site} : {magnitude}")
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
        MP_LOG.trace(f"Mosaicking for {huc} : {ahps_site} : {magnitude}")
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
        MP_LOG.trace(f"Mosaicking complete for {huc} : {ahps_site} : {magnitude}")
    except Exception:
        # Log errors and their tracebacks
        MP_LOG.error(f"Exception: running inundation for {huc}" + traceback.format_exc())

    # # Inundation.py appends the huc code to the supplied output_extent_grid for stage-based.
    # # Modify output_extent_grid to match inundation.py saved filename.
    # # Search for this file, if it didn't create, send message to log file.
    # base_file_path, extension = os.path.splitext(output_extent_grid)
    # saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)

    # MP_LOG.trace(f"saved_extent_grid_filename is {saved_extent_grid_filename}")

    # if not os.path.exists(saved_extent_grid_filename):
    #     MP_LOG.error('FAILURE_huc_{}:{}:{} map failed to create\n'.format(huc, ahps_site, magnitude))

    # TODO: Jun 17, 2024
    # There are a ton of intermediary files creatd in this process.
    # We get a tiff for each branch and branch zero PER magnitude.
    # For Flow based, we only need to keep the magnitude rollup mosiac.
    #   ie) matm1_action_extent.tif, matm1_minor_extent.tif, etc
    # For Stage Based, we have the same mosiac's as above which we want to keep but also
    #   a mosiac per stage and magnitude.
    #   ie). allm1_action_12p0ft_extent.tif, allm1_action_13p0ft_extent.tif (in addition to the magn_extent"
    #   as mentioned above)
    # For space reasons, we need to delete all of the intermediary files such as:
    #    Stage: grmn3_action_extent_0.tif, grmn3_action_extent_1933000003.tif. The give aways are a number before
    #        the .tif
    #    Flows: allm1_action_12p0ft_extent_01010002_0.tif, allm1_action_12p0ft_extent_01010002_7170000001.tif
    #       your give away is to just delete any file that has the HUC number in teh file name

    return


# This is part of an MP Pool
def post_process_huc_level(
    output_catfim_dir,
    job_number_inundate,
    ahps_dir_list,
    huc_dir,
    gpkg_dir,
    fim_version,
    huc,
    parent_log_output_file,
    parent_log_file_prefix,
):

    # Note: parent_log_file_prefix is "MP_post_process_{huc}", meaning all logs created by this function start
    #  with the phrase "MP_post_process_{huc}". This one rollups up to the master catfim log
    # This is setting up logging for this function to go up to the parent

    print(f"parent_log_output_file is {parent_log_output_file}")
    print(f"parent_log_file_prefix is {parent_log_file_prefix}")

    MP_LOG.MP_Log_setup(parent_log_output_file, parent_log_file_prefix)

    # Loop through ahps sites
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    print(f"ahps_dir_list is {ahps_dir_list}")

    for ahps_lid in ahps_dir_list:
        print()
        tifs_to_reformat_list = []
        mapping_huc_lid_dir = os.path.join(huc_dir, ahps_lid)
        MP_LOG.trace(f"mapping_huc_lid_dir is {mapping_huc_lid_dir}")

        # Append desired filenames to list. (notice.. no value after the word extent)
        tif_list = [x for x in os.listdir(mapping_huc_lid_dir) if x.endwith("extent.tif")]

        MP_LOG.trace(f"tif_list is {tif_list}")

        if len(tif_list) == 0:
            MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
            continue

        # Filter that to the right extent file(s)

        # if stage based, the file names looks like this: masm1_major_20p0ft_extent.tif
        #    but there also is masm1_major_extent.tif, so we want both
        # if flow based, the file name looks like this: masm1_action_extent.tif
        for tif in tif_list:
            MP_LOG.trace(f"tif is at {tif}")
            if ahps_lid in tif:
                tifs_to_reformat_list.append(os.path.join(mapping_huc_lid_dir, tif))

        MP_LOG.trace(f"tifs_to_reformat_list is {tifs_to_reformat_list}")

        # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
        # TODO: huh?  to the line above
        nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

        # We are going to do an MP in MP.
        child_log_file_prefix = "MP_reformat_tifs_{huc}"

        # TEMP DEBUG ADD BACK IN MAYBE AFTER DEBUGGING?
        MP_LOG.trace(f"Reformatting TIFs {ahps_lid} for {huc_dir}")
        with ProcessPoolExecutor(max_workers=job_number_inundate) as executor:
            for tif_to_process in tifs_to_reformat_list:
                # If not os.path.exists(tif_to_process):
                #    continue

                # If stage based, the file names looks like this: masm1_major_20p0ft_extent.tif
                #    but there also is masm1_major_extent.tif, so we want both
                # If flow based, the file name looks like this: masm1_action_extent.tif
                try:
                    file_name_parts = os.path.split(tif_to_process)
                    magnitude = file_name_parts[1]

                    if "ft" in file_name_parts[2]:  # stage based
                        try:
                            interval_stage = float(file_name_parts[2].replace('p', '.').replace("ft", ""))
                        except ValueError:
                            interval_stage = None
                            MP_LOG.error(
                                f"Value Error for {huc} - {ahps_lid} - magnitude {magnitude}"
                                " at {mapping_huc_lid_dir}"
                            )
                            MP_LOG.error(traceback.format_exc())

                        MP_LOG.trace(f"interval stage is {interval_stage}")

                    else:  # flow based
                        interval_stage = None
                        MP_LOG.trace("interval stage is None")

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
                        child_log_file_prefix,
                    )
                except Exception:
                    MP_LOG.error(
                        f"An ind reformat map error occured for {huc} - {ahps_lid} - magnitude {magnitude}"
                    )
                    MP_LOG.error(traceback.format_exc())
        # end of ProcessPoolExecutor

        # rolls up logs from child MP processes into this parent_log_output_file
        MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix)

    return


# This is not part of an MP process, but does need FLOG carried into it so it can use FLOG directly
def post_process_cat_fim_for_viz(
    output_catfim_dir, job_number_huc, job_number_inundate, fim_version, log_output_file
):

    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    FLOG.lprint("Start post processing TIFs (TIF extents into poly into gpkg)...")
    gpkg_dir = os.path.join(output_catfim_dir, 'gpkg')
    if not os.path.exists(gpkg_dir):
        os.mkdir(gpkg_dir)

    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')

    # Find the FIM version
    merged_layer = os.path.join(output_mapping_dir, 'catfim_library.gpkg')

    if not os.path.exists(merged_layer):  # prevents appending to existing output
        # huc_ahps_dir_list = os.listdir(output_mapping_dir)
        huc_ahps_dir_list = [
            x
            for x in os.listdir(output_mapping_dir)
            if os.path.isdir(os.path.join(output_mapping_dir, x)) and x[0] in ['0', '1', '2']
        ]

        # skip_list = ['errors', 'logs', 'gpkg', 'missing_files.txt', 'messages', merged_layer]

        # Loop through all categories
        child_log_file_prefix = "MP_post_process_{huc}"
        with ProcessPoolExecutor(max_workers=job_number_huc) as huc_exector:
            for huc in huc_ahps_dir_list:
                FLOG.lprint(f"TIF post processing for {huc}")
                # if huc in skip_list:
                #    continue

                huc_dir = os.path.join(output_mapping_dir, huc)

                try:
                    ahps_dir_list = [
                        x for x in os.listdir(huc_dir) if os.path.isdir(os.path.join(huc_dir, x))
                    ]
                    # ahps_dir_list = os.listdir(huc_dir)
                except NotADirectoryError:
                    FLOG.warning(f"{huc_dir} directory missing. Continuing on")
                    continue

                # If there's no mapping for a HUC, delete the HUC directory.
                if len(ahps_dir_list) == 0:
                    # Temp DEBUG
                    # os.rmdir(huc_dir)
                    FLOG.warning(f"no mapping for {huc}")
                    continue

                FLOG.trace("Just before post process huc level")
                huc_exector.submit(
                    post_process_huc_level,
                    output_catfim_dir,
                    job_number_inundate,
                    ahps_dir_list,
                    huc_dir,
                    gpkg_dir,
                    fim_version,
                    huc,
                    log_output_file,
                    child_log_file_prefix,
                )
                FLOG.trace("Just after post process huc level")

        # end of ProcessPoolExecutor

        # rolls up logs from child MP processes into this parent_log_output_file
        MP_LOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix)

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

    FLOG.lprint("End post processing TIFs...")

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
    parent_log_file_prefix,
):
    # interval stage might come in as null and that is ok

    # Note: parent_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this
    # function start with the phrase "MP_reformat_tifs_{huc}". This will rollup to the master
    # catfim logs

    # This is setting up logging for this function to go up to the parent
    MP_LOG.MP_Log_setup(parent_log_output_file, parent_log_file_prefix)

    try:
        MP_LOG.trace(
            f"{huc} : {ahps_lid} : {magnitude} -- Start reformat_inundation_maps" " (tif extent to gpkg poly)"
        )
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

        # TODO: likely need Alaska to be 3339 but not sure yet. If so, we might need a crs column?
        # or seperate alaska files?
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
        # TODO: might need to redo for Alaska
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
            MP_LOG.lprint(
                f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map saved"
                " as {diss_extent_filename}"
            )
        else:
            MP_LOG.warning(f"{huc} : {ahps_lid} : {magnitude} tif to gpkg, geodataframe is empty")

    except Exception:
        MP_LOG.error(f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map - Exception")
        MP_LOG.error(traceback.format_exc())
        pass
        # Log and clean out the gdb so it's not merged in later

    #        try:
    #            print(e)
    #            f = open(log_file, 'a+')
    #            f.write(str(diss_extent_filename) + " - dissolve error: " + str(e))
    #            f.close()
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
    log_output_file,
):

    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    FLOG.lprint('Begin mapping')
    start = time.time()

    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    if not os.path.exists(output_mapping_dir):
        os.mkdir(output_mapping_dir)

    FLOG.lprint("Generating Categorical FIM")
    prepare_for_inundation(
        fim_run_dir,
        output_flows_dir,
        output_mapping_dir,
        job_number_huc,
        job_number_inundate,
        depthtif,
        FLOG.LOG_FILE_PATH,
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

    end = time.time()
    elapsed_time = (end - start) / 60
    FLOG.lprint(f"Finished mapping in {str(elapsed_time).split('.')[0]} minutes")

    return


if __name__ == '__main__':

    """
    Sample Usage:
    python3 /foss_fim/tools/generate_categorical_fim_mapping.py -r "/outputs/rob_test_catfim_huc"
     -s "/data/catfim/rob_test/test_5_flow_based/flows" -o "/data/catfim/rob_test/test_5_flow_based"
     -jh 1 -jn 40

    """

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
        '-jh',
        '--job-number-huc',
        help='Number of processes to use for huc processing. Default is 1.',
        required=False,
        default="1",
        type=int,
    )
    parser.add_argument(
        '-jn',
        '--job-number-inundate',
        help='OPTIONAL: Number of processes to use for inundating'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'
        ' of the machine. Defaults to 1.',
        required=False,
        default=1,
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
    job_number_huc = int(args['job_number_huc'])
    depthtif = args['write_depth_tiff']
    log_output_file = os.path.join(output_catfim_dir, "logs", "gen_cat_mapping.log")

    manage_catfim_mapping(
        fim_run_dir, source_flow_dir, output_catfim_dir, job_number_huc, 1, depthtif, log_output_file
    )
