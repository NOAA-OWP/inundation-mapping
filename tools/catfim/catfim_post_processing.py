import argparse
import logging
import os
import traceback
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd

from dotenv import load_dotenv

import src.utils.shared_functions as sf


"""_summary_
    Overall processing steps (tenatively)

    Should not need to call any other of the catfim py files. Some of those master rollup
    functions should be moved here. Should not need to update the master sites or library files,
    only append them.

    1: Start up its own non-shared log system.  It can have its own log folder, and yes, each HUC
       has it's own log folder/files.

    1.b: load the runtime_arg.env if it needs anything from it.

    2: Validate HUCs data (has some sites and library data remaining)

    3: roll up all HUC level sites.csv/gpkg's and library files csv/gpkg into one big final set of files like we currently have.

    3.b Update the rolled up files for model_version (hand version) fields? TBD

    4: Roll up HUC logs?  Nah.. don't need it. A list of HUCs that we processed maybe?

    5: Roll up HUC error/warning logs?  seperate logs for warnign versus error?
       -- humnmm... if a HUC was re-run, it would have more than one possible error and/or warning file
       -- How do we roll up just the latest from each dir? and the rollup  here also needs date/time as it might
       have more then one set of files.


    this is taking over the functionality of the post_process_cat_fim_for_viz function... anything else?



"""


def catfim_post_processing(output_folder):

    is_logging_loaded = False

    # TEMPORARY DEBUGGING FUNCTIONALITY: Copy the contents of the input folder to a temp folder and work there.
    temp_output_folder = os.path.join(output_folder, "temp_post_process")
    os.mkdirs(temp_output_folder, exist_ok=True)
    shutil.copytree(output_folder, temp_output_folder, dirs_exist_ok=True)
    output_folder = temp_output_folder
    print('Using temporary output folder for post processing:', output_folder) ## TEMP DEBUG
    # REMOVE ABOVE BEFORE FLIGHT


    # Validate output_folder path
    if not os.path.exists(output_folder):
        raise Exception("CatFIM output path does not exist. Post-processing aborted.")

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        print("================================")

        # ---------------------
        # Load the runtime_args.env, error if it does not exist.
        # See generate_categorical_fim.py -> save_env_args(output_path)
        catfim_type = __load_runtime_args(output_folder)

        catfim_type_name = ""
        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        print(f"Start post-processing for {catfim_type_name} ;  (UTC): {dt_string}")
        print("")

        # Create filepath names and delete any pre-existing output files
        sites_file_path, library_file_path = __set_start_files_folders(output_folder, catfim_type_name)

        # ---------------------
        # Create a post-processing logger (Log folder may be shared with pre-processing)
        log_file_dir = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, "catfim_post_processing")
        print(f"  Logs will be save to {log_file_path}")

        # ---------------------
        # Validation checklist: 
        # - HUCs folder exists (this will check that generate_categorical_fim.py was run)
        # - data exists? might not need to pre-validate, though, because if it's possible it will become apparent pretty quickly
        # - valid HUCs exist

        # Validate that we have some huc sites / library data
        huc_path = os.path.join(output_folder, "hucs")
        if os.path.exists(huc_path):
            raise Exception("CatFIM output huc folder does not exist. Post-processing aborted.")

        # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/huc)

        # Rob's validation notes:
            # get list of hucs included
            # what if none?
            # roll up all HUC level sites.csv/gpkg's and library files csv/gpkg.
            # should always be at least one huc, but may not more depending on debugging

            # do we want to iterate each HUC folder looking for the existance of its final libary file
            # and count it?  If any one HUC did not get to a final gpkg, we know it aborted or failed somehow
            # and each HUC logs / prints would have told the user why  ???
            # Then we can show the user "x" hucs successfully processed.

            # Just because we have a HUC, does not mean we have a library files
            # And I guess it is possible we don't have a sites file either. ie) bad huc or huc with no sites

            # any final validation needed here? Maybe not other. Give warning but not
            # error that the file sites and library exists (again.. debugging)

        # START SECTION FROM OLD CODE: 

        output_mapping_dir = os.path.join(output_folder, 'mapping')
        gpkg_dir = os.path.join(output_mapping_dir, 'gpkg')
        os.makedirs(gpkg_dir, exist_ok=True)

        huc_ahps_dir_list = [
            x
            for x in os.listdir(output_mapping_dir)
            if os.path.isdir(os.path.join(output_mapping_dir, x)) and x[0] in ['0', '1', '2', '9']
        ]

        # # if we don't have a huc_ahps_dir_list, something went catestrophically bad # Jan 2026: We can remove this because it's done above. 
        # if len(huc_ahps_dir_list) == 0:
        #     raise Exception("Critical Error: Not possible to be here with no huc/ahps list")

        num_hucs = len(huc_ahps_dir_list)
        huc_index = 0

        # FLOG.lprint(f"Number of hucs to post process is {num_hucs}") # TODO: re-plug in logging
        print(f"Number of hucs to post process is {num_hucs}") # TEMP DEBUG

        # child_log_file_prefix = MP_LOG.MP_calc_prefix_name(log_output_file, "MP_post_process") # TODO: Jan 2026 Is this still needed? Removed for now

        # with ProcessPoolExecutor(max_workers=job_huc_ahps) as huc_exector: # TODO: Decide if we want to have multiproc here. Removed for now.

        for huc in huc_ahps_dir_list:
            # FLOG.lprint(f"TIF post processing for {huc}") # TODO: re-plug in logging
            print(f"TIF post processing for {huc}") # TEMP DEBUG

            huc_dir = os.path.join(output_mapping_dir, huc)
            progress_stmt = f"index {huc_index + 1} of {num_hucs}"
            huc_index += 1

            try:
                ahps_dir_list = [x for x in os.listdir(huc_dir) if os.path.isdir(os.path.join(huc_dir, x))]
                # ahps_dir_list = os.listdir(huc_dir)
            except NotADirectoryError:
                # FLOG.warning(f"{huc_dir} directory missing. Continuing on") # TODO: re-plug in logging
                print(f"WARNING: {huc_dir} directory missing. Continuing on") # TEMP DEBUG
                continue

            # If there's no mapping for a HUC, delete the HUC directory.
            if len(ahps_dir_list) == 0:
                os.rmdir(huc_dir)
                # FLOG.warning(f"no mapping for {huc}") # TODO: re-plug in logging
                print(f"WARNING: no mapping for {huc}") # TEMP DEBUG
                continue

            sys.exit("TEMP EXIT: post_process_huc removed for now, will bring back in later.") # TEMP DEBUG
            # TODO: Exiting before post_process_huc for now, will continue processing once the other parts of the code are working.
            huc_exector.submit( 
                post_process_huc,
                output_catfim_dir,
                ahps_dir_list,
                huc_dir,
                gpkg_dir,
                huc,
                log_output_file,
                child_log_file_prefix,
                progress_stmt,
            )

        # end of ProcessPoolExecutor # TEMP DEBUG removed for now

        # Roll up all logs from child MP processes into this parent_log_output_file

        # # Previous method: 
        # # rolls up logs from child MP processes into this parent_log_output_file
        # FLOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix, True)

        # Merge all layers
        gpkg_files = [x for x in os.listdir(gpkg_dir) if x.endswith('.gpkg')]
        # FLOG.lprint(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}") # TODO: re-plug in logging
        print(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}") # TEMP DEBUG

        gpkg_files.sort()

        merged_layers_gdf = None
        ctr = 0
        num_gpkg_files = len(gpkg_files)
        for gpkg_file in gpkg_files:

            # for ctr, layer in enumerate(gpkg_files):
            # FLOG.lprint(f"Merging gpkg ({ctr+1} of {len(gpkg_files)} - {}")
            # FLOG.trace(f"Merging gpkg ({ctr+1} of {num_gpkg_files} : {gpkg_file}") # TODO: re-plug in logging
            print(f"Merging gpkg ({ctr+1} of {num_gpkg_files} : {gpkg_file}") # TEMP DEBUG

            # Concatenate each /gpkg/{huc}_{aphs}_{magnitude}_extent.gpkg
            diss_extent_filename = os.path.join(gpkg_dir, gpkg_file)
            diss_extent_gdf = gpd.read_file(diss_extent_filename, engine='fiona')

            if 'interval_stage' in diss_extent_gdf.columns:
                # Update the stage column value to be the interval value if an interval values exists

                diss_extent_gdf.loc[diss_extent_gdf["interval_stage"] > 0, "stage"] = diss_extent_gdf[
                    "interval_stage"
                ]

            if ctr == 0:
                merged_layers_gdf = diss_extent_gdf
            else:
                merged_layers_gdf = pd.concat([merged_layers_gdf, diss_extent_gdf])

            del diss_extent_gdf # TODO: Add an option to only delete the intermediates sometimes?
            ctr += 1

        if merged_layers_gdf is None or len(merged_layers_gdf) == 0:
            raise Exception(f"No gpkgs found in {gpkg_dir}")

        # TODO: July 9, 2024: Consider deleting all of the interium .gpkg files in the gpkg folder.
        # It will get very big quick. But not yet.
        # shutil.rmtree(gpkg_dir)

        # Now dissolve based on ahps and magnitude (we no longer saved non dissolved versrons)
        # Aug 2024: We guessed on what might need to be dissolved from 4.4.0.0. In 4.4.0.0 there
        # are "_dissolved" versions of catfim files but no notes on why or how, but this script
        # did not do it. We are going to guess on what the dissolving rules are.
    
        if catfim_type_name == "flow_based":
            # FLOG.lprint("Dissolving flow based catfim_libary by ahps and magnitudes") # TODO: re-plug in logging
            print("Dissolving flow based catfim_libary by ahps and magnitudes") # TEMP DEBUG

            merged_layers_gdf = merged_layers_gdf.dissolve(by=['ahps_lid', 'magnitude'], as_index=False)

        if 'level_0' in merged_layers_gdf:
            merged_layers_gdf = merged_layers_gdf.drop(['level_0'], axis=1)

        if 'status' in merged_layers_gdf:
            merged_layers_gdf = merged_layers_gdf.drop(['status'], axis=1)

        if 'mapped' in merged_layers_gdf:
            merged_layers_gdf = merged_layers_gdf.drop(['mapped'], axis=1)

        output_file_name = f"{catfim_type_name}_catfim_library"

        # merged_layers_gdf["model_version"] = model_version # TODO: Figure out where to get the model version from (or if we actually even need this anymore?)
        merged_layers_gdf["product_version"] = catfim_type_name

        gpkg_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.gpkg')
        # FLOG.lprint(f"Saving catfim library gpkg version to {gpkg_file_path}") # TODO: re-plug in logging
        print(f"Saving catfim library gpkg version to {gpkg_file_path}") # TEMP DEBUG

        merged_layers_gdf.to_file(gpkg_file_path, driver='GPKG', engine="fiona")

        csv_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.csv')
        # FLOG.lprint(f"Saving catfim library csv version to {csv_file_path}") # TODO: re-plug in logging
        print(f"Saving catfim library csv version to {csv_file_path}") # TEMP DEBUG
        merged_layers_gdf.to_csv(csv_file_path)

        # FLOG.lprint("End post processing TIFs...") # TODO: re-plug in logging
        print("End post processing TIFs...") # TEMP DEBUG
        


        # END SECTION FROM OLD CODE




        # ---------------------
        # make csv versions of the two gpkg files

        # ---------------------
        # Rollup logs
        # Rollup huc Logs? Likely not.. just rollup error and warning logs.
        #   (humm. how to use only each HUCs latest one as it might have more than one if the HUC was run again)
        #   or maybe all? not sure what is smart here.
        #   search for files in each huc level for file names with _errors or _warnings

        logging.info("End CatFIM post-processing")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

    except Exception:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post-processing. Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm


def __load_runtime_args(output_folder):

    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)

    return os.getenv('CATFIM_TYPE')


def __set_start_files_folders(output_folder, catfim_type_name):

    # Note: all key other variables have already been validated

    # ================================
    # CLEANUP
    # Remove pre-existing output files / folders except anything in the log folder, we keep that one only.
    sites_file_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.gpkg")
    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    library_file_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.gpkg")
    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    # TODO: Do we also need to clean up any existing csv versions of these files?

    # Always keeps the logs folder

    return sites_file_path, library_file_path


# COPIED OVER FROM OLD CODE TODO: SIMPLIFY AND PLUG IN
# def post_process_huc(
#     output_catfim_dir,
#     ahps_dir_list,
#     huc_dir,
#     gpkg_dir,
#     huc,
#     parent_log_output_file,
#     child_log_file_prefix,
#     progress_stmt,
# ):
#     '''

#     This was part of an MP Pool
#     TODO: Aug 2024: job_number_inundate is not used well at all and is partially
#     with more cleanup to do later. Partially removed now.


#     '''

#     # Note: child_log_file_prefix is "MP_post_process_{huc}", meaning all logs created by this function start
#     #  with the phrase "MP_post_process_{huc}". This one rollups up to the master catfim log
#     # This is setting up logging for this function to go up to the parent
#     try:
#         MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)
#         MP_LOG.lprint(f'Post Processing {huc} ...')
#         MP_LOG.lprint(f'... {progress_stmt} ...')

#         # Loop through ahps sites
#         attributes_dir = os.path.join(output_catfim_dir, 'attributes')

#         for ahps_lid in ahps_dir_list:
#             tifs_to_reformat_list = []
#             mapping_huc_lid_dir = os.path.join(huc_dir, ahps_lid)
#             MP_LOG.trace(f"mapping_huc_lid_dir is {mapping_huc_lid_dir}")

#             # aka. ends with "extent.tif" which means it is a rolled up version up for there branches
#             tif_list = [x for x in os.listdir(mapping_huc_lid_dir) if ('extent.tif') in x]

#             if len(tif_list) == 0:
#                 # This is perfectly fine for there to be none
#                 # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
#                 continue

#             for tif in tif_list:
#                 tifs_to_reformat_list.append(os.path.join(mapping_huc_lid_dir, tif))

#             if len(tifs_to_reformat_list) == 0:
#                 # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
#                 continue

#             # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
#             nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

#             # There may not necessarily be an attributes.csv for this lid, depending on how flow processing went
#             # lots of lids fall out in the attributes or flow steps.
#             if os.path.exists(nws_lid_attributes_filename) == False:
#                 # MP_LOG.warning(f"{ahps_lid} has no attributes file (which may be perfectly fine)") # TODO: re-add logging
#                 print(f"{ahps_lid} has no attributes file (which may be perfectly fine)") # TEMP DEBUG
#                 continue

#             # We are going to do an MP in MP.
#             # child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
#             #    parent_log_output_file, "MP_reformat_tifs", huc
#             # )
#             # Weird case, we ahve to delete any of these files that might already exist (MP in MP)
#             # Get parent log dir
#             # log_dir = os.path.dirname(parent_log_output_file)
#             # old_refomat_log_files = glob.glob(os.path.join(log_dir, 'MP_reformat_tifs_*'))
#             # for log_file in old_refomat_log_files:
#             #     os.remove(log_file)

#             # we only have the rolled up, no branch versions by now
#             for tif_to_process in tifs_to_reformat_list:
#                 # If not os.path.exists(tif_to_process):
#                 #    continue

#                 # If stage based, the file names looks like this:
#                 #      masm1_major_extent.tif  (non-interval, whole number)
#                 #      masm1_major_20.6_extent.tif  (non-interval, float)
#                 #      masm1_major_20.0ft_extent.tif (interval)
#                 # If flow based, the file name looks like this: masm1_action_extent.tif
#                 # MP_LOG.trace(f".. Tif to Process = {tif_to_process}")
#                 try:

#                     tif_file_name = os.path.basename(tif_to_process)
#                     file_name_parts = tif_file_name.split("_")
#                     magnitude = file_name_parts[1]  # part 0 is the lid

#                     # but if it doesn't have "fti" at the end it is not an interval

#                     # careful. ft can be part of the site name, so only check part 3
#                     interval_stage = None
#                     is_interval = False
#                     if len(file_name_parts) >= 3 and "fti" in file_name_parts[2]:
#                         try:
#                             stage_val = file_name_parts[2].replace("fti", "")
#                             interval_stage = float(stage_val)
#                             is_interval = True
#                         except ValueError:
#                             interval_stage = None
#                             MP_LOG.error(
#                                 f"Value Error for {huc} - {ahps_lid} - magnitude {magnitude}"
#                                 f" at {mapping_huc_lid_dir}"
#                             )
#                             MP_LOG.error(traceback.format_exc())

#                     reformat_inundation_maps(
#                         ahps_lid,
#                         tif_to_process,
#                         gpkg_dir,
#                         huc,
#                         magnitude,
#                         nws_lid_attributes_filename,
#                         interval_stage,
#                         is_interval,
#                         parent_log_output_file,
#                         child_log_file_prefix,
#                     )
#                 except Exception:
#                     MP_LOG.error(
#                         f"An ind reformat map error occured for {huc} - {ahps_lid} - magnitude {magnitude}"
#                     )
#                     MP_LOG.error(traceback.format_exc())

#             # rolls up logs from child MP processes into this parent_log_output_file
#             # MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

#         # TODO:  Roll up the independent related ahps gpkgs into a huc level gkpg, still in the gpkg dir
#         # all of the gkpgs we want will have the huc number in front of it

#     except Exception:
#         MP_LOG.error(f"An error has occurred in post processing for {huc}")
#         MP_LOG.error(traceback.format_exc())

#     return

# COPIED OVER FROM OLD CODE TODO: SIMPLIFY AND PLUG IN
# def reformat_inundation_maps(
#     ahps_lid,
#     tif_to_process,
#     gpkg_dir,
#     huc,
#     magnitude,
#     nws_lid_attributes_filename,
#     interval_stage,
#     is_interval,
#     parent_log_output_file,
#     child_log_file_prefix,
# ):
#     '''
#     Converts an inundation raster (GeoTIFF) to a dissolved polygon GeoPackage with enriched attributes.

#     This function reads an inundation raster file, extracts inundated areas as polygons, dissolves them into a single multipolygon,
#     and joins additional attributes from a CSV file. The resulting GeoDataFrame is projected to Web Mercator and saved as a GeoPackage.
#     Logging is performed throughout the process, and special handling is included for interval stages and empty rasters.



#     '''

#     # Note: child_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this
#     # function start with the phrase will rollup to the master catfim logs

#     # This is setting up logging for this function to go up to the parent
#     MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

#     try:
#         MP_LOG.trace(
#             f"{huc} : {ahps_lid} : {magnitude} -- Start reformat_inundation_maps" " (tif extent to gpkg poly)"
#         )
#         # MP_LOG.trace(F"tif to process is {tif_to_process}")

#         # Convert raster to shapes
#         with rasterio.open(tif_to_process) as src:
#             image = src.read(1)
#             mask = image > 0

#         # Aggregate shapes
#         results = (
#             {'properties': {'extent': 1}, 'geometry': s}
#             for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
#         )

#         list_results = list(results)

#         # Check whether any shapes were found in the inundated tifs
#         # If not, log a message and return
#         if len(list_results) == 0:
#             MP_LOG.error(
#                 f"{huc} : {ahps_lid} : {magnitude} - No values above zero in inundated tif, "
#                 "so zero inundated shapes were found. See GitHub issue #1491 for details."
#             )
#             return

#         # Convert list of shapes to polygon
#         # lots of polys
#         extent_poly = gpd.GeoDataFrame.from_features(list_results, crs=src.crs)

#         # Dissolve polygons
#         extent_poly_diss = extent_poly.dissolve(by='extent')

#         # Update attributes
#         extent_poly_diss = extent_poly_diss.reset_index(drop=True)
#         extent_poly_diss['ahps_lid'] = ahps_lid
#         extent_poly_diss['magnitude'] = magnitude
#         extent_poly_diss['huc'] = huc
#         extent_poly_diss['interval_stage'] = interval_stage
#         extent_poly_diss['is_interval'] = is_interval

#         # Project to Web Mercator
#         extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

#         # Join attributes
#         nws_lid_attributes_table = pd.read_csv(nws_lid_attributes_filename, dtype={'huc': str})
#         nws_lid_attributes_table = nws_lid_attributes_table.loc[
#             (nws_lid_attributes_table.magnitude == magnitude) & (nws_lid_attributes_table.nws_lid == ahps_lid)
#         ]
#         extent_poly_diss = extent_poly_diss.merge(
#             nws_lid_attributes_table,
#             left_on=['ahps_lid', 'magnitude', 'huc'],
#             right_on=['nws_lid', 'magnitude', 'huc'],
#         )
#         # already has an ahps_lid column which we want and not the nws_lid column
#         extent_poly_diss = extent_poly_diss.drop(columns='nws_lid')

#         # Remove uncorrected stage from interval rows (to decrease potential for confusion)
#         extent_poly_diss.loc[extent_poly_diss['is_interval'] == True, 'stage_uncorrected'] = None

#         # Save dissolved multipolygon
#         handle = os.path.split(tif_to_process)[1].replace('.tif', '')
#         diss_extent_filename = os.path.join(gpkg_dir, f"{huc}_{handle}.gpkg")
#         extent_poly_diss["geometry"] = [
#             MultiPolygon([feature]) if type(feature) is Polygon else feature
#             for feature in extent_poly_diss["geometry"]
#         ]

#         if not extent_poly_diss.empty:
#             extent_poly_diss.to_file(
#                 diss_extent_filename, driver=getDriver(diss_extent_filename), index=False, engine='fiona'
#             )
#             # MP_LOG.trace(
#             #    f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map saved"
#             #    f" as {diss_extent_filename}"
#             # )
#         else:
#             MP_LOG.error(f"{huc} : {ahps_lid} : {magnitude} tif to gpkg, geodataframe is empty")

#     except ValueError as ve:
#         msg = f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map"
#         if "Assigning CRS to a GeoDataFrame without a geometry column is not supported" in ve:
#             MP_LOG.warning(f"{msg} - Warning: details: {ve}")
#         else:
#             MP_LOG.error(f"{msg} - Exception")
#             MP_LOG.error(traceback.format_exc())

#     except Exception:
#         MP_LOG.error(f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map - Exception")
#         MP_LOG.error(traceback.format_exc())

#     return

if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/catfim/catfim_post_processing.py -t /data/catfim/hand_4_8_7_2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Post Processing for CatFIM')

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    # call main program
    catfim_post_processing(**args)
