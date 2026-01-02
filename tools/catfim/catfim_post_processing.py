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

        output_mapping_dir = os.path.join(output_folder, 'mapping')
        gpkg_dir = os.path.join(output_mapping_dir, 'gpkg')
        os.makedirs(gpkg_dir, exist_ok=True)

        # huc_ahps_dir_list = [
        #     x
        #     for x in os.listdir(output_mapping_dir)
        #     if os.path.isdir(os.path.join(output_mapping_dir, x)) and x[0] in ['0', '1', '2', '9']
        # ]


        # num_hucs = len(huc_ahps_dir_list)
        # huc_index = 0

        # # FLOG.lprint(f"Number of hucs to post process is {num_hucs}") # TODO: re-plug in logging
        # print(f"Number of hucs to post process is {num_hucs}") # TEMP DEBUG



        # Merge all layers
        gpkg_files = [x for x in os.listdir(gpkg_dir) if x.endswith('.gpkg')]
        # FLOG.lprint(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}") # TODO: re-plug in logging
        print(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}") # TEMP DEBUG # TODO: Update pathing info

        gpkg_files.sort()

        merged_layers_gdf = None
        ctr = 0
        num_gpkg_files = len(gpkg_files)
        for gpkg_file in gpkg_files: # TODO: need to iterate through HUC folders


        ## START SECTION FROM OLD CODE - some of this might be useful, but likely needs to be simplified and cleaned up.


        #     # for ctr, layer in enumerate(gpkg_files):
        #     # FLOG.lprint(f"Merging gpkg ({ctr+1} of {len(gpkg_files)} - {}")
        #     # FLOG.trace(f"Merging gpkg ({ctr+1} of {num_gpkg_files} : {gpkg_file}") # TODO: re-plug in logging
        #     print(f"Merging gpkg ({ctr+1} of {num_gpkg_files} : {gpkg_file}") # TEMP DEBUG

        #     # Concatenate each /gpkg/{huc}_{aphs}_{magnitude}_extent.gpkg
        #     diss_extent_filename = os.path.join(gpkg_dir, gpkg_file)
        #     diss_extent_gdf = gpd.read_file(diss_extent_filename, engine='fiona')

        #     if 'interval_stage' in diss_extent_gdf.columns:
        #         # Update the stage column value to be the interval value if an interval values exists

        #         diss_extent_gdf.loc[diss_extent_gdf["interval_stage"] > 0, "stage"] = diss_extent_gdf[
        #             "interval_stage"
        #         ]

        #     if ctr == 0:
        #         merged_layers_gdf = diss_extent_gdf
        #     else:
        #         merged_layers_gdf = pd.concat([merged_layers_gdf, diss_extent_gdf])

        #     del diss_extent_gdf # TODO: Add an option to only delete the intermediates sometimes?
        #     ctr += 1

        # if merged_layers_gdf is None or len(merged_layers_gdf) == 0:
        #     raise Exception(f"No gpkgs found in {gpkg_dir}")

        # # TODO: July 9, 2024: Consider deleting all of the interium .gpkg files in the gpkg folder.
        # # It will get very big quick. But not yet.
        # # shutil.rmtree(gpkg_dir)

        # # Now dissolve based on ahps and magnitude (we no longer saved non dissolved versrons)
        # # Aug 2024: We guessed on what might need to be dissolved from 4.4.0.0. In 4.4.0.0 there
        # # are "_dissolved" versions of catfim files but no notes on why or how, but this script
        # # did not do it. We are going to guess on what the dissolving rules are.
    
        # if catfim_type_name == "flow_based":
        #     # FLOG.lprint("Dissolving flow based catfim_libary by ahps and magnitudes") # TODO: re-plug in logging
        #     print("Dissolving flow based catfim_libary by ahps and magnitudes") # TEMP DEBUG

        #     merged_layers_gdf = merged_layers_gdf.dissolve(by=['ahps_lid', 'magnitude'], as_index=False)

        # if 'level_0' in merged_layers_gdf:
        #     merged_layers_gdf = merged_layers_gdf.drop(['level_0'], axis=1)

        # if 'status' in merged_layers_gdf:
        #     merged_layers_gdf = merged_layers_gdf.drop(['status'], axis=1)

        # if 'mapped' in merged_layers_gdf:
        #     merged_layers_gdf = merged_layers_gdf.drop(['mapped'], axis=1)

        # output_file_name = f"{catfim_type_name}_catfim_library"

        # # merged_layers_gdf["model_version"] = model_version # TODO: Figure out where to get the model version from (or if we actually even need this anymore?)
        # merged_layers_gdf["product_version"] = catfim_type_name

        # gpkg_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.gpkg')
        # # FLOG.lprint(f"Saving catfim library gpkg version to {gpkg_file_path}") # TODO: re-plug in logging
        # print(f"Saving catfim library gpkg version to {gpkg_file_path}") # TEMP DEBUG

        # merged_layers_gdf.to_file(gpkg_file_path, driver='GPKG', engine="fiona")

        # csv_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.csv')
        # # FLOG.lprint(f"Saving catfim library csv version to {csv_file_path}") # TODO: re-plug in logging
        # print(f"Saving catfim library csv version to {csv_file_path}") # TEMP DEBUG
        # merged_layers_gdf.to_csv(csv_file_path)

        # # FLOG.lprint("End post processing TIFs...") # TODO: re-plug in logging
        # print("End post processing TIFs...") # TEMP DEBUG
        


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
