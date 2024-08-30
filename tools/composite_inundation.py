#!/usr/bin/env python3
import argparse

# from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
import concurrent.futures as cf
import copy
import json
import os
from datetime import datetime
from multiprocessing import Pool

import numpy as np
import pandas as pd
import rasterio
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from inundation import inundate
from mosaic_inundation import Mosaic_inundation
from tqdm import tqdm

from utils.shared_functions import FIM_Helpers as fh
from utils.shared_variables import elev_raster_ndv


class InundateModel_HUC(object):
    def __init__(self, model, source_directory, huc):
        self.model = model
        self.source_directory = source_directory
        self.huc = huc

    def inundate_huc(
        self,
        flows_file,
        composite_output_dir,
        output_name,
        log_file_path,
        num_workers_branches,
        no_cleanup,
        verbose,
    ):
        """
        Processing:
            Will inundate a single huc directory and if gms, will create an aggregate mosaic per huc

        Returns:
            The map file of the inundated raster.
        """

        source_huc_dir = os.path.join(self.source_directory, self.huc)

        # hucs do not need to exist as the list might have one huc from one
        # source directory that is not in another. TODO: how to log this.
        if not os.path.exists(source_huc_dir):
            print(f"HUC {self.huc} does not exist in {self.source_directory}")
            return None

        output_huc_dir = os.path.join(composite_output_dir, self.huc)
        # Create output directory if it does not exist
        if not os.path.isdir(output_huc_dir):
            os.mkdir(output_huc_dir)

        inundation_map_file = None

        output_raster_name = os.path.join(output_huc_dir, output_name)
        output_raster_name = fh.append_id_to_file_name(output_raster_name, [self.huc, self.model])

        if verbose:
            print(f"... Creating an inundation map for the FIM4 configuration for HUC {self.huc}...")

        if self.model in ["fr", "ms"]:
            if self.model == "ms":
                extent_friendly = "mainstem (MS)"
            elif self.model == "fr":
                extent_friendly = "full-resolution (FR)"
            rem = os.path.join(source_huc_dir, "rem_zeroed_masked.tif")
            catchments = os.path.join(source_huc_dir, "gw_catchments_reaches_filtered_addedAttributes.tif")
            catchment_poly = os.path.join(
                source_huc_dir, "gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg"
            )
            hydro_table = os.path.join(source_huc_dir, "hydroTable.csv")

            # Ensure that all of the required files exist in the huc directory
            for file in (rem, catchments, catchment_poly, hydro_table):
                if not os.path.exists(file):
                    raise Exception(
                        f"The following file does not exist within the supplied FIM directory:\n{file}"
                    )

            # Run inundation()
            # Must set workers to one as we are only processing one huc.
            map_file = inundate(
                rem,
                catchments,
                catchment_poly,
                hydro_table,
                flows_file,
                mask_type=None,
                num_workers=1,
                inundation_raster=output_raster_name,
                quiet=not verbose,
            )

            # if verbose:
            #    print("Inundation Response:")
            #    print(map_file)

            if len(map_file) == 0:
                raise Exception(f"Failed to inundate {extent_friendly} using the provided flows.")

            mosaic_file_path = map_file[0][0]
            inundation_map_file = [self.model, self.huc, mosaic_file_path]

        else:  # gms
            mosaic_file_path = produce_mosaicked_inundation(
                self.source_directory,
                [self.huc],
                flows_file,
                inundation_raster=output_raster_name,
                num_workers=num_workers_branches,
                remove_intermediate=not no_cleanup,
                verbose=verbose,
            )

            inundation_map_file = [self.model, self.huc, mosaic_file_path]

            if verbose:
                print(f"Inundation for HUC {self.huc} is complete")

        return inundation_map_file


class Composite_HUC(object):
    # Composites two source directories for a single huc
    # Note: The huc does not need to exist in both source directories
    @classmethod
    def composite_huc(self, args):
        huc = args["current_huc"]
        print(f"Processing huc {huc}")
        composite_model_map_files = []
        for model in args["models"]:
            # setup original fim processed directory
            if model == "ms":
                source_dir = args["fim_dir_ms"]
            elif model == "fr":
                source_dir = args["fim_dir_fr"]
            else:
                source_dir = args["gms_dir"]

            ci = InundateModel_HUC(model, source_dir, huc)
            map_file = ci.inundate_huc(
                args["flows_file"],
                args["composite_output_dir"],
                args["output_name"],
                args["log_file_path"],
                args["num_workers_branches"],
                args["no_cleanup"],
                args["verbose"],
            )
            if map_file is not None:
                composite_model_map_files.append(map_file)

        # Composite the two final model outputs
        inundation_map_file_df = pd.DataFrame(
            composite_model_map_files, columns=["model", "huc8", "inundation_rasters"]
        )

        if args["verbose"]:
            print("inundation_map_file_df")
            print(inundation_map_file_df)

        composite_file_output = os.path.join(args["composite_output_dir"], huc, args["output_name"])
        composite_file_output = fh.append_id_to_file_name(composite_file_output, huc)

        # NOTE: Leave workers as 1, it fails to composite correctly if more than one.
        #    - Also. by adding the is_mosaic_for_gms_branches = False, Mosaic_inudation
        #      will not auto add the HUC into the output name (its default behaviour)
        Mosaic_inundation(
            inundation_map_file_df,
            mosaic_attribute="inundation_rasters",
            mosaic_output=composite_file_output,
            mask=None,
            unit_attribute_name="huc8",
            nodata=elev_raster_ndv,
            workers=1,
            remove_inputs=not args["no_cleanup"],
            subset=None,
            verbose=args["verbose"],
        )

        if args["is_bin_raster"]:
            if args["verbose"]:
                print("Converting to binary")
            CompositeInundation.hydroid_to_binary(composite_file_output)


class CompositeInundation(object):
    @classmethod
    def run_composite(self, args):
        def __validate_args(args):
            """
            Runs `inundate()` on any two of the following:
                1) FIM 3.X mainstem (MS)
                2) FIM 3.X full-resolution (FR)
                3) FIM 4.x (gms)
                All three need to have outputs and composites results. Assumes that all products
            necessary for `inundate()` are in each huc8 folder.

            Parameters
            ----------
            fim_dir_ms : str
                Path to MS FIM directory. This should be an output directory from `fim_run.sh`.
            fim_dir_fr : str
                Path to FR FIM directory. This should be an output directory from `fim_run.sh`.
            gms_dir : str
                Path to FIM4 GMS directory. This should be an output directory from
                    `gms_run_unit, then gms_run_branch`.
            huc: str, optional
                If this value comes in, it shoudl be a single huc value. If it does not exist,
                we use all hucs in the give source directories.
            flows_file : str :
                Can be a single file path to a forecast csv or a comma-separated list of files.
            composite_output_dir : str
                Folder path to write outputs. It will be created if it does not exist.
            output_name : str, optional
                Name for output raster. If not specified, by default the raster will be named:
                    'inundation_composite_{flows_root}.tif'.
            is_bin_raster : bool, optional
                Flag to create binary raster as output.
            num_workers_huc : int, optional
                defaults to 1 and means the number of processes to be used for processing hucs
            num_workers_branches : int, optional
                defaults to 1 and means the number of processes to be used for processing gms branches
            no_cleanup : bool, optional
                If False, intermediate files are deleted.
            verbose : bool, optional
                show extra output.
            """

            if (args["fim_dir_ms"]) and (args["fim_dir_ms"].lower() == "none"):
                args["fim_dir_ms"] = None
            if (args["fim_dir_fr"]) and (args["fim_dir_fr"].lower() == "none"):
                args["fim_dir_fr"] = None
            if (args["gms_dir"]) and (args["gms_dir"].lower() == "none"):
                args["gms_dir"] = None

            # count number of input dir types and ensure their are no duplciates.
            dir_list_lowercase = []  # we use a forced lowercase to help ensure dups (might be mixed case)
            dir_list_raw = []
            missing_dir_msg = "{} directory of {} does not exist"
            args["models"] = []
            if args["fim_dir_ms"] is not None:
                args["models"].append("ms")
                assert os.path.isdir(args["fim_dir_ms"]), missing_dir_msg.format("ms", args["fim_dir_ms"])
                dir_list_raw.append(args["fim_dir_ms"])
                dir_list_lowercase.append(args["fim_dir_ms"].lower())

            if args["fim_dir_fr"] is not None:
                args["models"].append("fr")
                assert os.path.isdir(args["fim_dir_fr"]), missing_dir_msg.format("fr", args["fim_dir_fr"])
                dir_list_raw.append(args["fim_dir_fr"])
                dir_list_lowercase.append(args["fim_dir_fr"].lower())

            if args["gms_dir"] is not None:
                args["models"].append("gms")
                assert os.path.isdir(args["gms_dir"]), missing_dir_msg.format("gms", args["gms_dir"])
                dir_list_raw.append(args["gms_dir"])
                dir_list_lowercase.append(args["gms_dir"].lower())

            if not len(args["models"]) != 2 or (len(args["models"]) == 1 and "gms" not in args["models"]):
                raise ValueError("Must submit exactly two directories (ms, fr and/or gms")

            # check for duplicate dir names (submitting same dir for two args)
            if len(dir_list_lowercase) != len(set(dir_list_lowercase)):
                raise ValueError("The two sources directories are the same path.")

            if not os.path.exists(args["flows_file"]):
                print(f'{args["flows_file"]} does not exist. Please specify a flow file.')

            # check job numbers
            assert args["num_workers_huc"] >= 1, "Number of huc workers should be 1 or greater"
            assert args["num_workers_branches"] >= 1, "Number of branch workers should be 1 or greater"

            total_cpus_requested = args["num_workers_huc"] * args["num_workers_branches"]
            total_cpus_available = os.cpu_count()
            if total_cpus_requested > (total_cpus_available - 1):
                raise ValueError(
                    "The HUC job num of workers, {}, multiplied by the branch workers number, {}, "
                    "exceeds your machine's available CPU count of {} minus one. "
                    "Please lower the num_workers_huc or num_workers_branches"
                    "values accordingly.".format(
                        args["num_workers_huc"], total_cpus_available, args["num_workers_branches"]
                    )
                )

            # Create output directory if it does not exist
            if not os.path.isdir(args["composite_output_dir"]):
                os.mkdir(args["composite_output_dir"])

            # If no output name supplied, create one using the flows file name
            # the output_name is the final composite output file.
            # we also extract the basic file name without extension for use as the log file name
            if not args["output_name"]:
                flows_root = os.path.splitext(os.path.basename(args["flows_file"]))
                root_output_file_name = f"inundation_composite_{flows_root[0]}"
                args["output_name"] = f"{root_output_file_name}.tif"
            else:
                # see if the file name has a path or not, fail if it does
                output_file_name_split = os.path.split(args["output_name"])
                if args["output_name"] == output_file_name_split[0]:
                    raise ValueError(
                        """If submitting the -n (output file name), please ensure
                                        it has no pathing. You can also leave it blank if you like."""
                    )

                root_output_file_name = os.path.splitext(args["output_name"])[0]

            # setup log file and its directory
            # Note: Log files are only created at this time if verbose\
            args["log_file_path"] = None
            if args["verbose"]:
                args["log_file_path"] = os.path.join(args["composite_output_dir"], "mosaic_logs")
                if not os.path.isdir(args["log_file_path"]):
                    os.mkdir(args["log_file_path"])

            # Save run parameters up to this point
            args_file = os.path.join(args["composite_output_dir"], root_output_file_name + "_args.json")
            with open(args_file, "w") as json_file:
                json.dump(args, json_file)
                print(f"Args printed to file at {args_file}")

            # make combined huc list, NOTE: not all source dirs will have the same huc folders
            huc_list = set()
            if args["huc"] is not None:
                if (len(args["huc"]) != 8) or (not args["huc"].isnumeric()):
                    raise ValueError("Single huc value (-u arg) was submitted but appears invalid")
                else:
                    huc_list.add(args["huc"])
            else:
                for dir in dir_list_raw:
                    sub_dirs = [item for item in os.listdir(dir) if os.path.isdir(os.path.join(dir, item))]
                    # Some directories may not be hucs (log folders, etc)
                    huc_folders = [item for item in sub_dirs if item.isnumeric()]
                    huc_set = set(huc_folders)
                    huc_list.update(huc_set)  # will ensure no dups

            args["huc_list"] = huc_list

            return args

        args = __validate_args(args)

        huc_list = args["huc_list"]
        number_huc_workers = args["num_workers_huc"]
        # if len(huc_list == 1): # skip iterator
        if number_huc_workers == 1:
            for huc in sorted(huc_list):
                args["current_huc"] = huc
                Composite_HUC.composite_huc(args)
        else:
            print(f"Processing {len(huc_list)} hucs")
            args_list = []
            # sorted_hucs = sorted(huc_list)
            # params_items = [(args, huc) for huc in huc_list]
            for huc in sorted(huc_list):
                huc_args = copy.deepcopy(args)
                huc_args["current_huc"] = huc
                args_list.append(huc_args)

            with cf.ProcessPoolExecutor(max_workers=number_huc_workers) as executor:
                executor_gen = {
                    executor.submit(Composite_HUC.composite_huc, params): params for params in args_list
                }

                for future in tqdm(
                    cf.as_completed(executor_gen),
                    total=len(executor_gen),
                    desc=f"Running composite inundation with {number_huc_workers} workers",
                ):
                    executor_gen[future]

                try:
                    future.result()
                except Exception as exc:
                    print("{}, {}, {}".format(huc, exc.__class__.__name__, exc))

            print("All hucs have been processed")

    @staticmethod
    def hydroid_to_binary(hydroid_raster_filename):
        # Converts hydroid positive/negative grid to 1/0
        # to_bin = lambda x: np.where(x > 0, 1, np.where(x == 0, -9999, 0))
        # to_bin = lambda x: np.where(x > 0, 1, np.where(x != -9999, 0, -9999))
        def to_bin(x):
            return np.where(x > 0, 1, np.where(x != -9999, 0, -9999))

        hydroid_raster = rasterio.open(hydroid_raster_filename)
        profile = hydroid_raster.profile  # get profile for new raster creation later on
        profile["nodata"] = -9999
        bin_raster = to_bin(hydroid_raster.read(1))  # converts neg/pos to 0/1
        # Overwrite inundation raster
        with rasterio.open(hydroid_raster_filename, "w", **profile) as out_raster:
            out_raster.write(bin_raster.astype(hydroid_raster.profile["dtype"]), 1)
        del hydroid_raster, profile, bin_raster


if __name__ == "__main__":
    """
    Runs inundate and compositing on any and exactly two of the following:
        1) FIM 3.X mainstem (MS)
        2) FIM 3.X full-resolution (FR)
        3) FIM 4.x (gms)

    Examples of usage
    Notice: arg keys and values for some of the variants
    --------
    a) ms and fr (single huc)
    python3 /foss_fim/tools/composite_inundation.py
        -ms /outputs/inundation_test_1_FIM3_ms
        -fr /outputs/inundation_test_1_FIM3_fr
        -u /data/inputs/huc_lists/include_huc8.lst
        -f /data/test_cases/nws_test_cases/validation_data_nws/13090001/rgdt2/moderate/ahps_rgdt2_huc_13090001_flows_moderate.csv
        -o /outputs/inundation_test_1_comp/
        -n test_inundation.tif

    a) ms and gms (all hucs in each folder)
    python3 /foss_fim/tools/composite_inundation.py
        -ms /outputs/inundation_test_1_FIM3_ms
        -gms /outputs/inundation_test_1_gms
        -f /data/test_cases/nws_test_cases/validation_data_nws/13090001/rgdt2/moderate/ahps_rgdt2_huc_13090001_flows_moderate.csv
        -o /outputs/inundation_test_1_comp/
        -c -jh 3 -jb 20

    b) fr and gms (single huc)
    python3 /foss_fim/tools/composite_inundation.py
        -fr /outputs/inundation_test_1_FIM3_fr
        -gms /outputs/inundation_test_1_gms
        -u 13090001 -f /data/inputs/rating_curve/nwm_recur_flows/nwm3_17C_recurr_25_0_cms.csv
        -o /outputs/inundation_test_1_comp/
        -n test_inundation.tif
    """

    # parse arguments
    parser = argparse.ArgumentParser(
        description="""Inundate FIM 3 full resolution
                and mainstem outputs using a flow file and composite the results."""
    )
    parser.add_argument(
        "-ms",
        "--fim-dir-ms",
        help="Source directory that contains MS FIM outputs.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-fr",
        "--fim-dir-fr",
        help="Source directory that contains FR FIM outputs.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-gms",
        "--gms-dir",
        help="Source directory that contains FIM4 GMS outputs.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-u",
        "--huc",
        help="""(Optional) If a single HUC is provided, only that HUC will be processed.
                        If not submitted, all HUCs in the source directories will be used.""",
        required=False,
        default=None,
    )
    parser.add_argument("-f", "--flows-file", help="File path of flows csv.", required=True)
    parser.add_argument(
        "-o", "--composite-output-dir", help="Folder to write Composite Raster output.", required=True
    )
    parser.add_argument("-n", "--output-name", help="File name for output(s).", default=None, required=False)
    parser.add_argument(
        "-b",
        "--is-bin-raster",
        help="If flag is included, the output raster will be changed to wet/dry.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-jh",
        "--num-workers-huc",
        help="Number of processes to use for HUC scale operations. HUC and Batch job numbers should multiply"
        "to no more than one less than the CPU count of the machine.",
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        "-jb",
        "--num-workers-branches",
        help="Number of processes to use for Branch scale operations. HUC and Batch job numbers should multiply"
        "to no more than one less than the CPU count of the machine.",
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        "-c",
        "--no_cleanup",
        help="If flag used, intermediate rasters are NOT cleaned up.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v", "--verbose", help="Show additional outputs.", required=False, default=False, action="store_true"
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print("================================")
    print(f"Start composite inundation - {dt_string}")
    print()

    ci = CompositeInundation()
    ci.run_composite(args)

    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print("================================")
    print(f"End composite inundation - {dt_string}")

    # calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()
