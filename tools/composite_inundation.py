#!/usr/bin/env python3
import os, argparse, sys
import rasterio
import numpy as np
import pandas as pd
from datetime import datetime

from inundation import inundate
from gms_tools.mosaic_inundation import Mosaic_inundation
from gms_tools.inundate_gms import Inundate_gms

from utils.shared_functions import append_id_to_file_name
from utils.shared_variables import elev_raster_ndv

class CompositeInundation(object):

    def __init__(self, fim_dir_ms, fim_dir_fr, gms_dir,
                       huc, flows_file,
                       composite_output_dir, output_name,
                       is_bin_raster, is_depth_raster,
                       num_workers, do_clean_up, verbose):

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
            Path to FIM4 GMS directory. This should be an output directory from `gms_run_unit, then gms_run_branch`.
        huc : str
            (This will not be used for gms inundation)
            HUC8 to run `inundate()`. This should be a folder within both `fim_dir_ms` and `fim_dir_fr`.
            TODO: only takes one huc right now
        flows_file : str : 
            Can be a single file path to a forecast csv or a comma-separated list of files.
        composite_output_dir : str
            Folder path to write outputs. It will be created if it does not exist.
        output_name : str, optional
            Name for output raster. If not specified, by default the raster will be named 'inundation_composite_{flows_root}.tif'.
        is_bin_raster : bool, optional
            Flag to create binary raster as output. If no raster flags are passed, this is the default behavior.
        is_bin_raster : bool, optional
            TODO: is_depth_raster
        num_workers : int, optional
            defaults to 1 and means the number of processes to be used
        do_clean_up : bool, optional
            If True, intermediate files are deleted.
        verbose : bool, optional
            show extra output.
        """

        # Validates arguments and sets up some key variables needed for processing
        self.__validate_args(fim_dir_ms, fim_dir_fr, gms_dir,
                            huc, flows_file, composite_output_dir, output_name,
                            is_bin_raster, is_depth_raster,
                            num_workers, do_clean_up, verbose)

    def run_composite(self):

        """
        Returns
        -------
        None

        Notes
        -----
        - Note: Must have exactly two of three input types (ms, fr and/or gms) but can be any two.
        - Specifying a subset of the domain in rem or catchments to inundate on is achieved by the HUCs file or the forecast file.
        - If using a single HUC number, and the huc does not exist in the two applicable directories,
        an error will be issued.

        Examples
        --------
        notice -c which means clean (removed intermediate files)

        a) ms and fr (note arg keys)
        python3 /foss_fim/tools/composite_inundation.py -ms /outputs/inundation_test_1_FIM3_ms -fr /outputs/inundation_test_1_FIM3_fr -u 13090001 -f /data/test_cases/nws_test_cases/validation_data_nws/13090001/rgdt2/moderate/ahps_rgdt2_huc_13090001_flows_moderate.csv -o /outputs/inundation_test_1_comp/ -n test_inundation.tif -c

        a) ms and gms (note arg keys)
        python3 /foss_fim/tools/composite_inundation.py -ms /outputs/inundation_test_1_FIM3_ms -gms /outputs/inundation_test_1_gms -u 13090001 -f /data/test_cases/nws_test_cases/validation_data_nws/13090001/rgdt2/moderate/ahps_rgdt2_huc_13090001_flows_moderate.csv -o /outputs/inundation_test_1_comp/ -n test_inundation.tif -c

        b) fr and gms (note arg keys)
        python3 /foss_fim/tools/composite_inundation.py -fr /outputs/inundation_test_1_FIM3_fr -gms /outputs/inundation_test_1_gms -u 13090001 -f /data/test_cases/nws_test_cases/validation_data_nws/13090001/rgdt2/moderate/ahps_rgdt2_huc_13090001_flows_moderate.csv -o /outputs/inundation_test_1_comp/ -n test_inundation.tif -c

        """

        # TODO: Get this working for multiple hucs

        # Build inputs to inundate() based on the input folders and huc
        # TODO: this could be more than one huc !!!! 
        if self.verbose: print(f"HUC {self.huc}")

        inundation_map_file = []

        for model in self.models:

            # setup original fim/gms processed directory
            if model == "ms" : fim_dir = self.fim_dir_ms
            elif model == "fr" : fim_dir = self.fim_dir_fr
            else: fim_dir = self.gms_dir

            inundation_rast = None
            depth_rast = None

            if (self.is_bin_raster):
                inundation_rast = append_id_to_file_name(self.output_name, model)
                #inundation_rast = os.path.join(self.composite_output_dir, f'{self.huc}_inundation_{model}.tif')

            if (self.is_depth_raster): # validation ensured there is not a binary and a depth raster
                depth_rast = append_id_to_file_name(self.output_name, model)
                #depth_rast = os.path.join(self.composite_output_dir, f'{self.huc}_depth_{model}.tif')

            huc_dir = os.path.join(fim_dir, self.huc)

            if model == "ms":
                extent_friendly = "mainstem (MS)"
            elif model == "fr":
                extent_friendly = "full-resolution (FR)"
            else: # gms
                extent_friendly = "FIM4 GMS"
            grid_type = "an inundation" if self.is_bin_raster else "a depth"
            print(f"  Creating {grid_type} map for the {extent_friendly} configuration for HUC {self.huc}...")

            if model in ["fr", "ms"]:

                rem = os.path.join(huc_dir, 'rem_zeroed_masked.tif')
                catchments = os.path.join(huc_dir, 'gw_catchments_reaches_filtered_addedAttributes.tif')
                catchment_poly = os.path.join(huc_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
                hydro_table = os.path.join(huc_dir, 'hydroTable.csv')

                # Ensure that all of the required files exist in the huc directory
                for file in (rem, catchments, catchment_poly, hydro_table):
                    if not os.path.exists(file):
                        raise Exception(f"The following file does not exist within the supplied FIM directory:\n{file}")

                # Run inundation()
                result = inundate(rem, 
                                  catchments, 
                                  catchment_poly, 
                                  hydro_table,
                                  self.flows_file, 
                                  mask_type = None, 
                                  num_workers = 1,
                                  inundation_raster = inundation_rast,
                                  depths = depth_rast,
                                  quiet = not self.verbose)

                #if self.verbose:
                #    print("Inundation Response:")
                #    print(result)

                if len(result) == 0:
                    raise Exception(f"Failed to inundate {extent_friendly} using the provided flows.")
                
                # TODO: What if more than one items comes back in each list set
                result_inundation_raster = None
                result_depth_raster = None
                if (self.is_bin_raster):
                    result_inundation_raster = result[0][0]
                else: # is depth raster
                    result_depth_raster = result[1][0]

                inundation_map_file.append(
                            [model, self.huc, None, 
                            result_inundation_raster, result_depth_raster, None] )

            else:  # gms

                # TODO: takes only one huc right now
                map_file = Inundate_gms( hydrofabric_dir = fim_dir,
                                        forecast = self.flows_file, 
                                        num_workers = self.num_workers,
                                        hucs = self.huc,
                                        inundation_raster = inundation_rast,
                                        depths_raster = depth_rast,
                                        verbose = self.verbose,
                                        log_file = self.log_file,
                                        output_fileNames = self.inundation_list_file_path )
                
                mask_path_gms = os.path.join(huc_dir, 'wbd.gpkg')

                # we are going to mosaic the gms files first
                # NOTE: Leave workers as 1, it fails to composite correctly if more than one.
                mosaic_file_path = Mosaic_inundation( map_file, 
                                            mosaic_attribute = 'inundation_rasters',
                                            mosaic_output = inundation_rast,
                                            mask = mask_path_gms, 
                                            unit_attribute_name = 'huc8',
                                            nodata = elev_raster_ndv,
                                            workers = 1,
                                            remove_inputs = self.do_clean_up,
                                            subset = None,
                                            verbose = self.verbose )
                
                result_inundation_raster = None
                result_depth_raster = None
                if (self.is_bin_raster):
                    result_inundation_raster = mosaic_file_path
                else: # is depth raster
                    result_depth_raster = mosaic_file_path

                inundation_map_file.append(
                            [model, self.huc, None, 
                            result_inundation_raster, result_depth_raster, None] )
                
                if self.verbose: print("  ... complete")
        
        # Composite the models
        
        inundation_map_file_df = pd.DataFrame(inundation_map_file,
                                              columns = ['model', 'huc8', 'branchID',
                                                 'inundation_rasters', 'depths_rasters',
                                                 'inundation_polygons'])

        if self.verbose:                                                 
            print("inundation_map_file_df")
            print(inundation_map_file_df)

        # NOTE: Leave workers as 1, it fails to composite correctly if more than one.
        #    - Also. by adding the is_mosaic_for_gms_branches = False, Mosaic_inudation
        #      will not auto add the HUC into the output name (its default behaviour)
        Mosaic_inundation( inundation_map_file_df,
                           mosaic_attribute='inundation_rasters' if self.is_bin_raster else 'depths_rasters',
                           mosaic_output = self.output_name,
                           mask = None,
                           unit_attribute_name = 'huc8',
                           nodata = elev_raster_ndv,
                           workers = 1,
                           remove_inputs = self.do_clean_up,
                           subset = None,
                           verbose = self.verbose,
                           is_mosaic_for_gms_branches = False )

        # TODO
        #if self.is_bin_raster:
        #    hydroid_to_binary(__append_id_to_file_name(ouput_name, huc))
        

    def hydroid_to_binary(hydroid_raster_filename):
        '''Converts hydroid positive/negative grid to 1/0'''

        #to_bin = lambda x: np.where(x > 0, 1, np.where(x == 0, -9999, 0))
        to_bin = lambda x: np.where(x > 0, 1, np.where(x != -9999, 0, -9999))
        hydroid_raster = rasterio.open(hydroid_raster_filename)
        profile = hydroid_raster.profile # get profile for new raster creation later on
        profile['nodata'] = -9999
        bin_raster = to_bin(hydroid_raster.read(1)) # converts neg/pos to 0/1
        # Overwrite inundation raster
        with rasterio.open(hydroid_raster_filename, "w", **profile) as out_raster:
            out_raster.write(bin_raster.astype(hydroid_raster.profile['dtype']), 1)
        del hydroid_raster,profile,bin_raster


    def __validate_args(self, fim_dir_ms, fim_dir_fr, gms_dir,
                        huc, flows_file, composite_output_dir, output_name,
                        is_bin_raster, is_depth_raster,
                        num_workers, do_clean_up, verbose):

        self.fim_dir_ms             = fim_dir_ms
        self.fim_dir_fr             = fim_dir_fr
        self.gms_dir                = gms_dir
        #self.hucs                   = huc.replace(' ', '').split(',')
        self.huc                    = huc
        self.flows_file             = flows_file
        self.composite_output_dir   = composite_output_dir
        self.output_name            = output_name
        self.is_bin_raster          = bool(is_bin_raster)
        self.is_depth_raster        = bool(is_depth_raster)
        self.num_workers            = num_workers
        self.do_clean_up            = bool(do_clean_up)
        self.verbose                = bool(verbose)
        
        if (self.fim_dir_ms) and (self.fim_dir_ms.lower() == "none"):
            self.fim_dir_ms = None
        if (self.fim_dir_fr) and (self.fim_dir_fr.lower() == "none"):
            self.fim_dir_fr = None        
        if (self.gms_dir) and (self.gms_dir.lower() == "none"):
            self.gms_dir = None

        # TODO: 
        #fim_run_parent = os.path.join(os.environ['outputDataDir'], fim_run_dir)
        #assert os.path.exists(fim_run_parent), "Cannot locate " + fim_run_parent

        # TODO: Check that huc folders exist (for both applicable directories)

        # count number of input dir types and ensure their are no duplciates.
        dir_list = []        
        missing_dir_msg = "{} directory of {} does not exist"    
        self.models = []
        if (self.fim_dir_ms != None):
            self.models.append("ms")
            assert os.path.isdir(self.fim_dir_ms), missing_dir_msg.format(version_types, self.fim_dir_ms)
            dir_list.append(self.fim_dir_ms.lower())

        if (self.fim_dir_fr != None):
            self.models.append("fr")
            assert os.path.isdir(self.fim_dir_fr), missing_dir_msg.format(version_types, self.fim_dir_fr)
            dir_list.append(self.fim_dir_fr.lower())

        if (self.gms_dir != None):
            self.models.append("gms")
            assert os.path.isdir(self.gms_dir), missing_dir_msg.format(version_types, self.gms_dir)
            dir_list.append(self.gms_dir.lower())

        if (len(self.models) != 2):
            raise Exception("Must submit exactly two directories (ms, fr and/or gms")

        # check for duplicate dir names
        if len(dir_list) != len(set(dir_list)):
            raise Exception("The two sources directories are the same path.")

        # TODO: problem as it might be multiple 
        assert os.path.exists(self.flows_file), f'{self.flows_file} does not exist. Please specify a flow file.'

        # Could be zero. TODO: Nice to have: Check if more than system has available.
        assert self.num_workers >= 1, "Number of workers should be 1 or greater"

        # Create output directory if it does not exist
        if not os.path.isdir(self.composite_output_dir):
            os.mkdir(self.composite_output_dir)

        # If no output name supplied, create one using the flows file name 
        # the output_name is the final composite output file.
        # we also extract the basic file name without extension for use as the log file name
        if not self.output_name:
            flows_root = os.path.splitext(os.path.basename(self.flows_file))
            root_output_file_name = f'inundation_composite_{flows_root[0]}'
            self.output_name = os.path.join(self.composite_output_dir, f"{root_output_file_name}.tif")
        else:
            # see if the file name has a path or not, fail if it does
            output_file_name_split = os.path.split(self.output_name)
            if self.output_name == output_file_name_split[0]:
                raise Exception("""If submitting the -n (output file name), please ensure 
                                   it has no pathing. You can also leave it blank if you like.""")

            root_output_file_name = os.path.splitext(self.output_name)[0]

        self.output_name = os.path.join(self.composite_output_dir, self.output_name)

        # TODO: we have a problem with one versus multiple hucs and flow files
        #hucs = self.huc.replace(' ', '').split(',')

        # setup log file and its directory
        # Note: Log files are only created at this time if verbose\
        self.log_file = None
        self.inundation_list_file_path = None
        if (self.verbose):
            log_file_path = os.path.join(self.composite_output_dir, "mosaic_logs")
            if not os.path.isdir(log_file_path):
                os.mkdir(log_file_path)

            self.log_file = os.path.join(log_file_path, f"{root_output_file_name}_inundation_logfile.txt")

            # Another output file is a csv showing which inundation files were successfully
            # created. We will automatically create it and put it in the same directory folder.
            self.inundation_list_file_path = os.path.join(log_file_path,
                f"{root_output_file_name}_inundation_file_list.csv")

        assert not (self.is_bin_raster and self.is_depth_raster), "Cannot use both -b and -d flags"

        # Set inundation raster to True if no output type flags are passed
        if not (self.is_bin_raster or self.is_depth_raster):
            self.is_bin_raster = True

if __name__ == '__main__':
    
    # parse arguments
    parser = argparse.ArgumentParser(description="""Inundate FIM 3 full resolution
                and mainstem outputs using a flow file and composite the results.""")
    parser.add_argument('-ms','--fim-dir-ms', help='Source directory that contains MS FIM outputs.',
                required=False, default=None)
    parser.add_argument('-fr','--fim-dir-fr', help='Source directory that contains FR FIM outputs.', 
                required=False, default=None)
    parser.add_argument('-gms','--gms-dir', help='Source directory that contains FIM4 GMS outputs.', 
                required=False, default=None)
    parser.add_argument('-u','--huc', 
                help="""(optional). If a HUC is provided, only that HUC will be processed.
                     If not submitted, all HUCs in the source directories will be used.""",
                required=False)
    parser.add_argument('-f','--flows-file', 
                help='File path of flows csv.',
                required=True)
    parser.add_argument('-o','--composite-output-dir', help='Folder to write Composite Raster output.',
                required=True)
    parser.add_argument('-n','--output-name', help='File name for output(s).', 
                default=None, required=False)
    parser.add_argument('-b','--is-bin-raster', 
                help="""Output raster is a binary wet/dry grid. 
                This is the default if no raster flags are passed.""", 
                required=False, default=False, action='store_true')
    parser.add_argument('-d','--is-depth-raster', help='Output raster is a depth grid.',
                required=False, default=False, action='store_true')
    parser.add_argument('-j','--num-workers', help='Number of concurrent processes to run.',
                required=False, default=1, type=int)
    parser.add_argument('-c','--do_clean_up', help='If flag used, intermediate rasters are cleaned up.',
                required=False, default=False, action='store_true')
    parser.add_argument('-v','--verbose', help='Show additional outputs.',
                required=False, default=False, action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print("================================")
    print(f"Start composite inundation - {dt_string}")    
    print()

    ci = CompositeInundation(**args)
    ci.run_composite()

    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print("================================")
    print(f"End composite inundation - {dt_string}")

    # calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()

