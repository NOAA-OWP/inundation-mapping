#!/usr/bin/env python3

import logging
import os
import random
import re

# import shutil
import time
import traceback
from datetime import datetime, timezone

# import pandas as pd
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from tools_shared_variables import (  # INPUTS_DIR,; elev_raster_ndv,
    AHPS_BENCHMARK_CATEGORIES,
    MAGNITUDE_DICT,
    OUTPUTS_DIR,
    PREVIOUS_FIM_DIR,
    TEST_CASES_DIR,
)

import src.utils.shared_functions as sf
# from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import get_huc_vars


# from inundation import inundate
# from mosaic_inundation import Mosaic_inundation

# Aug 2026: compute_contingency_stats_from_rasters calls soem gval items
# and there is evidence that their may be some new memory leaks with this newer
# gdal and rasterio. In the interium, Gemini suggests copying this directly into
# the function to limit its scope and help with memory control.
# from tools_shared_functions import compute_contingency_stats_from_rasters


class Benchmark(object):
    AHPS_BENCHMARK_CATEGORIES = AHPS_BENCHMARK_CATEGORIES
    MAGNITUDE_DICT = MAGNITUDE_DICT

    def __init__(self, category):
        """Class that handles benchmark data.

        Parameters
        ----------
        category : str
            Category of the benchmark site. Should be one of ['ble', 'ifc', 'nws', 'usgs', 'ras2fim'].
        """

        self.category = category.lower()
        assert category in list(
            self.MAGNITUDE_DICT.keys()
        ), f"Category must be one of {list(self.MAGNITUDE_DICT.keys())}"
        self.validation_data = os.path.join(
            TEST_CASES_DIR, f'{self.category}_test_cases', f'validation_data_{self.category}'
        )
        self.is_ahps = True if self.category in self.AHPS_BENCHMARK_CATEGORIES else False

    def magnitudes(self):
        '''Returns the magnitudes associated with the benchmark category.'''
        return self.MAGNITUDE_DICT[self.category]

    def huc_data(self):
        '''Returns a dict of HUC8, magnitudes, and sites.'''
        huc_mags = {}
        if not os.path.exists(self.validation_data):
            return huc_mags

        for huc in os.listdir(self.validation_data):
            if not re.match(r'\d{8}', huc):
                continue
            huc_mags[huc] = self.data(huc)
        return huc_mags

    def data(self, huc):
        '''Returns a dict of magnitudes and sites for a given huc. Sites will be AHPS lids for
        AHPS sites and empty strings for non-AHPS sites.
        '''
        huc_dir = os.path.join(self.validation_data, huc)
        if not os.path.isdir(huc_dir):
            return {}
        if self.is_ahps:
            lids = os.listdir(huc_dir)

            mag_dict = {}
            for lid in lids:
                lid_dir = os.path.join(huc_dir, lid)
                for mag in [file for file in os.listdir(lid_dir) if file in self.magnitudes()]:
                    if mag in mag_dict:
                        mag_dict[mag].append(lid)
                    else:
                        mag_dict[mag] = [lid]
            return mag_dict
        else:
            mags = list(os.listdir(huc_dir))
            return {mag: [''] for mag in mags}


class Test_Case(Benchmark):
    def __init__(self, test_id, hand_version, archive=True):
        """Class that handles test cases, specifically running the alpha test.

        Parameters
        ----------
        test_id : str
            ID of the test case in huc8_category format, e.g. `12090201_ble`.
        hand_version : str
            Version of FIM to which this test_case belongs. This should correspond to the fim directory
            name in either `/data/previous_fim/` or `/outputs/`. ie) hand_4_9_10_10
        archive : bool
            If true, this test case outputs will be placed into the `official_versions` folder
            and the FIM model will be read from the `/data/previous_fim` folder.
            If false, it will be saved to the `testing_versions/` folder and the FIM model
            will be read from the `/outputs/` folder.

        """
        self.test_id = test_id
        self.huc, self.benchmark_cat = test_id.split('_')
        super().__init__(self.benchmark_cat)
        self.is_valid_hand_huc = False
        self.hand_version = hand_version
        self.archive = archive
        # FIM run directory path - uses HUC 6 for FIM 1 & 2

        self.fim_huc_dir = os.path.join(
            PREVIOUS_FIM_DIR if archive else OUTPUTS_DIR, self.hand_version, self.huc
        )

        # Test the HUC folder
        if os.path.exists(self.fim_huc_dir):
            # files that should exist if the huc finished processing correctly
            # If not... the huc failed.
            if os.path.isfile(os.path.join(self.fim_huc_dir, "hydrotable.csv")) or os.path.isfile(
                os.path.join(self.fim_huc_dir, "hydrotable.parquet")
            ):
                self.is_valid_hand_huc = True

        # Test case directory path
        self.test_case_dir = os.path.join(
            TEST_CASES_DIR,
            f'{self.benchmark_cat}_test_cases',
            test_id,
            'official_versions' if archive else 'testing_versions',
            hand_version,
        )

        # TODO: Jun 2026: Do we want it to create a bunch of empy dirs in test_cases?
        # if not os.path.exists(self.dir):
        #     os.makedirs(self.dir)
        # Benchmark data path
        self.benchmark_dir = os.path.join(self.validation_data, self.huc)

        huc_vars = get_huc_vars(self.huc)
        self.mask_dict = {
            'levees': {'path': huc_vars['levee_protected_areas'], 'buffer': None, 'operation': 'exclude'},
            'waterbodies': {'path': huc_vars['lakes'], 'buffer': None, 'operation': 'exclude'},
        }

    @classmethod
    def list_all_test_cases(cls, hand_version, archive, benchmark_categories=[]):
        """Returns a complete list of all benchmark category test cases as classes.

        Parameters
        ----------
        hand_version : str
            Version of FIM to which this test_case belongs. This should correspond to the fim directory
            name in either `/data/previous_fim/` or `/outputs/`. ie) hand_4_9_10_10
        archive : bool
            If true, this test case outputs will be placed into the `official_versions` folder
            and the FIM model will be read from the `/data/previous_fim` folder.
            If false, it will be saved to the `testing_versions/` folder and the FIM model
            will be read from the `/outputs/` folder.
        """
        if not benchmark_categories:
            benchmark_categories = list(cls.MAGNITUDE_DICT.keys())

        test_case_list = []
        for bench_cat in benchmark_categories:
            benchmark_class = Benchmark(bench_cat)
            benchmark_data = benchmark_class.huc_data()

            for huc in benchmark_data.keys():
                test_case_list.append(cls(f'{huc}_{bench_cat}', hand_version, archive))

        return test_case_list

    # Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
    def alpha_test(
        self,
        # mask_type='huc', has not been used for a while (fim 3)
        inclusion_area='',
        inclusion_area_buffer=0,
        overwrite=True,
        verbose=False,
        num_parent_workers=1,  # Used only for memory allocation management
        branch_workers=1,
        precalb_option=False,
        log_folder='',
        log_prefix='',
    ):
        '''Compares a FIM directory with benchmark data from a variety of sources.

        Parameters
        ----------
        # mask_type : str
        #     Mask type to feed into inundation.py.
        inclusion_area : int
            Area to include in agreement analysis.
        inclusion_area_buffer : int
            Buffer distance in meters to include outside of the model's domain.
        overwrite : bool
            If True, overwites pre-existing test cases within the test_cases directory.
        verbose : bool
            If True, prints out all pertinent data.
        num_parent_workers : int
            Number of worker processes assigned to original parent number of jobs
            for processpool or threadpool if applicable. Used in conjuction with the number
            of branch workers for memory allocation only.
        branch_workers : int
            Number of worker processes assigned to branch processing.
        log_folder: string
            As this function is being called as part of a MP, it needs its own log file and folder
        log_prefix: string
            This is the prefix of the log file name to be used. This code will add a unique value
            to ensure unique log files per MP
        '''

        # June 2026:
        # In inundate_gms.py, there is a threadpoolexecutor.
        # Note: rasterio opened files are never truly thread safe. But, most of our tools are processed
        # one branch at a time. Except synthesize_test_cases which has its own processpool so there could
        # be collisions there, but the sleep timer will help.

        # When this first starts, they all hit this function at the same time often hitting the same files.
        # This is mostly true when we have a huc with more than one benchmark type. One alpha_test case is
        # based on one huc + benchmark type. They will both aim to get huc level files at the same time.
        # Putting a random time sleeper helps manage that a little lowering resource needs a little and network
        # bottlenecks. random between 0 and 30 seconds
        time.sleep(random.randint(0, 30))

        start_time = datetime.now(timezone.utc)

        try:
            if log_folder != "":
                # Each logger get the name of fim_logger but each are in a ProcessPoolExecutor
                # so they will not collide. But giving it a specific name makes it easier
                # to share with a ThreadPoolExecutor in inundation

                # Also, each logger should have its own name.
                # Just get the system proc id as part of its uniquess
                sf.setup_file_logger(
                    log_file_dir=log_folder, log_file_name_prefix=f"{log_prefix}_{self.test_id}"
                )
                #                                     logger_name=f"alpha_test_worker_{os.getpid()}")
                time.sleep(0.2)  # gives time for the logger to fully instanitate

            if verbose:
                logging.info("")  # helps find the sections in the logs
                logging.info(f">>>>>>>>>> Started Alpha Test for {self.test_id}")
            else:
                logging.debug("")  # helps find the sections in the logs
                logging.debug(f">>>>>>>>>> Started Alpha Test for {self.test_id}")

            if not overwrite and os.path.isdir(self.test_case_dir):
                logging.warning(
                    f"Metrics for {self.test_case_dir} already exist. Use overwrite flag (-o) to overwrite metrics."
                )
                return

            self.stats_modes_list = ['total_area']

            if inclusion_area != '':
                inclusion_area_name = os.path.split(inclusion_area)[1].split('.')[0]  # Get layer name
                self.mask_dict.update(
                    {
                        inclusion_area_name: {
                            'path': inclusion_area,
                            'buffer': int(inclusion_area_buffer),
                            'operation': 'include',
                        }
                    }
                )
                # Append the concatenated inclusion_area_name and buffer.
                if inclusion_area_buffer == None:
                    inclusion_area_buffer = 0
                self.stats_modes_list.append(inclusion_area_name + '_b' + str(inclusion_area_buffer) + 'm')

            # ie) /data/test_cases/ble_test_cases/12090301_ble/testing_versions/Rob_alpha_test_3
            # Delete the directory if it exists
            # Sometimes with MP, os commands can collide in race conditions depending on what folders
            # are remove folders (folder in folder and possibly others)
            # if os.path.exists(self.dir):
            #     shutil.rmtree(self.dir, ignore_errors=True)
            # os.mkdir(self.dir)
            os.makedirs(self.test_case_dir, exist_ok=True)

            # Get the magnitudes and lids for the current huc and loop through them

            validation_data = self.data(self.huc)
            for magnitude in validation_data:
                for instance in validation_data[
                    magnitude
                ]:  # instance will be the lid for AHPS sites and '' for other sites
                    # For each site, inundate the REM and compute aggreement raster with stats
                    self._inundate_and_compute(
                        magnitude,
                        instance,
                        branch_workers=branch_workers,
                        num_parent_workers=num_parent_workers,
                        precalb_option=precalb_option,
                        verbose=verbose,
                    )

                # Clean up 'total_area' outputs from AHPS sites
                if self.is_ahps:
                    self.clean_ahps_outputs(os.path.join(self.test_case_dir, magnitude))

            # Jun 2026: With dropping FIM 3, this has no value anymore
            # Write out evaluation meta-data
            # self.write_metadata(calibrated, model)
            # logging.debug(f"Starting to write metadata file to {self.dir}")
            # self.write_metadata(calibrated)

        except KeyboardInterrupt as kiex:
            logging.critical(f"Program aborted via keyboard interrupt: {self.test_id}")
            # sys.exit(1)  # Note: you can not have this inside an MP as it won't really work
            raise kiex
        except Exception as ex:
            logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
            logging.critical(f"An exception has occured for {self.test_id}")
            logging.critical(traceback.format_exc())
            raise ex
        finally:
            if verbose:
                logging.info(
                    f">>>>>>>>>> Completed Alpha Test for {self.test_id}:"
                    f" Duration: {sf.calculate_duration_msg(start_time)}"
                )
            else:
                logging.debug(
                    f">>>>>>>>>> Completed Alpha Test for {self.test_id}:"
                    f" Duration: {sf.calculate_duration_msg(start_time)}"
                )
            # we are about to exit this child logger. The logger does not automatically
            # get killed until the procespool finishes all workers. But you can drop
            # the handlers before returning, basically killing the logger. This is important because if you don't, the logger will
            # keep the file open and you will not be able to delete it. This is a problem when you are running a lot of alpha_tests in a row and you want to delete the log
            # files after each one. So, we will drop the handlers here.
            logger = logging.getLogger()
            handlers = logger.handlers[:]
            for handler in handlers:
                handler.close()
                logger.removeHandler(handler)

    def _inundate_and_compute(self,
                              magnitude,
                              lid,
                              precalb_option,
                              branch_workers=1,
                              num_parent_workers=1,
                              verbose=False):

        # num_parent_workers=1,  # Used only for memory allocation management
        # used by both inundate_gms and mosiac_inundation which have child MT's

        # Aug 2026: compute_contingency_stats_from_rasters calls some gval items
        # and there is evidence that their may be some new memory leaks with this newer
        # gdal and rasterio. In the interium, Gemini suggests copying this directly into
        # the function to limit its scope and help with memory control.
        from tools_shared_functions import compute_contingency_stats_from_rasters

        '''Method for inundating and computing contingency rasters as part of the alpha_test.
        Used by both the alpha_test() and composite() methods.

         Parameters
         ----------
         magnitude : str
             Magnitude of the current benchmark site.
         lid : str
             lid of the current benchmark site. For non-AHPS sites, this should be an empty string ('').
        '''
        logging.debug(f"Preparing file paths for {self.test_case_dir} - ({self.huc}) - ({magnitude}0")

        test_case_out_dir = os.path.join(self.test_case_dir, magnitude)
        inundation_prefix = lid + '_' if lid else ''
        inundation_path = os.path.join(test_case_out_dir, f'{inundation_prefix}inundation_extent.tif')
        predicted_raster_path = inundation_path.replace('.tif', f'_{self.huc}.tif')
        agreement_raster = os.path.join(
            test_case_out_dir, (f'ahps_{lid}' if lid else '') + 'total_area_agreement.tif'
        )
        stats_json = os.path.join(test_case_out_dir, 'stats.json')
        stats_csv = os.path.join(test_case_out_dir, 'stats.csv')

        # Create directory
        # if not os.path.isdir(test_case_out_dir):
        #    os.mkdir(test_case_out_dir)

        # Benchmark raster and flow files
        benchmark_rast = (
            f'ahps_{lid}' if lid else self.benchmark_cat
        ) + f'_huc_{self.huc}_extent_{magnitude}.tif'
        benchmark_rast = os.path.join(self.benchmark_dir, lid, magnitude, benchmark_rast)
        benchmark_flows = benchmark_rast.replace(f'_extent_{magnitude}.tif', f'_flows_{magnitude}.csv')
        mask_dict_indiv = self.mask_dict.copy()
        if self.is_ahps:  # add domain shapefile to mask for AHPS sites
            domain = os.path.join(self.benchmark_dir, lid, f'{lid}_domain.shp')
            mask_dict_indiv.update({lid: {'path': domain, 'buffer': None, 'operation': 'include'}})
        # Check to make sure all relevant files exist

        # Save the mapping for huc, forecast to branch that were used in the final inundation rollup tif.
        inundation_mapping_file_path = predicted_raster_path.replace(".tif", ".csv")

        logging.debug(f"benchmark_rast is {benchmark_rast} and benchmark_flows is {benchmark_flows}")
        if (
            not os.path.isfile(benchmark_rast)
            or not os.path.isfile(benchmark_flows)
            or (self.is_ahps and not os.path.isfile(domain))
        ):
            return -1

        # Jul 2026: feature no longer as it was only used by fim3 code.
        # if not compute_only:  # composite alpha tests don't need to be inundated
        #     if model == "GMS":

        # Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
        produce_mosaicked_inundation(
            hydrofabric_dir=os.path.dirname(self.fim_huc_dir),
            hucs=self.huc,
            flow_file_path=benchmark_flows,
            output_raster_path=predicted_raster_path,
            # mask_path=os.path.join(self.fim_huc_dir, "wbd.gpkg"),
            inundation_mapping_file_path=inundation_mapping_file_path,
            verbose=verbose,
            num_threads=branch_workers,
            num_parent_workers=num_parent_workers,
            # num_workers=gms_workers,
            precalb_option=precalb_option,
            windowed=True,
        )

        # Create contingency rasters and stats
        # fh.vprint("Begin creating contingency rasters and stats", verbose)
        logging.debug(f"Begin creating contingency rasters and stats for benchmark_rast is {benchmark_rast}")
        if os.path.isfile(predicted_raster_path):
            compute_contingency_stats_from_rasters(
                predicted_raster_path,
                benchmark_rast,
                agreement_raster,
                stats_csv=stats_csv,
                stats_json=stats_json,
                mask_dict=mask_dict_indiv,
            )
        return

    # TODO: Jun 2026: Should add logging like we did in the function called alpha_test
    # This version, run_alpha_test, is currently only used by cache_metrics.py
    @classmethod
    def run_alpha_test(
        cls,
        hand_version,
        test_id,
        precalb_option=False,
        archive_results=False,
        inclusion_area='',
        inclusion_area_buffer=0,
        overwrite=True,
        verbose=False,
        branch_workers=1,
    ):

        # TODO: Jun 2026: If we keep using this one as just a few tools do, we we want to add some sort of
        # MP here?

        '''Class method for instantiating the test_case class and running alpha_test directly'''

        alpha_class = cls(test_id, hand_version, archive_results)
        alpha_class.alpha_test(
            inclusion_area, inclusion_area_buffer, overwrite, verbose, branch_workers, precalb_option
        )

    # Jun 2026: This file no longer has any value
    # def write_metadata(self, calibrated):
    #     '''Writes metadata files for a test_case directory.'''
    #     with open(os.path.join(self.dir, 'eval_metadata.json'), 'w') as meta:
    #         # eval_meta = {'calibrated': calibrated, 'model': model}
    #         eval_meta = {'calibrated': calibrated}
    # meta.write(json.dumps(eval_meta, indent=2))

    def clean_ahps_outputs(self, magnitude_directory):
        '''Cleans up `total_area` files from an input AHPS magnitude directory.'''
        if os.path.exists(magnitude_directory):
            output_file_list = [
                os.path.join(magnitude_directory, of) for of in os.listdir(magnitude_directory)
            ]
            for output_file in output_file_list:
                if "total_area" in output_file:
                    os.remove(output_file)

    def get_current_agreements(self):
        '''Returns a list of all agreement rasters currently existing for the test_case.'''
        agreement_list = []
        for mag in os.listdir(self.test_case_dir):
            mag_dir = os.path.join(self.test_case_dir, mag)
            if not os.path.isdir(mag_dir):
                continue

            for f in os.listdir(mag_dir):
                if f.endswith('agreement.tif'):  # sometimes there are {xxxx}.tif.aux.xml
                    agreement_list.append(os.path.join(mag_dir, f))
        return agreement_list
