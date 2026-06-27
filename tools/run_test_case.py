#!/usr/bin/env python3

import logging
import os
import random
import re
import shutil
import time
import traceback
from datetime import datetime, timezone

# import pandas as pd
from inundate_mosaic_wrapper import produce_mosaicked_inundation

# from inundation import inundate
# from mosaic_inundation import Mosaic_inundation
from tools_shared_functions import compute_contingency_stats_from_rasters
from tools_shared_variables import (  # INPUTS_DIR,; elev_raster_ndv,
    AHPS_BENCHMARK_CATEGORIES,
    MAGNITUDE_DICT,
    OUTPUTS_DIR,
    PREVIOUS_FIM_DIR,
    TEST_CASES_DIR,
)

import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


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
        # TODO: Jun 2026: Do we want it to create a bunch of empy dirs in test_cases?
        self.dir = os.path.join(
            TEST_CASES_DIR,
            f'{self.benchmark_cat}_test_cases',
            test_id,
            'official_versions' if archive else 'testing_versions',
            hand_version,
        )

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        # Benchmark data path
        self.benchmark_dir = os.path.join(self.validation_data, self.huc)

        if self.huc[:2] == '19':
            self.mask_dict = {
                'levees': {
                    'path': os.getenv('input_nld_levee_protected_areas_Alaska'),
                    'buffer': None,
                    'operation': 'exclude',
                },
                'waterbodies': {
                    # 'path': '/data/inputs/nwm_hydrofabric/nwm_lakes.gpkg',
                    'path': os.getenv('input_nwm_lakes_Alaska'),
                    'buffer': None,
                    'operation': 'exclude',
                },
            }

        else:
            self.mask_dict = {
                'levees': {
                    'path': os.getenv('input_nld_levee_protected_areas'),
                    'buffer': None,
                    'operation': 'exclude',
                },
                'waterbodies': {'path': os.getenv('input_nwm_lakes'), 'buffer': None, 'operation': 'exclude'},
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

    def alpha_test(
        self,
        # mask_type='huc', has not been used for a while (fim 3)
        inclusion_area='',
        inclusion_area_buffer=0,
        overwrite=True,
        verbose=False,
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
        branch_workers : int
            Number of worker processes assigned to branch processing.
        log_folder: string
            As this function is being called as part of a MP, it needs its own log file and folder
        log_prefix: string
            This is the prefix of the log file name to be used. This code will add a unique value
            to ensure unique log files per MP
        '''

        # June 2026:
        # When this first starts, they all hit this function at the same time often hitting the same files.
        # This is mostly true when we have a huc with more than one benchmark type. One alpha_test case is
        # based on one huc + benchmark type. They will both aim to get huc level files at the same time.
        # Putting a random time sleeper helps manage that a little lowering resource needs a little and network
        # bottlenecks. random between 0 and 5 seconds
        time.sleep(random.randint(0, 5))

        start_time = datetime.now(timezone.utc)
        try:
            if log_folder != "":
                # Each logger get the name of fim_logger but each are in a ProcessPoolExecutor
                # so they will not collide. But giving it a specific name makes it easier
                # to share with a ThreadPoolExecutor in inundation
                log_file_path = sf.setup_file_logger(log_folder, f"{log_prefix}_{self.test_id}")

            if verbose:
                logging.info("")  # helps find the sections in the logs
                logging.info(f">>>>>>>>>> Started Alpha Test for {self.test_id}")
            else:
                logging.debug("")  # helps find the sections in the logs
                logging.debug(">>>>>>>>>>>>>>>>>>>>>")
                logging.debug(f"Started Alpha Test for {self.test_id}")

            if not overwrite and os.path.isdir(self.dir):
                logging.warning(
                    f"Metrics for {self.dir} already exist. Use overwrite flag (-o) to overwrite metrics."
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
            os.makedirs(self.dir, exist_ok=True)

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
                        precalb_option=precalb_option,
                        verbose=verbose,
                        log_file_path=log_file_path,
                    )

                # Clean up 'total_area' outputs from AHPS sites
                if self.is_ahps:
                    self.clean_ahps_outputs(os.path.join(self.dir, magnitude))

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
                logging.debug(">>>>>>>>>>>>>>>>>>>>>")
                logging.debug(
                    f"Completed Alpha Test for {self.test_id}:"
                    f" Duration: {sf.calculate_duration_msg(start_time)}"
                )

    def _inundate_and_compute(
        self, magnitude, lid, precalb_option, branch_workers=1, verbose=False, log_file_path=""
    ):
        '''Method for inundating and computing contingency rasters as part of the alpha_test.
        Used by both the alpha_test() and composite() methods.

         Parameters
         ----------
         magnitude : str
             Magnitude of the current benchmark site.
         lid : str
             lid of the current benchmark site. For non-AHPS sites, this should be an empty string ('').
        '''
        logging.debug(f"Preparing file paths for {self.dir} - ({self.huc})")

        test_case_out_dir = os.path.join(self.dir, magnitude)
        inundation_prefix = lid + '_' if lid else ''
        inundation_path = os.path.join(test_case_out_dir, f'{inundation_prefix}inundation_extent.tif')
        predicted_raster_path = inundation_path.replace('.tif', f'_{self.huc}.tif')
        agreement_raster = os.path.join(
            test_case_out_dir, (f'ahps_{lid}' if lid else '') + 'total_area_agreement.tif'
        )
        stats_json = os.path.join(test_case_out_dir, 'stats.json')
        stats_csv = os.path.join(test_case_out_dir, 'stats.csv')

        # Create directory
        if not os.path.isdir(test_case_out_dir):
            os.mkdir(test_case_out_dir)

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

        logging.debug(f"benchmark_rast is {benchmark_rast} and benchmark_flows is {benchmark_flows}")
        if (
            not os.path.isfile(benchmark_rast)
            or not os.path.isfile(benchmark_flows)
            or (self.is_ahps and not os.path.isfile(domain))
        ):
            return -1

        produce_mosaicked_inundation(
            hydrofabric_dir=os.path.dirname(self.fim_huc_dir),
            hucs=self.huc,
            flow_file=benchmark_flows,
            inundation_raster=predicted_raster_path,
            verbose=verbose,
            num_workers=branch_workers,
            precalb_option=precalb_option,
            windowed=True,
            log_file=log_file_path,
            show_progress_bar=False,
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
        output_file_list = [os.path.join(magnitude_directory, of) for of in os.listdir(magnitude_directory)]
        for output_file in output_file_list:
            if "total_area" in output_file:
                os.remove(output_file)

    def get_current_agreements(self):
        '''Returns a list of all agreement rasters currently existing for the test_case.'''
        agreement_list = []
        for mag in os.listdir(self.dir):
            mag_dir = os.path.join(self.dir, mag)
            if not os.path.isdir(mag_dir):
                continue

            for f in os.listdir(mag_dir):
                if f.endswith('agreement.tif'):  # sometimes there are {xxxx}.tif.aux.xml
                    agreement_list.append(os.path.join(mag_dir, f))
        return agreement_list
