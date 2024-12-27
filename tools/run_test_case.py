#!/usr/bin/env python3

import json
import os
import re
import shutil
import sys
import traceback

import pandas as pd
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from inundation import inundate
from mosaic_inundation import Mosaic_inundation
from tools_shared_functions import compute_contingency_stats_from_rasters
from tools_shared_variables import (
    AHPS_BENCHMARK_CATEGORIES,
    INPUTS_DIR,
    MAGNITUDE_DICT,
    OUTPUTS_DIR,
    PREVIOUS_FIM_DIR,
    TEST_CASES_DIR,
    elev_raster_ndv,
)

from utils.shared_functions import FIM_Helpers as fh


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
    def __init__(self, test_id, version, archive=True):
        """Class that handles test cases, specifically running the alpha test.

        Parameters
        ----------
        test_id : str
            ID of the test case in huc8_category format, e.g. `12090201_ble`.
        version : str
            Version of FIM to which this test_case belongs. This should correspond to the fim directory
            name in either `/data/previous_fim/` or `/outputs/`.
        archive : bool
            If true, this test case outputs will be placed into the `official_versions` folder
            and the FIM model will be read from the `/data/previous_fim` folder.
            If false, it will be saved to the `testing_versions/` folder and the FIM model
            will be read from the `/outputs/` folder.

        """
        self.test_id = test_id
        self.huc, self.benchmark_cat = test_id.split('_')
        super().__init__(self.benchmark_cat)
        self.version = version
        self.archive = archive
        # FIM run directory path - uses HUC 6 for FIM 1 & 2
        self.fim_dir = os.path.join(
            PREVIOUS_FIM_DIR if archive else OUTPUTS_DIR,
            self.version,
            self.huc if not re.search('^fim_[1,2]', version, re.IGNORECASE) else self.huc[:6],
        )
        # Test case directory path
        self.dir = os.path.join(
            TEST_CASES_DIR,
            f'{self.benchmark_cat}_test_cases',
            test_id,
            'official_versions' if archive else 'testing_versions',
            version,
        )
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        # Benchmark data path
        self.benchmark_dir = os.path.join(self.validation_data, self.huc)

        # Create list of shapefile paths to use as exclusion areas.
        self.mask_dict = {
            'levees': {
                'path': '/data/inputs/nld_vectors/Levee_protected_areas.gpkg',
                'buffer': None,
                'operation': 'exclude',
            },
            'waterbodies': {
                'path': '/data/inputs/nwm_hydrofabric/nwm_lakes.gpkg',
                'buffer': None,
                'operation': 'exclude',
            },
        }

    @classmethod
    def list_all_test_cases(cls, version, archive, benchmark_categories=[]):
        """Returns a complete list of all benchmark category test cases as classes.

        Parameters
        ----------
        version : str
            Version of FIM to which this test_case belongs. This should correspond to the fim directory
            name in either `/data/previous_fim/` or `/outputs/`.
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
                test_case_list.append(cls(f'{huc}_{bench_cat}', version, archive))

        return test_case_list

    def alpha_test(
        self,
        calibrated=False,
        model='',
        mask_type='huc',
        inclusion_area='',
        inclusion_area_buffer=0,
        overwrite=True,
        verbose=False,
        gms_workers=1,
    ):
        '''Compares a FIM directory with benchmark data from a variety of sources.

        Parameters
        ----------
        calibrated : bool
            Whether or not this FIM version is calibrated.
        model : str
            MS or FR extent of the model. This value will be written to the eval_metadata.json.
        mask_type : str
            Mask type to feed into inundation.py.
        inclusion_area : int
            Area to include in agreement analysis.
        inclusion_area_buffer : int
            Buffer distance in meters to include outside of the model's domain.
        overwrite : bool
            If True, overwites pre-existing test cases within the test_cases directory.
        verbose : bool
            If True, prints out all pertinent data.
        gms_workers : int
            Number of worker processes assigned to GMS processing.
        '''

        try:
            if not overwrite and os.path.isdir(self.dir):
                print(f"Metrics for {self.dir} already exist. Use overwrite flag (-o) to overwrite metrics.")
                return

            fh.vprint(f"Starting alpha test for {self.dir}", verbose)

            self.stats_modes_list = ['total_area']

            # Create paths to fim_run outputs for use in inundate()
            if model != 'GMS':
                self.rem = os.path.join(self.fim_dir, 'rem_zeroed_masked.tif')
                if not os.path.exists(self.rem):
                    self.rem = os.path.join(self.fim_dir, 'rem_clipped_zeroed_masked.tif')
                self.catchments = os.path.join(
                    self.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes.tif'
                )
                if not os.path.exists(self.catchments):
                    self.catchments = os.path.join(
                        self.fim_dir, 'gw_catchments_reaches_clipped_addedAttributes.tif'
                    )
                self.mask_type = mask_type
                if mask_type == 'huc':
                    self.catchment_poly = ''
                else:
                    self.catchment_poly = os.path.join(
                        self.fim_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg'
                    )
                self.hydro_table = os.path.join(self.fim_dir, 'hydroTable.csv')

            # Map necessary inputs for inundate().
            self.hucs, self.hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

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

            # Delete the directory if it exists
            if os.path.exists(self.dir):
                shutil.rmtree(self.dir)
            os.mkdir(self.dir)

            # Get the magnitudes and lids for the current huc and loop through them
            validation_data = self.data(self.huc)
            for magnitude in validation_data:
                for instance in validation_data[
                    magnitude
                ]:  # instance will be the lid for AHPS sites and '' for other sites
                    # For each site, inundate the REM and compute aggreement raster with stats
                    self._inundate_and_compute(
                        magnitude, instance, model=model, verbose=verbose, gms_workers=gms_workers
                    )

                # Clean up 'total_area' outputs from AHPS sites
                if self.is_ahps:
                    self.clean_ahps_outputs(os.path.join(self.dir, magnitude))

            # Write out evaluation meta-data
            self.write_metadata(calibrated, model)

        except KeyboardInterrupt:
            print("Program aborted via keyboard interrupt")
            sys.exit(1)
        except Exception as ex:
            print(ex)
            # Temporarily adding stack trace
            print(f"trace for {self.test_id} -------------\n", traceback.format_exc())
            sys.exit(1)

    def _inundate_and_compute(
        self, magnitude, lid, compute_only=False, model='', verbose=False, gms_workers=1
    ):
        '''Method for inundating and computing contingency rasters as part of the alpha_test.
        Used by both the alpha_test() and composite() methods.

         Parameters
         ----------
         magnitude : str
             Magnitude of the current benchmark site.
         lid : str
             lid of the current benchmark site. For non-AHPS sites, this should be an empty string ('').
         compute_only : bool
             If true, skips inundation and only computes contingency stats.
        '''
        # Output files
        fh.vprint("Creating output files", verbose)

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
        if (
            not os.path.isfile(benchmark_rast)
            or not os.path.isfile(benchmark_flows)
            or (self.is_ahps and not os.path.isfile(domain))
        ):
            return -1

        # Inundate REM
        if not compute_only:  # composite alpha tests don't need to be inundated
            if model == "GMS":
                produce_mosaicked_inundation(
                    os.path.dirname(self.fim_dir),
                    self.huc,
                    benchmark_flows,
                    inundation_raster=predicted_raster_path,
                    mask=os.path.join(self.fim_dir, "wbd.gpkg"),
                    verbose=verbose,
                )

            # FIM v3 and before
            else:
                fh.vprint("Begin FIM v3 (or earlier) Inundation", verbose)
                inundate_result = inundate(
                    self.rem,
                    self.catchments,
                    self.catchment_poly,
                    self.hydro_table,
                    benchmark_flows,
                    self.mask_type,
                    hucs=self.hucs,
                    hucs_layerName=self.hucs_layerName,
                    subset_hucs=self.huc,
                    num_workers=1,
                    aggregate=False,
                    inundation_raster=inundation_path,
                    inundation_polygon=None,
                    depths=None,
                    out_raster_profile=None,
                    out_vector_profile=None,
                    quiet=True,
                )
                if inundate_result != 0:
                    return inundate_result

        # Create contingency rasters and stats
        fh.vprint("Begin creating contingency rasters and stats", verbose)
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

    @classmethod
    def run_alpha_test(
        cls,
        version,
        test_id,
        magnitude,
        calibrated,
        model,
        archive_results=False,
        mask_type='huc',
        inclusion_area='',
        inclusion_area_buffer=0,
        light_run=False,
        overwrite=True,
        verbose=False,
        gms_workers=1,
    ):
        '''Class method for instantiating the test_case class and running alpha_test directly'''

        alpha_class = cls(test_id, version, archive_results)
        alpha_class.alpha_test(
            calibrated,
            model,
            mask_type,
            inclusion_area,
            inclusion_area_buffer,
            overwrite,
            verbose,
            gms_workers,
        )

    def composite(self, version_2, calibrated=False, overwrite=True, verbose=False):
        '''Class method for compositing MS and FR inundation and creating an agreement raster with stats

        Parameters
        ----------
        version_2 : str
            Version with which to composite.
        calibrated : bool
            Whether or not this FIM version is calibrated.
        overwrite : bool
            If True, overwites pre-existing test cases within the test_cases directory.
        '''

        if re.match(r'(.*)(_ms|_fr)', self.version):
            composite_version_name = re.sub(r'(.*)(_ms|_fr)', r'\1_comp', self.version, count=1)
        else:
            composite_version_name = re.sub(r'(.*)(_ms|_fr)', r'\1_comp', version_2, count=1)

        fh.vprint(f"Begin composite for version : {composite_version_name}", verbose)

        composite_test_case = Test_Case(self.test_id, composite_version_name, self.archive)
        input_test_case_2 = Test_Case(self.test_id, version_2, self.archive)
        composite_test_case.stats_modes_list = ['total_area']

        if not overwrite and os.path.isdir(composite_test_case.dir):
            return

        # Delete the directory if it exists
        if os.path.exists(composite_test_case.dir):
            shutil.rmtree(composite_test_case.dir)

        validation_data = composite_test_case.data(composite_test_case.huc)
        for magnitude in validation_data:
            for instance in validation_data[
                magnitude
            ]:  # instance will be the lid for AHPS sites and '' for other sites (ble/ifc/ras2fim)
                inundation_prefix = instance + '_' if instance else ''

                input_inundation = os.path.join(
                    self.dir, magnitude, f'{inundation_prefix}inundation_extent_{self.huc}.tif'
                )
                input_inundation_2 = os.path.join(
                    input_test_case_2.dir,
                    magnitude,
                    f'{inundation_prefix}inundation_extent_{input_test_case_2.huc}.tif',
                )
                output_inundation = os.path.join(
                    composite_test_case.dir, magnitude, f'{inundation_prefix}inundation_extent.tif'
                )

                if os.path.isfile(input_inundation) and os.path.isfile(input_inundation_2):
                    inundation_map_file = pd.DataFrame(
                        {
                            'huc8': [composite_test_case.huc] * 2,
                            'branchID': [None] * 2,
                            'inundation_rasters': [input_inundation, input_inundation_2],
                            'depths_rasters': [None] * 2,
                            'inundation_polygons': [None] * 2,
                        }
                    )
                    os.makedirs(os.path.dirname(output_inundation), exist_ok=True)

                    fh.vprint(f"Begin mosaic inundation for version : {composite_version_name}", verbose)
                    Mosaic_inundation(
                        inundation_map_file,
                        mosaic_attribute='inundation_rasters',
                        mosaic_output=output_inundation,
                        mask=None,
                        unit_attribute_name='huc8',
                        nodata=elev_raster_ndv,
                        workers=1,
                        remove_inputs=False,
                        subset=None,
                        verbose=False,
                    )
                    composite_test_case._inundate_and_compute(magnitude, instance, compute_only=True)

                elif os.path.isfile(input_inundation) or os.path.isfile(input_inundation_2):
                    # If only one model (MS or FR) has inundation, simply copy over all files as the composite
                    single_test_case = self if os.path.isfile(input_inundation) else input_test_case_2
                    shutil.copytree(
                        single_test_case.dir,
                        re.sub(r'(.*)(_ms|_fr)', r'\1_comp', single_test_case.dir, count=1),
                    )
                    composite_test_case.write_metadata(calibrated, 'COMP')
                    return

            # Clean up 'total_area' outputs from AHPS sites
            if composite_test_case.is_ahps:
                composite_test_case.clean_ahps_outputs(os.path.join(composite_test_case.dir, magnitude))

        composite_test_case.write_metadata(calibrated, 'COMP')

    def write_metadata(self, calibrated, model):
        '''Writes metadata files for a test_case directory.'''
        with open(os.path.join(self.dir, 'eval_metadata.json'), 'w') as meta:
            eval_meta = {'calibrated': calibrated, 'model': model}
            meta.write(json.dumps(eval_meta, indent=2))

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
