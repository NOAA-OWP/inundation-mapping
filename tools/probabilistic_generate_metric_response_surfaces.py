import os
import warnings
from glob import glob
from itertools import product
from typing import List, Optional

import numpy as np
import rioxarray as rxr
import xarray as xr
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from probabilistic_inundation import get_subdivided_src
from tqdm.notebook import tqdm


warnings.filterwarnings("ignore")


def create_flood_maps(
    hydrofabric_dir: str,
    fim_outputs_dir: str,
    output_folder_name: str,
    channel_mannings_n: List[float],
    overbank_mannings_n: List[float],
    slope_adjustments: List[float],
    hucs: List[str],
    flows: List[str],
    overwrite: Optional[bool] = False,
    num_threads: Optional[int] = 1,
    num_jobs: Optional[int] = 1,
    log_file: Optional[str] = None,
    windowed: Optional[bool] = False,
):
    """
    Creates flood maps for different channel parameters

    Parameters
    ----------
    hydrofabric_dir : str
        Directory containing hydrofabric data
    fim_outputs_dir : str
        Directory to create output files
    output_folder_name : str
        Folder to organize output files for this run
    channel_mannings_n : List[float]
        List of channel mannings numbers
    overbank_mannings_n : List[float]
        List of overbank mannings numbers
    slope_adjustments : List[float]
        List of slope adjustments
    hucs : List[str]
        List of huc08s to process (currently one processes one huc at a time at this scope)
    flows : List[str]
        List of flow files to process
    overwrite : Optional[bool], default = False
        Whether to overwrite existing files
    num_threads : Optional[int], default = 1
        Number of threads to run operation
    num_jobs : Optional[int], default = 1
        Number of processes to run operation
    log_file : Optional[str], default = None
        File to write statements to

    """
    # Count of how many maps will be created
    total_combinations = 0
    for N, obank_N, s, huc, flow in product(
        channel_mannings_n, overbank_mannings_n, slope_adjustments, hucs, flows
    ):
        if obank_N >= N:
            total_combinations += 1

    print("Total combinations:", total_combinations)

    # Iterate through all combinations of datasets and parameters
    loop_idx = 1
    for N, obank_N, s, huc, flow_path in (
        pbar := tqdm(product(channel_mannings_n, overbank_mannings_n, slope_adjustments, hucs, flows))
    ):

        filename = flow_path.split('/')[-1].split('.')[0].replace('flows', 'extent')

        # Skip if overbank N is smaller than channel N
        if obank_N < N:
            continue

        pbar.set_description(f"Running combination {loop_idx} of {total_combinations} for HUC {huc}")

        # Make directories if they do not exist
        base_output_path = os.path.join(fim_outputs_dir, output_folder_name, str(huc))
        os.makedirs(base_output_path, exist_ok=True)
        src_output_path = os.path.join(base_output_path, 'srcs')
        os.makedirs(src_output_path, exist_ok=True)

        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(base_output_path, f'{filename}_{N}_{obank_N}_{s}.tif')

        if os.path.exists(final_inundation_path) and not overwrite:
            continue

        mask_path = os.path.join(hydrofabric_dir, str(huc), 'wbd.gpkg')

        # Sub-divide src
        src_output_file = "htable_branch" + "_{0}" + f"_{N}_{obank_N}_{s}.feather"

        # Open the original hydrotable
        all_branches = glob(os.path.join(hydrofabric_dir, huc, "branches", "*"))
        all_branches = [x.split('/')[-1] for x in all_branches]

        for branch in all_branches:
            if not os.path.exists(os.path.join(src_output_path, src_output_path.format(branch))) or overwrite:
                get_subdivided_src(
                    hydrofabric_dir, huc, branch, N, obank_N, s, src_output_path, src_output_file
                )

        # Make inundation extent output
        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_path,
            hydro_table_df=os.path.join(src_output_path, src_output_file),
            inundation_raster=final_inundation_path,
            mask=mask_path,
            verbose=False,
            num_workers=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            log_file=log_file,
            remove_intermediate=True,
        )

        ds = rxr.open_rasterio(final_inundation_path)
        nodata, crs = ds.rio.nodata, ds.rio.crs
        nodata_mask = ds == nodata
        ds.data = xr.where(ds < 0, 0, ds)
        ds.data = xr.where(ds > 0, 1, ds)
        ds.data = xr.where(nodata_mask, 2, ds)

        ds.rio.write_crs(crs, inplace=True)
        ds.rio.write_nodata(2, inplace=True)

        ds.rio.to_raster(final_inundation_path, driver="COG", dtype=np.int8)

        loop_idx += 1


def evaluate_maps(
    benchmarks: List[str],
    fim_outputs_dir: str,
    output_folder_name: str,
    huc: str,
    overwrite: Optional[bool] = False,
):
    """
    Evaluate the processed flood maps and write result metrics to directory

    Parameters
    ----------
    benchmarks : List[str]
        List of benchmarks to process
    fim_outputs_dir : str
        Directory to create output files
    output_folder_name : str
        Folder to organize output files for this run
    huc : str
        huc08 to process (currently one processes one huc at a time at this scope)
    overwrite : Optional[bool], default = False
        Whether to overwrite existing files

    """

    PAIRING_DICT = {
        (0, 0): 0,
        (0, 1): 1,
        (0, np.nan): 10,
        (1, 0): 2,
        (1, 1): 3,
        (1, np.nan): 10,
        (4, 0): 4,
        (4, 1): 4,
        (4, np.nan): 10,
        (np.nan, 0): 10,
        (np.nan, 1): 10,
        (np.nan, np.nan): 10,
    }

    # Create metrics path if it does not exist
    metrics_path = os.path.join(fim_outputs_dir, output_folder_name, huc, "metrics")
    os.makedirs(metrics_path, exist_ok=True)

    loop_idx = 1
    for bench in (pbar := tqdm(benchmarks)):

        pbar.set_description(f"Running benchmark comparisons {loop_idx} of {len(benchmarks)}")

        file_name = bench.split('/')[-1].split('.')[0]
        site, flow = file_name.split('_')[0], file_name.split('_')[-1]

        # Open benchmark dataset
        b_mark = rxr.open_rasterio(bench, mask_and_scale=True)

        # Find all relevant candidate datasets
        candidate_maps = glob(
            os.path.join(fim_outputs_dir, output_folder_name, huc, f'{bench.split("/")[-1].split(".")[0]}*')
        )

        c_idx = 1

        for c_path in (cbar := tqdm(candidate_maps)):

            metrics_file = os.path.splitext(c_path)[0] + '.feather'
            metrics_output = metrics_path + '/' + metrics_file.split('/')[-1]

            if os.path.exists(metrics_output) and overwrite is False:
                continue

            cbar.set_description(f"Running candidate maps {c_idx} of {len(candidate_maps)}")

            # Open candidate dataset
            cand = rxr.open_rasterio(c_path, mask_and_scale=True)

            # Run a categorical evaluation
            agreement_map, cross_tabulation_table, metric_table = cand.gval.categorical_compare(
                b_mark,
                positive_categories=[1],
                negative_categories=[0],
                comparison_function="pairing_dict",
                pairing_dict=PAIRING_DICT,
            )

            mannings = c_path.split('/')[-1].split('_')[-3]
            overbank_mannings = c_path.split('/')[-1].split('_')[-2]
            slope = os.path.splitext(c_path.split('_')[-1])[0]

            # Resolution 10 meters
            metric_table['sqkm2'] = np.nansum(cand.values) / 100
            b_mark.close()
            cand.close()
            del agreement_map, cross_tabulation_table, cand

            metric_table['mannings n'] = mannings
            metric_table['overbank_mannings_n'] = overbank_mannings
            metric_table['slope_adjustment'] = slope
            metric_table['flow'] = flow
            metric_table['site'] = site

            metric_table.to_feather(metrics_output)

            del metric_table
            c_idx += 1

        loop_idx += 1


if __name__ == "__main__":

    # Base directory
    base_dir = '../'
    huc = "12090301"

    # Outputs Directory
    outputs_dir = os.path.join(base_dir, 'outputs')

    # Folder Specific Commands
    ble_flow_paths = glob(os.path.join(outputs_dir, "validation", "validation_data_ble", huc, '*', "*flow*"))

    nws_flow_paths = glob(
        os.path.join(outputs_dir, "validation", "validation_data_nws", huc, '*', "*", "*flows*.csv")
    )

    ble_benchmarks = glob(os.path.join(outputs_dir, "validation", "validation_data_ble", huc, '*', "*.tif"))

    nws_benchmarks = glob(
        os.path.join(outputs_dir, "validation", "validation_data_nws", huc, '*', '*', "*.tif")
    )

    flows = np.unique(
        np.hstack(
            [
                [x for x in ble_flow_paths if 'action' not in x],
                [x for x in nws_flow_paths if 'action' not in x],
            ]
        )
    )

    benchmarks = np.unique(
        np.hstack([[x for x in ble_benchmarks], [x for x in nws_benchmarks if 'action' not in x]])
    )

    arguments = {
        'hydrofabric_dir': os.path.join(outputs_dir, 'fim_outputs'),
        'fim_outputs_dir': os.path.join('./gridded'),
        'output_folder_name': 'full_test',
        #     'channel_mannings_n': [.03, .04, .05, .06, .07, .08, .09, .1],
        'channel_mannings_n': [0.06],
        #     'overbank_mannings_n': [.07, .08, .09, .1, .11, .12, .13, .14, .15],
        'overbank_mannings_n': [0.12],
        #     'slope_adjustments': [-.1, -.05, -.01, -.001, 0, .001, .01, .05, .1],
        'slope_adjustments': [0],
        #     'sites': sites,
        'flows': flows,
        'hucs': [huc],
        'num_jobs': 8,
        'num_threads': 8,
        'overwrite': True,
        'windowed': False,
    }

    create_flood_maps(**arguments)

    evaluate_maps(
        benchmarks=benchmarks,
        fim_outputs_dir=arguments['fim_outputs_dir'],
        output_folder_name=arguments['output_folder_name'],
        huc=huc,
        overwrite=True,
    )
