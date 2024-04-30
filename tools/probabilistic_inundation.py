import argparse
import ast
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import xarray as xr
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation
from scipy import stats
from tqdm import tqdm


def inundate_probabilistic(ensembles: str, parameters: str, base_dir: str, huc: str, final_output_dir: str):
    """
    Method to probabilistically inundate based on provided ensembles

    Parameters
    ----------
    ensembles: str
        Path to load medium range ensembles
    parameters: str
        Path to load fit parameters to distributions
    base_dir: str
        Base directory of FIM output
    huc: str
        Huc to process probabilistic FIM
    final_output_dir: str
        Name of final output directory

    """

    # Load datasets
    ensembles = xr.open_dataset(ensembles)
    parameters_df = pd.read_csv(parameters, index_col='feature_id')

    # Outputs directory
    outputs_dir = os.path.join(base_dir, 'outputs')

    # Hydrofabric directory
    hydrofabric_dir = os.path.join(outputs_dir, 'fim_outputs')

    # Fim outputs directour
    fim_outputs_dir = os.path.join(outputs_dir, 'fim_files')

    # Path to predicted inundation file.
    predicted_raster_path = os.path.join(fim_outputs_dir, 'inundation_arb.tif')

    # Masks for waterbodies
    mask = os.path.join(hydrofabric_dir, huc, 'wbd.gpkg')

    # Percentiles and data to add
    percentiles = {'95': 5, '90': 10, '80': 20, '75': 25, '50': 50, '25': 75, '20': 80, '10': 90, '5': 95}
    percentile_values = {
        'feature_id': [],
        '95': [],
        '90': [],
        '80': [],
        '75': [],
        '50': [],
        '25': [],
        '20': [],
        '10': [],
        '5': [],
    }

    # Distributions
    dist_dict = {
        "expon": stats.expon,
        "gamma": stats.gamma,
        "genextreme": stats.genextreme,
        "genpareto": stats.genpareto,
        "gumbel_r": stats.gumbel_r,
        "kappa": stats.kappa4,
        "pearson3": stats.pearson3,
        "norm": stats.norm,
        "weibull_min": stats.weibull_min,
    }

    # Slice of time in forecast (possibly changed to
    forecast_time = ensembles.coords['reference_time'] + np.timedelta64(1, 'W')

    # For each feature in the provided ensembles
    for feature in ensembles.coords['feature_id']:

        # If feature exists in parameters DF
        try:
            ensemble_forecast = ensembles.sel(
                {'time': forecast_time, 'feature_id': feature, 'member': ['1', '2', '3', '4', '5', '6']}
            )['streamflow']

            parameters = parameters_df.loc[int(feature)].copy()

            # Create probability distribution
            params = ast.literal_eval(parameters['parameters'])
            params['size'] = 14975

            try:
                r = dist_dict[parameters[0]].rvs(**params)
            except Exception:
                percentile_values['feature_id'].append(int(feature))
                for key, val in percentiles.items():
                    percentile_values[key].append(float(ensemble_forecast.sel({'member': '1'})))
                continue

            # Sorty values and apply weibull exceedance estimates
            sorted_r = np.sort(r)

            weibull_prob_estimates = [1 - (i / (14975 + 1)) for i in range(14975)]

            # Get weibull estimates for each ensemble
            likelihoods = np.interp(ensemble_forecast.values, sorted_r, weibull_prob_estimates)

            # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
            # (In place of assessing a final distribution for the time being)
            scaled_likelihoods = (
                likelihoods / np.sum(np.squeeze(likelihoods) * np.linspace(1, 0.9, 6)) * 10000
            )

            values = []
            for value, scale in zip(ensemble_forecast.values, scaled_likelihoods):
                values.append(np.repeat(value, int(scale)))

            final_values = np.hstack(values).ravel()

            # Get percentiles of streamflow
            percentile_values['feature_id'].append(int(feature))
            for key, val in percentiles.items():
                percentile_values[key].append(np.percentile(final_values, val))

        except Exception:
            percentile_values['feature_id'].append(int(feature))
            for key, val in percentiles.items():
                percentile_values[key].append(float(ensemble_forecast.sel({'member': '1'})))

    # Apply inundation map to each percentile
    for key, val in percentiles.items():
        final_inundation_path = os.path.join(fim_outputs_dir, final_output_dir, f'{key}.tif')
        flow_file = f'{base_dir}/{huc}_{key}_flow.csv'

        df = pd.DataFrame(
            {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[key]}
        )
        df.to_csv(flow_file, index=False)

        map_file = Inundate_gms(
            hydrofabric_dir=hydrofabric_dir,
            forecast=flow_file,
            hucs=huc,
            inundation_raster=predicted_raster_path,
            verbose=True,
        )

        Mosaic_inundation(
            map_file,
            mosaic_attribute='inundation_rasters',
            mosaic_output=final_inundation_path,
            mask=mask,
            unit_attribute_name='huc8',
        )


def progress_bar_handler(executor_dict, verbose, desc):
    """Show progress of operation

    Parameters
    ----------
    executor_dict: dict
        Keys as futures and HUC ids as values
    verbose: bool
        Whether to print more progress
    desc: str
        Description of the process
    """

    for future in tqdm(
        as_completed(executor_dict), total=len(executor_dict), disable=(not verbose), desc=desc
    ):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


def inundate_hucs(
    ensembles: str, parameters: str, base_dir: str, hucs: list, final_output_dir: str, num_jobs: int
):
    """Driver for running probabilistic inundation on selected HUCs

    Parameters
    ----------
    ensembles: str
        Location of nws ensemble NetCDF file
    parameters: str
        Location of parameter CSV file
    base_dir: str
        Directory with the output and hydrofabric directories
    hucs: list
        HUCs to process probabilistic inundation for
    final_output_dir: str
        Location to output maps
    num_jobs: int
        Number of jobs to process

    """
    with ProcessPoolExecutor(max_workers=num_jobs) as executor:
        executor_dict = {}
        for huc in hucs:

            try:
                future = executor.submit(
                    inundate_probabilistic,
                    ensembles=ensembles,
                    parameters=parameters,
                    base_dir=base_dir,
                    huc=huc,
                    final_output_dir=final_output_dir,
                )
                executor_dict[future] = huc

            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

                # Send the executor to the progress bar and wait for all MS tasks to finish
        progress_bar_handler(
            executor_dict, True, f"Running {huc} probabilistic inundation with {num_jobs} workers"
        )


if __name__ == '__main__':

    """
    Example Usage:

    python probabilistic_inundation.py
    -e ../gfs_ensembles_12010001.nc
    -p ../params_12010001.csv
    -b "../"
    -hc 12010001
    -o example
    -j 1
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description="Run probabilistic inundation on selected HUCs")

    parser.add_argument(
        "-e", "--ensembles", help="REQUIRED: Location of ensembles NetCDF file", required=True
    )

    parser.add_argument("-p", "--parameters", help='REQUIRED: Location of parameters CSV file', required=True)

    parser.add_argument(
        "-b", "--base_dir", help="REQUIRED: Base directory with fim outputs and hydrofabric", required=True
    )

    parser.add_argument(
        "-hc", "--hucs", nargs="*", help="REQUIRED: HUCs to process probabilistic inundation", required=True
    )

    parser.add_argument(
        "-o",
        "--final_output_dir",
        help="REQUIRED: Directory to output results of probabilistic inundation",
        required=False,
    )

    parser.add_argument(
        "-j", "--num_jobs", type=int, help="REQUIRED: Number of jobs to process HUCs", required=True
    )

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        inundate_hucs(**args)

    except Exception:
        print("The following error has occured:\n", traceback.format_exc())
