import argparse
import ast
import gc
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from glob import glob
from typing import Dict, Tuple, Union

import gval
import numpy as np
import pandas as pd
import rioxarray as rxr
import xarray as xr
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from scipy.stats import (
    expon,
    gamma,
    genextreme,
    genpareto,
    gumbel_r,
    kappa4,
    norm,
    pearson3,
    truncexpon,
    weibull_min,
)
from tqdm import tqdm

from subdiv_chan_obank_src import run_prep


def get_fim_probability_distributions(
    posterior_dist: str = None, huc: int = None
) -> Tuple[gamma, gamma, gamma]:
    """
    Gets either bayesian updated distributions or default distributions for respective huc

    Parameters
    ---------
    posterior_dist : str, default=None
        Name of csv file that has posteriod distribution parameters
    huc : int
        Identifier for the huc of interest

    Returns
    -------
    Tuple[gamma, gamma, gamma]
        Gamma distributions for channel Manning roughness overbank Manning roughness, and slope adjustment

    """

    if posterior_dist is None:

        # Default weibull likelihood for channel manning roughness
        channel_dist = weibull_min(c=1.5, scale=0.0367, loc=0.032)

        # Default weibull likelihood for overbank manning roughness
        obank_dist = weibull_min(c=2, scale=0.035, loc=0.09)

        # Default weibull likelihood for slope adjustment
        slope_dist = weibull_min(c=3.1, scale=0.095, loc=-0.01)

    else:

        raise NotImplementedError("Currently not implemented")

    return channel_dist, obank_dist, slope_dist


def generate_streamflow_percentiles(
    feature: int, ensembles: xr.Dataset, forecast_time: np.datetime64, params_weibull: pd.DataFrame
) -> Dict[str, Union[int, float]]:
    """
    Calculates Percentiles for the streamflow distribution

    Parameters
    ----------
    feature : int
        ID of feature to process
    ensembles : xr.Dataset
        NWM medium range ensembles
    forecast_time : np.datetime64
        Forecast time to slice
    params_weibull : pd.DataFrame
        Parameters for features

    Returns
    -------
    dict
        Dictionary of percentiles for streamflow distribution and feature_id
    """

    # Distributions
    dist_dict = {
        "expon": expon,
        "gamma": gamma,
        "genextreme": genextreme,
        "genpareto": genpareto,
        "gumbel_r": gumbel_r,
        "kappa": kappa4,
        "pearson3": pearson3,
        "norm": norm,
        "weibull_min": weibull_min,
    }

    ensemble_forecast = ensembles.sel(
        {'time': forecast_time, 'feature_id': feature, 'member': ['1', '2', '3', '4', '5', '6']}
    )['streamflow']

    if int(feature) not in params_weibull.index:
        return {
            'feature_id': int(feature),
            '90': float(ensemble_forecast.sel({'member': '1'})),
            '75': float(ensemble_forecast.sel({'member': '1'})),
            '50': float(ensemble_forecast.sel({'member': '1'})),
            '25': float(ensemble_forecast.sel({'member': '1'})),
            '10': float(ensemble_forecast.sel({'member': '1'})),
        }
    else:
        parameters = params_weibull.loc[int(feature)]

    # Create probability distribution
    params = ast.literal_eval(parameters['parameters'])

    params['size'] = 16071

    try:
        r = dist_dict[parameters['distribution_name']].rvs(**params)
    except Exception:

        return {
            'feature_id': int(feature),
            '90': float(ensemble_forecast.sel({'member': '1'})),
            '75': float(ensemble_forecast.sel({'member': '1'})),
            '50': float(ensemble_forecast.sel({'member': '1'})),
            '25': float(ensemble_forecast.sel({'member': '1'})),
            '10': float(ensemble_forecast.sel({'member': '1'})),
        }

    # Sort values and apply weibull exceedance estimates
    sorted_r = np.sort(r)

    weibull_prob_estimates = [1 - (i / (16071 + 1)) for i in range(16071)]

    # Get weibull estimates for each ensemble
    likelihoods = np.squeeze(np.interp(ensemble_forecast.values, sorted_r, weibull_prob_estimates))

    # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
    # (In place of assessing a final distribution for the time being)
    scaled_likelihoods = likelihoods / np.sum(likelihoods) * np.linspace(1, 0.9, 6) * 10000

    # Create data to fit truncated exponential distribution
    values = []
    for value, scale in zip(ensemble_forecast.values, scaled_likelihoods):
        values.append(np.repeat(value, int(scale)))

    streamflow_expon_values = np.hstack(values).ravel()

    if not np.all(streamflow_expon_values == streamflow_expon_values[0]):

        b, loc, scale = truncexpon.fit(streamflow_expon_values, loc=np.min(streamflow_expon_values))

        # Generate 10000 random values from distribution
        final_values = truncexpon.rvs(b=b, loc=loc, scale=scale, size=10000)

    else:
        final_values = np.repeat(streamflow_expon_values[0], 10000)

    # Get percentiles of streamflow
    return {
        'feature_id': int(feature),
        '90': np.max([0, np.percentile(final_values, 10)]),
        '75': np.max([0, np.percentile(final_values, 25)]),
        '50': np.max([0, np.percentile(final_values, 50)]),
        '25': np.max([0, np.percentile(final_values, 75)]),
        '10': np.max([0, np.percentile(final_values, 90)]),
    }


def inundate_probabilistic(
    ensembles: str,
    parameters: str,
    base_dir: str,
    huc: str,
    mosaic_prob_output_name: str,
    posterior_dist: str = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
):
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
    mosaic_prob_output_name: str
        Name of final mosaiced probabilistic FIM
    posterior_dist: str = None
        Name of posterior df
    day: int = 6
        Days ahead to pick from reference forecast time
    hour: int = 0,
        Hours ahead to pick from reference forecast time
    overwrite: bool = False
        Whether to overwrite existing output

    """

    # Load datasets
    ensembles = xr.open_dataset(ensembles)

    parameters_df = pd.read_csv(parameters)
    params_weibull = parameters_df.loc[parameters_df['distribution_name'] == 'weibull_min']
    params_weibull.set_index('feature_id', inplace=True)

    # Outputs directory
    outputs_dir = os.path.join(base_dir, 'outputs')

    # Hydrofabric directory
    hydrofabric_dir = os.path.join(outputs_dir, 'fim_outputs')

    # Fim outputs directory
    fim_outputs_dir = os.path.join(outputs_dir, 'fim_files')

    # Masks for waterbodies
    mask_path = os.path.join(hydrofabric_dir, huc, 'wbd.gpkg')

    # Slice of time in forecast (possibly changed to
    # forecast_time = ensembles.coords['reference_time'] + np.timedelta64(1, 'W')
    forecast_time = ensembles.coords['reference_time'] + np.timedelta64(day, 'D') + np.timedelta64(hour, 'h')

    # Percentiles and data to add
    percentiles = {'90': 10, '75': 25, '50': 50, '25': 75, '10': 90}
    percentile_values = {'feature_id': [], '90': [], '75': [], '50': [], '25': [], '10': []}

    features = ensembles.coords['feature_id']

    # For each feature in the provided ensembles
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        executor_dict = {}
        for feat in features:

            try:
                future = executor.submit(
                    generate_streamflow_percentiles,
                    feature=feat,
                    ensembles=ensembles,
                    forecast_time=forecast_time,
                    params_weibull=params_weibull,
                )
                executor_dict[future] = feat

            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

        # Send the executor to the progress bar and wait for all MS tasks to finish
        results = progress_bar_handler(
            executor_dict, True, f"Running streamflow percentiles with {num_jobs} workers"
        )

        for res in results:
            percentile_values['feature_id'].append(res['feature_id'])
            percentile_values['90'].append(res['90'])
            percentile_values['75'].append(res['75'])
            percentile_values['50'].append(res['50'])
            percentile_values['25'].append(res['25'])
            percentile_values['10'].append(res['10'])

    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=huc
    )

    # Apply inundation map to each percentile
    for percentile, val in percentiles.items():

        channel_n = channel_dist.ppf(1 - int(percentile) / 100)
        overbank_n = obank_dist.ppf(1 - int(percentile) / 100)
        slope_adj = slope_dist.ppf(int(percentile) / 100)

        # Make directories if they do not exist
        output_folder_name = '/'.join(mosaic_prob_output_name.split('/')[:-1]).replace('./', '')
        output_file_name = mosaic_prob_output_name.split('/')[-1]
        base_output_path = os.path.join(fim_outputs_dir, output_folder_name, str(huc))
        src_output_path = os.path.join(base_output_path, 'srcs')

        # Create directories if they do not exist
        os.makedirs(base_output_path, exist_ok=True)
        os.makedirs(src_output_path, exist_ok=True)

        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(
            base_output_path, f'extent_{percentile}_v10_day{day}_hour{hour}.tif'
        )

        # Skip if the file exists
        if os.path.exists(final_inundation_path) and not overwrite:
            continue

        # Open the original hydrotable
        htable_og = pd.read_csv(
            os.path.join(hydrofabric_dir, str(huc), 'hydrotable.csv'),
            dtype={
                'HUC': str,
                'feature_id': str,
                'HydroID': str,
                'stage': float,
                'discharge_cms': float,
                'LakeID': int,
                'last_updated': object,
                'submitter': object,
                'obs_source': object,
            },
        )

        dfs = []

        # Change the slope of each branch
        crosswalk_srcs = [
            x
            for x in glob(f'{hydrofabric_dir}/{huc}/branches/*/src_full_crosswalked_*.csv')
            if '_og' not in x
        ]
        for c_src in crosswalk_srcs:

            og_file = os.path.splitext(c_src)[0] + '_og.csv'
            if not os.path.exists(og_file):
                og_src = pd.read_csv(c_src)
                og_src.to_csv(og_file, index=False)
                del og_src

            og_src = pd.read_csv(og_file)

            og_src['SLOPE'] = np.max([og_src['SLOPE'] + slope_adj, np.repeat(1e-5, og_src.shape[0])], axis=0)

            og_src.to_csv(c_src, index=False)

        # Change Mannings N
        fs_og = htable_og['feature_id'].unique()

        mannings_df = pd.DataFrame({'feature_id': fs_og, 'channel_n': channel_n, 'overbank_n': overbank_n})
        manning_path = os.path.join(src_output_path, 'manning_table.csv')
        mannings_df.to_csv(manning_path, index=False)

        # Subdivide the channels
        suffix = "prob_adjusted"
        run_prep(
            fim_dir=hydrofabric_dir,
            mann_n_table=manning_path,
            output_suffix=suffix,
            number_of_jobs=num_jobs,
            verbose=False,
            src_plot_option=False,
            process_huc=huc,
        )

        # Create new hydrotable to pass in to inundation
        srcs = glob(f'{hydrofabric_dir}/{huc}/branches/*/hydroTable*{suffix}.csv')
        for src in srcs:
            branch = int(src.split('/')[-2])
            df = pd.read_csv(
                src,
                dtype={
                    'HUC': str,
                    'feature_id': str,
                    'HydroID': str,
                    'stage': float,
                    'discharge_cms': float,
                    'LakeID': int,
                    'last_updated': object,
                    'submitter': object,
                    'obs_source': object,
                },
            )
            df.insert(1, 'branch_id', branch)
            dfs.append(df)

        new_htable = pd.concat(dfs)
        new_htable = new_htable.sort_values(['branch_id', 'feature_id', 'stage']).reset_index(drop=True)

        # CHANGE depending on structure in EFS *****
        flow_file = f'{base_dir}/{huc}_{percentile}_flow.csv'

        df = pd.DataFrame(
            {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[percentile]}
        )
        df.to_csv(flow_file, index=False)

        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_file,
            hydro_table_df=new_htable,
            inundation_raster=final_inundation_path,
            mask=mask_path,
            verbose=True,
            num_workers=num_jobs,
        )

        ds = rxr.open_rasterio(final_inundation_path)
        nodata, crs = ds.rio.nodata, ds.rio.crs
        ds2 = xr.where(ds == nodata, 0, ds)
        ds3 = xr.where(ds2 < 0, 0, ds2)
        ds4 = xr.where(ds3 > 0, 1, ds3)
        ds5 = ds4.rio.set_crs(crs)
        ds6 = ds5.rio.set_nodata(0)
        ds6.rio.to_raster(final_inundation_path, driver="COG")

        del ds, ds2, ds3, ds4, ds5, ds6
        gc.collect()

    path = base_output_path
    files = ['90', '75', '50', '25', '10']

    xrs = []

    # For every percentile inundation map convert values to percentile
    for file in files:
        file_name = f'{path}/extent_{file}_v10_day{day}_hour{hour}.tif'
        raster = rxr.open_rasterio(file_name)
        rst1 = xr.where(raster > 0, int(file), raster)

        rst2 = rst1.rio.write_crs(raster.rio.crs)
        xrs.append(rst2)
        os.remove(file_name)

    # Remove SRC path
    shutil.rmtree(src_output_path)

    # Merge all converted rasters and output
    merge_ds = xr.concat(xrs, dim="band")
    max_ds = merge_ds.max(dim='band').assign_coords({"band": 1})
    max_ds = max_ds.rio.write_nodata(0)
    polygon = max_ds.gval.vectorize_data()
    polygon.to_file(os.path.join(base_output_path, output_file_name))


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
    results = []
    for future in tqdm(
        as_completed(executor_dict), total=len(executor_dict), disable=(not verbose), desc=desc
    ):
        try:
            results.append(future.result())
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))

    return results


def inundate_hucs(
    ensembles: str,
    parameters: str,
    base_dir: str,
    hucs: list,
    mosaic_prob_output_name: str,
    posterior_dist: str = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
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
    mosaic_prob_output_name: str
        Name of final mosaiced probabilistic FIM
    posterior_dist: str = None
        Name of posterior df
    day: int = 6
        Days ahead to pick from reference forecast time
    hour: int = 0,
        Hours ahead to pick from reference forecast time
    overwrite: bool = False
        Whether to overwrite existing output
    num_jobs: int
        Number of jobs to process

    """
    for huc in hucs:
        inundate_probabilistic(
            ensembles=ensembles,
            parameters=parameters,
            base_dir=base_dir,
            huc=huc,
            mosaic_prob_output_name=f"{mosaic_prob_output_name[:mosaic_prob_output_name.rfind('.')]}_{huc}.gpkg",
            posterior_dist=posterior_dist,
            day=day,
            hour=hour,
            overwrite=overwrite,
            num_jobs=num_jobs,
        )


if __name__ == '__main__':

    """
    Example Usage:

    python ./probabilistic_inundation.py
        -e ./gfs_ensembles_03070107.nc
        -p ./plink_recurr.csv
        -b "./"
        -hc 03070107
        -f ./example2/mosaic_prob
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
        "-f",
        "--mosaic_prob_output_name",
        help="REQUIRED: Name of final mosaiced probabilistic FIM file",
        required=True,
    )

    parser.add_argument(
        "-pd",
        "--posterior_dist",
        nargs="*",
        help="OPTIONAL: HUCs to process probabilistic inundation",
        required=False,
    )

    parser.add_argument(
        "-d",
        "--day",
        default=6,
        help="OPTIONAL: Days ahead of reference time to get forecast",
        required=False,
    )

    parser.add_argument(
        "-hr",
        "--hour",
        default=0,
        help="OPTIONAL: Hours ahead of reference time to get forecast",
        required=False,
    )

    parser.add_argument(
        "-ow",
        "--overwrite",
        default=False,
        help="OPTIONAL: Whether to overwrite existing output",
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
