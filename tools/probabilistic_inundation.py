import argparse
import ast
import gc
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob
from typing import Tuple

import numpy as np
import pandas as pd
import rioxarray as rxr
import xarray as xr
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation
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

        # Default gamma likelihood for channel manning roughness
        a = 6
        loc = 0.02
        scale = 0.0076

        channel_dist = gamma(a=a, loc=loc, scale=scale)

        # Default gamma likelihood for overbank manning roughness
        a = 14
        loc = 0.07
        scale = 0.004

        obank_dist = gamma(a=a, loc=loc, scale=scale)

        # Default gamma likelihood for slope adjustment
        a = 1.5
        loc = -0.02
        scale = 0.008

        slope_dist = gamma(a=a, loc=loc, scale=scale)

    else:

        raise NotImplementedError("Currently not implemented")

    return channel_dist, obank_dist, slope_dist


def inundate_probabilistic(
    ensembles: str,
    parameters: str,
    base_dir: str,
    huc: str,
    output_folder_name: str,
    mosaic_prob_output_name: str,
    posterior_dist: str = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
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
    output_folder_name: str
        Name of final output directory
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

    print("forecast_time", forecast_time.values[0], day, hour)

    # Percentiles and data to add
    percentiles = {'90': 10, '75': 25, '50': 50, '25': 75, '10': 90}
    percentile_values = {'feature_id': [], '90': [], '75': [], '50': [], '25': [], '10': []}

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

    features = ensembles.coords['feature_id']

    # For each feature in the provided ensembles
    for feature in tqdm(features):

        ensemble_forecast = ensembles.sel(
            {'time': forecast_time, 'feature_id': feature, 'member': ['1', '2', '3', '4', '5', '6']}
        )['streamflow']

        if int(feature) not in parameters_df.index:
            #         print('continue')
            percentile_values['feature_id'].append(int(feature))
            for key, val in percentiles.items():
                percentile_values[key].append(float(ensemble_forecast.sel({'member': '3'})))
        else:
            parameters = params_weibull.loc[int(feature)].copy()

        # Create probability distribution
        params = ast.literal_eval(parameters['parameters'])
        params['size'] = 16071

        try:
            r = dist_dict[parameters['distribution_name']].rvs(**params)
        except Exception:
            #         print('fail')
            percentile_values['feature_id'].append(int(feature))
            for key, val in percentiles.items():
                percentile_values[key].append(float(ensemble_forecast.sel({'member': '3'})))
        #     continue

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
        percentile_values['feature_id'].append(str(int(feature)))
        for key, val in percentiles.items():
            percentile_values[key].append(np.max([0, np.percentile(final_values, val)]))

    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=huc
    )

    # Apply inundation map to each percentile
    for percentile, val in percentiles.items():

        channel_n = channel_dist.ppf(1 - int(percentile) / 100)
        overbank_n = obank_dist.ppf(1 - int(percentile) / 100)
        slope_adj = slope_dist.ppf(int(percentile) / 100)

        # Make directories if they do not exist
        base_output_path = os.path.join(fim_outputs_dir, output_folder_name, str(huc))
        src_output_path = os.path.join(base_output_path, 'srcs')

        if not os.path.exists(base_output_path):
            os.makedirs(base_output_path)

        if not os.path.exists(src_output_path):
            os.makedirs(src_output_path)

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
            number_of_jobs=8,
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

        # Create inundation maps and mosaic
        map_file = Inundate_gms(
            hydrofabric_dir=hydrofabric_dir,
            forecast=flow_file,
            hucs=[huc],
            inundation_raster=os.path.join(fim_outputs_dir, f'inundation_{huc}.tif'),
            hydro_table_df=new_htable,
            verbose=True,
        )

        Mosaic_inundation(
            map_file,
            mosaic_attribute='inundation_rasters',
            mosaic_output=final_inundation_path,
            mask=mask_path,
            unit_attribute_name='huc8',
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
        raster = rxr.open_rasterio(f'{path}/extent_{file}_v10_day{day}_hour{hour}.tif')
        rst1 = xr.where(raster > 0, int(file), raster)

        rst2 = rst1.rio.write_crs(raster.rio.crs)
        xrs.append(rst2)

    # Merge all converted rasters and output
    merge_ds = xr.concat(xrs, dim="band")
    max_ds = merge_ds.max(dim='band').assign_coords({"band": 1})
    max_ds = max_ds.rio.write_nodata(0)
    max_ds.rio.to_raster(os.path.join(base_output_path, mosaic_prob_output_name), driver="COG", dtype=np.int8)


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
    ensembles: str,
    parameters: str,
    base_dir: str,
    hucs: list,
    output_folder_name: str,
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
    output_folder_name: str
        Location to output maps
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
                    output_folder_name=output_folder_name,
                    mosaic_prob_output_name=f"{mosaic_prob_output_name.split('.')[0]}_{huc}.tif",
                    posterior_dist=posterior_dist,
                    day=day,
                    hour=hour,
                    overwrite=overwrite,
                )
                executor_dict[future] = huc

            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

        # Send the executor to the progress bar and wait for all MS tasks to finish
        progress_bar_handler(executor_dict, True, f"Running probabilistic inundation with {num_jobs} workers")


if __name__ == '__main__':

    """
    Example Usage:

    python ./probabilistic_inundation.py
        -e ./gfs_ensembles_03070107.nc
        -p ./plink_recurr.csv
        -b "./"
        -hc 03070107
        -o example2
        -f mosaic_prob
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
        "--output_folder_name",
        help="REQUIRED: Directory to output results of probabilistic inundation",
        required=True,
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
