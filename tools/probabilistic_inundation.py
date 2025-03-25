import argparse
import ast
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from glob import glob
from typing import Dict, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from inundate_mosaic_wrapper_optimized import produce_mosaicked_inundation
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
from shapely.geometry import shape
from tqdm import tqdm


# import gdal
# gdal.UseExceptions()


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
    feature: int, ensemble_forecast: xr.Dataset, params_weibull: pd.DataFrame
) -> Dict[str, Union[int, float]]:
    """
    Calculates Percentiles for the streamflow distribution

    Parameters
    ----------
    feature : int
        ID of feature to process
    ensemble_forecast : xr.Dataset
        NWM medium range ensembles
    # forecast_time : np.datetime64
    #     Forecast time to slice
    params_weibull : pd.DataFrame
        Parameters for features

    Returns
    -------
    dict
        Dictionary of percentiles for streamflow distribution and feature_id
    """

    # print('starting percentiles')
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

    # params['size'] = 16071

    try:
        r = dist_dict[parameters['distribution_name']](**params)
        # .rvs(**params))
    except Exception:
        return {
            'feature_id': int(feature),
            '90': float(ensemble_forecast.sel({'member': '1'})),
            '75': float(ensemble_forecast.sel({'member': '1'})),
            '50': float(ensemble_forecast.sel({'member': '1'})),
            '25': float(ensemble_forecast.sel({'member': '1'})),
            '10': float(ensemble_forecast.sel({'member': '1'})),
        }

    likelihoods = np.array([1 - r.cdf(x) for x in ensemble_forecast.values])

    # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
    scaled_likelihoods = np.squeeze(likelihoods / np.sum(likelihoods)) * np.linspace(1, 0.9, 6) * 10000

    # Create data to fit truncated exponential distribution
    values = []

    for value, scale in zip(np.squeeze(ensemble_forecast.values), scaled_likelihoods):
        if np.isnan(value):
            value = 0

        if np.isnan(scale):
            scale = 1

        values.append(np.repeat(value, int(scale)))

    streamflow_expon_values = np.hstack(values).ravel()

    if not np.all(streamflow_expon_values == streamflow_expon_values[0]):
        # Generate 10000 random values from distribution
        trunc_expon = truncexpon(
            *truncexpon.fit(streamflow_expon_values, loc=np.min(streamflow_expon_values))
        )

        return {
            'feature_id': int(feature),
            '90': np.max([0, trunc_expon.ppf(0.1)]),
            '75': np.max([0, trunc_expon.ppf(0.25)]),
            '50': np.max([0, trunc_expon.ppf(0.5)]),
            '25': np.max([0, trunc_expon.ppf(0.75)]),
            '10': np.max([0, trunc_expon.ppf(0.9)]),
        }

    else:
        return {
            'feature_id': int(feature),
            '90': np.max([0, streamflow_expon_values[0]]),
            '75': np.max([0, streamflow_expon_values[0]]),
            '50': np.max([0, streamflow_expon_values[0]]),
            '25': np.max([0, streamflow_expon_values[0]]),
            '10': np.max([0, streamflow_expon_values[0]]),
        }


def get_subdivided_src(
    hydrofabric_dir,
    huc,
    branch,
    channel_manning,
    overbank_manning,
    slope_adj,
    htable_directory,
    htable_output,
):
    df_src = pd.read_csv(
        os.path.join(hydrofabric_dir, huc, 'branches', branch, f"src_full_crosswalked_{branch}.csv")
    )
    df_htable = pd.read_csv(
        os.path.join(hydrofabric_dir, huc, 'branches', branch, f"hydroTable_{branch}.csv"),
        dtype={'HUC': str, 'last_updated': object, 'submitter': object, 'obs_source': object},
    )

    # Subdivide Geometry ----------------------------------------------------------------------------------

    df_src['Volume_chan (m3)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'],
        df_src['Volume (m3)'],
        (
            df_src['Volume_bankfull']
            + ((df_src['Stage'] - df_src['Stage_bankfull']) * df_src['SurfArea_bankfull'])
        ),
    )
    df_src['BedArea_chan (m2)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'], df_src['BedArea (m2)'], df_src['BedArea_bankfull']
    )
    df_src['WettedPerimeter_chan (m)'] = np.where(
        df_src['Stage'] <= df_src['Stage_bankfull'],
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000),
        (df_src['BedArea_chan (m2)'] / df_src['LENGTHKM'] / 1000)
        + ((df_src['Stage'] - df_src['Stage_bankfull']) * 2),
    )

    ## Calculate overbank volume & bed area
    df_src['Volume_obank (m3)'] = np.where(
        df_src['Stage'] > df_src['Stage_bankfull'], (df_src['Volume (m3)'] - df_src['Volume_chan (m3)']), 0.0
    )
    df_src['BedArea_obank (m2)'] = np.where(
        df_src['Stage'] > df_src['Stage_bankfull'],
        (df_src['BedArea (m2)'] - df_src['BedArea_chan (m2)']),
        0.0,
    )
    df_src['WettedPerimeter_obank (m)'] = df_src['BedArea_obank (m2)'] / df_src['LENGTHKM'] / 1000

    # Subdivide Geometry ----------------------------------------------------------------------------------

    df_src['channel_n'] = channel_manning
    df_src['overbank_n'] = overbank_manning

    df_src['subdiv_applied'] = np.where(df_src['Stage_bankfull'].isnull(), False, True)  # creat

    # Subdivide Manning Eq --------------------------------------------------------------------------------

    df_src = df_src.drop(
        ['WetArea_chan (m2)', 'HydraulicRadius_chan (m)', 'Discharge_chan (m3s-1)', 'Velocity_chan (m/s)'],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_chan (m2)'] = df_src['Volume_chan (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_chan (m)'] = df_src['WetArea_chan (m2)'] / df_src['WettedPerimeter_chan (m)']
    df_src['HydraulicRadius_chan (m)'].fillna(0, inplace=True)
    df_src['Discharge_chan (m3s-1)'] = (
        df_src['WetArea_chan (m2)']
        * pow(df_src['HydraulicRadius_chan (m)'], 2.0 / 3)
        * pow(np.max([df_src['SLOPE'] + slope_adj, np.repeat(1e-5, df_src.shape[0])], axis=0), 0.5)
        / df_src['channel_n']
    )
    df_src['Velocity_chan (m/s)'] = df_src['Discharge_chan (m3s-1)'] / df_src['WetArea_chan (m2)']
    df_src['Velocity_chan (m/s)'].fillna(0, inplace=True)

    ## Calculate discharge (overbank) using Manning's equation
    df_src = df_src.drop(
        [
            'WetArea_obank (m2)',
            'HydraulicRadius_obank (m)',
            'Discharge_obank (m3s-1)',
            'Velocity_obank (m/s)',
        ],
        axis=1,
        errors='ignore',
    )  # drop these cols (in case subdiv was previously performed)
    df_src['WetArea_obank (m2)'] = df_src['Volume_obank (m3)'] / df_src['LENGTHKM'] / 1000
    df_src['HydraulicRadius_obank (m)'] = df_src['WetArea_obank (m2)'] / df_src['WettedPerimeter_obank (m)']
    df_src = df_src.replace([np.inf, -np.inf], np.nan)  # need to replace inf instances (divide by 0)
    df_src['HydraulicRadius_obank (m)'].fillna(0, inplace=True)
    df_src['Discharge_obank (m3s-1)'] = (
        df_src['WetArea_obank (m2)']
        * pow(df_src['HydraulicRadius_obank (m)'], 2.0 / 3)
        * pow(np.max([df_src['SLOPE'] + slope_adj, np.repeat(1e-5, df_src.shape[0])], axis=0), 0.5)
        / df_src['overbank_n']
    )
    df_src['Velocity_obank (m/s)'] = df_src['Discharge_obank (m3s-1)'] / df_src['WetArea_obank (m2)']
    df_src['Velocity_obank (m/s)'].fillna(0, inplace=True)

    ## Calcuate the total of the subdivided discharge (channel + overbank)
    df_src = df_src.drop(
        ['Discharge (m3s-1)_subdiv'], axis=1, errors='ignore'
    )  # drop these cols (in case subdiv was previously performed)
    df_src['Discharge (m3s-1)_subdiv'] = df_src['Discharge_chan (m3s-1)'] + df_src['Discharge_obank (m3s-1)']
    df_src.loc[df_src['Stage'] == 0, ['Discharge (m3s-1)_subdiv']] = 0

    # Subdivide Manning Eq --------------------------------------------------------------------------------

    ## Use the default discharge column when vmann is not being applied
    df_src['Discharge (m3s-1)_subdiv'] = np.where(
        df_src['subdiv_applied'] == False, df_src['Discharge (m3s-1)'], df_src['Discharge (m3s-1)_subdiv']
    )  # reset the discharge value back to the original if vmann=false

    df_src = df_src[
        [
            'HydroID',
            'Stage',
            'Bathymetry_source',
            'subdiv_applied',
            'channel_n',
            'overbank_n',
            'Discharge (m3s-1)_subdiv',
        ]
    ]

    df_src = df_src.rename(columns={'Stage': 'stage', 'Discharge (m3s-1)_subdiv': 'subdiv_discharge_cms'})
    df_src['discharge_cms'] = df_src[
        'subdiv_discharge_cms'
    ]  # create a copy of vmann modified discharge (used to track future changes)

    ## drop the previously modified discharge column to be replaced with updated version
    df_htable = df_htable.drop(
        [
            'subdiv_applied',
            'discharge_cms',
            'overbank_n',
            'channel_n',
            'subdiv_discharge_cms',
            'Bathymetry_source',
        ],
        axis=1,
        errors='ignore',
    )
    df_htable = df_htable.merge(
        df_src, how='left', left_on=['HydroID', 'stage'], right_on=['HydroID', 'stage']
    )

    df_htable['branch_id'] = int(branch)
    df_htable['LakeID'] = -999
    df_htable['HydroID'] = df_htable['HydroID'].astype(str)
    df_htable['feature_id'] = df_htable['feature_id'].astype(str)

    output_table = os.path.join(htable_directory, htable_output.format(branch))
    df_htable.to_feather(output_table)


def inundate_probabilistic(
    ensembles: str,
    parameters: str,
    hydrofabric_dir: str,
    outputs_dir: str,
    huc: str,
    mosaic_prob_output_name: str,
    posterior_dist: str = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
    num_threads: int = 1,
    windowed: bool = False,
    output_raster: bool = False,
    quiet: bool = True,
    log_file: str = None,
):
    """
    Method to probabilistically inundate based on provided ensembles

    Parameters
    ----------
    ensembles: str
        Path to load medium range ensembles
    parameters: str
        Path to load fit parameters to distributions
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    output_dir: str
        Directory to write output files
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
    num_jobs: int
        Number of processes to parallelize over
    num_threads: int
        Number of threads to parallelize over
    windowed: bool = False
        Whether to run inundation in windowed mode for memory conservation
    output_raster: bool = False
        Whether to keep the output raster along with the vector output
    log_file: str = None
        Filepath of log file

    """

    # Load datasets
    ensembles = xr.open_dataset(ensembles)

    parameters_df = pd.read_csv(parameters)
    params_weibull = parameters_df.loc[parameters_df['distribution_name'] == 'weibull_min']
    params_weibull.set_index('feature_id', inplace=True)

    # Fim outputs directory
    fim_outputs_dir = outputs_dir

    # Masks for waterbodies
    mask_path = os.path.join(hydrofabric_dir, huc, 'wbd.gpkg')

    # Slice of time in forecast (possibly changed to
    forecast_time = (
        ensembles.coords['reference_time'].values[-1] + np.timedelta64(day, 'D') + np.timedelta64(hour, 'h')
    )

    # Percentiles and data to add
    percentiles = {'90': 10, '75': 25, '50': 50, '25': 75, '10': 90}
    percentile_values = {'feature_id': [], '90': [], '75': [], '50': [], '25': [], '10': []}

    features = ensembles.coords['feature_id']

    # For each feature in the provided ensembles
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor_dict = {}
        for feat in features:

            feat = feat if isinstance(feat, int) else int(feat)

            ensemble_forecast = ensembles.sel(
                {'time': forecast_time, 'feature_id': feat, 'member': ['1', '2', '3', '4', '5', '6']}
            )['streamflow']

            try:
                future = executor.submit(
                    generate_streamflow_percentiles,
                    feature=feat,
                    ensemble_forecast=ensemble_forecast.copy(),
                    params_weibull=params_weibull,
                )
                executor_dict[future] = feat

            except Exception as ex:
                print("Something went wrong")
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

        # Send the executor to the progress bar and wait for all MS tasks to finish
        results = progress_bar_handler(
            executor_dict, not quiet, f"Running streamflow percentiles with {num_threads} workers"
        )

        for res in results:
            percentile_values['feature_id'].append(res['feature_id'])
            percentile_values['90'].append(res['90'])
            percentile_values['75'].append(res['75'])
            percentile_values['50'].append(res['50'])
            percentile_values['25'].append(res['25'])
            percentile_values['10'].append(res['10'])

    ensembles.close()
    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=huc
    )

    # print('NUMBA_CACHE_DIR', os.environ["NUMBA_CACHE_DIR"])
    # print('Initial Contents of NUMBA_CACHE_DIR', os.listdir(os.environ['NUMBA_CACHE_DIR']))

    # Apply inundation map to each percentile
    for percentile, val in percentiles.items():

        channel_n = channel_dist.ppf(1 - int(percentile) / 100)
        overbank_n = obank_dist.ppf(1 - int(percentile) / 100)
        slope_adj = slope_dist.ppf(int(percentile) / 100)

        # Make directories if they do not exist
        output_file_name = mosaic_prob_output_name.split('/')[-1]
        base_output_path = os.path.join(fim_outputs_dir, str(huc))
        src_output_path = os.path.join(base_output_path, 'srcs')
        htable_output_path = os.path.join(base_output_path, 'srcs')
        flow_path = os.path.join(base_output_path, 'flows')

        # Create directories if they do not exist
        os.makedirs(base_output_path, exist_ok=True)
        os.makedirs(src_output_path, exist_ok=True)
        os.makedirs(htable_output_path, exist_ok=True)
        os.makedirs(flow_path, exist_ok=True)

        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(
            base_output_path, f'extent_{percentile}_v10_day{day}_hour{hour}.tif'
        )

        # Skip if the file exists
        if os.path.exists(final_inundation_path) and not overwrite:
            continue

        # Open the original hydrotable
        all_branches = glob(os.path.join(hydrofabric_dir, huc, "branches", "*"))
        all_branches = [x.split('/')[-1] for x in all_branches]

        htable_output_file = "htable_{0}.feather"
        for branch in all_branches:
            get_subdivided_src(
                hydrofabric_dir,
                huc,
                branch,
                channel_n,
                overbank_n,
                slope_adj,
                htable_output_path,
                htable_output_file,
            )

        # CHANGE depending on structure in EFS *****
        flow_file = os.path.join(flow_path, f'{huc}_{percentile}_flow.csv')

        df = pd.DataFrame(
            {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[percentile]}
        )
        df.to_csv(flow_file, index=False)

        # print("Before produce_mosaicked inundation", time.localtime())
        # Temporarily constrained to one run for lambda
        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_file,
            hydro_table_df=os.path.join(htable_output_path, htable_output_file),
            inundation_raster=final_inundation_path,
            mask=mask_path,
            verbose=True,
            num_workers=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            log_file=log_file,
        )
        # print("Before final manipulation", time.localtime())
        # print("file exists: ", os.path.exists(final_inundation_path))
        # print('Contents of NUMBA_CACHE_DIR', os.listdir(os.environ['NUMBA_CACHE_DIR']))

    percentiles
    percentile_files = [
        f'{base_output_path}/extent_{file}_v10_day{day}_hour{hour}.tif' for file in list(percentiles.keys())
    ]

    # For every percentile inundation map convert values to percentile
    datasets = [rasterio.open(file) for file in percentile_files]
    windows = [windows for _, windows in datasets[0].block_windows()]
    profile = datasets[0].profile
    raster_crs = datasets[0].crs
    profile.update(dtype=np.int8)

    def merge_percentiles(
        ds: list, percentiles: list, window: rasterio.windows.Window, wrst=rasterio.io.DatasetWriter
    ):
        arrays = []
        for d, p in zip(ds, percentiles):
            data = d.read(1, window=window)
            data[np.where(data > 0)] = np.int8(p)
            arrays.append(data)

        merged = np.max(arrays, axis=0)

        wrst.write(merged, window=window, indexes=1)

    executor = ThreadPoolExecutor(max_workers=1)

    def __data_generator(datasets, percentiles, windows, wrst):
        for window in windows:
            yield datasets, percentiles, window, wrst

    def _vprint(message, verbose):
        if verbose:
            print(message)

    out_rast = os.path.join(base_output_path, output_file_name.replace(".gpkg", ".tif"))
    with rasterio.open(out_rast, "w+", **profile) as write_rst:
        dgen = __data_generator(datasets, list(percentiles.keys()), windows, write_rst)
        results = {executor.submit(merge_percentiles, *wg): 1 for wg in dgen}

        for future in as_completed(results):
            try:
                future.result()
            except Exception as exc:
                _vprint("Exception {} for {}".format(exc, results[future]), not quiet)
            else:
                if results[future] is not None:
                    _vprint("... {} complete".format(results[future]), not quiet)
                else:
                    _vprint("... complete", not quiet)

    # Close datasets
    for ds in datasets:
        ds.close()

    with rasterio.open(out_rast, 'r') as rst:
        shapes = rasterio.features.shapes(rst.read(1), mask=None, transform=rst.transform)

        polygons = []
        for geom, value in shapes:
            polygon = shape(geom)
            polygons.append((polygon, value))

        data = []
        for polygon, value in polygons:
            data.append({'geometry': polygon, 'value': value})
        gdf = gpd.GeoDataFrame(data, crs=raster_crs)
        gdf.to_file(os.path.join(base_output_path, output_file_name))

    for file in percentile_files:
        os.remove(file)

    if output_raster is False:
        os.remove(out_rast)
    # Remove SRC path
    shutil.rmtree(src_output_path)
    shutil.rmtree(flow_path)


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
    hydrofabric_dir: str,
    outputs_dir: str,
    hucs: list,
    mosaic_prob_output_name: str,
    posterior_dist: str = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
    num_threads: int = 1,
    windowed: bool = False,
    output_raster: bool = False,
    quiet: bool = True,
    log_file: str = None,
):
    """Driver for running probabilistic inundation on selected HUCs

    Parameters
    ----------
    ensembles: str
        Location of nws ensemble NetCDF file
    parameters: str
        Location of parameter CSV file
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    outputs_dir: str
        Directory to write output files
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
        Number of processes to parallelize over
    num_threads: int
        Number of threads to parallelize over
    windowed: bool = False
        Whether to run inundation in windowed mode for memory conservation
    output_raster: bool = False
        Whether to keep the output raster along with the vector output
    quiet: bool = False
        Whether to be verbose or not
    log_file: str = None
        Filepath of log file

    """
    for huc in hucs:
        inundate_probabilistic(
            ensembles=ensembles,
            parameters=parameters,
            hydrofabric_dir=hydrofabric_dir,
            outputs_dir=outputs_dir,
            huc=huc,
            mosaic_prob_output_name=f"{mosaic_prob_output_name[:mosaic_prob_output_name.rfind('.')]}_{huc}.gpkg",
            posterior_dist=posterior_dist,
            day=day,
            hour=hour,
            overwrite=overwrite,
            num_jobs=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            output_raster=output_raster,
            quiet=quiet,
            log_file=log_file,
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
        "-hd",
        "--hydrofabric_dir",
        help="REQUIRED: Base directory with fim outputs and hydrofabric",
        required=True,
    )

    parser.add_argument(
        "-od", "--outputs_dir", help="REQUIRED: Directory with fim outputs and hydrofabric", required=True
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
        "-or",
        "--output_raster",
        default=False,
        help="OPTIONAL: Whether to keep final raster output along with vector",
        required=False,
    )

    parser.add_argument(
        "-q", "--quiet", default=True, help="OPTIONAL: Whether to be verbose or not", required=False
    )

    parser.add_argument(
        "-j", "--num_jobs", type=int, help="REQUIRED: Number of jobs to process HUCs", required=True
    )

    parser.add_argument(
        "-t", "--num_threads", type=int, help="REQUIRED: Number of threads to process HUCs", required=True
    )

    parser.add_argument(
        "-w", "--windowed", type=bool, help="REQUIRED: Number of threads to process HUCs", required=True
    )

    parser.add_argument("-l", "--log_file", type=str, help="OPTIONAL: Filepath for log file", required=False)

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        inundate_hucs(**args)

    except Exception:
        print("The following error has occured:\n", traceback.format_exc())
