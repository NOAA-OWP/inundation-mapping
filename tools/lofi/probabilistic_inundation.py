import argparse
import ast
import os
import shutil
import warnings
from concurrent.futures import as_completed
from contextlib import ExitStack
from functools import partial
from typing import Dict, Optional, Tuple, Union

import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from rasterio import features as riofeat
from scipy.stats import expon, gamma, genextreme, genpareto, gumbel_r, kappa4, norm, pearson3, weibull_min
from shapely.geometry import shape
from tqdm.auto import tqdm

from utils.shared_functions import s3_or_local_glob


def get_fim_probability_distributions(
    posterior_dist: Optional[pd.DataFrame] = None, huc: Optional[int] = None, magnitude: Optional[int] = 2
) -> Tuple[weibull_min, weibull_min, weibull_min]:
    """
    Gets either bayesian updated distributions or default distributions for respective huc

    Parameters
    ---------
    posterior_dist : Optional[Union[str, pd.DataFrame]], default = None
        Name of csv file that has posterior distribution parameters
    huc: Optional[int], default = None
        Huc to get distribution for if posterior_dist is not None
    magnitude: Optional[int], default = None
        Calculated magnitude of forecast

    Returns
    -------
    Tuple[weibull_min, wibull_min, weibull_min]
        Weibull distributions for channel Manning roughness, overbank Manning roughness, and slope adjustment

    """

    if posterior_dist is None:

        # Default weibull likelihood for channel manning roughness
        channel_dist = weibull_min(c=8.5, scale=0.07, loc=-0.07)

        # Default weibull likelihood for overbank manning roughness
        obank_dist = weibull_min(c=8.5, scale=0.07, loc=-0.07)

        # Default weibull likelihood for slope adjustment
        slope_dist = weibull_min(c=0.85, scale=0.005, loc=-0.0015)

    else:
        variables = ['channel_manning_roughness', 'overbank_manning_roughness', 'slope_adjustment']
        dist_params = ['c', 'scale', 'loc']

        posterior_df = posterior_dist

        if huc is not None and 'huc' in posterior_df.columns:
            posterior_df = posterior_df[posterior_df['huc'] == int(huc)]

        if 'magnitude' in posterior_df.columns:
            if magnitude is None:
                posterior_df = posterior_df.iloc[:3]
            else:
                posterior_df = posterior_df[posterior_df['magnitude'] == magnitude]

        dist = []
        posterior_df = posterior_df.set_index('parameter_name')
        for variable in variables:
            dist_args = {key: value for key, value in zip(dist_params, posterior_df.loc[variable].values)}
            dist.append(weibull_min(**dist_args))

        channel_dist, obank_dist, slope_dist = tuple(dist)

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

    dkeys = ['90', '75', '50', '25', '10']

    # If there is no feature in the NWM parameters file
    if feature not in params_weibull.index:
        rv = dict.fromkeys(dkeys, float(ensemble_forecast.sel({'ensemble': '1'})['streamflow']))
        rv['feature_id'] = str(feature)
        return rv
    else:
        parameters = params_weibull.loc[feature]

    # Create probability distribution
    params = ast.literal_eval(parameters['parameters'])

    try:
        r = dist_dict[parameters['distribution_name']](**params)

    except Exception:
        rv = dict.fromkeys(dkeys, float(ensemble_forecast.sel({'ensemble': '1'})['streamflow']))
        rv['feature_id'] = str(feature)
        return rv

    streamflow_values = np.squeeze(ensemble_forecast['streamflow'].values)

    if not np.allclose(streamflow_values, streamflow_values[0]):
        streamflow_values[np.isnan(streamflow_values)] = np.nanmean(streamflow_values)
        likelihoods = 1 - r.cdf(streamflow_values)

        # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
        scaled_likelihoods = np.squeeze(likelihoods / np.sum(likelihoods)) * np.linspace(1, 0.9, 6) * 10000

        # Interpolate streamflow values so that member1 represents the 50th percentile
        top = np.interp([10, 25, 50], [10, 50], [np.min(scaled_likelihoods), scaled_likelihoods[0]])[::-1]

        top_scaled = np.interp(
            top,
            [np.min(scaled_likelihoods), scaled_likelihoods[0]],
            [np.max(streamflow_values), streamflow_values[0]],
        )

        bottom = np.interp([50, 75, 90], [50, 90], [scaled_likelihoods[0], np.max(scaled_likelihoods)])[::-1]
        bottom_scaled = np.interp(
            bottom,
            [scaled_likelihoods[0], np.max(scaled_likelihoods)],
            [streamflow_values[0], np.min(streamflow_values)],
        )

        percentile_values = np.hstack([bottom_scaled, top_scaled[1:]])

        return {
            '90': max(0, percentile_values[0]),
            '75': max(0, percentile_values[1]),
            '50': max(0, streamflow_values[0]),
            '25': max(0, percentile_values[3]),
            '10': max(0, percentile_values[4]),
            'feature_id': str(feature),
        }

    else:
        rv = dict.fromkeys(dkeys, max(0, streamflow_values[0]))
        rv['feature_id'] = str(feature)
        return rv


def analyze_nonmonotonic_src(srcs_df, channel_manning, slope_adj):

    srcs_df.loc[srcs_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

    cond_chan = srcs_df['bankfull_proxy'] == 'channel'
    srcs_df_chan = srcs_df[cond_chan]
    non_monotonic_index = srcs_df_chan.index[srcs_df_chan['Discharge (m3s-1)'].diff().lt(0)].tolist()

    # Recalculate 'Discharge' values before the last non-monotonic row
    # Note: No change has been applied on WetArea, Volume, LENGTHKM
    if non_monotonic_index:
        # Get the target values from the last non-monotonic index
        target_idx = non_monotonic_index[-1]
        target_numCells = srcs_df.loc[target_idx, 'Number of Cells']
        target_SurfaceArea = srcs_df.loc[target_idx, 'SurfaceArea (m2)']
        target_BedArea = srcs_df.loc[target_idx, 'BedArea (m2)']

        # Define the slice (up to but not including target_idx)
        row_slice = slice(0, target_idx)

        # Assign target values to the selected rows
        srcs_df.loc[row_slice, 'Number of Cells'] = target_numCells
        srcs_df.loc[row_slice, 'SurfaceArea (m2)'] = target_SurfaceArea
        srcs_df.loc[row_slice, 'BedArea (m2)'] = target_BedArea

        # Recalculate discharge variables
        length_km = srcs_df.loc[row_slice, 'LENGTHKM']
        # Avoid division by zero
        length_km = length_km.replace(0, np.nan)

        target_TopWidth = target_SurfaceArea / length_km / 1000
        target_WettedPerimeter = target_BedArea / length_km / 1000

        wet_area = srcs_df.loc[row_slice, 'WetArea (m2)']
        target_HydraulicRadius = wet_area / target_WettedPerimeter

        srcs_df.loc[row_slice, 'TopWidth (m)'] = target_TopWidth
        srcs_df.loc[row_slice, 'WettedPerimeter (m)'] = target_WettedPerimeter
        srcs_df.loc[row_slice, 'HydraulicRadius (m)'] = target_HydraulicRadius
        srcs_df['HydraulicRadius (m)'] = srcs_df['HydraulicRadius (m)'].fillna(0)

        # Recalculate Discharge (m3s-1) for the selected rows
        srcs_df.loc[row_slice, 'Discharge (m3s-1)'] = (
            wet_area
            * (srcs_df.loc[row_slice, 'HydraulicRadius (m)'] ** (2.0 / 3))
            * pow(
                np.max(
                    [
                        srcs_df.loc[row_slice, 'SLOPE'] + slope_adj,
                        np.repeat(1e-5, srcs_df.loc[row_slice].shape[0]),
                    ],
                    axis=0,
                ),
                0.5,
            )
            / srcs_df['channel_n']
            + channel_manning
        )

    return srcs_df


def get_subdivided_src(hydrofabric_dir, huc, branch, channel_manning, overbank_manning, slope_adj):
    """
    Method for subdividing a synthetic rating curve based on the high water threshold

    Parameters
    ----------
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    huc: str
        Huc to process probabilistic FIM
    branch: str
        Name of final mosaiced probabilistic FIM
    channel_manning: float
        Value for channel manning roughness
    overbank_manning: float
        Value for overbank manning roughness
    slope_adj: float
        Adjustment of the calculated slope

    """

    with fsspec.open(
        os.path.join(hydrofabric_dir, huc, 'branches', branch, f"src_full_crosswalked_{branch}.csv"),
        mode='rt',
        encoding='utf-8',
    ) as f:  # Use 'rt' for text mode, and specify encoding
        df_src = pd.read_csv(f)

    with fsspec.open(
        os.path.join(hydrofabric_dir, huc, "hydrotable.parquet"), mode='rb'
    ) as f:  # Use 'rt' for text mode, and specify encoding
        df_htable = pd.read_parquet(f, filters=[('branch_id', '==', int(branch))])

    df_htable = df_htable.sort_values(['HydroID', 'feature_id', 'stage'])

    # Subdivide Geometry ----------------------------------------------------------------------------------
    df_src['channel_n'] = df_src['channel_n'] + channel_manning
    df_src['overbank_n'] = df_src['overbank_n'] + overbank_manning
    df_src['subdiv_applied'] = ~df_src['Stage_bankfull'].isnull()  # creat

    df_src['Discharge_chan (m3s-1)'] = (
        df_src['WetArea_chan (m2)']
        * pow(df_src['HydraulicRadius_chan (m)'], 2.0 / 3)
        * pow(np.max([df_src['SLOPE'] + slope_adj, np.repeat(1e-5, df_src.shape[0])], axis=0), 0.5)
        / df_src['channel_n']
    )

    df_src['Discharge_obank (m3s-1)'] = (
        df_src['WetArea_obank (m2)']
        * pow(df_src['HydraulicRadius_obank (m)'], 2.0 / 3)
        * pow(np.max([df_src['SLOPE'] + slope_adj, np.repeat(1e-5, df_src.shape[0])], axis=0), 0.5)
        / df_src['overbank_n']
    )

    # Calculate the total of the subdivided discharge (channel + overbank)
    df_src = df_src.drop(
        ['Discharge (m3s-1)_subdiv'], axis=1, errors='ignore'
    )  # drop these cols (in case subdiv was previously performed)
    df_src['Discharge (m3s-1)_subdiv'] = df_src['Discharge_chan (m3s-1)'] + df_src['Discharge_obank (m3s-1)']
    df_src.loc[df_src['Stage'] == 0, ['Discharge (m3s-1)_subdiv']] = 0

    # Subdivide Manning Eq --------------------------------------------------------------------------------

    # Use the default discharge column when vmann is not being applied
    df_src['Discharge (m3s-1)_subdiv'] = np.where(
        ~df_src['subdiv_applied'], df_src['Discharge (m3s-1)'], df_src['Discharge (m3s-1)_subdiv']
    )  # reset the discharge value back to the original if vmann=false

    nonmonotonic_partial = partial(
        analyze_nonmonotonic_src, slope_adj=slope_adj, channel_manning=channel_manning
    )

    df_src = df_src.groupby('HydroID', group_keys=False).apply(nonmonotonic_partial, include_groups=False)

    df_src = df_src[
        [
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

    df_htable["discharge_cms"] = df_src['discharge_cms'].values
    df_htable['precalb_discharge_cms'] = 0

    df_htable.set_index('HUC', append=True, inplace=True)

    return df_htable


def inundate_probabilistic(
    ensembles: xr.Dataset,
    parameters: pd.DataFrame,
    hydrofabric_dir: str,
    outputs_dir: str,
    huc: str,
    mosaic_prob_output_name: str,
    posterior_dist: Optional[pd.DataFrame] = None,
    day: Optional[int] = 6,
    hour: Optional[int] = 0,
    overwrite: Optional[bool] = False,
    num_jobs: Optional[int] = 1,
    num_threads: Optional[int] = 1,
    windowed: Optional[bool] = False,
    output_raster: Optional[bool] = False,
    quiet: Optional[bool] = True,
    log_file: Optional[str] = None,
    output_vector: Optional[bool] = True,
):
    """
    Method to probabilistically inundate based on provided ensembles

    Parameters
    ----------
    ensembles: xr.Dataset
        Path to load medium range ensembles
    parameters: pd.DataFrame
        Path to load fit parameters to distributions
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    outputs_dir: str
        Directory to write output files
    huc: str
        Huc to process probabilistic FIM
    mosaic_prob_output_name: str
        Name of final mosaiced probabilistic FIM
    posterior_dist: Optional[Union[str, pd.DataFrame]] = None
        Name of posterior df
    day: Optional[int], default = 6
        Days ahead to pick from reference forecast time
    hour: Optional[int], default = 0,
        Hours ahead to pick from reference forecast time
    overwrite: Optional[bool], default = False
        Whether to overwrite existing output
    num_jobs: Optional[int], default = 1
        Number of processes to parallelize over
    num_threads: Optional[int], default = 1
        Number of threads to parallelize over
    windowed: Optional[bool], default = False
        Whether to run inundation in windowed mode for memory conservation
    output_raster: Optional[bool], default = False
        Whether to keep the output raster
    quiet : Optional[bool], default=False
        Quiet output
    log_file: Optional[str], default = None
        Filepath of log file
    output_vector: Optional[bool], default = True
        Whether to create vector output

    """

    if output_raster is False and output_vector is False:
        raise ValueError("Either output_raster or output_vector must be set to True")

    if isinstance(parameters, str):
        parameters_df = pd.read_parquet(parameters)
    elif isinstance(parameters, pd.DataFrame):
        parameters_df = parameters
    else:
        raise ValueError("Either parameters must be a str or pd.DataFrame")

    params_weibull = parameters.loc[parameters_df['distribution_name'] == 'weibull_min']
    params_weibull = params_weibull.set_index('feature_id')

    # Fim outputs directory
    fim_outputs_dir = outputs_dir

    # Masks for HUC Domain
    mask_path = os.path.join(hydrofabric_dir, huc, 'wbd.gpkg')

    # Percentiles and data to add
    percentiles = {'90': 10, '75': 25, '50': 50, '25': 75, '10': 90}
    percentile_values = {'feature_id': [], '90': [], '75': [], '50': [], '25': [], '10': []}

    features = ensembles.coords['feature_id']

    # For each feature in the provided ensembles

    # Generate streamflow likelihoods for each feature
    for feat in map(int, features):
        ensemble_forecast = ensembles.sel({'feature_id': feat})

        res = generate_streamflow_percentiles(
            feature=feat, ensemble_forecast=ensemble_forecast, params_weibull=params_weibull
        )

        percentile_values['feature_id'].append(res['feature_id'])
        percentile_values['90'].append(res['90'])
        percentile_values['75'].append(res['75'])
        percentile_values['50'].append(res['50'])
        percentile_values['25'].append(res['25'])
        percentile_values['10'].append(res['10'])

    magnitude = ensembles.attrs['magnitude'] if 'magnitude' in ensembles.attrs else None

    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=int(huc), magnitude=magnitude
    )

    # Make directories if they do not exist
    output_file_name = os.path.basename(mosaic_prob_output_name)
    base_output_path = os.path.join(fim_outputs_dir, huc)

    # Create directory if it does not exist
    os.makedirs(base_output_path, exist_ok=True)

    # Find the original hydrotable
    all_branches = s3_or_local_glob(os.path.join(hydrofabric_dir, huc, "branches", "*"))
    all_branches = list(map(os.path.basename, all_branches))

    # Apply inundation map to each percentile
    for percentile, val in percentiles.items():
        channel_n = channel_dist.ppf(1 - int(percentile) / 100)
        overbank_n = obank_dist.ppf(1 - int(percentile) / 100)
        slope_adj = slope_dist.ppf(int(percentile) / 100)

        if percentile == '50':
            channel_n, overbank_n, slope_adj = 0, 0, 0

        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(
            base_output_path, f'extent_{percentile}_v10_day{day}_hour{hour}.tif'
        )

        # Skip if the file exists
        if os.path.exists(final_inundation_path) and not overwrite:
            continue

        h_tables = []
        for branch in all_branches:
            h_table = get_subdivided_src(hydrofabric_dir, huc, branch, channel_n, overbank_n, slope_adj)
            h_tables.append(h_table)

        final_src = pd.concat(h_tables)

        flow_df = pd.DataFrame(
            {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[percentile]}
        )

        flow_df = flow_df.set_index('feature_id')

        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_df,
            hydro_table_df=final_src,
            inundation_raster=final_inundation_path,
            mask=mask_path,
            verbose=not quiet,
            num_workers=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            log_file=log_file,
        )

    # percentiles
    percentile_files = [
        f'{base_output_path}/extent_{file}_v10_day{day}_hour{hour}.tif' for file in percentiles.keys()
    ]

    # For every percentile inundation map convert values to percentile
    with ExitStack() as stack:
        datasets = [stack.enter_context(rasterio.open(file)) for file in percentile_files]
        windows = [windows for _, windows in datasets[0].block_windows()]
        profile = datasets[0].profile
        odtype = profile['dtype']
        raster_crs = datasets[0].crs
        nodata = profile['nodata']
        profile.update(dtype=np.int8, nodata=127, tiled=True, compress=profile.get('compress', 'DEFLATE'))

        out_rast = os.path.join(base_output_path, output_file_name.replace(".gpkg", ".tif"))
        with rasterio.open(out_rast, "w+", **profile) as write_rst:
            for window in windows:
                maxx = np.zeros((window.height, window.width), dtype=odtype)
                tmpm = np.zeros_like(maxx)
                mask = np.empty((window.height, window.width), dtype='bool')
                nodata_mask = np.empty((window.height, window.width), dtype='bool')
                for d, p in zip(datasets, percentiles):
                    d.read(1, out=tmpm, window=window)

                    # Only run on the last percentile (greatest extent possible)
                    if p == "10":
                        np.equal(tmpm, nodata, out=nodata_mask)

                    # equivalent to np.where(tmpm > 0, int(p), 0)
                    np.greater(tmpm, 0, out=mask)
                    tmpm.fill(0)
                    np.copyto(tmpm, int(p), where=mask)

                    np.maximum(maxx, tmpm, out=maxx)

                    # Only run on the last percentile (greatest extent possible)
                    if p == "10":
                        np.copyto(maxx, 127, where=nodata_mask)

                write_rst.write(maxx, window=window, indexes=1)

    if output_vector is True:

        out_vec = os.path.join(base_output_path, output_file_name.replace(".tif", ".gpkg"))

        def _make_geometry(shapes):
            for p, v in shapes:
                yield shape(p), v

        with rasterio.open(out_rast, 'r') as rst:
            shapes = riofeat.shapes(rst.read(1), mask=None, transform=rst.transform)
            gdf = gpd.GeoDataFrame(_make_geometry(shapes), columns=['geometry', 'value'], crs=raster_crs)
            gdf = gdf.set_geometry('geometry')
            gdf.to_file(out_vec)

    for file in percentile_files:
        os.remove(file)

    if output_raster is False:
        os.remove(out_rast)


def progress_bar_handler(executor_dict, verbose, desc) -> list:
    """Show progress of operation

    Parameters
    ----------
    executor_dict: dict
        Keys as futures and HUC ids as values
    verbose: bool
        Whether to print more progress
    desc: str
        Description of the process

    Returns
    -------
    list
        Results from performing parallelized task

    """
    results = []
    for future in tqdm(
        as_completed(executor_dict), total=len(executor_dict), disable=(not verbose), desc=desc
    ):
        # try:
        results.append(future.result())
        # except Exception as exc:
        #     print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))

    return results


def inundate_hucs(
    ensembles: str,
    parameters: str,
    hydrofabric_dir: str,
    outputs_dir: str,
    hucs: list,
    mosaic_prob_output_name: str,
    posterior_dist: Optional[str] = None,
    day: Optional[int] = 6,
    hour: Optional[int] = 0,
    overwrite: Optional[bool] = False,
    num_jobs: Optional[int] = 1,
    num_threads: Optional[int] = 1,
    windowed: Optional[bool] = False,
    output_raster: Optional[bool] = False,
    quiet: Optional[bool] = True,
    log_file: Optional[str] = None,
    output_vector: Optional[bool] = True,
):
    """
    Driver for running probabilistic inundation on selected HUCs

    Parameters
    ----------
    ensembles: str
        Location of nws ensemble NetCDF file
    parameters: str
        Location of parameter parquet file
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    outputs_dir: str
        Directory to write output files
    hucs: list
        HUCs to process probabilistic inundation for
    mosaic_prob_output_name: str
        Name of final mosaiced probabilistic FIM
    posterior_dist: Optional[str], default = None
        Name of posterior df
    day: Optional[int], default = 6
        Days ahead to pick from reference forecast time
    hour: Optional[int], default = 0,
        Hours ahead to pick from reference forecast time
    overwrite: Optional[bool], default = False
        Whether to overwrite existing output
    num_jobs: Optional[int], default = 1
        Number of processes to parallelize over
    num_threads: Optional[int], default = 1
        Number of threads to parallelize over
    windowed: Optional[bool], default = False
        Whether to run inundation in windowed mode for memory conservation
    output_raster: Optional[bool], default = False
        Whether to keep the output raster output
    quiet: Optional[bool], default = False
        Whether to be verbose or not
    log_file: Optional[str], default = None
        Filepath of log file
    output_vector: Optional[bool], default = True
        Whether to create vector output

    """

    parameters_df = pd.read_parquet(parameters)

    if posterior_dist is not None:
        posterior_df = pd.read_parquet(posterior_dist)
    else:
        posterior_df = None

    with xr.open_dataset(ensembles) as ensembles_ds:
        for huc in hucs:
            inundate_probabilistic(
                ensembles=ensembles_ds,
                parameters=parameters_df,
                hydrofabric_dir=hydrofabric_dir,
                outputs_dir=outputs_dir,
                huc=huc,
                mosaic_prob_output_name=f"{mosaic_prob_output_name[:mosaic_prob_output_name.rfind('.')]}_{huc}.gpkg",
                posterior_dist=posterior_df,
                day=day,
                hour=hour,
                overwrite=overwrite,
                num_jobs=num_jobs,
                num_threads=num_threads,
                windowed=windowed,
                output_raster=output_raster,
                quiet=quiet,
                log_file=log_file,
                output_vector=output_vector,
            )


if __name__ == '__main__':
    """
    Example Usage:

    python ./probabilistic_inundation.py
        -e ./gfs_ensembles_03070107.nc
        -p ./plink_recurr.csv
        -hd /data/previous_fim/hand_4_5_11_1/
        -od /outputs/probabilistic_test
        -hc 03070107
        -f ./example2/mosaic_prob
        -j 1
        -t 1
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
        help="OPTIONAL: Path to posterior distribution configuration file",
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
        "--overwrite",
        action='store_true',
        help="OPTIONAL: Whether to overwrite existing output",
        required=False,
    )

    parser.add_argument(
        "-r",
        "--output_raster",
        help="OPTIONAL: Whether to keep final raster output",
        action='store_true',
        required=False,
    )

    parser.add_argument(
        "-v",
        "--output_vector",
        help="OPTIONAL: Whether to create final vector output",
        action='store_true',
        required=False,
    )

    parser.add_argument("-q", "--quiet", action='store_true', help="OPTIONAL: Whether to be verbose or not")

    parser.add_argument(
        "-j", "--num_jobs", default=1, type=int, help="REQUIRED: Number of jobs to process HUCs"
    )

    parser.add_argument(
        "-t", "--num_threads", default=1, type=int, help="REQUIRED: Number of threads to process HUCs"
    )

    parser.add_argument(
        "-w",
        "--windowed",
        action='store_true',
        help="OPTIONAL: Whether to run inundation in windowed mode for memory conservation ",
        required=False,
    )

    parser.add_argument("-l", "--log_file", type=str, help="OPTIONAL: Filepath for log file", required=False)

    args = vars(parser.parse_args())

    inundate_hucs(**args)
