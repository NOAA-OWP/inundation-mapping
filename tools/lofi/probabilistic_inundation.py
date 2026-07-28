import argparse
import ast
import os
import shutil
import warnings
from concurrent.futures import as_completed
from contextlib import ExitStack
from typing import Dict, Optional, Tuple, Union

import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from scipy.interpolate import PchipInterpolator
from scipy.stats import (
    expon,
    gamma,
    genextreme,
    genpareto,
    gumbel_r,
    kappa4,
    norm,
    pearson3,
    rv_continuous,
    weibull_min,
)
from shapely.geometry import shape
from tqdm import tqdm

from utils.shared_functions import s3_or_local_glob, use_pandas_3_behavior


def get_fim_probability_distributions(
    posterior_dist: Optional[Union[str, pd.DataFrame]] = None, huc: Optional[int] = None
) -> Tuple[weibull_min, weibull_min, weibull_min]:
    """
    Gets either bayesian updated distributions or default distributions for respective huc

    Parameters
    ---------
    posterior_dist : Optional[Union[str, pd.DataFrame]], default = None
        Name of csv file that has posterior distribution parameters
    huc: Optional[int], default = None
        Huc to get distribution for if posterior_dist is not None

    Returns
    -------
    Tuple[weibull_min, wibull_min, weibull_min]
        Weibull distributions for channel Manning roughness, overbank Manning roughness, and slope adjustment

    """

    if posterior_dist is None:
        # Default weibull likelihood for channel manning roughness
        channel_dist = weibull_min(c=1.5, scale=0.0367, loc=0.032)

        # Default weibull likelihood for overbank manning roughness
        obank_dist = weibull_min(c=2, scale=0.035, loc=0.09)

        # Default weibull likelihood for slope adjustment
        slope_dist = weibull_min(c=4, scale=0.95 / 10, loc=-0.0867)

    else:
        variables = ['channel_manning_roughness', 'overbank_manning_roughness', 'slope_adjustment']
        dist_params = ['c', 'scale', 'loc']

        if isinstance(posterior_dist, str):
            posterior_df = pd.read_csv(posterior_dist)
        else:
            posterior_df = posterior_dist

        if huc is not None and 'huc' in posterior_df.columns:
            posterior_df = posterior_df[posterior_df['huc'] == huc]

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
        rv['feature_id'] = feature
        return rv
    else:
        parameters = params_weibull.loc[feature]

    # Create probability distribution
    params = ast.literal_eval(parameters['parameters'])

    try:
        r = dist_dict[parameters['distribution_name']](**params)

    except Exception:
        rv = dict.fromkeys(dkeys, float(ensemble_forecast.sel({'ensemble': '1'})['streamflow']))
        rv['feature_id'] = feature
        return rv

    streamflow_values = ensemble_forecast['streamflow'].values
    likelihoods = 1 - r.cdf(streamflow_values)

    # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
    scaled_likelihoods = np.squeeze(likelihoods / np.sum(likelihoods)) * np.linspace(1, 0.9, 6) * 10000

    # Create data to fit truncated exponential distribution
    ef_values = np.where(np.isnan(streamflow_values), 0, streamflow_values)
    sl_values = np.where(np.isnan(scaled_likelihoods), 1, scaled_likelihoods).astype(int)
    streamflow_expon_values = np.repeat(ef_values.ravel(), sl_values.ravel())

    # Check to see if all values are the same, if so grab the first, otherwise get their point percent functions
    if not np.allclose(streamflow_expon_values, streamflow_expon_values[0]):
        streamflow_list = [(value, index) for index, value in enumerate(np.squeeze(ef_values))]
        streamflow_list.sort()
        x_points = np.squeeze([item[0] for item in streamflow_list])
        x_indices = [item[1] for item in streamflow_list]
        cumsum = np.cumsum(scaled_likelihoods[x_indices] / 1e4)
        cdf_points = np.interp(cumsum, [np.min(cumsum), np.max(cumsum)], [0.05, 0.95])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            coefficientsx = np.polyfit(x_points, cdf_points, 2)
            coefficientsy = np.polyfit(cdf_points, x_points, 2)

        polynomial_functionx = np.poly1d(coefficientsx)
        polynomial_functiony = np.poly1d(coefficientsy)
        x_fitx = np.linspace(min(x_points), max(x_points), 100)  # Generate more points for a smooth curve
        y_fity = polynomial_functionx(x_fitx)

        y_fitx = np.linspace(min(cdf_points), max(cdf_points), 100)  # Generate more points for a smooth curve
        x_fity = polynomial_functiony(y_fitx)

        custom_cdf_func = PchipInterpolator(x_fitx, y_fity, extrapolate=True)
        custom_ppf_func = PchipInterpolator(y_fitx, x_fity, extrapolate=True)

        class CustomInterpDist(rv_continuous):
            def _cdf(self, x):
                return custom_cdf_func(x)

            def _ppf(self, q):
                return custom_ppf_func(q)

        custom_dist = CustomInterpDist(a=min(x_points), b=max(x_points), name="CustomInterpDist")

        return {
            '90': max(0, custom_dist.ppf(0.1)),
            '75': max(0, custom_dist.ppf(0.25)),
            '50': max(0, custom_dist.ppf(0.5)),
            '25': max(0, custom_dist.ppf(0.75)),
            '10': max(0, custom_dist.ppf(0.9)),
            'feature_id': feature,
        }

    else:
        rv = dict.fromkeys(dkeys, max(0, streamflow_expon_values[0]))
        rv['feature_id'] = feature
        return rv


@use_pandas_3_behavior()
def compute_manning_subdivision(df_src, channel_manning, overbank_manning, slope_adj, eps=1e-5):
    # Extract columns as numpy arrays. Ordering must not change during computation
    # The following variables should be views (ie memory not owned by this function)
    vstage = df_src['Stage'].to_numpy()
    vstage_bf = df_src['Stage_bankfull'].to_numpy()
    vvol = df_src['Volume (m3)'].to_numpy()
    vvol_bf = df_src['Volume_bankfull'].to_numpy()
    vsurf_area_bf = df_src['SurfArea_bankfull'].to_numpy()
    vbedarea = df_src['BedArea (m2)'].to_numpy()
    vbedarea_bf = df_src['BedArea_bankfull'].to_numpy()
    vlengthkm = df_src['LENGTHKM'].to_numpy()
    vslope_rise = df_src['SLOPE_RISE_RUN'].to_numpy()
    vslope_main = df_src['SLOPE'].to_numpy()
    vq_orig = df_src['Discharge (m3s-1)'].to_numpy()

    # The memory buffers in the following code are allocated and managed very carefully.
    # Please understand how memory is allocated and used before making *any* changes.
    # References to arrays are cleared when they are no longer needed in order
    # to keep each array referenced by only 1 reference.
    lengthm = vlengthkm * 1000
    mask = vstage <= vstage_bf
    delta_stage = vstage - vstage_bf

    vol_chan = delta_stage * vsurf_area_bf
    np.add(vol_chan, vvol_bf, out=vol_chan) # Estimated channel volume
    np.minimum(vol_chan, vvol, out=vol_chan) # ensure that estimated doesn't exceed actual volume
    np.copyto(vol_chan, vvol, where=mask) # Use actual volume where stage is below bankfull

    # Compute volume overbank
    vol_obank = vvol - vol_chan
    np.maximum(vol_obank, 0.0, out=vol_obank) # Ensure that vol_obank is always positive
    np.putmask(vol_obank, mask, 0.0) # Set overbank to 0 where stage doesn't exceed bankfull

    wetarea_chan = np.divide(vol_chan, lengthm, out=vol_chan)
    del vol_chan

    # Compute channel bedarea
    bedarea_chan = np.where(mask, vbedarea, vbedarea_bf)
    np.minimum(bedarea_chan, vbedarea_bf, out=bedarea_chan, where=mask)

    bedarea_obank = vbedarea - bedarea_chan
    np.maximum(bedarea_obank, 0.0, out=bedarea_obank)
    np.putmask(bedarea_obank, mask, 0.0)

    wettedperim_chan = bedarea_chan / lengthm
    np.multiply(delta_stage, 2, out=delta_stage)
    np.add(wettedperim_chan, delta_stage, out=wettedperim_chan, where=mask)
    del delta_stage, bedarea_chan

    np.maximum(wettedperim_chan, eps, out=wettedperim_chan)
    hydraulicrad_chan = np.divide(wetarea_chan, wettedperim_chan, out=wettedperim_chan)
    del wettedperim_chan

    hydraulicrad_chan = np.maximum(hydraulicrad_chan, 0.0, out=hydraulicrad_chan)
    np.power(hydraulicrad_chan, 2/3, out=hydraulicrad_chan)

    # Compute channel discharge
    q_chan = np.multiply(wetarea_chan, hydraulicrad_chan, out=wetarea_chan)
    del wetarea_chan

    slope = np.add(vslope_rise, slope_adj, out=hydraulicrad_chan)
    np.maximum(slope, eps, out=slope)
    np.sqrt(slope, out=slope)
    del hydraulicrad_chan

    np.multiply(q_chan, slope, out=q_chan)
    np.multiply(q_chan, 1/np.float64(channel_manning), out=q_chan)

    wetarea_obank = np.divide(vol_obank, lengthm, out=vol_obank)
    del vol_obank

    wettedperim_obank = np.divide(bedarea_obank, lengthm, out=bedarea_obank)
    np.maximum(wettedperim_obank, eps, out=wettedperim_obank)
    del bedarea_obank

    hydraulicrad_obank = np.divide(wetarea_obank, wettedperim_obank, out=wettedperim_obank)
    np.maximum(hydraulicrad_obank, 0.0, out=hydraulicrad_obank)
    np.power(hydraulicrad_obank, 2/3, out=hydraulicrad_obank)

    q_obank = np.multiply(wetarea_obank, hydraulicrad_obank, out=wetarea_obank)
    del wetarea_obank, hydraulicrad_obank

    np.add(vslope_main, slope_adj, out=slope)
    np.maximum(slope, eps, out=slope)
    np.sqrt(slope, out=slope)

    np.multiply(q_obank, slope, out=q_obank)
    np.multiply(q_obank, 1/np.float64(overbank_manning), out=q_obank)
    del slope

    # Compute total discharge
    q_total = np.add(q_chan, q_obank, out=q_chan)
    del q_chan, q_obank
    np.equal(vstage, 0, out=mask)
    np.putmask(q_total, mask, 0.0)

    subdiv_applied = np.isnan(vstage_bf, out=mask)
    np.putmask(q_total, subdiv_applied, vq_orig)
    np.logical_not(subdiv_applied, out=subdiv_applied)
    return subdiv_applied, q_total


@use_pandas_3_behavior()
def read_crosswalk(hydrofabric_dir, huc, branch):
    read_cols = [
            'Stage', 'Stage_bankfull', 'Volume (m3)', 'Volume_bankfull',
            'SurfArea_bankfull', 'BedArea (m2)', 'BedArea_bankfull',
            'LENGTHKM', 'SLOPE_RISE_RUN', 'SLOPE', 'Bathymetry_source',
            'HydroID', 'Discharge (m3s-1)'
    ]
    path = os.path.join(hydrofabric_dir, huc, 'branches', branch, f"src_full_crosswalked_{branch}.csv")
    df_src = pd.read_csv(path, engine='pyarrow', usecols=read_cols)
    return df_src


@use_pandas_3_behavior()
def get_computed_subdivisions(df_src, channel_manning, overbank_manning, slope_adj):
    subdiv_applied, final_discharge = compute_manning_subdivision(df_src, channel_manning, overbank_manning, slope_adj)

    # We copy because we want to release df_src afterward
    df_computed = pd.DataFrame({
            'HydroID': df_src['HydroID'],
            'stage': df_src['Stage'],
            'Bathymetry_source': df_src['Bathymetry_source'],
            'subdiv_applied': subdiv_applied,
            'channel_n': channel_manning,
            'overbank_n': overbank_manning,
            'subdiv_discharge_cms': final_discharge,
            'discharge_cms': final_discharge  # create a copy of vmann modified discharge (used to track future changes)
        }, copy=False)

    return df_computed


@use_pandas_3_behavior()
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
    htable_directory: str
        Directory to synthetic rating curves
    htable_output: str
        To get synthetic rating curve

    """
    df_src = read_crosswalk(hydrofabric_dir, huc, branch)
    df_computed = get_computed_subdivisions(df_src, channel_manning, overbank_manning, slope_adj)
    del df_src
    
    # drop the previously modified discharge column to be replaced with updated version
    path = os.path.join(hydrofabric_dir, huc, "hydrotable.parquet")

    htable_cols = ['HydroID', 'feature_id', 
                   'HUC', 'branch_id', 'stage',
                   'SurfaceArea (m2)', 'LakeID']
    df_htable = pd.read_parquet(path, engine='pyarrow', filters=[('branch_id', '==', int(branch))],
                               columns=htable_cols)
    df_htable = df_htable.reset_index()
    df_htable = df_htable.astype({'HUC': "string[pyarrow]", 'HydroID': int, 'feature_id': "string[pyarrow]"})

    df_htable = df_htable.merge(
        df_computed, how='left', left_on=['HydroID', 'stage'], right_on=['HydroID', 'stage']
    )

    df_htable['branch_id'] = int(branch)
    df_htable['LakeID'] = -999
    df_htable['precalb_discharge_cms'] = 0

    output_table = os.path.join(htable_directory, htable_output.format(branch))
    df_htable.to_feather(output_table)
    return output_table


def inundate_probabilistic(
    ensembles: str,
    parameters: str,
    hydrofabric_dir: str,
    outputs_dir: str,
    huc: str,
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
    Method to probabilistically inundate based on provided ensembles

    Parameters
    ----------
    ensembles: str
        Path to load medium range ensembles
    parameters: str
        Path to load fit parameters to distributions
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    outputs_dir: str
        Directory to write output files
    huc: str
        Huc to process probabilistic FIM
    mosaic_prob_output_name: str
        Name of final mosaiced probabilistic FIM
    posterior_dist: str = None
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

    # Load datasets
    ensembles = xr.open_dataset(ensembles, engine="h5netcdf")

    parameters_df = pd.read_parquet(parameters)
    params_weibull = parameters_df.loc[parameters_df['distribution_name'] == 'weibull_min']
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

    ensembles.close()
    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=huc
    )

    # Make directories if they do not exist
    output_file_name = os.path.basename(mosaic_prob_output_name)
    base_output_path = os.path.join(fim_outputs_dir, str(huc))
    src_output_path = os.path.join(base_output_path, 'srcs')
    htable_output_path = src_output_path
    flow_path = os.path.join(base_output_path, 'flows')

    # Create directories if they do not exist
    os.makedirs(base_output_path, exist_ok=True)
    os.makedirs(src_output_path, exist_ok=True)
    os.makedirs(flow_path, exist_ok=True)

    # Find the original hydrotable
    all_branches = s3_or_local_glob(os.path.join(hydrofabric_dir, huc, "branches", "*"))
    all_branches = list(map(os.path.basename, all_branches))

    # Apply inundation map to each percentile
    for percentile, val in percentiles.items():
        channel_n = channel_dist.ppf(1 - int(percentile) / 100)
        overbank_n = obank_dist.ppf(1 - int(percentile) / 100)
        slope_adj = slope_dist.ppf(int(percentile) / 100)

        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(
            base_output_path, f'extent_{percentile}_v10_day{day}_hour{hour}.tif'
        )

        # Skip if the file exists
        if os.path.exists(final_inundation_path) and not overwrite:
            continue

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

        flow_file = os.path.join(flow_path, f'{huc}_{percentile}_flow.csv')

        df = pd.DataFrame(
            {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[percentile]}
        )
        df.to_csv(flow_file, index=False)

        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_file,
            hydro_table_df=os.path.join(htable_output_path, htable_output_file),
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
        raster_crs = datasets[0].crs
        nodata = profile['nodata']
        profile.update(dtype=np.int8, nodata=127, tiled=True, compress=profile.get('compress', 'DEFLATE'))

        out_rast = os.path.join(base_output_path, output_file_name.replace(".gpkg", ".tif"))
        with rasterio.open(out_rast, "w+", **profile) as write_rst:
            for window in windows:
                arrays = []
                for d, p in zip(datasets, percentiles):
                    data = d.read(1, window=window)
                    nodata_mask = data == nodata
                    data = np.where(data > 0, int(p), 0)
                    data[nodata_mask] = -10000
                    arrays.append(data)

                merged = np.max(arrays, axis=0)
                merged[merged == -10000] = 127
                write_rst.write(merged, window=window, indexes=1)

    if output_vector is True:

        out_vec = os.path.join(base_output_path, output_file_name.replace(".tif", ".gpkg"))

        def _make_geometry(shapes):
            for p, v in shapes:
                yield shape(p), v

        with rasterio.open(out_rast, 'r') as rst:
            shapes = rasterio.features.shapes(rst.read(1), mask=None, transform=rst.transform)
            gdf = gpd.GeoDataFrame(_make_geometry(shapes), columns=['geometry', 'value'], crs=raster_crs)
            gdf = gdf.set_geometry('geometry')
            gdf.to_file(out_vec)

    for file in percentile_files:
        os.remove(file)

    if output_raster is False:
        os.remove(out_rast)

    # Remove SRC path and flow path
    shutil.rmtree(src_output_path)
    shutil.rmtree(flow_path)


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
