import argparse
import ast
import os
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
from rasterio import features as riofeat
from scipy.stats import expon, gamma, genextreme, genpareto, gumbel_r, kappa4, norm, pearson3, weibull_min
from shapely.geometry import shape
from tqdm.auto import tqdm

from utils.io import write_geodataframe
from utils.shared_functions import s3_or_local_glob, s3_or_local_path_exists, is_local_path, use_pandas_3_behavior


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


def generate_streamflow_percentiles_vec(
        ensemble_streamflow, params_weibull, percentiles
):
    """Vectorize the computation of weibull distribution"""
    feature_ids = ensemble_streamflow.indexes['feature_id']
    perc_df = pd.DataFrame(columns=percentiles, index=feature_ids.astype('string[pyarrow]'), dtype=float)

    # For features that have no params, copy first ensemble streamflow
    weibull_nomask = ~perc_df.index.isin(params_weibull.index.astype('string[pyarrow]'))
    perc_df.loc[weibull_nomask] = ensemble_streamflow.sel(feature_id=feature_ids[weibull_nomask], ensemble="1").to_numpy()[:, np.newaxis]

    inter_ids = feature_ids.intersection(params_weibull.index.astype(feature_ids.dtype))
    ensemble_subset = ensemble_streamflow.sel(feature_id=inter_ids)
    # weibull_subset = params_weibull.loc[inter_ids]
    inter_ids = inter_ids.astype('string[pyarrow]')

    # wv = weibull_min(c=weibull_subset['param.c'].to_numpy(),
    #                  loc=weibull_subset['param.loc'].to_numpy(),
    #                  scale=weibull_subset['param.scale'].to_numpy())

    # If all values from ensemble streamflow forecasts are not identical or virtually the same
    if not np.allclose(ensemble_subset, ensemble_subset[0, 0]):

        # Impute any values that are nan with the mean of the numeric values
        ensemble_subset = ensemble_subset.fillna(ensemble_subset.mean(dim='ensemble'))
        # likelihoods = wv.sf(ensemble_subset)

        # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
        # scaled_likelihoods = np.squeeze(likelihoods / np.sum(likelihoods)) * np.linspace(1, 0.9, 6)

        # minlik = scaled_likelihoods.min(axis=1)
        # maxlik = scaled_likelihoods.max(axis=1)

        # Interpolate streamflow values so that member 1 represents the 50th percentile
        # top = np.interp([10, 25, 50], [10, 50], [minlik, scaled_likelihoods[0]])[::-1]

        # top_scaled = np.interp(
        #     top,
        #     [minlik, scaled_likelihoods[0]],
        #     [np.max(ensemble_subset), ensemble_subset[0]],
        # )
        top_scaled = np.interp(
            [10, 25, 50], 
            [10, 50], 
            [ensemble_subset.max(), ensemble_subset[0, 0]]
        )[::-1]

        # bottom = np.interp([50, 75, 90], [50, 90], [scaled_likelihoods[0], maxlik])[::-1]
        # bottom_scaled = np.interp(
        #     bottom,
        #     [scaled_likelihoods[0], maxlik],
        #     [ensemble_subset[0], np.min(ensemble_subset)],
        # )

        bottom_scaled = np.interp(
            [50, 75, 90],
            [50, 90],
            [ensemble_subset[0, 0], ensemble_subset.min()],
        )[::-1]

        percentile_values = np.hstack([bottom_scaled, top_scaled[1:]])
        np.maximum(0, percentile_values, out=percentile_values)
        perc_df.loc[inter_ids] = percentile_values
    else:
        perc_df.loc[inter_ids] = max(0, ensemble_subset[0,0])

    return perc_df
    

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

    # If all values from ensemble streamflow forecasts are not identical or virtually the same
    if not np.allclose(streamflow_values, streamflow_values[0]):

        # Impute any values that are nan with the mean of the numeric values
        streamflow_values[np.isnan(streamflow_values)] = np.nanmean(streamflow_values)
        likelihoods = 1 - r.cdf(streamflow_values)

        # Scale the likelihoods to equal 1 and then generate a dataset given their likelihood
        scaled_likelihoods = np.squeeze(likelihoods / np.sum(likelihoods)) * np.linspace(1, 0.9, 6) * 10000

        # Interpolate streamflow values so that member 1 represents the 50th percentile
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


# TODO: Replace this code with future LoFI Optimization
# def analyze_nonmonotonic_src(srcs_df):
#     """
#     Check for any non-monotonically increasing discharge and enforce monotonicity.

#     Parameters
#     ----------
#     srcs_df : pd.DataFrame
#         Original synthetic rating curve DataFrame.

#     Returns
#     -------
#     pd.DataFrame
#         Synthetic rating curve DataFrame equal to original or adjusted for discharge monotonicity.

#     """

#     srcs_df.loc[srcs_df['Stage'] == 0, 'Discharge (m3s-1)_subdiv'] = 0

#     cond_chan = srcs_df['bankfull_proxy'] == 'channel'
#     srcs_df_chan = srcs_df[cond_chan]
#     non_monotonic_index = srcs_df_chan.index[srcs_df_chan['Discharge (m3s-1)_subdiv'].diff().lt(0)].tolist()

#     # Recalculate 'Discharge' values before the last non-monotonic row
#     # Note: No change has been applied on WetArea, Volume, LENGTHKM
#     if non_monotonic_index:
#         # Get the target values from the last non-monotonic index
#         target_idx = non_monotonic_index[-1]
#         target_numCells = srcs_df.loc[target_idx, 'Number of Cells']
#         target_SurfaceArea = srcs_df.loc[target_idx, 'SurfaceArea (m2)']
#         target_BedArea = srcs_df.loc[target_idx, 'BedArea (m2)']

#         # Define the slice (up to but not including target_idx)
#         row_slice = slice(0, target_idx)

#         # Assign target values to the selected rows
#         srcs_df.loc[row_slice, 'Number of Cells'] = target_numCells
#         srcs_df.loc[row_slice, 'SurfaceArea (m2)'] = target_SurfaceArea
#         srcs_df.loc[row_slice, 'BedArea (m2)'] = target_BedArea

#         # Recalculate discharge variables
#         length_km = srcs_df.loc[row_slice, 'LENGTHKM']
#         # Avoid division by zero
#         length_km = length_km.replace(0, np.nan)

#         target_TopWidth = target_SurfaceArea / length_km / 1000
#         target_WettedPerimeter = target_BedArea / length_km / 1000

#         wet_area = srcs_df.loc[row_slice, 'WetArea (m2)']
#         target_HydraulicRadius = wet_area / target_WettedPerimeter

#         srcs_df.loc[row_slice, 'TopWidth (m)'] = target_TopWidth
#         srcs_df.loc[row_slice, 'WettedPerimeter (m)'] = target_WettedPerimeter
#         srcs_df.loc[row_slice, 'HydraulicRadius (m)'] = target_HydraulicRadius
#         srcs_df['HydraulicRadius (m)'] = srcs_df['HydraulicRadius (m)'].fillna(0)

#         # Recalculate Discharge (m3s-1) for the selected rows
#         srcs_df.loc[row_slice, 'Discharge (m3s-1)_subdiv'] = (
#             wet_area
#             * (srcs_df.loc[row_slice, 'HydraulicRadius (m)'] ** (2.0 / 3))
#             * pow(
#                 np.maximum(srcs_df.loc[row_slice, 'SLOPE'], np.repeat(1e-5, srcs_df.loc[row_slice].shape[0])),
#                 0.5,
#             )
#             / srcs_df['channel_n']
#         )

#     return srcs_df


@use_pandas_3_behavior()
def compute_manning_subdivision(df_src, eps=1e-5):
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
    vslope_main = df_src['SLOPE'].to_numpy()
    vq_orig = df_src['Discharge (m3s-1)'].to_numpy()
    vchann = df_src['channel_n'].to_numpy()
    vobn = df_src['overbank_n'].to_numpy()

    # The memory buffers in the following code are allocated and managed very carefully.
    # Please understand how memory is allocated and used before making *any* changes.
    # References to arrays are cleared when they are no longer needed in order
    # to keep each array referenced by only 1 reference.
    lengthm = vlengthkm * 1000
    mask = vstage <= vstage_bf
    delta_stage = vstage - vstage_bf

    vol_chan = delta_stage * vsurf_area_bf
    np.add(vol_chan, vvol_bf, out=vol_chan)  # Estimated channel volume
    np.minimum(vol_chan, vvol, out=vol_chan)  # ensure that estimated doesn't exceed actual volume
    np.copyto(vol_chan, vvol, where=mask)  # Use actual volume where stage is below bankfull

    # Compute volume overbank
    vol_obank = vvol - vol_chan
    np.maximum(vol_obank, 0.0, out=vol_obank)  # Ensure that vol_obank is always positive
    np.putmask(vol_obank, mask, 0.0)  # Set overbank to 0 where stage doesn't exceed bankfull

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
    np.power(hydraulicrad_chan, 2 / 3, out=hydraulicrad_chan)

    # Compute channel discharge
    q_chan = np.multiply(wetarea_chan, hydraulicrad_chan, out=wetarea_chan)
    del wetarea_chan

    slope = np.maximum(vslope_main, eps, out=hydraulicrad_chan)
    np.sqrt(slope, out=slope)
    del hydraulicrad_chan

    np.multiply(q_chan, slope, out=q_chan)
    np.divide(q_chan, vchann, out=q_chan)

    wetarea_obank = np.divide(vol_obank, lengthm, out=vol_obank)
    del vol_obank

    wettedperim_obank = np.divide(bedarea_obank, lengthm, out=bedarea_obank)
    np.maximum(wettedperim_obank, eps, out=wettedperim_obank)
    del bedarea_obank

    hydraulicrad_obank = np.divide(wetarea_obank, wettedperim_obank, out=wettedperim_obank)
    np.maximum(hydraulicrad_obank, 0.0, out=hydraulicrad_obank)
    np.power(hydraulicrad_obank, 2 / 3, out=hydraulicrad_obank)

    q_obank = np.multiply(wetarea_obank, hydraulicrad_obank, out=wetarea_obank)
    del wetarea_obank, hydraulicrad_obank

    np.maximum(vslope_main, eps, out=slope)
    np.sqrt(slope, out=slope)

    np.multiply(q_obank, slope, out=q_obank)
    np.divide(q_obank, vobn, out=q_obank)
    del slope

    # Compute total discharge
    q_total = np.add(q_chan, q_obank, out=q_chan)
    del q_chan, q_obank
    np.equal(vstage, 0, out=mask)
    np.putmask(q_total, mask, 0.0)

    subdiv_applied = np.isnan(vstage_bf, out=mask)
    np.copyto(q_total, vq_orig, where=subdiv_applied)
    np.logical_not(subdiv_applied, out=subdiv_applied)
    return subdiv_applied, q_total


@use_pandas_3_behavior()
def read_crosswalk(hydrofabric_dir, huc, branch):
    read_cols = [
        'Stage',
        'Stage_bankfull',
        'Volume (m3)',
        'Volume_bankfull',
        'SurfArea_bankfull',
        'BedArea (m2)',
        'BedArea_bankfull',
        'LENGTHKM',
        'SLOPE',
        'channel_n',
        'overbank_n',
        'Bathymetry_source',
        'HydroID',
        'Discharge (m3s-1)',
    ]
    path = os.path.join(hydrofabric_dir, huc, 'branches', branch, f"src_full_crosswalked_{branch}.csv")
    df_src = pd.read_csv(path, engine='pyarrow', usecols=read_cols, dtype={'HydroID': 'string[pyarrow]'})
    return df_src


@use_pandas_3_behavior()
def get_subdivided_src(crosswalk, hydrotable):
    """
    Method for subdividing a synthetic rating curve based on the high water threshold

    Parameters
    ----------
    crosswalk: pd.DataFrame
        Crosswalk dataframe
    hydrotable: pd.DataFrame
        Hydrotable dataframe
    """
    _, final_discharge = compute_manning_subdivision(crosswalk)

    # We copy because we want to release df_src afterward
    df_computed = pd.DataFrame(
        {
            'HydroID': crosswalk['HydroID'],
            'stage': crosswalk['Stage'],
            #'subdiv_discharge_cms': final_discharge,
            'discharge_cms': final_discharge,  # create a copy of vmann modified discharge (used to track future changes)
        },
        copy=False,
    )
    df_computed = df_computed.set_index(["HydroID", "stage"])
    return df_computed

    df_htable = hydrotable.merge(
        df_computed, how='left', left_on=['HydroID', 'stage'], right_on=['HydroID', 'stage']
    )
    df_htable = df_htable.set_index(['HydroID', 'stage'])
    df_htable['precalb_discharge_cms'] = 0
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

    params_weibull = parameters.loc[parameters['distribution_name'] == 'weibull_min']
    params_weibull = params_weibull.set_index('feature_id')

    # Masks for HUC Domain
    mask_path = os.path.join(hydrofabric_dir, huc, 'wbd.gpkg')

    # Percentiles and data to add
    #percentiles = {'90': 10, '75': 25, '50': 50, '25': 75, '10': 90}
    percentiles = (90, 75, 50, 25, 10)

    # Generate streamflow likelihoods for each feature
    percentile_values = generate_streamflow_percentiles_vec(ensembles["streamflow"], params_weibull, percentiles)

    magnitude = ensembles.attrs['magnitude'] if 'magnitude' in ensembles.attrs else None

    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=int(huc), magnitude=magnitude
    )

    # Make directories if they do not exist
    output_file_name = os.path.basename(mosaic_prob_output_name)
    base_output_path = os.path.join(outputs_dir, huc)

    # Create directory if it does not exist
    if is_local_path(base_output_path):
        os.makedirs(base_output_path, exist_ok=True)

    # Find the original hydrotable
    all_branches = s3_or_local_glob(os.path.join(hydrofabric_dir, huc, "branches", "*"))
    all_branches = list(map(os.path.basename, all_branches))

    htable_cols = ['HydroID', 'feature_id', 'HUC', 'branch_id', 'stage', 'SurfaceArea (m2)', 'LakeID']
    df_htable = pd.read_parquet(
        os.path.join(hydrofabric_dir, huc, "hydrotable.parquet"),
        engine='pyarrow',
        columns=htable_cols
    )
    df_htable = df_htable.reset_index()
    df_htable = df_htable.astype({'HUC': "string[pyarrow]", 'HydroID': 'string[pyarrow]', 'feature_id': "string[pyarrow]"})
    df_htable["precalb_discharge_cms"] = 0

    adj_cols = ['channel_n', 'overbank_n', 'SLOPE']
    crosswalk_static_cols = ['HydroID', 'Stage', 'Bathymetry_source']

    # Apply inundation map to each percentile
    branch_percentile_df = []
    for branch, htable_branch in df_htable.groupby("branch_id", as_index=False):
        crosswalk = read_crosswalk(hydrofabric_dir, huc, str(branch))

        # Copy the channel_n, overbank_n, and SLOPE values
        adj_copies = crosswalk[adj_cols].copy()

        # Collect all the subdivided hydrotables
        h_tables = []
        for percentile in percentiles:
            if percentile == 50:
                crosswalk[adj_cols] = adj_copies
            else:
                channel_n_adj = channel_dist.ppf(1 - percentile / 100)
                overbank_n_adj = obank_dist.ppf(1 - percentile / 100)
                slope_adj = slope_dist.ppf(percentile / 100)
                # Adjust the channel, overbank, and slope parameters
                crosswalk[adj_cols] = adj_copies + [channel_n_adj, overbank_n_adj, slope_adj]

            h_table = get_subdivided_src(crosswalk, htable_branch)
            h_table = h_table.rename(columns={n: f"{n}.{percentile}" for n in h_table.columns if n.startswith("discharge_cms")})
            h_tables.append(h_table)
        p_table = pd.concat(h_tables, axis=1)
        del h_tables
        branch_percentile_df.append(p_table)

        # flow_df = pd.DataFrame(
        #     {"feature_id": percentile_values['feature_id'], "discharge": percentile_values[percentile]}
        # )
    htable_req_static_cols = [
        "branch_id",
        "feature_id",
        "HydroID",
        "stage",
        "HUC",
        "LakeID",
        "precalb_discharge_cms"
    ]

    inundation_paths = []
    full_p_table = df_htable.merge(pd.concat(branch_percentile_df), how='left', left_on=["HydroID", "stage"], right_index=True)
    del branch_percentile_df
    for percentile in percentiles:
        # Establish directory to save the final mosaiced inundation
        final_inundation_path = os.path.join(
            base_output_path, f'extent_{percentile}_v10_day{day}_hour{hour}.tif'
        )
        inundation_paths.append(final_inundation_path)

        # Skip if the file exists
        if not overwrite and s3_or_local_path_exists(final_inundation_path):
            continue

        pcol = f"discharge_cms.{percentile}"
        subhdf = full_p_table[htable_req_static_cols + [pcol]]
        subhdf = subhdf.rename(columns={pcol: "discharge"})

        flow_df = percentile_values[percentile].to_frame()

        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_df,
            hydro_table_df=subhdf,
            inundation_raster=final_inundation_path,
            mask=mask_path,
            verbose=not quiet,
            num_workers=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            log_file=log_file,
        )

    # For every percentile inundation map convert values to percentile
    with ExitStack() as stack:
        datasets = [stack.enter_context(rasterio.open(file)) for file in inundation_paths]
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
            write_geodataframe(gdf, out_vec)

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
