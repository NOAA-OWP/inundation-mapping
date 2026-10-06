import argparse
import os
import time
from contextlib import ExitStack
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from rasterio import features as riofeat
from rasterio import windows as riowin
from scipy.interpolate import make_interp_spline
from scipy.stats import weibull_min
from shapely.geometry import shape

from utils.io import write_geodataframe
from utils.shared_functions import is_local_path, s3_or_local_path_exists, use_pandas_3_behavior


@use_pandas_3_behavior()
def get_fim_probability_distributions(
    posterior_dist: Optional[pd.DataFrame] = None, huc: Optional[int] = None, magnitude: Optional[int] = 2
) -> tuple[weibull_min, weibull_min, weibull_min]:
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
    tuple[weibull_min, wibull_min, weibull_min]
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


@use_pandas_3_behavior()
def generate_streamflow_percentiles(ensemble_streamflow, params_weibull, percentiles):
    """Vectorize the computation of weibull distribution

    Parameters
    ---------
    ensemble_streamflow: xr.DataArray
        Ensemble streamflow values
    params_weibull: pd.DataFrame
        Distribution parameters.
    percentiles: Sequence
        Percentiles to compute such as [90, 50, 10]

    Returns
    -------
    pd.DataFrame
        Computed percentile values
    """
    feature_ids = ensemble_streamflow.indexes['feature_id']
    perc_df = pd.DataFrame(columns=percentiles, index=feature_ids.astype('string[pyarrow]'), dtype=float)

    # For features that have no params, copy first ensemble streamflow
    weibull_nomask = ~perc_df.index.isin(params_weibull.index.astype('string[pyarrow]'))
    perc_df.loc[weibull_nomask] = ensemble_streamflow.sel(
        feature_id=feature_ids[weibull_nomask], ensemble="1"
    ).to_numpy()[:, np.newaxis]

    inter_ids = feature_ids.intersection(params_weibull.index.astype(feature_ids.dtype))
    if len(inter_ids) > 0:
        print(f"Interpolating {len(inter_ids)} feature_ids...")
        ensemble_subset = ensemble_streamflow.sel(feature_id=inter_ids)
        inter_ids = inter_ids.astype('string[pyarrow]')

        ensemble_subset = ensemble_subset.fillna(ensemble_subset.mean(dim='ensemble'))

        val = ensemble_subset.sel(ensemble="1").to_numpy()
        max_val = ensemble_subset.max(dim='ensemble').to_numpy()
        min_val = ensemble_subset.min(dim='ensemble').to_numpy()

        # k=1 is necessary for linear interpolation
        spline = make_interp_spline([10, 50, 90], [max_val, val, min_val], k=1)
        percentile_values = spline(percentiles).T

        np.maximum(0, percentile_values, out=percentile_values)
        perc_df.loc[inter_ids] = percentile_values
    return perc_df


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
    np.copyto(vol_obank, 0.0, where=mask)  # Set overbank to 0 where stage doesn't exceed bankfull

    wetarea_chan = np.divide(vol_chan, lengthm, out=vol_chan)
    del vol_chan

    # Compute channel bedarea
    bedarea_chan = np.where(mask, vbedarea, vbedarea_bf)
    np.minimum(bedarea_chan, vbedarea_bf, out=bedarea_chan, where=mask)

    bedarea_obank = vbedarea - bedarea_chan
    np.maximum(bedarea_obank, 0.0, out=bedarea_obank)
    np.copyto(bedarea_obank, 0.0, where=mask)

    wettedperim_chan = bedarea_chan / lengthm
    np.multiply(delta_stage, 2, out=delta_stage)
    np.logical_not(mask, out=mask)
    np.add(wettedperim_chan, delta_stage, out=wettedperim_chan, where=mask)
    np.logical_not(mask, out=mask)
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

    np.multiply(q_obank, slope, out=q_obank)
    np.divide(q_obank, vobn, out=q_obank)
    del slope

    # Compute total discharge
    q_total = np.add(q_chan, q_obank, out=q_chan)
    del q_chan, q_obank
    np.equal(vstage, 0, out=mask)
    np.copyto(q_total, 0.0, where=mask)

    subdiv_applied = np.isnan(vstage_bf, out=mask)
    np.copyto(q_total, vq_orig, where=subdiv_applied)
    np.logical_not(subdiv_applied, out=subdiv_applied)
    return subdiv_applied, q_total


@use_pandas_3_behavior()
def read_crosswalk(hydrofabric_dir, huc, branch):
    """Read crosswalk csv

    Parameters
    ---------
    hydrofabric_dir: str
        Directory with the hydrofabric directories
    huc: str
        Huc
    branch: str
        Branch

    Returns
    -------
    pd.DataFrame
        Crosswalk for particular huc and branch.
    """

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
def get_subdivided_src(crosswalk):
    """
    Method for subdividing a synthetic rating curve based on the high water threshold

    Parameters
    ----------
    crosswalk: pd.DataFrame
        Crosswalk dataframe

    Returns
    -------
    pd.DataFrame:
        Computed discharge, indexed by HydroID and stage.
    """
    _, final_discharge = compute_manning_subdivision(crosswalk)

    # We copy because we want to release df_src afterward
    df_computed = pd.DataFrame(
        {
            'HydroID': crosswalk['HydroID'],
            'stage': crosswalk['Stage'],
            'subdiv_discharge_cms': final_discharge,
            'discharge_cms': final_discharge,  # create a copy of vmann modified discharge (used to track future changes)
        },
        copy=False,
    )
    df_computed = df_computed.set_index(["HydroID", "stage"])
    return df_computed


@use_pandas_3_behavior()
def inundate_probabilistic(
    streamflow_percentiles,
    percentiles,
    hydrofabric_dir: str,
    outputs_dir: str,
    huc: str,
    mosaic_prob_output_name: str,
    posterior_dist: Optional[pd.DataFrame] = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
    num_threads: int = 1,
    windowed: bool = False,
    output_raster: bool = False,
    quiet: bool = True,
    log_file: Optional[str] = None,
    output_vector: bool = True,
):
    """
    Method to probabilistically inundate based on provided ensembles

    Parameters
    ----------
    streamflow_percentiles: pd.DataFrame
        Streamflow percentile values.
    parameters: list | tuple
        Percentiles ie [90, 50, 10]
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
    day: int, default = 6
        Days ahead to pick from reference forecast time
    hour: int, default = 0,
        Hours ahead to pick from reference forecast time
    overwrite: bool, default = False
        Whether to overwrite existing output
    num_jobs: int, default = 1
        Number of processes to parallelize over
    num_threads: int, default = 1
        Number of threads to parallelize over
    windowed: bool, default = False
        Whether to run inundation in windowed mode for memory conservation
    output_raster: bool, default = False
        Whether to keep the output raster
    quiet : bool, default=False
        Quiet output
    log_file: Optional[str], default = None
        Filepath of log file
    output_vector: bool, default = True
        Whether to create vector output

    """

    if output_raster is False and output_vector is False:
        raise ValueError("Either output_raster or output_vector must be set to True")

    channel_dist, obank_dist, slope_dist = get_fim_probability_distributions(
        posterior_dist=posterior_dist, huc=int(huc)
    )

    # Make directories if they do not exist
    output_file_name = os.path.basename(mosaic_prob_output_name)
    base_output_path = os.path.join(outputs_dir, huc)

    # Create directory if it does not exist
    if is_local_path(base_output_path):
        os.makedirs(base_output_path, exist_ok=True)

    htable_cols = ['HydroID', 'feature_id', 'HUC', 'branch_id', 'stage', 'SurfaceArea (m2)', 'LakeID']
    df_htable = pd.read_parquet(
        os.path.join(hydrofabric_dir, huc, "hydrotable.parquet"), engine='pyarrow', columns=htable_cols
    )
    df_htable = df_htable.reset_index()
    df_htable = df_htable.astype(
        {'HUC': "string[pyarrow]", 'HydroID': 'string[pyarrow]', 'feature_id': "string[pyarrow]"}
    )
    df_htable["precalb_discharge_cms"] = 0

    adj_cols = ['channel_n', 'overbank_n', 'SLOPE']

    # Apply inundation map to each percentile
    branch_percentile_df = []
    print("Computing branch percentile hydrotables...")
    start = time.perf_counter()
    for branch in df_htable['branch_id'].unique():
        crosswalk = read_crosswalk(hydrofabric_dir, huc, str(branch))

        # Copy the channel_n, overbank_n, and SLOPE values
        adj_copies = crosswalk[adj_cols].copy()

        # Collect all the subdivided hydrotables
        h_tables = []
        for percentile in percentiles:
            if percentile == 50:
                crosswalk[adj_cols] = adj_copies
            else:
                p = percentile / 100
                channel_n_adj = channel_dist.isf(p)
                overbank_n_adj = obank_dist.isf(p)
                slope_adj = slope_dist.ppf(p)
                # Adjust the channel, overbank, and slope parameters
                crosswalk[adj_cols] = adj_copies + [channel_n_adj, overbank_n_adj, slope_adj]

            h_table = get_subdivided_src(crosswalk)
            h_table = h_table.rename(
                columns={n: f"{n}.{percentile}" for n in h_table.columns if n.startswith("discharge_cms")}
            )
            h_tables.append(h_table)
            del h_table
        p_table = pd.concat(h_tables, axis=1)
        p_table['branch_id'] = branch
        p_table = p_table.set_index("branch_id", append=True)
        branch_percentile_df.append(p_table)
        del h_tables, p_table
        del crosswalk
        del adj_copies
    print(f"[HUC: {huc}]: Branch percentile discharges {round(time.perf_counter() - start, 2)}s")

    htable_req_static_cols = [
        "branch_id",
        "feature_id",
        "HydroID",
        "stage",
        "HUC",
        "LakeID",
        "precalb_discharge_cms",
    ]

    inundation_paths = []
    branch_df = pd.concat(branch_percentile_df)
    full_p_table = df_htable.merge(
        branch_df, how='left', left_on=["HydroID", "stage", "branch_id"], right_index=True
    )
    full_p_table = full_p_table.sort_values(['branch_id', 'feature_id', 'HydroID', 'stage']).reset_index()
    del df_htable
    del branch_percentile_df, branch_df
    start = time.perf_counter()
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
        subhdf = subhdf.rename(columns={pcol: "discharge_cms"})

        flow_df = streamflow_percentiles[percentile].to_frame()
        flow_df = flow_df.rename(columns={percentile: "discharge"})

        print("producing mosaicked inundation for percentile", percentile)
        produce_mosaicked_inundation(
            hydrofabric_dir,
            huc,
            flow_df,
            hydro_table_df=subhdf,
            inundation_raster=final_inundation_path,
            mask=None,
            verbose=not quiet,
            num_workers=num_jobs,
            num_threads=num_threads,
            windowed=windowed,
            log_file=log_file,
        )

        # Release objects
        del flow_df, subhdf
    del full_p_table
    print(f"[HUC: {huc}]: Mosaicked inundation {round(time.perf_counter() - start, 2)}s")

    # For every percentile inundation map convert values to percentile
    start = time.perf_counter()
    with ExitStack() as stack:
        datasets = [stack.enter_context(rasterio.open(file)) for file in inundation_paths]
        profile = datasets[0].profile
        odtype = profile['dtype']
        raster_crs = datasets[0].crs
        nodata = profile['nodata']
        profile.update(
            dtype=np.int8,
            nodata=127,
            compress=profile.get('compress', 'DEFLATE'),
            driver='COG',
            sparse_ok="YES",
            resampling='NEAREST',
            blocksize=512,
        )

        out_rast = os.path.join(base_output_path, output_file_name.replace(".gpkg", ".tif"))
        with rasterio.open(out_rast, "w+", **profile) as write_rst:
            x = profile['blocksize'] * 2
            write_win = riowin.Window(0, 0, height=write_rst.height, width=write_rst.width)
            for window in riowin.subdivide(write_win, x, x):
                maxx = np.zeros((window.height, window.width), dtype=odtype)
                tmpm = np.zeros_like(maxx)
                mask = np.empty((window.height, window.width), dtype='bool')
                nodata_mask = np.empty_like(mask)
                for d, p in zip(datasets, percentiles):
                    d.read(1, out=tmpm, window=window)

                    # Only run on the last percentile (greatest extent possible)
                    if p == percentiles[-1]:
                        np.equal(tmpm, nodata, out=nodata_mask)

                    # equivalent to np.where(tmpm > 0, int(p), 0)
                    np.greater(tmpm, 0, out=mask)
                    tmpm.fill(0)
                    np.copyto(tmpm, int(p), where=mask)

                    np.maximum(maxx, tmpm, out=maxx)

                np.copyto(maxx, 127, where=nodata_mask)
                write_rst.write(maxx, window=window, indexes=1)
    print(f"[HUC: {huc}]: Writing max raster {round(time.perf_counter() - start, 2)}s")

    if output_vector is True:

        out_vec = os.path.join(base_output_path, output_file_name.replace(".tif", ".gpkg"))

        def _make_geometry(shapes):
            for p, v in shapes:
                yield shape(p), v

        with rasterio.open(out_rast, 'r') as rst:
            shapes = riofeat.shapes(rasterio.band(rst, 1))
            gdf = gpd.GeoDataFrame(_make_geometry(shapes), columns=['geometry', 'value'], crs=raster_crs)
            gdf = gdf.set_geometry('geometry')
            write_geodataframe(gdf, out_vec)

    for file in inundation_paths:
        os.remove(file)

    if output_raster is False:
        os.remove(out_rast)


@use_pandas_3_behavior()
def inundate_hucs(
    ensembles: str,
    parameters: str,
    hydrofabric_dir: str,
    outputs_dir: str,
    hucs: list,
    mosaic_prob_output_name: str,
    posterior_dist: Optional[str] = None,
    day: int = 6,
    hour: int = 0,
    overwrite: bool = False,
    num_jobs: int = 1,
    num_threads: int = 1,
    windowed: bool = False,
    output_raster: bool = False,
    quiet: bool = True,
    log_file: Optional[str] = None,
    output_vector: bool = True,
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

    percentiles = (90, 75, 50, 25, 10)
    with xr.open_dataset(ensembles) as ensembles_ds:
        percentile_values = generate_streamflow_percentiles(
            ensembles_ds['streamflow'].max(dim='time'), parameters_df, percentiles
        )

    if posterior_dist is not None:
        posterior_df = pd.read_parquet(posterior_dist)
    else:
        posterior_df = None

    for huc in hucs:
        inundate_probabilistic(
            percentile_values,
            percentiles,
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
