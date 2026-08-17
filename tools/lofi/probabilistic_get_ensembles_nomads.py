import io
import os
import ssl
import warnings
from datetime import datetime, timedelta
from typing import List, Union

import geopandas as gpd
import numpy as np
import requests
import xarray as xr
from tqdm.auto import tqdm


warnings.filterwarnings("ignore")
ssl.SSLContext.verify_mode = property(lambda self: ssl.CERT_NONE, lambda self, newval: None)


GFS_URL = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/v3.0/nwm.{0}'
    '/medium_range_mem{1}/nwm.t{2}z.medium_range.channel_rt_{1}.f{3}.conus.nc'
)

NBM_URL = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}'
    '/medium_range_blend/nwm.t{1}z.medium_range_blend.channel_rt.f{2}.conus.nc'
)

SHORT_URL = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}'
    '/short_range/nwm.t{1}z.short_range.channel_rt.f{2}.conus.nc'
)


def try_again(url, tries=0) -> Union[xr.Dataset, None]:
    """Recursive method to try URL up to 5 times

    Parameters
    ----------
    url : str
        Url to try and retrieve forecast data
    tries : int
        Number of time to try getting forecast data

    Returns
    -------
    Union[xr.Dataset, None]
        Either the xr.Dataset if successful or None

    """

    if tries < 5:
        try:
            content = requests.get(url)
            ds = xr.open_dataset(io.BytesIO(content.content))
            return ds
        except (requests.exceptions.Timeout, ValueError, EOFError):
            tries = tries + 1
            try_again(url, tries)
    else:
        return None


def concatenate_datasets(datasets: List[xr.Dataset]) -> xr.Dataset:
    """Concatenate forecast datasets by member

    Parameters
    ----------
    datasets : List[xr.Dataset]

    Returns
    -------
    xr.Dataset
        Dataset concatenated by the member dimension
    """

    tots = []
    for x, ds in zip(['1', '2', '3', '4', '5', '6'], datasets):
        tmp = ds.assign_coords({'ensemble': x})
        tmp = tmp.drop_vars(['qSfcLatRunoff', 'qBucket', 'qBtmVertRunoff', 'nudge', 'velocity'])
        tmp.expand_dims(dim={'ensemble': 1})
        tots.append(tmp)

    return xr.concat(tots, dim="ensemble")


def aggregate_forecasts(
    ensembles: xr.Dataset, aggregate_forecast_method: str = "max_to_forecast", day: int = 5, hour: int = 0
) -> xr.Dataset:
    """
    Aggregate ensemble metrics based

    Parameters
    ----------
    ensembles : xr.Dataset
        Dataset of medium range or short range forecasts
    aggregate_forecast_method : str, default = "max_to_forecast"
        Method to aggregate ensembles.  Options ["max_to_forecast". "timeslice_max_of_any_feature_id",
        "timeslice_max_sum", "timeslice"]
    day : int, default = 5
        How many days in the future to use in aggregation
    hour: int, default = 0
        How many hours in addition to days to use in aggregation

    Returns
    -------
    xr.Dataset
        Time aggregated ensemble forecasts

    """
    reference_time = ensembles.coords['reference_time'].values[-1]
    forecast_time = reference_time + np.timedelta64(day, 'D') + np.timedelta64(hour, 'h')

    # For each feature in the provided ensembles

    # Get max streamflow for every feature up to forecast time
    if aggregate_forecast_method == "max_to_forecast":
        sel_forecast = ensembles.sel({'time': slice(reference_time, forecast_time)}).max('time')

    # Timeslice representing the max streamflow for any feature id in time up to forecast time
    elif aggregate_forecast_method == "timeslice_max_of_any_feature_id":
        sel_forecast = ensembles.isel(
            {
                'time': ensembles.sel({'time': slice(reference_time, forecast_time)})
                .max(['feature_id', 'ensemble'], skipna=True)
                .argmax()
            }
        )

    # Timeslice representing the max sum of streamflow for all feature ids in time up to forecast time
    elif aggregate_forecast_method == "timeslice_max_sum":
        sel_forecast = ensembles.isel(
            {
                'time': ensembles.sel({'time': slice(reference_time, forecast_time)})
                .sum(['feature_id', 'ensemble'], skipna=True)
                .argmax()
            }
        )

    # Timeslice at forecast time
    else:
        sel_forecast = ensembles.sel({'time': forecast_time})

    return sel_forecast


def get_nomad_ensembles(
    temp_download_path: str,
    dt: str,
    hr: str,
    feature_ids: List[int],
    ensemble_type: str,
    output_path: str,
    aggregate_forecast_method: str = "max_to_forecast",
    days_ahead: int = 5,
    hours_ahead: int = 0,
    delete_temp_files: bool = True,
    overwrite: bool = True,
):
    """Get Ensembles From NOMAD service

    Parameters
    ----------
    temp_download_path: str
        Directory to store temporary files in case of service outage
    dt: str
        Datetime string of reference time of interest (must be today or yesterday)
    hr: str
        Hour of reference time of interest
    feature_ids: List[int]
        Feature IDs to use
    ensemble_type: str
        Type of ensemble forecast to generate
    output_path: str
        Path to save ensemble forecast NetCDF
    aggregate_forecast_method: str, default = "max_to_forecast"
         Method to aggregate ensembles.  Options ["max_to_forecast". "timeslice_max_of_any_feature_id",
        "timeslice_max_sum", "timeslice"]
    days_ahead: int, default = 5
        How many days ahead to calculate time aggregate of foreacast
    hours_ahead: int, default = 0
        How many hours ahead to calculate time aggregate of forecast
    delete_temp_files: bool = True
        Whether to delete temporary files or not
    overwrite: bool = True
        Whether to overwrite temporary files that exist or use them
    """

    temp_files = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        if ensemble_type == "nbm":
            for idx in tqdm(range(1, 7), leave=False):

                ds_lists = []
                if dt != datetime.now().strftime('%Y%m%d') or int(hr) < 6:
                    raise (
                        "Not enough time samples to get six ensembles for nbm.  Pick at least today at 6 AM"
                    )

                temp_dt = datetime.strptime(dt + f' {hr}', '%Y%m%d %H') - timedelta(hours=(idx - 1) * 6)
                temp_dt_str, temp_hr = temp_dt.strftime('%Y%m%d'), temp_dt.strftime('%H')

                for x in tqdm(range(1, 241), leave=False):
                    temp_file_name = os.path.join(temp_download_path, f'nbm_{idx}_{x}.nc')
                    temp_files.append(temp_file_name)
                    if overwrite or not os.path.exists(temp_file_name):
                        url = NBM_URL.format(temp_dt_str, temp_hr.zfill(2), str(x).zfill(3))
                        ds = try_again(url)
                        if ds is not None:
                            ds = ds.sel({'feature_id': feature_ids})
                            ds.to_netcdf(temp_file_name)
                            ds_lists.append(ds)
                    else:
                        ds = xr.open_dataset(temp_file_name)
                        ds_lists.append(ds)

            concat_ds = concatenate_datasets(ds_lists)
            concat_ds = aggregate_forecasts(
                concat_ds,
                aggregate_forecast_method=aggregate_forecast_method,
                day=days_ahead,
                hour=hours_ahead,
            )
            concat_ds = concat_ds.dropna('feature_id')
            concat_ds.to_netcdf(output_path)

        if ensemble_type == "srf":
            for idx in tqdm(range(1, 7), leave=False):

                ds_lists = []
                if dt != datetime.now().strftime('%Y%m%d') and int(hr) < 6:
                    raise "Not enough time samples to get six ensembles for srf.  Pick at least yesterday at 6 AM"

                if days_ahead != 0 and hours_ahead > 13:
                    raise """SRF forecast with 6 ensembles can only forecast 13 hours ahead. Chosen:
                    \n days_ahead: {days_ahead} \n hours_ahead: {hours_ahead}"""

                temp_dt = datetime.strptime(dt + f' {hr}', '%Y%m%d %H') - timedelta(hours=(idx - 1) * 1)
                temp_dt_str, temp_hr = temp_dt.strftime('%Y%m%d'), temp_dt.strftime('%H')

                for x in tqdm(range(1, 19), leave=False):
                    temp_file_name = os.path.join(temp_download_path, f'srf_{idx}_{x}.nc')
                    temp_files.append(temp_file_name)
                    if overwrite or not os.path.exists(temp_file_name):
                        url = SHORT_URL.format(temp_dt_str, temp_hr.zfill(2), str(x).zfill(3))
                        ds = try_again(url)
                        if ds is not None:
                            ds = ds.sel({'feature_id': feature_ids})
                            ds.to_netcdf(temp_file_name)
                            ds_lists.append(ds)
                    else:
                        ds = xr.open_dataset(temp_file_name)
                        ds_lists.append(ds)

            concat_ds = concatenate_datasets(ds_lists)
            concat_ds = aggregate_forecasts(
                concat_ds,
                aggregate_forecast_method=aggregate_forecast_method,
                day=days_ahead,
                hour=hours_ahead,
            )
            concat_ds = concat_ds.dropna('feature_id')
            concat_ds.to_netcdf(output_path)

        if ensemble_type == "gfs":
            for idx in tqdm(range(1, 7), leave=False):

                ds_lists = []
                for x in tqdm(range(1, 205), leave=False):
                    temp_file_name = os.path.join(temp_download_path, f'gfs_{idx}_{x}.nc')
                    temp_files.append(temp_file_name)
                    if overwrite or not os.path.exists(temp_file_name):
                        url = GFS_URL.format(dt, idx, hr.zfill(2), str(x).zfill(3))
                        ds = try_again(url)
                        if ds is not None:
                            ds = ds.sel({'feature_id': feature_ids})
                            ds.to_netcdf(temp_file_name)
                            ds_lists.append(ds)
                    else:
                        ds = xr.open_dataset(temp_file_name)
                        ds_lists.append(ds)

            concat_ds = concatenate_datasets(ds_lists)
            concat_ds = aggregate_forecasts(
                concat_ds,
                aggregate_forecast_method=aggregate_forecast_method,
                day=days_ahead,
                hour=hours_ahead,
            )
            concat_ds = concat_ds.dropna('feature_id')
            concat_ds.to_netcdf(output_path)

        if delete_temp_files:
            for f in temp_files:
                os.remove(f)


if __name__ == '__main__':
    d_path = '../../ensembles/test/nomads/'
    output_paths = [
        os.path.join(d_path, 'nbm_final.nc'),
        os.path.join(d_path, 'gfs_final.nc'),
        os.path.join(d_path, 'srf_final.nc'),
    ]
    huc08 = "05110005"
    gdf = gpd.read_file(f'../../outputs/outputs/{huc08}/nwm_subset_streams.gpkg')
    dt = datetime.now().strftime('%Y%m%d')
    hr = '6'
    overwrite = False
    delete_temp_files = False

    get_nomad_ensembles(
        temp_download_path=d_path,
        dt=dt,
        hr=hr,
        feature_ids=gdf['ID'].unique(),
        ensemble_type="nbm",
        output_path=output_paths[0],
        aggregate_forecast_method="max_to_forecast",
        days_ahead=5,
        hours_ahead=0,
        delete_temp_files=delete_temp_files,
        overwrite=overwrite,
    )

    get_nomad_ensembles(
        temp_download_path=d_path,
        dt=dt,
        hr=hr,
        feature_ids=gdf['ID'].unique(),
        ensemble_type="gfs",
        output_path=output_paths[1],
        aggregate_forecast_method="max_to_forecast",
        days_ahead=5,
        hours_ahead=0,
        delete_temp_files=delete_temp_files,
        overwrite=overwrite,
    )

    get_nomad_ensembles(
        temp_download_path=d_path,
        dt=dt,
        hr=hr,
        feature_ids=gdf['ID'].unique(),
        ensemble_type="srf",
        output_path=output_paths[2],
        aggregate_forecast_method="max_to_forecast",
        days_ahead=0,
        hours_ahead=13,
        delete_temp_files=delete_temp_files,
        overwrite=overwrite,
    )
