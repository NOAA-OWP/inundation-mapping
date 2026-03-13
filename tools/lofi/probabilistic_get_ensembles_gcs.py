import gc
import os
from datetime import datetime, timedelta
from typing import List, Union

import gcsfs
import geopandas as gpd
import numpy as np
import xarray as xr
from tqdm import tqdm


NBM_ENSEMBLE_URL = "gs://national-water-model/nwm.{0}/{1}/nwm.t{2}z.{1}.channel_rt.f{3}.conus.nc"
GFS_ENSEMBLE_URL = "gs://national-water-model/nwm.{0}/{1}/nwm.t{2}z.medium_range.channel_rt_{4}.f{3}.conus.nc"
SHORT_ENSEMBLE_URL = "gs://national-water-model/nwm.{0}/{1}/nwm.t{2}z.{1}.channel_rt.f{3}.conus.nc"


def get_gcs_ensembles(
    dt: str,
    hour: str,
    ens_type: str,
    feature_ids: List[int],
    output_path: str,
    output_name: str,
    aggregate_forecast_method: str = "max_to_forecast",
    days_ahead: int = 5,
    hours_ahead: int = 0,
):
    """
    Method to collect ensembles for NOMADS service

    Parameters
    ----------
    dt : str
        Date time string to get gcs forecast data, can be any date the product was available and account for the forecast window.
    hour : str
        Hours to get gcs forecast data.
    ens_type : str
        An ensemble type from the following list, "gfs", "nbm", "short", "noda".
    feature_ids : List[int]
        Feature IDs to get from the ensemble files.
    output_path : str
        Path to save ensemble files.
    output_name : str
        Name of final processed ensemble file.
    aggregate_forecast_method : str, default = "max_to_forecast"
        Method to aggregate ensembles.  Options ["max_to_forecast". "timeslice_max_of_any_feature_id",
        "timeslice_max_sum", "timeslice"]
    days_ahead : int, default = 5
        How many days in the future to use in aggregation
    hours_ahead: int, default = 0
        How many hours in addition to days to use in aggregation


    """

    gcs = gcsfs.GCSFileSystem()

    # National Blend of Models Forced Medium Range (6 successively older forecasts)
    if ens_type == "nbm":

        forecast_type = 'medium_range_blend'
        members = ['1', '2', '3', '4', '5', '6']

        dts = []
        og_time = dt = datetime.strptime(f'{dt}-{hour}', '%Y%m%d-%H')
        dts.append(dt)

        for x in range(len(members) - 1):
            dt = dt - timedelta(hours=6)
            dts.append(dt)

        datetimes, hours = [x.strftime('%Y%m%d') for x in dts], [x.strftime('%H') for x in dts]

        # Get forecast times (3 hour timestep)
        forecast_times = []
        for num in np.arange(1, 241, 1):
            num = str(num)
            if len(num) == 2:
                num = '0' + num
            elif len(num) == 1:
                num = '00' + num
            forecast_times.append(num)

        # Get nbm forecasts
        ds_list = []
        nofiles = []

        for dtime, hr, mem in tqdm(zip(datetimes, hours, members)):
            intermediate_list = []
            if len(str(hr)) < 2:
                hr = f'0{hr}'

            for ft in tqdm(forecast_times):

                try:
                    openfile = gcs.open(NBM_ENSEMBLE_URL.format(dtime, forecast_type, hr, ft), mode='rb')
                    dsgcs = xr.open_dataset(openfile)
                    time = dsgcs.coords['time']
                    dsgcs = dsgcs['streamflow']
                    dsgcs = dsgcs.sel({'feature_id': feature_ids})
                    dsgcs = dsgcs.expand_dims(ensemble=[mem])
                    dsgcs = dsgcs.expand_dims(time=time)
                    intermediate_list.append(dsgcs)
                except (ValueError, OSError):
                    nofiles.append(NBM_ENSEMBLE_URL.format(dtime, forecast_type, mem, hr, ft))

            int_concat = xr.concat(intermediate_list, "time", join="inner")
            ds_list.append(int_concat)
            del intermediate_list
            gc.collect()

        final_ds = xr.concat(ds_list, "ensemble")
        final_ds = final_ds.expand_dims(reference_time=[og_time])

        final_ds = aggregate_forecasts(
            final_ds, aggregate_forecast_method=aggregate_forecast_method, day=days_ahead, hour=hours_ahead
        )
        final_ds = final_ds.dropna('feature_id')
        final_ds.to_netcdf(os.path.join(output_path, output_name))

    # Global Forecasting System Medium Range (Current forecast with 6 successively older forcings)
    elif ens_type == "gfs":

        forecast_type = 'medium_range'

        dt = datetime.strptime(f'{dt} {hour}', '%Y%m%d %H')
        datetimes, hours = [dt.strftime('%Y%m%d')], [hour]
        members = ['1', '2', '3', '4', '5', '6']

        if len(str(hours[0])) < 2:
            hours[0] = f'0{hour[0]}'

        # Get forecast times (3 hour timestep)
        forecast_times = []
        for num in np.arange(1, 205, 1):
            num = str(num)
            if len(num) == 2:
                num = '0' + num
            elif len(num) == 1:
                num = '00' + num
            forecast_times.append(num)

        ds_list = []
        nofiles = []

        for mem in tqdm(members):
            intermediate_list = []
            for ft in tqdm(forecast_times):
                try:
                    openfile = gcs.open(
                        GFS_ENSEMBLE_URL.format(
                            datetimes[0], forecast_type + f'_mem{mem}', hours[0], ft, mem
                        ),
                        mode='rb',
                    )
                    dsgcs = xr.open_dataset(openfile)
                    reference_time = dsgcs.coords['reference_time']
                    time = dsgcs.coords['time']
                    dsgcs = dsgcs['streamflow']
                    dsgcs = dsgcs.sel({'feature_id': feature_ids})
                    dsgcs = dsgcs.expand_dims(ensemble=[mem])
                    dsgcs = dsgcs.expand_dims(reference_time=reference_time)
                    dsgcs = dsgcs.expand_dims(time=time)
                    intermediate_list.append(dsgcs)

                except (ValueError, OSError):
                    nofiles.append(
                        GFS_ENSEMBLE_URL.format(datetimes[0], forecast_type + f'_mem{mem}', hours[0], ft, mem)
                    )

            int_concat = xr.concat(intermediate_list, "time", join="inner")
            ds_list.append(int_concat)
            del intermediate_list
            gc.collect()

        final_ds = xr.concat(ds_list, "ensemble")

        final_ds = aggregate_forecasts(
            final_ds, aggregate_forecast_method=aggregate_forecast_method, day=days_ahead, hour=hours_ahead
        )
        final_ds = final_ds.dropna('feature_id')
        final_ds.to_netcdf(os.path.join(output_path, output_name))

    # Short Range Forecasts (Current forecast with 6 successively older forcings)
    elif ens_type == "srf":

        forecast_type = 'short_range'
        members = ['1', '2', '3', '4', '5', '6']

        dts = []
        og_time = dt = datetime.strptime(f'{dt}-{hour}', '%Y%m%d-%H')
        dts.append(dt)

        for x in range(len(members) - 1):
            dt = dt - timedelta(hours=1)
            dts.append(dt)

        datetimes, hours = [x.strftime('%Y%m%d') for x in dts], [x.strftime('%H') for x in dts]

        # Get forecast times (3 hour timestep)
        forecast_times = []
        for num in np.arange(1, 19, 1):
            num = str(num)
            if len(num) == 2:
                num = '0' + num
            elif len(num) == 1:
                num = '00' + num
            forecast_times.append(num)

        # Get nbm forecasts
        ds_list = []
        nofiles = []

        for dtime, hr, mem in tqdm(zip(datetimes, hours, members)):
            intermediate_list = []
            if len(str(hr)) < 2:
                hr = f'0{hr}'

            for ft in tqdm(forecast_times):

                try:
                    openfile = gcs.open(SHORT_ENSEMBLE_URL.format(dtime, forecast_type, hr, ft), mode='rb')
                    dsgcs = xr.open_dataset(openfile)
                    time = dsgcs.coords['time']
                    dsgcs = dsgcs['streamflow']
                    dsgcs = dsgcs.sel({'feature_id': feature_ids})
                    dsgcs = dsgcs.expand_dims(ensemble=[mem])
                    dsgcs = dsgcs.expand_dims(time=time)
                    intermediate_list.append(dsgcs)
                except (ValueError, OSError):
                    nofiles.append(NBM_ENSEMBLE_URL.format(dtime, forecast_type, mem, hr, ft))

            int_concat = xr.concat(intermediate_list, "time", join="inner")
            ds_list.append(int_concat)
            del intermediate_list
            gc.collect()

        final_ds = xr.concat(ds_list, "ensemble")
        final_ds = final_ds.expand_dims(reference_time=[og_time])

        final_ds = aggregate_forecasts(
            final_ds, aggregate_forecast_method=aggregate_forecast_method, day=days_ahead, hour=hours_ahead
        )
        final_ds = final_ds.dropna('feature_id')
        final_ds.to_netcdf(os.path.join(output_path, output_name))

    else:
        raise ValueError(f"Ensemble type {ens_type} not supported")


def aggregate_forecasts(
    ensembles: Union[xr.Dataset, xr.DataArray],
    aggregate_forecast_method: str = "max_to_forecast",
    day: int = 5,
    hour: int = 0,
) -> Union[xr.Dataset, xr.DataArray]:
    """
    Aggregate ensemble metrics based

    Parameters
    ----------
    ensembles : Union[xr.Dataset, xr.DataArray]
        Dataset or DataArray of medium range or short range forecasts
    aggregate_forecast_method : str
        Method to aggregate ensembles.  Options ["max_to_forecast". "timeslice_max_of_any_feature_id",
        "timeslice_max_sum", "timeslice"]
    day : int
        How many days in the future to use in aggregation
    hour: int
        How many hours in addition to days to use in aggregation

    Returns
    -------
    Union[xr.Dataset, xr.DataArray]
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


if __name__ == '__main__':

    huc = "05110005"
    # Way to get feature ids
    streams = gpd.read_file(f'../../outputs/outputs/{huc}/nwm_subset_streams.gpkg')
    # Reference time
    dt = '20250402'
    hr = '0'

    get_gcs_ensembles(
        dt=dt,
        hour=hr,
        ens_type='gfs',
        feature_ids=streams['ID'].unique(),
        output_path="../../ensembles/test/gcs",
        output_name=f"{huc}_ensembles_gfs.nc",
    )

    get_gcs_ensembles(
        dt=dt,
        hour=hr,
        ens_type='nbm',
        feature_ids=streams['ID'].unique(),
        output_path="../../ensembles/test/gcs",
        output_name=f"{huc}_ensembles_nbm.nc",
    )

    get_gcs_ensembles(
        dt=dt,
        hour=hr,
        ens_type='srf',
        feature_ids=streams['ID'].unique(),
        output_path="../../ensembles/test/gcs",
        output_name=f"{huc}_ensembles_srf.nc",
        days_ahead=0,
        hours_ahead=13,
        aggregate_forecast_method="time_slice",
    )
