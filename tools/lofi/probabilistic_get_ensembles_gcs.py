import gc
import os
from datetime import datetime
from typing import List

import gcsfs
import geopandas as gpd
import numpy as np
import xarray as xr
from tqdm.notebook import tqdm


nbm_ensemble_url = "gs://national-water-model/nwm.{0}/{1}/nwm.t{2}z.{1}.channel_rt.f{3}.conus.nc"
gfs_ensemble_url = "gs://national-water-model/nwm.{0}/{1}/nwm.t{2}z.medium_range.channel_rt_{4}.f{3}.conus.nc"


def get_gcs_ensembles(
    dt: str, hour: str, ens_type: str, feature_ids: List[int], output_path: str, output_name: str
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

    """

    gcs = gcsfs.GCSFileSystem()

    if ens_type == "nbm":

        forecast_type = 'medium_range_blend'

        dt = datetime.strptime(f'{dt}-{hour}', '%Y%m%d-%H')
        datetimes, hours = [dt.strftime('%Y%m%d')], [hour]
        members = ['nbm']

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

            if len(str(hr)) < 2:
                hr = f'0{hr}'

            for ft in tqdm(forecast_times):

                try:
                    openfile = gcs.open(nbm_ensemble_url.format(dtime, forecast_type, hr, ft), mode='rb')
                    dsgcs = xr.open_dataset(openfile)
                    reference_time = dsgcs.coords['reference_time']
                    time = dsgcs.coords['time']
                    dsgcs = dsgcs['streamflow']
                    dsgcs = dsgcs.sel({'feature_id': feature_ids})
                    dsgcs = dsgcs.expand_dims(member=[mem])
                    dsgcs = dsgcs.expand_dims(reference_time=reference_time)
                    dsgcs = dsgcs.expand_dims(time=time)
                    ds_list.append(dsgcs)
                except (ValueError, OSError):

                    nofiles.append(nbm_ensemble_url.format(dtime, forecast_type, mem, hr, ft))

        final_ds = xr.concat(ds_list[0:240], "time", join="inner")
        final_ds.to_netcdf(os.path.join(output_path, output_name))

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
                        gfs_ensemble_url.format(
                            datetimes[0], forecast_type + f'_mem{mem}', hours[0], ft, mem
                        ),
                        mode='rb',
                    )
                    dsgcs = xr.open_dataset(openfile)
                    reference_time = dsgcs.coords['reference_time']
                    time = dsgcs.coords['time']
                    dsgcs = dsgcs['streamflow']
                    dsgcs = dsgcs.sel({'feature_id': feature_ids})
                    dsgcs = dsgcs.expand_dims(member=[mem])
                    dsgcs = dsgcs.expand_dims(reference_time=reference_time)
                    dsgcs = dsgcs.expand_dims(time=time)
                    intermediate_list.append(dsgcs)

                except (ValueError, OSError):
                    nofiles.append(
                        gfs_ensemble_url.format(datetimes[0], forecast_type + f'_mem{mem}', hours[0], ft, mem)
                    )

            int_concat = xr.concat(intermediate_list, "time", join="inner")
            ds_list.append(int_concat)
            del intermediate_list
            gc.collect()

        final_ds = xr.concat(ds_list, "member")
        final_ds.to_netcdf(os.path.join(output_path, output_name))

    else:
        raise ValueError(f"Ensemble type {ens_type} not supported")


if __name__ == '__main__':

    huc = "05110005"
    # Way to get feature ids
    streams = gpd.read_file(f'../../outputs/fim_outputs_test/{huc}/nwm_subset_streams.gpkg')
    # Reference time
    dt = '20250402'
    hr = '0'

    get_gcs_ensembles(
        dt=dt,
        hour=hr,
        ens_type='nbm',
        feature_ids=streams['ID'].unique(),
        output_path="../../ensembles",
        output_name=f"{huc}_ensembles_nbm.nc",
    )
