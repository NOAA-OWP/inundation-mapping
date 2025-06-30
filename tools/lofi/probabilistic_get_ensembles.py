import io
import os
import ssl
import time
import warnings
from typing import List, Union

import geopandas as gpd
import requests
import xarray as xr
from tqdm.notebook import tqdm


warnings.filterwarnings("ignore")
ssl.SSLContext.verify_mode = property(lambda self: ssl.CERT_NONE, lambda self, newval: None)

gfs_url = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/v3.0/nwm.{0}'
    '/medium_range_mem{1}/nwm.t00z.medium_range.channel_rt_{1}.f{2}.conus.nc'
)

noda_url = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}'
    '/medium_range_no_da/nwm.t00z.medium_range_no_da.channel_rt.f{1}.conus.nc'
)


nbm_url = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}'
    + '/medium_range_blend/nwm.t00z.medium_range_blend.channel_rt.f{1}.conus.nc'
)


short_url = (
    'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{0}'
    + '/short_range/nwm.t00z.short_range.channel_rt.f{1}.conus.nc'
)


def try_again(url: str, tries: int = 0) -> Union[xr.Dataset, None]:
    """
    Utility to attempt a download recursively until max tries are exceeded

    Parameters
    ----------
    url : str
        Url to NOMADS service
    tries : int
        Number of attempts to download files

    Returns
    -------


    """
    if tries < 5:
        try:
            content = requests.get(url)
            ds = xr.open_dataset(io.BytesIO(content.content))
            return ds

        except (ValueError, OSError):
            tries = tries + 1
            try_again(url, tries)
    else:
        return None


def get_nomads_ensembles(dt: str, ens_type: str, feature_ids: List[int], output_path: str, output_name: str):
    """
    Method to collect ensembles for NOMADS service

    Parameters
    ----------
    dt : str
        Date time string to get nomad ensembles
    ens_type
    """

    master_lists = []

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        if ens_type == "noda":
            ds_lists = []
            for x in tqdm(range(3, 241, 3)):
                url = noda_url.format(dt, ('00' + str(x))[-3:])
                ds = try_again(url)
                if ds is not None:
                    ds = ds.sel({'feature_id': feature_ids})
                    ds.to_netcdf(os.path.join(output_path, f'no_da_{x}.nc'))
                    ds_lists.append(ds)
            master_lists.append(ds_lists)

            final_list = [xr.concat(master_lists[0], dim="time")]

        elif ens_type == "nbm":
            ds_lists = []
            for x in tqdm(range(1, 241)):
                url = nbm_url.format(dt, ('00' + str(x))[-3:])
                ds = try_again(url)
                if ds is not None:
                    ds = ds.sel({'feature_id': feature_ids})
                    ds.to_netcdf(os.path.join(output_path, f'nbm_{x}.nc'))
                    ds_lists.append(ds)
            master_lists.append(ds_lists)

            final_list = [xr.concat(master_lists[0], dim="time")]

        elif ens_type == "short":
            ds_lists = []
            for x in tqdm(range(1, 19)):
                url = short_url.format(dt, ('00' + str(x))[-3:])
                ds = try_again(url)
                if ds is not None:
                    ds = ds.sel({'feature_id': feature_ids})
                    ds.to_netcdf(os.path.join(output_path, f'srf_{x}.nc'))
                    ds_lists.append(ds)
            master_lists.append(ds_lists)

            final_list = [xr.concat(master_lists[8], dim="time")]

        elif ens_type == "gfs":
            for idx in tqdm(range(1, 7)):
                ds_lists = []
                for x in tqdm(range(1, 205)):
                    url = gfs_url.format(dt, idx, ('00' + str(x))[-3:])
                    ds = try_again(url)
                    if ds is not None:
                        ds = ds.sel({'feature_id': feature_ids})
                        ds.to_netcdf(os.path.join(output_path, f'gfs_{idx}_{x}.nc'))
                        ds_lists.append(ds)
                master_lists.append(ds_lists)

            concat1 = xr.concat(master_lists[0], dim="time")
            concat2 = xr.concat(master_lists[1], dim="time")
            concat3 = xr.concat(master_lists[2], dim="time")
            concat4 = xr.concat(master_lists[3], dim="time")
            concat5 = xr.concat(master_lists[4], dim="time")
            concat6 = xr.concat(master_lists[5], dim="time")
            final_list = [concat1, concat2, concat3, concat4, concat5, concat6]

        else:
            raise ValueError(f"Ensemble type {ens_type} not supported")

        concat_datasets(final_list, ens_type, output_path, output_name)

        print(time.localtime())


def concat_datasets(ds_list, ens_type, output_path, output_name):
    tots = []

    if ens_type == "gfs":

        for x, ds in zip(['1', '2', '3', '4', '5', '6'], ds_list):
            tmp = ds.assign_coords({'member': x})
            try:
                tmp = tmp.drop_vars(['qSfcLatRunoff', 'qBucket', 'qBtmVertRunoff'])
            except ValueError:
                print('Variables do not exist to drop')
            tots.append(tmp.expand_dims(dim={'member': 1}))

    else:
        ds = ds_list[0]
        tmp = ds.assign_coords({'member': ens_type})
        tots.append(tmp.expand_dims(dim={'member': 1}))

    concat_tot = xr.concat(tots, dim="member")
    concat_tot = concat_tot.drop_vars('crs').interpolate_na(dim='time')

    os.makedirs(output_path, exist_ok=True)
    concat_tot.to_netcdf(f'{output_path}/{output_name}.nc')


if __name__ == '__main__':

    # Getting feature ids from hydrofabric stream network
    huc = '18100100'
    streams = gpd.read_file(f'../../outputs/{huc}/nwm_subset_streams.gpkg')

    get_nomads_ensembles()
