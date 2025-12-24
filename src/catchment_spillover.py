#!/usr/bin/env python3

import argparse

import numpy as np
import pandas as pd
import pyflwdir
import rasterio
from scipy import ndimage


def save_raster(array, filename, crs, transform):
    # save updated_dtf raster
    with rasterio.open(
        filename,
        'w',
        driver='GTiff',
        height=array.shape[1],
        width=array.shape[2],
        count=1,
        dtype=array.dtype,
        crs=crs,
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(array)


def iterate_spillover(dem_tif, rem_tif, flow_direction_tif, max_iterations=20, pct_change_threshold=1.0):
    if max_iterations <= 0:
        return

    with rasterio.open(dem_tif) as dem, rasterio.open(rem_tif) as rem:
        crs = dem.crs
        dem_nodata = dem.profile['nodata']
        dem = dem.read()
        dem[np.where(dem == dem_nodata)] = np.nan

        rem_transform = rem.transform
        rem_sub25 = rem.read()

        # Set all pixels above the SRC calculation height to nan
        rem_sub25[np.where(rem_sub25 > 25.3)] = rem.profile['nodata']
        rem_sub25[np.where(rem_sub25 == rem.profile['nodata'])] = np.nan
        rem = rem.read()

    rem_change = []
    previous_rem = None
    for i in range(max_iterations):
        print(f"Iteration {i+1} of {max_iterations}")
        rem = catchment_spillover(dem, rem, flow_direction_tif, i)

        if i > 0:
            change_in_rem = rem - previous_rem
            percent_change = (np.nanmean(change_in_rem) / np.nanmean(previous_rem)) * 100.0
            rem_change.append(percent_change)
            print(f"Percent change in REM: {percent_change:.2f}%")

            # Stop if percent change is less than 1% or max iterations reached
            if percent_change > -pct_change_threshold:
                break

        previous_rem = rem

    print("Final iteration completed. Saving final REM.")
    save_raster(rem, rem_tif, crs, rem_transform)

    # Save rem_change as a pandas DataFrame csv
    rem_change_df = pd.DataFrame(rem_change, columns=['percent_change'])
    rem_change_df.to_csv('/outputs/temp/rem_change.csv', index=False)


def catchment_spillover(dem, rem, flow_direction_tif, iteration):
    thalweg_elev = dem - rem
    # This removes some weird very high values from the thalweg_elev raster
    # More research on these areas are needed.
    thalweg_elev[np.where(thalweg_elev > np.nanmax(dem))] = np.nan

    # The 3x3 max filter identifies which cells might spill over into neighbors
    max_thalweg_elev = ndimage.maximum_filter(thalweg_elev, size=3)
    # Recalculate depth-to-flood by subtracting the new reference (thalweg) elevation from the DEM
    # new_thalweg_elev = np.where(max_thalweg_elev > thalweg_elev, max_thalweg_elev - dem, np.nan)
    updated_dtf = np.where((dem - max_thalweg_elev) < rem, dem - max_thalweg_elev, rem)
    # spillover_locations = np.where(updated_dtf != rem, updated_dtf, np.nan)

    # save_raster(dem - max_thalweg_elev, f'updated_dtf_reference_elev_{branch_id}_{iteration}.tif', branch_dir, crs, rem_transform)
    # save_raster(max_thalweg_elev, f'max_thalweg_elev_{branch_id}_{iteration}.tif', branch_dir, crs, rem_transform)
    # save_raster(updated_dtf, f'updated_dtf_spillover_{branch_id}_{iteration}.tif', branch_dir, crs, rem_transform)
    # save_raster(new_thalweg_elev, f'new_thalweg_elev_spillover_{branch_id}_{iteration}.tif', branch_dir, crs, rem_transform)
    # save_raster(spillover_locations, f'spillover_locations_{branch_id}_{iteration}.tif', branch_dir, crs, rem_transform)

    with rasterio.open(flow_direction_tif, "r") as src:
        flwdir = src.read(1)
        crs = src.crs

        flwdir[np.where(flwdir == 1)] = 16
        flwdir[np.where(flwdir == 2)] = 19
        flwdir[np.where(flwdir == 3)] = 18
        flwdir[np.where(flwdir == 4)] = 17
        flwdir[np.where(flwdir == 5)] = 14
        flwdir[np.where(flwdir == 6)] = 11
        flwdir[np.where(flwdir == 7)] = 12
        flwdir[np.where(flwdir == 8)] = 13
        flwdir[np.where(flwdir == 0)] = 15
        flwdir = np.where(flwdir != src.profile['nodata'], flwdir - 10, 255).astype(np.int16)

        flw = pyflwdir.from_array(
            flwdir,
            ftype='ldd',
            check_ftype=False,
            transform=src.transform,
            latlon=crs.is_geographic,
            cache=True,
        )
    # flw = pyflwdir.from_dem(dem[0], dem_nodata)
    # print(np.unique(flw.to_array('ldd')))

    # Fill the new depth-to-flood values to downhill cells
    DTF_w_downhill = flw.fillnodata(np.where(updated_dtf != rem, updated_dtf, -9999), -9999, how='min')
    # Stop the new depth-to-flood values where it meets the backfill flooding (equals original HAND values)
    DTF_w_downhill = np.where(DTF_w_downhill < rem, DTF_w_downhill, -9999)

    # save_raster(DTF_w_downhill, f'/outputs/temp/DTF_w_downhill_{iteration}.tif', crs, rem_transform)

    # Set the no data to nans
    # DTF_w_downhill_nans = np.where(DTF_w_downhill == -9999, np.nan, DTF_w_downhill)
    # save_raster(DTF_w_downhill_nans, f'/outputs/temp/DTF_w_downhill_nans_{iteration}.tif', crs, rem_transform)

    '''###############
    # Fill the new depth-to-flood values to downhill cells
    reference_elev = np.where(updated_dtf != -9999., dem - updated_dtf, -9999.),
    reference_elev_w_downhill = flw.fillnodata(
        np.where(updated_dtf != rem, reference_elev, -9999),
        -9999,
        how='max',
    )
    # Stop the new depth-to-flood values where it meets the backfill flooding (equals original HAND values)
    #reference_elev_w_downhill = np.where(reference_elev_w_downhill < dem, reference_elev_w_downhill, -9999)
    #DTF_w_downhill_nans = np.where((DTF_w_downhill == -9999) | (DTF_w_downhill < catchment_rem), np.nan, DTF_w_downhill)

    # Set the no data to nans
    reference_elev_w_downhill_nans = np.where(reference_elev_w_downhill == -9999, np.nan, reference_elev_w_downhill)
    ###############'''

    # DTF_w_downhill = flw.fillnodata(DTF_w_downhill,-9999,'up')
    # DTF_w_downhill_HAND = flw.fillnodata(DTF_w_downhill - dem,-9999,'up')

    # In order to backfill flood from the spillover cells, calculate a new
    # reference elevation by subtracting the depth-to-flood value from the DEM.
    # Backfill the new reference elevation uphill.
    updated_reference_elev = flw.fillnodata(
        np.where(DTF_w_downhill != -9999.0, dem - DTF_w_downhill, -9999.0), -9999, 'up'
    )
    # save_raster(
    #     updated_reference_elev, f'/outputs/temp/updated_reference_elev_{iteration}.tif', crs, rem_transform
    # )

    updated_reference_elev_nans = np.where(updated_reference_elev == -9999.0, np.nan, updated_reference_elev)
    # DTF_w_downhill_HAND_nans[np.where(DTF_w_downhill_HAND <= 0)] = np.nan
    # save_raster(
    #     updated_reference_elev_nans,
    #     f'/outputs/temp/updated_reference_elev_nans_{iteration}.tif',
    #     crs,
    #     rem_transform,
    # )

    '''
    # Attemped to translate the reference elevation uphill, but it didn't turn out right
    updated_reference_elev = flw.fillnodata(
        reference_elev_w_downhill[0],
        -9999,
        'up')
    updated_reference_elev_nans = np.where(updated_reference_elev == -9999., np.nan, updated_reference_elev)
    '''

    # Calculate the final depth-to-inundation by subtracting the backfilled
    # reference elevation from the DEM.
    DTF_w_downhill_backfill = dem - updated_reference_elev_nans
    # save_raster(
    #     DTF_w_downhill_backfill, f'/outputs/temp/DTF_w_downhill_backfill_{iteration}.tif', crs, rem_transform
    # )

    # Replace original REM values where they are greater
    DTF_w_downhill_backfill = np.where(DTF_w_downhill_backfill < rem, DTF_w_downhill_backfill, rem)

    # px, py = np.gradient(rem[0], 2)
    # slope = np.sqrt(px ** 2 + py ** 2)

    # # If needed in degrees, convert using
    # slope_deg = np.degrees(np.arctan(slope))

    return DTF_w_downhill_backfill


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate spillover flooding for a given branch.")

    parser.add_argument("--dem_tif", type=str, help="Path to DEM TIFF file.")
    parser.add_argument("--rem_tif", type=str, help="Path to REM TIFF file.")
    parser.add_argument("--flow_direction_tif", type=str, help="Path to flow direction TIFF file.")
    parser.add_argument(
        "--max_iterations", type=int, default=5, help="Number of spillover iterations to perform."
    )
    parser.add_argument(
        "--pct_change_threshold", type=float, default=1.0, help="Percent change threshold to stop iterations."
    )
    args = parser.parse_args()

    iterate_spillover(**vars(args))
