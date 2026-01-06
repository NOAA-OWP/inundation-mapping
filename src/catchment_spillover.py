#!/usr/bin/env python3

import argparse
import os

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

    with rasterio.open(dem_tif) as dem, rasterio.open(rem_tif) as rem:
        crs = dem.crs
        dem_nodata = dem.profile['nodata']
        dem = dem.read()
        dem[np.where(dem == dem_nodata)] = np.nan

        rem_transform = rem.transform
        rem_nodata = rem.profile['nodata']

        rem = rem.read()
        rem[np.where(rem == rem_nodata)] = np.nan
        rem_mask = rem[rem != np.nan]

    rem_change = []
    previous_rem = None
    for i in range(max_iterations):
        print(f"Iteration {i+1} of {max_iterations}")
        rem = catchment_spillover(dem, rem, rem_mask, flow_direction_tif, crs, rem_transform)

        if i > 0:
            change_in_rem = rem - previous_rem
            percent_change = -(np.nanmean(change_in_rem) / np.nanmean(previous_rem)) * 100.0
            rem_change.append(percent_change)
            print(f"Percent change in REM: {percent_change:.2f}%")

            # Stop if percent change is less than the specified threshold or max iterations reached
            if percent_change < pct_change_threshold:
                break

        previous_rem = rem

    print("Final iteration completed. Saving final REM.")
    save_raster(rem, rem_tif, crs, rem_transform)

    # Save rem_change as a pandas DataFrame csv
    rem_change_df = pd.DataFrame(rem_change, columns=['percent_change'])
    rem_change_df.to_csv(os.path.join(os.path.dirname(rem_tif), 'rem_change.csv'), index=False)


def catchment_spillover(dem, rem, rem_mask, flow_direction_tif, crs=None, rem_transform=None):
    # Calculate the pixel catchment thalweg elevation
    thalweg_elev = dem - rem

    # The 3x3 max filter identifies which cells might spill over into neighbors
    max_thalweg_elev = ndimage.maximum_filter(thalweg_elev, size=3)
    max_thalweg_elev[np.where(rem_mask == np.nan)] = np.nan

    # Recalculate depth-to-flood by subtracting the new reference (thalweg) elevation from the DEM
    # new_thalweg_elev = np.where(max_thalweg_elev > thalweg_elev, max_thalweg_elev - dem, np.nan)
    updated_dtf = np.where((dem - max_thalweg_elev) < rem, dem - max_thalweg_elev, rem)
    # spillover_locations = np.where(updated_dtf != rem, updated_dtf, np.nan)

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

    # Set the no data to nans
    # DTF_w_downhill_nans = np.where(DTF_w_downhill == -9999, np.nan, DTF_w_downhill)

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

    updated_reference_elev_nans = np.where(updated_reference_elev == -9999.0, np.nan, updated_reference_elev)

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

    # Replace original REM values where they are greater
    DTF_w_downhill_backfill_final = np.where(DTF_w_downhill_backfill < rem, DTF_w_downhill_backfill, rem)

    DTF_w_downhill_backfill_final[DTF_w_downhill_backfill_final < 0] = 0

    # px, py = np.gradient(rem[0], 2)
    # slope = np.sqrt(px ** 2 + py ** 2)

    # # If needed in degrees, convert using
    # slope_deg = np.degrees(np.arctan(slope))
    # DTF_w_downhill_backfill[np.where(DTF_w_downhill_backfill <= 0)] = 0

    return DTF_w_downhill_backfill_final


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
