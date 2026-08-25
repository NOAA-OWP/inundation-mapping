#!/usr/bin/env python3
"""
src/make_rem.py
---------------
Calculates REM / HAND (Relative Elevation Model / Height Above Nearest Drainage)
in RAM using Numba-accelerated minimum elevation dictionary mapping across
pixel watersheds and thalweg cells.
"""

import numpy as np
from numba import njit, typed, types
from osgeo import gdal


@njit
def _make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, thalweg_window, dem_ndv):
    """Populates dictionary of pixel catchment IDs to minimum thalweg elevation."""
    for i in range(len(flat_catchments)):
        cm = flat_catchments[i]
        elev = flat_dem[i]

        # Strictly ignore background/NoData catchment IDs and NoData DEM pixels
        if cm > 0 and thalweg_window[i] == 1 and elev != dem_ndv and not np.isnan(elev):
            if cm in catchment_min_dict:
                if elev < catchment_min_dict[cm]:
                    catchment_min_dict[cm] = elev
            else:
                catchment_min_dict[cm] = elev
    return catchment_min_dict


@njit
def _calculate_rem(flat_dem, catchment_min_dict, flat_catchments, global_min_elev, dem_ndv):
    """Subtracts mapped thalweg minimum elevation from DEM for each catchment cell."""
    rem_window = np.full(len(flat_dem), fill_value=dem_ndv, dtype=np.float32)
    for i in range(len(flat_catchments)):
        elev = flat_dem[i]
        cm = flat_catchments[i]

        # Only compute for valid terrain and valid catchments
        if cm > 0 and elev != dem_ndv and not np.isnan(elev):
            if cm in catchment_min_dict:
                min_elev = catchment_min_dict[cm]
            else:
                min_elev = global_min_elev

            if min_elev != dem_ndv and not np.isnan(min_elev):
                rem_window[i] = elev - min_elev
    return rem_window


def create_rem_in_memory(
    dem_ds: gdal.Dataset,
    pixel_watersheds_ds: gdal.Dataset,
    thalweg_ds: gdal.Dataset,
    nodata_val: float = None,
) -> gdal.Dataset:
    """Calculates unmasked relative elevation model (HAND) in RAM matching original Numba logic."""
    dem_band = dem_ds.GetRasterBand(1)

    # Dynamically extract NoData from GDAL band, fallback to -999999.0 or passed nodata_val
    band_ndv = dem_band.GetNoDataValue()
    if band_ndv is not None:
        dem_ndv = float(band_ndv)
    elif nodata_val is not None:
        dem_ndv = float(nodata_val)
    else:
        dem_ndv = -999999.0

    flat_dem = dem_band.ReadAsArray().ravel().astype(np.float32)

    ws_band = pixel_watersheds_ds.GetRasterBand(1)
    flat_catchments = ws_band.ReadAsArray().ravel().astype(np.int32)

    if thalweg_ds is not None:
        thalweg_band = thalweg_ds.GetRasterBand(1)
        thalweg_window = thalweg_band.ReadAsArray().ravel().astype(np.int32)
    else:
        thalweg_window = np.ones_like(flat_catchments, dtype=np.int32)

    # 1. Build catchment minimum elevation dictionary using Numba
    catchment_min_dict = typed.Dict.empty(types.int32, types.float32)
    catchment_min_dict = _make_catchment_min_dict(
        flat_dem, catchment_min_dict, flat_catchments, thalweg_window, dem_ndv
    )

    # 2. Global fallback minimum
    if len(catchment_min_dict) > 0:
        global_min_elev = float(min(catchment_min_dict.values()))
    else:
        valid_dem = flat_dem[(flat_dem != dem_ndv) & (~np.isnan(flat_dem)) & (flat_catchments > 0)]
        global_min_elev = float(valid_dem.min()) if len(valid_dem) > 0 else dem_ndv

    # 3. Calculate REM array
    rem_flat = _calculate_rem(flat_dem, catchment_min_dict, flat_catchments, global_min_elev, dem_ndv)
    rem_arr = rem_flat.reshape((dem_ds.RasterYSize, dem_ds.RasterXSize))

    # 4. Construct GDAL In-Memory Dataset
    driver = gdal.GetDriverByName("MEM")
    cols, rows = dem_ds.RasterXSize, dem_ds.RasterYSize
    out_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(dem_ndv)
    out_band.WriteArray(rem_arr)
    out_ds.FlushCache()

    return out_ds
