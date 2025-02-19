#!/usr/bin/env python3

import argparse

import numpy as np
from osgeo import gdal


def preprocess_benchmark_static(benchmark_raster, reference_raster, out_raster_path=None):
    '''
    This function will preprocess a benchmark dataset for purposes of evaluating FIM output.
    A benchmark dataset will be transformed using properties (CRS, resolution) from an input
    reference dataset. The benchmark raster will also be converted to a boolean (True/False) raster
    with inundated areas (True or 1) and dry areas (False or 0).

    Parameters
    ----------
    benchmark_raster : STRING
        Path to benchmark raster (e.g. BLE elevation or WSE elevations).
    reference_raster : STRING
        Path to reference raster (e.g. output from inundation.py).
    out_raster_path : STRING, optional
        Path to the output raster. The default is None.

    Returns
    -------
    boolean_benchmark : Numpy Array
        The preprocessed benchmark array.
    profile : STRING
        Raster profile information for the preprocessed benchmark array
            (required for writing to output dataset).

    '''
    # # Set arbitrary no data value that is not possible value of the benchmark dataset. It is reassigned later.
    # nodata_value = -2147483648

    # Open and read raster and benchmark rasters
    reference = gdal.Open(reference_raster)
    benchmark = gdal.Open(benchmark_raster)

    ulx, reference_xRes, xskew, uly, yskew, reference_yRes = reference.GetGeoTransform()

    # ulx, xres, xskew, uly, yskew, yres  = benchmark.GetGeoTransform()
    # sizeX = benchmark.RasterXSize * xres
    # sizeY = benchmark.RasterYSize * yres
    # lrx = ulx + sizeX
    # lry = uly + sizeY

    gdal.Warp(
        out_raster_path,
        benchmark,
        outputType=gdal.GDT_Float32,
        dstSRS=reference.GetProjection(),
        xRes=reference_xRes,
        yRes=reference_yRes,
        resampleAlg=gdal.GRA_Bilinear,
    )

    #  outputBounds=[ulx, lry, lrx, uly],
    #  outputBoundsSRS=benchmark.GetProjection(),
    #  resampleAlg=gdal.GRA_Bilinear,
    #  multithread=True)

    # # Determine the new transform and dimensions of reprojected/resampled raster
    # new_transform, new_width, new_height = calculate_default_transform(
    #     benchmark.crs,
    #     reference.crs,
    #     benchmark.width,
    #     benchmark.height,
    #     *benchmark.bounds,
    #     resolution=reference.res,
    # )

    # # Define an empty array that is same dimensions as output by the "calculate_default_transform" command
    # benchmark_projected = np.empty((new_height, new_width), dtype=np.int32)

    # # Reproject and resample the benchmark dataset. Bilinear resampling due to continuous depth data
    # reproject(
    #     benchmark_arr,
    #     destination=benchmark_projected,
    #     src_transform=benchmark.transform,
    #     src_crs=benchmark.crs,
    #     src_nodata=benchmark.nodata,
    #     dst_transform=new_transform,
    #     dst_crs=reference.crs,
    #     dst_nodata=nodata_value,
    #     dst_resolution=reference.res,
    #     resampling=Resampling.bilinear,
    # )

    # profile = reference.profile

    # # Convert entire depth grid to boolean (1 = Flood, 0 = No Flood)
    # boolean_benchmark = np.where(benchmark_projected != nodata_value, 1, 0)

    # # Update profile (data type, NODATA, transform, width/height)
    # profile.update(transform=new_transform)
    # profile.update(dtype=rasterio.int8)
    # # Update NODATA to some integer so we can keep int8 datatype. There are no NODATA in the raster dataset
    # profile.update(nodata=2)
    # profile.update(width=new_width)
    # profile.update(height=new_height)

    # # Write out preprocessed benchmark array to raster if path is supplied
    # if out_raster_path is not None:
    #     with rasterio.Env():
    #         # Write out reassigned values to raster dataset
    #         with rasterio.open(out_raster_path, 'w', **profile) as dst:
    #             dst.write(boolean_benchmark.astype('int8'), 1)
    # return boolean_benchmark.astype('int8'), profile


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Preprocess BLE grids (in tiff format) for use in run_test_cast.py. '
        'Preprocessing includes reprojecting and converting to boolean raster (1 = Flooding, 0 = No Flooding)'
    )
    parser.add_argument(
        '-b',
        '--benchmark-raster',
        help='BLE depth or water surface elevation grid (in GTiff format).',
        required=True,
    )
    parser.add_argument(
        '-r',
        '--reference-raster',
        help='Benchmark will use reference raster to set CRS and resolution to reference raster CRS.',
        required=True,
    )
    parser.add_argument(
        '-o', '--out-raster-path', help='Output raster path (include name and extension).', required=True
    )

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())
    # Run preprocess benchmark function
    preprocess_benchmark_static(**args)
