#!/usr/bin/env python3

import argparse
import os

import numpy as np
import rasterio
import rasterio.mask
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation
from rasterio.fill import fillnodata
from tools_shared_variables import elev_raster_ndv


def interpolate_wse(
    inundation_depth_raster,
    hydroconditioned_dem,
    output_depth_raster,
    output_interpolated_wse=None,
    max_distance=20,
    smooth_iterations=2,
):
    """
    Attempts to overcome the catchment boundary issue by computing water surface elevation
    and interpolating missing inundated areas between catchments. Please see the relevant
    pull request for more information and screenshots:
    https://github.com/NOAA-OWP/inundation-mapping/pull/1048

    Parameters
    ----------
    inundation_depth_raster : str
        Input path for a depth raster.
    hydroconditioned_dem : str
        Input path for the hydroconditioned DEM used to create the above depth raster.
        This should be the path to dem_thalwegCond_{}.tif.
    output_depth_raster : str
        Output path for the resulting interpolated depth raster.
    output_interpolated_wse : str , optional
        Output path for the intermediate output water surface elevation raster.
        This raster is only saved if this parameter is set. Default is None.
    max_distance : int , optional
        The maximum number of pixels to search in all directions to find values to
        interpolate from. The default is 20.
    smooth_iterations : int , optional
        The number of 3x3 smoothing filter passes to run. The default is 2.

    """
    with rasterio.open(inundation_depth_raster) as depth:
        depth_rast = depth.read(1)
        profile = depth.profile
    with rasterio.open(hydroconditioned_dem) as huc_dem:
        dem = huc_dem.read()
        dem[np.where(dem == huc_dem.profile['nodata'])] = np.nan

    # Calculate water surface elevation grid
    wse_rast = depth_rast + dem
    wse_rast = np.where(depth_rast == profile['nodata'], profile['nodata'], wse_rast)
    wse_rast = np.where(depth_rast == 0.0, 0.0, wse_rast)

    # Run interpolation
    wse_interpolated = fillnodata(
        wse_rast,
        mask=wse_rast.astype(np.intc),
        max_search_distance=max_distance,
        smoothing_iterations=smooth_iterations,
    )

    # Write interpolated water surface elevation raster if specified
    if output_interpolated_wse:
        with rasterio.open(output_interpolated_wse, 'w', **profile) as dst:
            wse_interpolated[np.where(np.isnan(wse_interpolated))] = profile['nodata']
            dst.write(wse_interpolated)

    # Calculate depth from new interpolated WSE
    final_depth = wse_interpolated - dem
    final_depth[np.where(final_depth <= 0)] = profile['nodata']
    # Remove levees
    final_depth[np.where(dem == profile['nodata'])] = profile['nodata']
    # Write interpolated depth raster
    with rasterio.open(output_depth_raster, 'w', **profile) as dst:
        dst.write(final_depth)


def inundate_with_catchment_spillover(
    hydrofabric_dir,
    hucs,
    flow_file,
    depths_raster,
    output_fileNames=None,
    max_distance=20,
    smooth_iterations=2,
    num_workers=1,
    keep_intermediate=False,
    log_file=None,
    verbose=False,
):
    print("Running Inundation")
    map_file = Inundate_gms(
        hydrofabric_dir=hydrofabric_dir,
        forecast=flow_file,
        num_workers=num_workers,
        hucs=hucs,
        depths_raster=depths_raster,
        verbose=verbose,
        log_file=log_file,
        output_fileNames=output_fileNames,
    )

    print("Interpolating water surfaces for each branch")
    for index, row in map_file.iterrows():
        # Hydroconditioned DEM filename
        dem = os.path.join(
            hydrofabric_dir,
            row['huc8'],
            'branches',
            row['branchID'],
            'dem_thalwegCond_{}.tif'.format(row['branchID']),
        )
        assert os.path.isfile(dem), (
            "Cannot find hydroconditioned DEM. Ensure that dem_thalwegCond_{}.tif "
            "is present in all branch directories."
        )
        # Compute new depth rasters and overwrite
        interpolate_wse(
            row['depths_rasters'],
            dem,
            row['depths_rasters'],
            output_interpolated_wse=None,
            max_distance=max_distance,
            smooth_iterations=smooth_iterations,
        )

    print("Mosaicking branches together")
    Mosaic_inundation(
        map_file,
        mosaic_attribute='depths_rasters',
        mosaic_output=depths_raster,
        mask=None,
        unit_attribute_name='huc8',
        nodata=elev_raster_ndv,
        workers=1,
        remove_inputs=not keep_intermediate,
        subset=None,
        verbose=verbose,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''
This script produces inundation depths and attempts to overcome the catchment boundary issue
by interpolating water surface elevations between catchments. Water surface calculations require
the hydroconditioned DEM (dem_thalwegCond_{}.tif) for computation, however, this file is not in
the standard outputs from fim_pipeline.sh. Therefore, users may have to re-run fim_pipeline.sh
with dem_thalwegCond_{}.tif removed from all deny lists.

Sample Usage :
python interpolate_water_surface.py -y /outputs/fim_pipline_outputs -u 17110009 17110010 \
    -f /home/user/interpolated_fim/custom_flow_file.csv -d /home/user/interpolated_fim/depth_raster.tif
'''
    )
    parser.add_argument(
        "-y",
        "--hydrofabric_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-u", "--hucs", help="List of HUCS to run", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-d",
        "--depths-raster",
        help="Depths raster output. Only writes if designated. Appends HUC code in batch mode.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-md",
        "--max-distance",
        help="The maximum number of pixels to search in all directions to find values to interpolate from. The default is 20.",
        required=False,
        default=20,
        type=int,
    )
    parser.add_argument(
        "-si",
        "--smooth-iterations",
        help="The number of 3x3 smoothing filter passes to run. The default is 2.",
        required=False,
        default=2,
        type=int,
    )

    parser.add_argument("-w", "--num-workers", help="Number of workers.", required=False, default=1, type=int)
    parser.add_argument(
        "-k",
        "--keep-intermediate",
        help="Keeps intermediate products, i.e. individual branch inundation.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Verbose printing. Not tested.",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    inundate_with_catchment_spillover(**args)
