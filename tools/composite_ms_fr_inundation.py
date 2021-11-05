#!/usr/bin/env python3
import os, sys, shutil, argparse
import pandas as pd
from multiprocessing import Pool

from inundation import inundate
from gms_tools.inundate_gms import Inundate_gms
from gms_tools.mosaic_inundation import Mosaic_inundation
from gms_tools.overlapping_inundation import OverlapWindowMerge


def composite_inundation(fim_dir_ms, fim_dir_fr, huc, forecast, composite_output_dir, clean, quiet):
    """
    Run `inundate()` on FIM 3.X mainstem (MS) and full-resolution (FR) outputs and composite results. Assumes that all `fim_run` products 
    necessary for `inundate()` are in each huc8 folder.

    Parameters
    ----------
    fim_dir_ms : str
        Path to MS FIM directory. This should be an output directory from `fim_run.sh`.
    fim_dir_fr : str
        Path to FR FIM directory. This should be an output directory from `fim_run.sh`.
    huc : str, can be a single huc or comma-separated list of hucs
        HUC8 to run `inundate()`. This should be a folder within both `fim_dir_ms` and `fim_dir_fr`.
    forecast : str or pandas.DataFrame, can be a single file or a comma-separated list of files
        File path to forecast csv or Pandas DataFrame with correct column names.
    composite_output : str
        Folder path to write outputs. The output(s) will be named 'inundation_composite_{huc}.tif'
    quiet : bool, optional
        Quiet output.

    Returns
    -------
    None

    Raises
    ------
    TypeError
        Wrong input data types
    AssertionError
        Wrong input data types

    Notes
    -----
    - Specifying a subset of the domain in rem or catchments to inundate on is achieved by the HUCs file or the forecast file.

    Examples
    --------
    >>> import composite_ms_fr_inundation
    >>> composite_ms_fr_inundation.composite_inundation(
            '/home/user/fim_ouput_mainstem',
            '/home/user/fim_ouput_fullres',
            '12090301',
            '/home/user/forecast_file.csv',
            '/home/user/fim_inundation_composite',
            True,
            False)
    """

    # Build inputs to inundate() based on the input folders and huc
    print(f"HUC {huc}")
    for extent in (fim_dir_ms, fim_dir_fr):
        rem = os.path.join(extent, huc, 'rem_zeroed_masked.tif')
        catchments = os.path.join(extent, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
        catchment_poly = os.path.join(extent, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
        hydro_table = os.path.join(extent, huc, 'hydroTable.csv')
        inundation_raster = os.path.join(extent, huc, 'inundation.tif')

        # Run inundation()
        print(f"Inundating {os.path.basename(extent)}")
        result = inundate(rem,catchments,catchment_poly,hydro_table,forecast,mask_type=None,inundation_raster=inundation_raster,
                quiet=quiet)
        if result != 0:
            raise Exception(f"Failed to inundate {rem} using the provided flows.")

    # Inundation raster file paths
    fr_inundation_raster = os.path.join(fim_dir_fr, huc, 'inundation.tif')
    ms_inundation_raster = os.path.join(fim_dir_ms, huc, 'inundation.tif')

    # Composite MS and FR rasters
    inundation_map_file = { 
                        'huc8' : [huc] * 2,
                        'branchID' : [None] * 2,
                        'inundation_rasters' : [fr_inundation_raster, ms_inundation_raster],
                        'depths_rasters' : [None] * 2,
                        'inundation_polygons' : [None] * 2
                        }
    inundation_map_file = pd.DataFrame(inundation_map_file)
    
    flows_root = os.path.splitext(os.path.basename(forecast))[0]
    composite_output = os.path.join(composite_output_dir, f'inundation_composite_{flows_root}.tif')
    print("Compositing inundation rasters")
    Mosaic_inundation(
                        inundation_map_file,mosaic_attribute='inundation_rasters',
                        mosaic_output=composite_output,
                        mask=catchment_poly,unit_attribute_name='huc8',
                        nodata=None,workers=1,
                        remove_inputs=clean,
                        subset=None,verbose=not quiet
                        )


if __name__ == '__main__':
    
    # parse arguments
    parser = argparse.ArgumentParser(description='Rapid inundation mapping for FOSS FIM. Operates in single-HUC and batch modes.')
    parser.add_argument('-ms','--fim-dir-ms',help='Directory that contains MS FIM outputs.',required=True)
    parser.add_argument('-fr','--fim-dir-fr',help='Directory that contains FR FIM outputs.',required=True)
    parser.add_argument('-u','--huc',help='HUC within FIM directories to inunundate. Can be a comma-separated list.',required=True)
    parser.add_argument('-f','--forecast',help='File path of forecast csv or comma-separated list of paths if running multiple HUCs',required=True)
    parser.add_argument('-o','--ouput-dir',help='Folder to write Composite Raster output.',required=True)
    parser.add_argument('-j','--num-workers',help='Number of concurrent processes',required=False,default=1,type=int)
    parser.add_argument('-c','--clean',help='If flag used, intermediate rasters are NOT cleaned up.',required=False,default=True,action='store_false')
    parser.add_argument('-q','--quiet',help='Quiet terminal output.',required=False,default=False,action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    fim_dir_ms  = args['fim_dir_ms']
    fim_dir_fr  = args['fim_dir_fr']
    hucs        = args['huc'].replace(' ', '').split(',')
    forecasts   = args['forecast'].replace(' ', '').split(',')
    num_workers = int(args['num_workers'])
    output_dir  = args['ouput_dir']
    clean       = bool(args['clean'])
    quiet       = bool(args['quiet'])
    assert num_workers >= 1, "Number of workers should be 1 or greater"
    assert len(forecasts) == len(hucs), "Number of hucs must be equal to the number of forecasts provided"

    # Create nested list for input into multi-threading
    arg_list = []
    for huc, forecast in zip(hucs, forecasts):
        arg_list.append((fim_dir_ms, fim_dir_fr, huc, forecast, output_dir, clean, quiet))

    # Multi-thread for each huc in input hucs
    with Pool(processes=num_workers) as pool:
        # Run composite_inundation()
        pool.starmap(composite_inundation, arg_list)
