#!/usr/bin/env python3
import os, argparse, rasterio
import numpy as np
import pandas as pd
from multiprocessing import Pool

from inundation import inundate
from gms_tools.mosaic_inundation import Mosaic_inundation, __append_id_to_file_name


def composite_inundation(fim_dir_ms, fim_dir_fr, huc, flows, composite_output_dir, ouput_name='',
                         bin_rast_flag=False, depth_rast_flag=False, clean=True, quiet=True):
    """
    Run `inundate()` on FIM 3.X mainstem (MS) and full-resolution (FR) outputs and composite results. Assumes that all `fim_run` products 
    necessary for `inundate()` are in each huc8 folder.

    Parameters
    ----------
    fim_dir_ms : str
        Path to MS FIM directory. This should be an output directory from `fim_run.sh`.
    fim_dir_fr : str
        Path to FR FIM directory. This should be an output directory from `fim_run.sh`.
    huc : str
        HUC8 to run `inundate()`. This should be a folder within both `fim_dir_ms` and `fim_dir_fr`.
    flows : str or pandas.DataFrame, can be a single file or a comma-separated list of files
        File path to forecast csv or Pandas DataFrame with correct column names.
    composite_output_dir : str
        Folder path to write outputs. It will be created if it does not exist.
    ouput_name : str, optional
        Name for output raster. If not specified, by default the raster will be named 'inundation_composite_{flows_root}.tif'.
    bin_rast_flag : bool, optional
        Flag to create binary raster as output. If no raster flags are passed, this is the default behavior.
    depth_rast_flag : bool, optional
        Flag to create depth raster as output.
    clean : bool, optional
        If True, intermediate files are deleted.
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
    # Set inundation raster to True if no output type flags are passed
    if not (bin_rast_flag or depth_rast_flag):
        bin_rast_flag = True
    assert not (bin_rast_flag and depth_rast_flag), 'Output can only be binary or depth grid, not both'
    assert os.path.isdir(fim_dir_ms), f'{fim_dir_ms} is not a directory. Please specify an existing MS FIM directory.'
    assert os.path.isdir(fim_dir_fr), f'{fim_dir_fr} is not a directory. Please specify an existing FR FIM directory.'
    assert os.path.exists(flows), f'{flows} does not exist. Please specify a flow file.'

    # Instantiate output variables
    var_keeper = {
        'ms': {
            'dir': fim_dir_ms,
            'outputs': {
                'inundation_rast': os.path.join(composite_output_dir, f'inundation_ms_{huc}.tif') if bin_rast_flag else None,
                'depth_rast':      os.path.join(composite_output_dir, f'depth_ms_{huc}.tif') if depth_rast_flag else None
            }
        },
        'fr': {
            'dir': fim_dir_fr,
            'outputs': {
                'inundation_rast': os.path.join(composite_output_dir, f'inundation_fr_{huc}.tif') if bin_rast_flag else None,
                'depth_rast':      os.path.join(composite_output_dir, f'depth_fr_{huc}.tif') if depth_rast_flag else None
            }
        }
    }
    # Build inputs to inundate() based on the input folders and huc
    if not quiet: print(f"HUC {huc}")
    for extent in var_keeper:
        rem = os.path.join(var_keeper[extent]['dir'], huc, 'rem_zeroed_masked.tif')
        catchments = os.path.join(var_keeper[extent]['dir'], huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
        catchment_poly = os.path.join(var_keeper[extent]['dir'], huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
        hydro_table = os.path.join(var_keeper[extent]['dir'], huc, 'hydroTable.csv')

        # Ensure that all of the required files exist in the huc directory
        for file in (rem, catchments, catchment_poly, hydro_table):
            if not os.path.exists(file):
                raise Exception(f"The following file does not exist within the supplied FIM directory:\n{file}")

        # Run inundation()
        extent_friendly = "mainstem (MS)" if extent=="ms" else "full-resolution (FR)"
        if not quiet: print(f"  Creating an inundation map for the {extent_friendly} configuration...")
        result = inundate(rem,catchments,catchment_poly,hydro_table,flows,mask_type=None,
                inundation_raster=  var_keeper[extent]['outputs']['inundation_rast'],
                depths=             var_keeper[extent]['outputs']['depth_rast'],
                quiet=              quiet)
        if result != 0:
            raise Exception(f"Failed to inundate {rem} using the provided flows.")

    # If no output name supplied, create one using the flows file name 
    if not ouput_name:
        flows_root = os.path.splitext(os.path.basename(flows))[0]
        ouput_name = os.path.join(composite_output_dir, f'inundation_composite_{flows_root}.tif')
    else:
        ouput_name = os.path.join(composite_output_dir, ouput_name)
    
    # Composite MS and FR
    inundation_map_file = { 
                    'huc8' : [huc] * 2,
                    'branchID' : [None] * 2,
                    'inundation_rasters':  [var_keeper['fr']['outputs']['inundation_rast'], 
                                            var_keeper['ms']['outputs']['inundation_rast']],
                    'depths_rasters':      [var_keeper['fr']['outputs']['depth_rast'], 
                                            var_keeper['ms']['outputs']['depth_rast']]
                    }
    inundation_map_file = pd.DataFrame(inundation_map_file)
    Mosaic_inundation(
                    inundation_map_file,
                    mosaic_attribute='depths_rasters' if depth_rast_flag else 'inundation_rasters',
                    mosaic_output=ouput_name,
                    mask=catchment_poly,
                    unit_attribute_name='huc8',
                    nodata=-9999,
                    workers=1,
                    remove_inputs=clean,
                    subset=None,verbose=not quiet
                    )
    if bin_rast_flag:
        hydroid_to_binary(__append_id_to_file_name(ouput_name, huc))

def hydroid_to_binary(hydroid_raster_filename):
    '''Converts hydroid positive/negative grid to 1/0'''

    #to_bin = lambda x: np.where(x > 0, 1, np.where(x == 0, -9999, 0))
    to_bin = lambda x: np.where(x > 0, 1, np.where(x != -9999, 0, -9999))
    hydroid_raster = rasterio.open(hydroid_raster_filename)
    profile = hydroid_raster.profile # get profile for new raster creation later on
    profile['nodata'] = -9999
    bin_raster = to_bin(hydroid_raster.read(1)) # converts neg/pos to 0/1
    # Overwrite inundation raster
    with rasterio.open(hydroid_raster_filename, "w", **profile) as out_raster:
        out_raster.write(bin_raster.astype(hydroid_raster.profile['dtype']), 1)
    del hydroid_raster,profile,bin_raster


if __name__ == '__main__':
    
    # parse arguments
    parser = argparse.ArgumentParser(description='Inundate FIM 3 full resolution and mainstem outputs using a flow file and composite the results.')
    parser.add_argument('-ms','--fim-dir-ms',help='Directory that contains MS FIM outputs.',required=True)
    parser.add_argument('-fr','--fim-dir-fr',help='Directory that contains FR FIM outputs.',required=True)
    parser.add_argument('-u','--huc',help='HUC within FIM directories to inunundate. Can be a comma-separated list.',required=True)
    parser.add_argument('-f','--flows-file',help='File path of flows csv or comma-separated list of paths if running multiple HUCs',required=True)
    parser.add_argument('-o','--ouput-dir',help='Folder to write Composite Raster output.',required=True)
    parser.add_argument('-n','--ouput-name',help='File name for output(s).',default=None,required=False)
    parser.add_argument('-b','--bin-raster',help='Output raster is a binary wet/dry grid. This is the default if no raster flags are passed.',required=False,default=False,action='store_true')
    parser.add_argument('-d','--depth-raster',help='Output raster is a depth grid.',required=False,default=False,action='store_true')
    parser.add_argument('-j','--num-workers',help='Number of concurrent processesto run.',required=False,default=1,type=int)
    parser.add_argument('-c','--clean',help='If flag used, intermediate rasters are NOT cleaned up.',required=False,default=True,action='store_false')
    parser.add_argument('-q','--quiet',help='Quiet terminal output.',required=False,default=False,action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    fim_dir_ms    = args['fim_dir_ms']
    fim_dir_fr    = args['fim_dir_fr']
    hucs          = args['huc'].replace(' ', '').split(',')
    flows_files   = args['flows_file'].replace(' ', '').split(',')
    num_workers   = int(args['num_workers'])
    output_dir    = args['ouput_dir']
    ouput_name    = args['ouput_name']
    bin_raster    = bool(args['bin_raster'])
    depth_raster  = bool(args['depth_raster'])
    clean         = bool(args['clean'])
    quiet         = bool(args['quiet'])

    assert num_workers >= 1, "Number of workers should be 1 or greater"
    assert len(flows_files) == len(hucs), "Number of hucs must be equal to the number of forecasts provided"
    assert not (bin_raster and depth_raster), "Cannot use both -b and -d flags"

    # Create output directory if it does not exist
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    # Create nested list for input into multi-threading
    arg_list = []
    for huc, flows_file in zip(hucs, flows_files):
        arg_list.append((fim_dir_ms, fim_dir_fr, huc, flows_file, output_dir, ouput_name, bin_raster, depth_raster, clean, quiet))

    # Multi-thread for each huc in input hucs
    with Pool(processes=num_workers) as pool:
        # Run composite_inundation()
        pool.starmap(composite_inundation, arg_list)
