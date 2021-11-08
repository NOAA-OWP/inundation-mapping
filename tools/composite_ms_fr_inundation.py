#!/usr/bin/env python3
import os, sys, shutil, argparse
import pandas as pd
import geopandas as gpd
from multiprocessing import Pool

from inundation import inundate
from gms_tools.mosaic_inundation import Mosaic_inundation


def composite_inundation(fim_dir_ms, fim_dir_fr, huc, forecast, composite_output_dir, ouput_name,
                         bool_rast_flag, depth_rast_flag, polygon_flag, clean, quiet):
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
    # Set inundation raster to True if no output type flags are passed
    if not (bool_rast_flag or depth_rast_flag or polygon_flag):
        bool_rast_flag = True
    # Instantiate output variables
    var_keeper = {
        'ms': {
            'dir': fim_dir_ms,
            'outputs': {
                'inundation_rast': os.path.join(fim_dir_ms, huc, 'inundation.tif') if bool_rast_flag else None,
                'depth_rast':      os.path.join(fim_dir_ms, huc, 'depth.tif') if depth_rast_flag else None,
                'inundation_poly': os.path.join(fim_dir_ms, huc, 'inundation_poly.gpkg') if polygon_flag else None
            }
        },
        'fr': {
            'dir': fim_dir_fr,
            'outputs': {
                'inundation_rast': os.path.join(fim_dir_fr, huc, 'inundation.tif') if bool_rast_flag else None,
                'depth_rast':      os.path.join(fim_dir_fr, huc, 'depth.tif') if depth_rast_flag else None,
                'inundation_poly': os.path.join(fim_dir_fr, huc, 'inundation_poly.gpkg') if polygon_flag else None
            }
        }
    }
    # Build inputs to inundate() based on the input folders and huc
    print(f"HUC {huc}")
    for extent in var_keeper:
        rem = os.path.join(var_keeper[extent]['dir'], huc, 'rem_zeroed_masked.tif')
        catchments = os.path.join(var_keeper[extent]['dir'], huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
        catchment_poly = os.path.join(var_keeper[extent]['dir'], huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
        hydro_table = os.path.join(var_keeper[extent]['dir'], huc, 'hydroTable.csv')

        # Run inundation()
        print(f"  Inundating {extent}")
        result = inundate(rem,catchments,catchment_poly,hydro_table,forecast,mask_type=None,
                inundation_raster=  var_keeper[extent]['outputs']['inundation_rast'],
                inundation_polygon= var_keeper[extent]['outputs']['inundation_poly'],
                depths=             var_keeper[extent]['outputs']['depth_rast'],
                quiet=              quiet)
        if result != 0:
            raise Exception(f"Failed to inundate {rem} using the provided flows.")

    # If no output name supplied, create one using the forecast file name 
    if not ouput_name:
        flows_root = os.path.splitext(os.path.basename(forecast))[0]
        ouput_name = os.path.join(composite_output_dir, f'inundation_composite_{flows_root}.tif')
    else:
        ouput_name = os.path.join(composite_output_dir, ouput_name)
    
    # Composite rasters if specified, otherwise union polygons
    if depth_rast_flag or bool_rast_flag:
        print("Compositing inundation rasters")
        # Composite MS and FR
        inundation_map_file = { 
                        'huc8' : [huc] * 2,
                        'branchID' : [None] * 2,
                        'inundation_rasters':  [var_keeper['fr']['outputs']['inundation_rast'], 
                                                var_keeper['ms']['outputs']['inundation_rast']],
                        'depths_rasters':      [var_keeper['fr']['outputs']['depth_rast'], 
                                                var_keeper['ms']['outputs']['depth_rast']],
                        'inundation_polygons': [var_keeper['fr']['outputs']['inundation_poly'], 
                                                var_keeper['ms']['outputs']['inundation_poly']],
                        }
        inundation_map_file = pd.DataFrame(inundation_map_file)
        Mosaic_inundation(
                        inundation_map_file,
                        mosaic_attribute='depths_rasters' if depth_rast_flag else 'inundation_rasters',
                        mosaic_output=ouput_name,
                        mask=catchment_poly,
                        unit_attribute_name='huc8',
                        workers=1,
                        remove_inputs=clean,
                        subset=None,verbose=not quiet
                        )
    else:
        print("Compositing inundation polygons")
        ouput_name = ouput_name.replace('.tif', '.gpkg')
        union(var_keeper['ms']['outputs']['inundation_poly'],
              var_keeper['fr']['outputs']['inundation_poly'],
              ouput_name,
              clean)

def union(poly1_filepath, poly2_filepath, outfile, clean):

    # Load geopackages
    poly1 =  gpd.GeoDataFrame({"geometry":[gpd.read_file(poly1_filepath).unary_union]})
    poly2 =  gpd.GeoDataFrame({"geometry":[gpd.read_file(poly2_filepath).unary_union]})
    #poly1 =  gpd.read_file(poly1_filepath).unary_union
    #poly2 =  gpd.read_file(poly2_filepath).unary_union

    # Perform union
    #poly1.union(poly2)
    comp = gpd.overlay(poly1, poly2, how='union')
    print(outfile)
    comp.to_file(outfile, driver='GPKG', layer='inundation_composite')

    # Clean up inputs
    if clean:
        os.remove(poly1_filepath)
        os.remove(poly2_filepath)

    return

if __name__ == '__main__':
    
    # parse arguments
    parser = argparse.ArgumentParser(description='Rapid inundation mapping for FOSS FIM. Operates in single-HUC and batch modes.')
    parser.add_argument('-ms','--fim-dir-ms',help='Directory that contains MS FIM outputs.',required=True)
    parser.add_argument('-fr','--fim-dir-fr',help='Directory that contains FR FIM outputs.',required=True)
    parser.add_argument('-u','--huc',help='HUC within FIM directories to inunundate. Can be a comma-separated list.',required=True)
    parser.add_argument('-f','--flows_file',help='File path of forecast csv or comma-separated list of paths if running multiple HUCs',required=True)
    parser.add_argument('-o','--ouput-dir',help='Folder to write Composite Raster output.',required=True)
    parser.add_argument('-n','--ouput-name',help='File name for output(s).',default=None,required=False)
    parser.add_argument('-b','--bool-rast',help='.',required=False,default=False,action='store_true')
    parser.add_argument('-d','--depth-rast',help='.',required=False,default=False,action='store_true')
    parser.add_argument('-p','--polygon',help='.',required=False,default=False,action='store_true')
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
    ouput_name  = args['ouput_name']
    bool_rast   = bool(args['bool_rast'])
    depth_rast  = bool(args['depth_rast'])
    polygon     = bool(args['polygon'])
    clean       = bool(args['clean'])
    quiet       = bool(args['quiet'])

    assert num_workers >= 1, "Number of workers should be 1 or greater"
    assert len(forecasts) == len(hucs), "Number of hucs must be equal to the number of forecasts provided"

    # Create nested list for input into multi-threading
    arg_list = []
    for huc, forecast in zip(hucs, forecasts):
        arg_list.append((fim_dir_ms, fim_dir_fr, huc, forecast, output_dir, ouput_name, bool_rast, depth_rast, polygon, clean, quiet))

    # Multi-thread for each huc in input hucs
    with Pool(processes=num_workers) as pool:
        # Run composite_inundation()
        pool.starmap(composite_inundation, arg_list)
