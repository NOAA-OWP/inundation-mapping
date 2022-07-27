import argparse
import os
import rasterio
from osgeo import gdal
import pandas as pd
from inundation import inundate
import multiprocessing
from multiprocessing import Pool
from gms_tools.mosaic_inundation import Mosaic_inundation
import shutil
import time
import logging

INUN_REVIEW_DIR = r'/data/inundation_review/inundation_nwm_recurr/'
INUN_OUTPUT_DIR = r'/data/inundation_review/inundate_nation/'
INPUTS_DIR = r'/data/inputs'
OUTPUT_BOOL_PARENT_DIR = '/data/inundation_review/inundate_nation/bool_temp/'
DEFAULT_OUTPUT_DIR = '/data/inundation_review/inundate_nation/mosaic_output/'
PREP_PROJECTION = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.2572221010042,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'

'''
The script involves several steps need to produce a national inundation mosaic tif file using FIM outputs and a NWM recurr flow file

Processing Steps
- Create a hucs processing list
- Pass huc procs_list to multiprocessing function to produce MS & FR inundation rasters
- Create boolean rasters for all inundation rasters
- Perform MS + FR mosaic operation
- Perform national mosaic operation for all avialable HUCs by resolution (using virtual raster)
- Ouput new FIM mosaic raster

Inputs
- fr-fim-run-dir:     fim directory containing individual HUC FIM rasters (output from inundation.py)
- ms-fim-run-dir:     directory location for output mosaic file
- huc_list:           OPTIONAL: HUC within FIM directories to inunundate. Can be a comma-separated list.
- output-dir:         OPTIONAL: The path to a directory to write the outputs. If not used, the inundation_nation directory is used by default
- magnitude-list:     OPTIONAL: List of NWM recurr flow intervals to process (Default: 100_0) (Other options: 2_0 5_0 10_0 25_0 50_0 100_0)
- depth:              OPTIONAL: use flag to produce inundation depth rasters (default=False)
- mosaic-fr-ms:       OPTIONAL: use flag to NOT produce mosaic (MS + FR) FIM extent rasters for each huc (default=True)
- mosaic-nation:      OPTIONAL: use flag to NOT produce nation mosaic of FIM boolean rasters (default=True)
- fr-ms-nation-outputs: OPTIONAL flag to output a national mosaic tiff for MS & FR in addition to default composite (Warning: long runtime) Default=False
- overwrite:          OPTIONAL: use flag to overwrite existing FIM inundation extent rasters (default=False)
- job-number:         OPTIONAL: the number of muliprocessing jobs to use

Outputs
- national-mosaic:    boolean inundation raster containing all available inundated HUCs
'''

def magnitude_loop(magnitude,magnitude_list,magnitude_output_dir,fr_fim_run_dir,ms_fim_run_dir,depth_option,mosaic_nations_option,nation_out_ms_fr,overwrite_flag,fim_version,job_number):    
    procs_list = []
    for huc in hucs_list:
        if os.path.isdir(fr_fim_run_dir + os.sep + huc):
            config = 'fr'
            procs_list.append([fr_fim_run_dir, huc, magnitude, magnitude_output_dir, config, nwm_recurr_file, depth_option, overwrite_flag])
        else:
            logging.warning('FR FIM outputs do not exists for huc: ' + fr_fim_run_dir + os.sep + huc)
        if os.path.isdir(ms_fim_run_dir + os.sep + huc):
            config = 'ms'
            procs_list.append([ms_fim_run_dir, huc, magnitude, magnitude_output_dir, config, nwm_recurr_file, depth_option, overwrite_flag])
        else:
            logging.warning('MS FIM outputs do not exists for huc: ' + ms_fim_run_dir + os.sep + huc)
            
    ## 1) Pass huc procs_list to multiprocessing function to produce MS & FR inundation rasters
    multi_process_inundation(run_inundation, procs_list)
    logging.warning('Completed FIM generation...')

    ## 2) Create boolean rasters for all inundation rasters
    logging.warning("\n\nPerforming boolean raster process...")
    output_bool_dir = magnitude_output_dir
    procs_list_bool = []
    for rasfile in os.listdir(magnitude_output_dir):
        if rasfile.endswith('.tif') and "extent" in rasfile and 'bool' not in rasfile and 'composite' not in rasfile:
            #p = magnitude_output_dir + rasfile
            procs_list_bool.append([magnitude_output_dir,rasfile,output_bool_dir])
    # Multiprocess --> create boolean inundation rasters for all hucs
    assert len(procs_list_bool) > 0, 'ERROR: Did not find any valid FIM extent rasters: ' + magnitude_output_dir
    multi_process_boolean(create_bool_rasters, procs_list_bool)

    ## 3) Perform MS + FR mosaic operation
    if mosaic_ms_fr_option:
        logging.warning("\n\nPerforming MS + FR composite process...")
        procs_list_ms_fr =[]
        for huc in hucs_list:
            fr_bool_file = magnitude_output_dir + os.sep + 'bool_' + magnitude + '_fr_inund_extent_' + huc + '.tif'
            ms_bool_file = magnitude_output_dir + os.sep + 'bool_' + magnitude + '_ms_inund_extent_' + huc + '.tif'
            if os.path.isfile(fr_bool_file) and os.path.isfile(ms_bool_file): # if fr and ms bool rasters exist for huc then create a composite
                procs_list_ms_fr.append([magnitude_output_dir, magnitude, fr_fim_run_dir, huc])
            elif os.path.isfile(fr_bool_file):
                shutil.copy(fr_bool_file,magnitude_output_dir + os.sep + 'bool_' + magnitude + '_composite_inund_extent_' + huc + '.tif') #copy the fr bool raster and name it with composite
            elif os.path.isfile(ms_bool_file):
                logging.warning('WEIRD!!!!: found a MS file but not a FR for HUC: ' + str(huc))
        if len(procs_list_ms_fr) > 0:
            multi_process_composite(mosaic_ms_fr_fim, procs_list_ms_fr)

    ## 4) Perform national mosaic operation for all avialable HUCs by resolution (using virtual raster)
    if mosaic_nations_option:
        logging.warning("\n\nPerforming mosaic process to combine all nation hucs inundation...")
        # Perform VRT creation and final mosaic using boolean rasters
        output_mos_dir = DEFAULT_OUTPUT_DIR
        if not os.path.exists(output_mos_dir):
            logging.warning('Creating new output directory: ' + str(output_mos_dir))
            os.mkdir(output_mos_dir)
        vrt_raster_mosaic_nation(magnitude,output_bool_dir,output_mos_dir,fim_version,nation_out_ms_fr)

def run_inundation(args):
    """
    This script is a wrapper for the inundate function and is designed for multiprocessing.
    
    Args:
        args (list): [fim_run_dir (str), huc (str), magnitude (str), magnitude_output_dir (str), config (str), forecast (str), depth_option (str)]
    
    """
    
    fim_run_dir = args[0]
    huc = args[1]
    magnitude = args[2]
    magnitude_output_dir = args[3]
    config = args[4]
    forecast = args[5]
    depth_option = args[6]
    overwrite_flag = args[7]
    
    # Define file paths for use in inundate().
    fim_run_parent = os.path.join(fim_run_dir, huc)
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    mask_type = 'filter'
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')
    catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    inundation_raster = os.path.join(magnitude_output_dir, magnitude + '_' + config + '_inund_extent.tif')
    depth_raster = os.path.join(magnitude_output_dir, magnitude + '_' + config + '_inund_depth.tif')
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    # Check that hydroTable file size is reasonable
    fsize = os.path.getsize(hydro_table) * 0.000001
    if fsize > 400:
        logging.warning('WARNING: ' + str(huc) + ' hydroTable.csv file size is greater than 400mb - expect slow run time!')

    logging.warning('Inundating: ' + str(huc) + ' ' + config)
    
    # Run inundate once for depth (turned off by default) and once for extent.
    if depth_option:
        if not os.path.isfile(depth_raster[:-4] + '_' + str(huc) + '.tif') or overwrite_flag:
            logging.warning("Running the NWM recurrence intervals for HUC inundation (depth): " + huc + ", " + magnitude + "...\n")
            inundate(
                    rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                    subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=None,inundation_polygon=None,
                    depths=depth_raster,out_raster_profile=None,out_vector_profile=None,quiet=True
                    )
        else:
            logging.warning("Inundation raster already exists for huc (skipping): " + str(huc) + " - use overwrite flag to reproduce raster")
            
    if not os.path.isfile(inundation_raster[:-4] + '_' + str(huc) + '.tif') or overwrite_flag:
        logging.warning("Running the NWM recurrence intervals for HUC inundation (extent): " + huc + ", " + magnitude + "...")
        inundate(rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                 depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True)
    else:
        logging.warning("Inundation raster already exists for huc (skipping): " + str(huc) + " - use overwrite flag to reproduce raster")

def create_bool_rasters(args):
    in_raster_dir = args[0]
    rasfile = args[1]
    output_bool_dir = args[2]

    logging.warning("Calculating boolean inundate raster: " + rasfile)
    p = in_raster_dir + os.sep + rasfile
    raster = rasterio.open(p)
    profile = raster.profile
    array = raster.read()
    del raster
    array[array>0] = 1
    array[array<=0] = 0
    # And then change the band count to 1, set the
    # dtype to uint8, and specify LZW compression.
    profile.update(driver="GTiff",
                height=array.shape[1],
                width=array.shape[2],
                tiled=True,
                nodata=0,
                blockxsize=512, 
                blockysize=512,
                dtype='int8',
                crs=PREP_PROJECTION,
                compress='lzw')
    with rasterio.open(output_bool_dir + os.sep + "bool_" + rasfile, 'w', **profile) as dst:
        dst.write(array.astype(rasterio.int8))

def mosaic_ms_fr_fim(args):
    magnitude_output_dir = args[0]
    magnitude            = args[1]
    fr_fim_run_dir       = args[2]
    huc                  = args[3]
    # Composite MS and FR
    logging.warning('Performing MS + FR composite for huc: ' + str(huc))
    inundation_map_file = { 
            'huc8' : [huc] * 2,
            'branchID' : [None] * 2,
            'inundation_rasters':  [magnitude_output_dir + os.sep + 'bool_' + magnitude + '_fr_inund_extent_' + huc + '.tif', 
                                    magnitude_output_dir + os.sep + 'bool_' + magnitude + '_ms_inund_extent_' + huc + '.tif']
            }
    output_name = os.path.join(magnitude_output_dir, 'bool_' + magnitude + '_composite_inund_extent' + '.tif')
    logging.warning(output_name)
    inundation_map_file = pd.DataFrame(inundation_map_file)
    catchment_poly = os.path.join(fr_fim_run_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    Mosaic_inundation(
                    inundation_map_file,
                    mosaic_attribute='inundation_rasters',
                    mosaic_output=output_name,
                    mask=catchment_poly,
                    unit_attribute_name='huc8',
                    nodata=0,
                    workers=1,
                    remove_inputs=False,
                    subset=None,verbose=not False
                    )
    return(output_name)
    logging.warning('Completed composite for huc: ' + str(huc))

def vrt_raster_mosaic_nation(magnitude, output_bool_dir, output_mos_dir, fim_version, nation_out_ms_fr):
    if nation_out_ms_fr:
        res_queue = ['_ms_','_fr_','_composite_']
    else:
        res_queue = ['_composite_']
    for res in res_queue:
        raster_to_mosaic = []
        for rasfile in os.listdir(output_bool_dir):
            if rasfile.endswith('.tif') and "bool_" in rasfile and res in rasfile:
                p = output_bool_dir + os.sep + rasfile
                logging.warning("Processing: " + p)
                raster_to_mosaic.append(p)

        logging.warning("Creating virtual raster...")
        vrt = gdal.BuildVRT(output_mos_dir + "merged_" + magnitude + ".vrt", raster_to_mosaic)

        logging.warning("Building raster mosaic: " + str(output_mos_dir + fim_version + res + "mosaic.tif"))
        gdal.Translate(output_mos_dir + fim_version + res + "mosaic_" + magnitude + ".tif", vrt, xRes = 10, yRes = -10, creationOptions = ['COMPRESS=LZW','TILED=YES','PREDICTOR=2'])
        vrt = None

def multi_process_inundation(run_inundation, procs_list):
    logging.warning(f"Performing inundation for {len(procs_list)} hucs using {job_number} jobs")
    with Pool(processes=job_number) as pool:
        pool.map(run_inundation, procs_list)
        logging.warning("Multiprocessing inundation pool jobs completed")  
        pool.close()
        pool.join()   

def multi_process_boolean(create_bool_rasters, procs_list_bool):
    logging.warning(f"Calculating boolean inundation rasters for {len(procs_list_bool)} files using {job_number} jobs")
    with Pool(processes=job_number) as pool:
        pool.map(create_bool_rasters, procs_list_bool)
        pool.close()
        pool.join()
        pool.terminate()

def multi_process_composite(mosaic_ms_fr_fim, procs_list):
    logging.warning(f"Calculating composite inundation rasters for {len(procs_list)} files using {job_number} jobs")
    with Pool(processes=job_number) as pool:
        pool.map(mosaic_ms_fr_fim, procs_list)
        pool.close()
        pool.join()
        pool.terminate()  

if __name__ == '__main__':
    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using streamflow recurrence interflow data. Inundation outputs are stored in the /inundation_review/inundation_nwm_recurr/ directory.')
    parser.add_argument('-fr','--fr-fim-run-dir',help='Name of directory containing outputs of FR fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test_fr)',required=True)
    parser.add_argument('-ms','--ms-fim-run-dir',help='Name of directory containing outputs of MS fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test_ms)',required=True)
    parser.add_argument('-u','--huc',help='OPTIONAL: HUC(s) within FIM directories to inunundate. Can be a comma-separated list. (will look for HUCs in the FR FIM outputs directory if None provided)',required=False,default=None)
    parser.add_argument('-o', '--output-dir',help='OPTIONAL: The path to a directory to write the outputs. If not used, the inundation_nation directory is used by default -> type=str',required=False, default=None)
    parser.add_argument('-m', '--magnitude-list', help = 'OPTIONAL: List of NWM recurr flow intervals to process (Default: 100_0) (Other options: 2_0 5_0 10_0 25_0 50_0 100_0)', nargs = '+', default = ['100_0'], required = False)
    parser.add_argument('-d', '--depth',help='OPTIONAL: use flag to produce inundation depth rasters (default=False)',default=False, action='store_true')
    parser.add_argument('-c', '--mosaic-fr-ms',help='OPTIONAL: use flag to NOT produce mosaic (MS + FR) FIM extent rasters for each huc (default=True)', default=True, action='store_false')
    parser.add_argument('-s', '--mosaic-nation',help='OPTIONAL: use flag to NOT produce nation mosaic of FIM boolean rasters (default=True)', default=True, action='store_false')
    parser.add_argument('-smf', '--fr-ms-nation-outputs',help='OPTIONAL: use flag to output a national mosaic tiff for MS & FR in addition to default composite (Warning: long runtime) Default=False', default=False, action='store_true')
    parser.add_argument('-x', '--overwrite',help='OPTIONAL: use flag to overwrite existing FIM inundation extent rasters (default=False)',default=False,action='store_true')
    parser.add_argument('-j', '--job-number',help='OPTIONAL: The number of multiprocessing jobs (default=2)',required=False,default=2)
        
    args = vars(parser.parse_args())

    fr_fim_run_dir  = args['fr_fim_run_dir']
    ms_fim_run_dir  = args['ms_fim_run_dir']
    hucs_input      = args['huc']
    output_dir      = args['output_dir']
    depth_option    = args['depth']
    magnitude_list  = args['magnitude_list']
    mosaic_ms_fr_option   = args['mosaic_fr_ms']
    mosaic_nations_option   = args['mosaic_nation']
    nation_out_ms_fr   = args['fr_ms_nation_outputs']
    overwrite_flag  = args['overwrite']
    job_number      = int(args['job_number'])

    assert os.path.isdir(fr_fim_run_dir), 'ERROR: could not find the input FR fim_dir location: ' + str(fr_fim_run_dir)
    print("Input FR FIM Directory: " + str(fr_fim_run_dir))
    assert os.path.isdir(ms_fim_run_dir), 'ERROR: could not find the input MS fim_dir location: ' + str(ms_fim_run_dir)
    print("Input MS FIM Directory: " + str(ms_fim_run_dir))
    
    if hucs_input == None:
        print("Creating huc list from the FR fim_dir...")
        hucs  = os.listdir(fr_fim_run_dir)
        hucs_list = []
        for huc in hucs:
            if os.path.isdir(fr_fim_run_dir + os.sep + huc):
                if huc != 'logs' and huc != 'log' and huc != 'aggregate_fim_outputs':
                    hucs_list.append(huc)
        print("Found " + str(len(hucs)) + " hucs to inundate...")
    else:
        hucs_list = hucs_input.replace(' ', '').split(',')
    assert len(hucs_list)>0, 'ERROR: could not produce a list of hucs to inundate'

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    fim_version = os.path.basename(os.path.normpath(fr_fim_run_dir)).replace('_fr','')
    print("Using fim version: " + str(fim_version))

    for magnitude in magnitude_list:
        if output_dir == None:
            output_dir = INUN_OUTPUT_DIR + fim_version
        if not os.path.exists(output_dir):
            print('Creating new output directory: ' + str(output_dir))
            os.mkdir(output_dir)

        magnitude_output_dir = os.path.join(output_dir, magnitude)
        if not os.path.exists(magnitude_output_dir):
            os.mkdir(magnitude_output_dir)
            print(magnitude_output_dir)

        # Create log output
        level    = logging.WARNING # using WARNING level to avoid benign? info messages ("Failed to auto identify EPSG: 7")
        format   = '  %(message)s'
        handlers = [logging.FileHandler(os.path.join(output_dir, 'log_inundate_nation_' + str(magnitude) + '.log')), logging.StreamHandler()]
        logging.basicConfig(level = level, format = format, handlers = handlers)

        logging.warning("Preparing to generate inundation outputs for NWM flow recurr magnitude: " + str(magnitude))
        nwm_recurr_file = os.path.join(INUN_REVIEW_DIR, 'nwm_recurr_flow_data', 'nwm21_17C_recurr_' + magnitude + '_cms.csv')
        assert os.path.isfile(nwm_recurr_file), 'ERROR: could not find the input NWM recurr flow file: ' + str(nwm_recurr_file)
        logging.warning("Using NWM v2.1 17C recurrence flow data")
        logging.warning("Input flow file: " + str(nwm_recurr_file))
        
        magnitude_loop(magnitude,magnitude_list,magnitude_output_dir,fr_fim_run_dir,ms_fim_run_dir,depth_option,mosaic_nations_option,nation_out_ms_fr,overwrite_flag,fim_version,job_number)
