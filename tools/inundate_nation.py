import argparse
import os
import rasterio
from rasterio.merge import merge
from osgeo import gdal, ogr

from inundation import inundate
import multiprocessing
from multiprocessing import Pool

INUN_REVIEW_DIR = r'/data/inundation_review/inundation_nwm_recurr/'
INUN_OUTPUT_DIR = r'/data/inundation_review/inundate_nation/'
INPUTS_DIR = r'/data/inputs'
OUTPUT_BOOL_PARENT_DIR = '/data/inundation_review/inundate_nation/bool_temp/'
DEFAULT_OUTPUT_DIR = '/data/inundation_review/inundate_nation/mosaic_output/'
PREP_PROJECTION = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.2572221010042,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'


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
    
    # Define file paths for use in inundate().
    fim_run_parent = os.path.join(fim_run_dir, huc)
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    mask_type = 'huc'
    catchment_poly = ''
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')
    catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    inundation_raster = os.path.join(magnitude_output_dir, magnitude + '_' + config + '_inund_extent.tif')
    depth_raster = os.path.join(magnitude_output_dir, magnitude + '_' + config + '_inund_depth.tif')
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    # Run inundate() once for depth and once for extent.
    if not os.path.exists(depth_raster) and depth_option:
        print("Running the NWM recurrence intervals for HUC inundation (depth): " + huc + ", " + magnitude + "...\n")
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=None,inundation_polygon=None,
                 depths=depth_raster,out_raster_profile=None,out_vector_profile=None,quiet=True
                )
        
    if not os.path.exists(inundation_raster):
        print("Running the NWM recurrence intervals for HUC inundation (extent): " + huc + ", " + magnitude + "...")
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                 depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
                )

def create_bool_rasters(args):
    in_raster_dir = args[0]
    rasfile = args[1]
    output_bool_dir = args[2]

    print("Calculating boolean inundate raster: " + rasfile)
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


def vrt_raster_mosaic(output_bool_dir, output_mos_dir, fim_version_tag):
    #raster_to_mosaic = ['data/temp/ryan/inundate_nation/25_0_ms/25_0_ms_inund_extent_12090301.tif','data/temp/ryan/inundate_nation/25_0_ms/25_0_ms_inund_extent_12090302.tif']
    raster_to_mosaic = []
    for rasfile in os.listdir(output_bool_dir):
        if rasfile.endswith('.tif') and "extent" in rasfile:
            p = output_bool_dir + os.sep + rasfile
            print("Processing: " + p)
            raster_to_mosaic.append(p)

    print("Creating virtual raster...")
    vrt = gdal.BuildVRT(output_mos_dir + "merged.vrt", raster_to_mosaic)

    print("Building raster mosaic: " + str(output_mos_dir + fim_version_tag + "_mosaic.tif"))
    gdal.Translate(output_mos_dir + fim_version_tag + "_mosaic.tif", vrt, xRes = 10, yRes = -10, creationOptions = ['COMPRESS=LZW','TILED=YES','PREDICTOR=2'])
    vrt = None        

if __name__ == '__main__':
    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using streamflow recurrence interflow data. Inundation outputs are stored in the /inundation_review/inundation_nwm_recurr/ directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test)',required=True)
    parser.add_argument('-o', '--output-dir',help='Optional: The path to a directory to write the outputs. If not used, the inundation_nation directory is used by default -> type=str',required=False, default="")
    parser.add_argument('-m', '--magnitude-list', help = 'List of NWM recurr flow intervals to process (Default: 100_0) (Other options: 2_0 5_0 10_0 25_0 50_0 100_0)', nargs = '+', default = ['100_0'], required = False)
    parser.add_argument('-d', '--depth',help='Optional flag to produce inundation depth rasters (extent raster created by default)', action='store_true')
    parser.add_argument('-s', '--mosaic',help='Optional flag to mosaic FIM extent rasters (use inundate_nation.py)', action='store_true')
    parser.add_argument('-j', '--job-number',help='The number of jobs',required=False,default=1)
        
    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    output_dir = args['output_dir']
    depth_option = args['depth']
    magnitude_list = args['magnitude_list']
    mosaic_option = args['mosaic']
    job_number = int(args['job_number'])

    assert os.path.isdir(fim_run_dir), 'ERROR: could not find the input fim_dir location: ' + str(fim_run_dir)
    print("Input FIM Directory: " + str(fim_run_dir))
    huc_list = os.listdir(fim_run_dir)
    fim_version = os.path.basename(os.path.normpath(fim_run_dir))
    print("Using fim version: " + str(fim_version))
    exit
    for magnitude in magnitude_list:
        print("Preparing to generate inundation outputs for magnitude: " + str(magnitude))
        nwm_recurr_file = os.path.join(INUN_REVIEW_DIR, 'nwm_recurr_flow_data', 'nwm21_17C_recurr_' + magnitude + '_cms.csv')
        assert os.path.isfile(nwm_recurr_file), 'ERROR: could not find the input NWM recurr flow file: ' + str(nwm_recurr_file)
        print("Input flow file: " + str(nwm_recurr_file))
        
        if 'ms' in fim_version:
            config = 'ms'
        elif 'fr' in fim_version:
            config = 'fr'
        else:
            config = '-'

        if output_dir == "":
            output_dir = INUN_OUTPUT_DIR
        if not os.path.exists(output_dir):
            print('Creating new output directory: ' + str(output_dir))
            os.mkdir(output_dir)
            
        procs_list = []
        
        for huc in huc_list:
            if huc != 'logs' and huc != 'aggregate_fim_outputs':
                for magnitude in magnitude_list:
                    magnitude_output_dir = os.path.join(output_dir, magnitude + '_' + config  + '_' + fim_version)
                    if not os.path.exists(magnitude_output_dir):
                        os.mkdir(magnitude_output_dir)
                        print(magnitude_output_dir)
                    procs_list.append([fim_run_dir, huc, magnitude, magnitude_output_dir, config, nwm_recurr_file, depth_option])
                
        # Multiprocess.
        if job_number > 1:
            with Pool(processes=job_number) as pool:
                pool.map(run_inundation, procs_list)

        # Perform mosaic operation
        if mosaic_option:
            print("Performing mosaic process...")
            fim_version_tag = os.path.basename(os.path.normpath(magnitude_output_dir))
            print("fim_version: " + fim_version_tag)
            output_bool_dir = os.path.join(OUTPUT_BOOL_PARENT_DIR, fim_version_tag)
            if not os.path.exists(output_bool_dir):
                print('Creating new output directory for boolean temporary outputs: ' + str(output_bool_dir))
                os.mkdir(output_bool_dir)

            output_mos_dir = DEFAULT_OUTPUT_DIR
            if not os.path.exists(output_mos_dir):
                print('Creating new output directory: ' + str(output_mos_dir))
                os.mkdir(output_mos_dir)

            if job_number > available_cores:
                job_number = available_cores - 1
                print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

            procs_list = []
            for rasfile in os.listdir(magnitude_output_dir):
                if rasfile.endswith('.tif') and "extent" in rasfile:
                    #p = magnitude_output_dir + rasfile
                    procs_list.append([magnitude_output_dir,rasfile,output_bool_dir])

            # Multiprocess --> create boolean inundation rasters for all hucs
            if len(procs_list) > 0:
                with Pool(processes=job_number) as pool:
                    pool.map(create_bool_rasters, procs_list)
            else:
                print('Did not find any valid FIM extent rasters: ' + magnitude_output_dir)

            # Perform VRT creation and final mosaic using boolean rasters
            vrt_raster_mosaic(output_bool_dir,output_mos_dir,fim_version_tag)

