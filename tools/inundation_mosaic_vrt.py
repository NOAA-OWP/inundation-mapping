# Created: 3/1/2022
# Primary developer(s): ryan.spies@noaa.gov

import os
import sys
import csv
import argparse
import rasterio
from rasterio.merge import merge
from osgeo import gdal, ogr
import multiprocessing
from multiprocessing import Pool

PREP_PROJECTION = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.2572221010042,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'

'''
The script ingests multiple HUC inundation extent rasters, converts them to boolean (0 or 1), and mosaics them together

Processing Steps
- Locate raster FIM extent files in input directory and create a list of hucs to process
- Use multiprocessing
- Create boolean extent rasters for each huc
- Use gdal virtual raster to create a mosaic of all boolean rasters
- Ouput new FIM mosaic raster

Inputs
- raster_directory:     fim directory containing individual HUC FIM rasters (output from inundation.py)
- output_path:          directory location for output mosaic file

Outputs
- raster_mosaic:        raster .tif file
'''

OUTPUT_BOOL_PARENT_DIR = '/data/inundation_review/inundate_nation/bool_temp/'
DEFAULT_OUTPUT_DIR = '/data/inundation_review/inundate_nation/mosaic_output/'

def create_bool_rasters(args):
    in_raster_dir = args[0]
    rasfile = args[1]
    output_bool_dir = args[2]

    print("Calculating boolean inundate raster: " + rasfile)
    p = in_raster_dir + rasfile
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


def vrt_raster_mosaic(output_bool_dir, ouput_dir, fim_version):
    #raster_to_mosaic = ['data/temp/ryan/inundate_nation/25_0_ms/25_0_ms_inund_extent_12090301.tif','data/temp/ryan/inundate_nation/25_0_ms/25_0_ms_inund_extent_12090302.tif']
    raster_to_mosaic = []
    for rasfile in os.listdir(output_bool_dir):
        if rasfile.endswith('.tif') and "extent" in rasfile:
            p = output_bool_dir + os.sep + rasfile
            print("Processing: " + p)
            raster_to_mosaic.append(p)

    print("Creating virtual raster...")
    vrt = gdal.BuildVRT(ouput_dir + "merged.vrt", raster_to_mosaic)

    print("Building raster mosaic...")
    gdal.Translate(output_dir + fim_version + "_mosaic.tif", vrt, xRes = 10, yRes = -10, creationOptions = ['COMPRESS=LZW','TILED=YES','PREDICTOR=2'])
    vrt = None

if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Converts huc inundation extent rasters to boolean rasters and then creates a mosaic of all input rasters')
    parser.add_argument('-in_dir','--inund-rast-dir',help='Parent directory of FIM inundation rasters',required=True)
    parser.add_argument('-out_dir','--out-mosaic-dir',help='Directory to output raster mosaic (if blank - use default location)',required=False,default="")
    parser.add_argument('-j','--job-number',help='Number of jobs to use',required=False,default=2)

    args = vars(parser.parse_args())

    input_raster_extent_dir = args['inund_rast_dir']
    output_dir = args['out_mosaic_dir']
    job_number = int(args['job_number'])

    assert os.path.isdir(input_raster_extent_dir), 'ERROR: could not find the input raster directory location: ' + str(input_raster_extent_dir)
    print("Input Raster Directory: " + str(input_raster_extent_dir))
    fim_version = os.path.basename(os.path.normpath(input_raster_extent_dir))
    print("fim_version: " + fim_version)
    
    output_bool_dir = os.path.join(OUTPUT_BOOL_PARENT_DIR, fim_version)
    if not os.path.exists(output_bool_dir):
        print('Creating new output directory for boolean temporary outputs: ' + str(output_bool_dir))
        os.mkdir(output_bool_dir)

    if output_dir == "":
        output_dir = DEFAULT_OUTPUT_DIR
    if not os.path.exists(output_dir):
        print('Creating new output directory: ' + str(output_dir))
        os.mkdir(output_dir)

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    procs_list = []
    for rasfile in os.listdir(input_raster_extent_dir):
        if rasfile.endswith('.tif') and "extent" in rasfile:
            #p = input_raster_extent_dir + rasfile
            procs_list.append([input_raster_extent_dir,rasfile,output_bool_dir])

    # Multiprocess --> create boolean inundation rasters for all hucs
    if len(procs_list) > 0:
        with Pool(processes=job_number) as pool:
            pool.map(create_bool_rasters, procs_list)
    else:
        print('Did not find any valid FIM extent rasters: ' + input_raster_extent_dir)

    # Perform VRT creation and final mosaic using boolean rasters
    vrt_raster_mosaic(output_bool_dir,output_dir,fim_version)