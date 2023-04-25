#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import subprocess
import sys
import traceback
from rasterio.enums import Resampling
import numpy as np
import pandas as pd
import geopandas as gpd
from itertools import product
import shapely
from shapely.geometry import box, Polygon, MultiPolygon
from osgeo.gdal import BuildVRT, BuildVRTOptions
import dask
from tqdm.dask import TqdmCallback
import py3dep
import xarray as xr
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from tqdm import tqdm
from typing import Union
sys.path.append('/foss_fim/src')
import utils.shared_variables as sv
import utils.shared_functions as sf

from utils.shared_functions import FIM_Helpers as fh

# local constants (until changed to input param)
# This URL is part of a series of vrt data available from USGS via an S3 Bucket.
# for more info see: "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/". The odd folder numbering is
# a translation of arc seconds with 13m  being 1/3 arc second or 10 meters.
# __USGS_3DEP_10M_VRT_URL = r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'  # 10m = 13 (1/3 arc second)


def acquire_and_preprocess_3dep_dems(extent_file_path:str,
                                     dem_resolution:int,
                                     target_output_folder_path:str = '', 
                                     number_of_jobs:int = 1, 
                                     retry:bool = False,
                                     tile_size:Union[int,float] = 1000,
                                     ndv:Union[float,int] = sv.elev_raster_ndv,
                                     wbd_buffer:Union[float,int]=2000
                                    ):
    
    '''
    Overview
    ----------
    This will download 3dep rasters from USGS using USGS vrts.
    By default USGS 3Dep stores all their rasters in lat/long (northing and easting).
    By us downloading the rasters using WBD huc clips and gdal, we an accomplish a few extra
    steps.
        1) Ensure the projection types that are downloaded are consistant and controlled.
           We are going to download them as NAD83 basic (espg: 4269) which is consistant
           with other data sources, even though FIM defaults to ESRI:102039. We will
           change that as we add the clipped version per HUC8.
        2) ensure we are adjusting blocksizes, compression and other raster params
        3) Create the 3dep rasters in the size we want (default at huc for now)
        
    Notes:
        - As this is a very low use tool, all values such as the USGS vrt path, output
          folder paths, huc unit level (huc), etc are all hardcoded
        
    Parameters
    ----------
        - extent_file_path (str):
            Location of where the extent files that are to be used as clip extent against
            the USGS 3Dep vrt url.
            ie) \data\inputs\wbd\huc
            
        - target_output_folder_path (str):
            The output location of the new 3dep dem files. When the param is not submitted,
            it will be sent to /data/input/usgs/3dep_dems/10m/.
    
        - number_of_jobs (int):
            This program supports multiple procs if multiple procs/cores are available.
            
        - retry (True / False):
            If retry is True and the file exists (either the raw downloaded DEM and/or)
            the projected one, then skip it
    '''
    # -------------------
    # Validation
    total_cpus_available = os.cpu_count() - 1
    if number_of_jobs > total_cpus_available:
        raise ValueError(f'The number of jobs {number_of_jobs}'\
                          'exceeds your machine\'s available CPU count minus one. '\
                          'Please lower the number of jobs '\
                          'values accordingly.'
                        )

    if (not os.path.exists(extent_file_path)):
        raise ValueError(f'extent_file_path value of {extent_file_path}'\
                          ' not set to a valid path')
    
    if (target_output_folder_path is None) or (target_output_folder_path == ""):
        target_output_folder_path = os.environ['usgs_3dep_dems_10m']
    
    if (not os.path.exists(target_output_folder_path)):
        raise ValueError(f"Output folder path {target_output_folder_path} does not exist" )
   
    # -------------------
    # setup logs
    start_time = datetime.now()
    fh.print_start_header('Loading 3dep dems', start_time)
   
    #print(f"Downloading to {target_output_folder_path}")
    __setup_logger(target_output_folder_path)
    logging.info(f"Downloading to {target_output_folder_path}")
    
    
    # -------------------
    # processing
    
    # Get the WBD .gpkg files (or clip extent)
    extent_file_names = fh.get_file_names(extent_file_path, 'gpkg')
    msg = f"Extent files coming from {extent_file_path}"
    print(msg)
    logging.info(msg)
   
    # download dems, setting projection, block size, etc
    __download_usgs_dems(extent_file_names, dem_resolution, tile_size, wbd_buffer, target_output_folder_path, number_of_jobs, retry)

    polygonize(target_output_folder_path)
    
    end_time = datetime.now()
    fh.print_end_header('Loading 3dep dems', start_time, end_time)
    print(f'---- NOTE: Remember to scan the log file for any failures')
    logging.info(fh.print_date_time_duration(start_time, end_time))


def retrieve_and_reproject_3dep_for_huc( extent_file:str,
                                         tile_size:Union[int,float],
                                         dem_resolution:int,
                                         target_output_folder_path:str=None,
                                         ndv:Union[float,int]=sv.elev_raster_ndv,
                                         number_of_jobs:int=1,
                                         wbd_buffer:Union[float,int]=2000,
                                         retry:bool=False
                                       ):

    def __fishnet_geometry(geometry,cell_size_meters=1000,cell_buffer=10):
        
        xmin, ymin, xmax, ymax = geometry.bounds.values[0]

        grid_cells = [ gpd.GeoSeries(data=box(x0, y0, 
                                              x0-(cell_size_meters+cell_buffer), 
                                              y0+(cell_size_meters+cell_buffer)), 
                                     crs=geometry.crs)
                       for x0, y0 in product( np.arange(xmin,xmax + cell_size_meters,cell_size_meters),
                       np.arange(ymin,ymax + cell_size_meters,cell_size_meters) )
                     ]


        grid_cells = [gc.intersection(geometry)[0] for gc in grid_cells]

        check_intersection = lambda g: (not g.is_empty) & g.is_valid & ( isinstance(g,Polygon) | isinstance(g,MultiPolygon) )

        grid_cells = filter(check_intersection,grid_cells)
        grid_cells = list(grid_cells)

        return(grid_cells)


    print('Retrieving and Processing 3DEP Data ...')

    # directory location
    os.makedirs(target_output_folder_path,exist_ok=True)

    extent = gpd.read_file(extent_file)

    huc = str.split(os.path.splitext(os.path.basename(extent_file))[0],'_')[1]

    # get geometry
    geometry = extent['geometry']

    # buffer
    geometry = geometry.buffer(wbd_buffer)
    
    # fishnet geometry
    gbs = __fishnet_geometry(geometry,tile_size,dem_resolution*4)
    geometry_boxes = gbs

    target_output_folder_paths = [target_output_folder_path] * len(gbs)
    hucs = [huc] * len(gbs)

    number_of_boxes = len(geometry_boxes)
    input_dict = [ geometry_boxes, 
                   [dem_resolution for _ in range(number_of_boxes)], 
                   [geometry.crs for _ in range(number_of_boxes)],
                   [ndv for _ in range(number_of_boxes)],
                   target_output_folder_paths,
                   hucs,
                   [int(idx) for idx in range(number_of_boxes)]
                 ]


    def __get_tile_from_nhd(geometry,huc):

        # open
        huc = huc[:4]
        nhd_dem_fp = os.path.join('data','inputs','nhdplus_rasters',f'HRNHDPlusRasters{huc}','elev_m.tif')
        nhd_dem = xr.open_rasterio(nhd_dem_fp)
        
        # clipping
        geometry = gpd.GeoSeries([geometry]).to_json()
        
        try:
            nhd_dem = nhd_dem.rio.clip(geometry)
        except ValueError:
            print('No tiles for NHD')
            return None

        return(nhd_dem)


    def __retrieve_and_process_single_3dep_dem(geometry,dem_resolution,wbd_crs,ndv,target_output_folder_path,huc,idx):

        max_retries = 5; retries = 0
        nhd_failed, failed_3dep = False, False
        while True:
            try:
                
                dem = py3dep.get_map( 'DEM',
                                      geometry=geometry,
                                      resolution=dem_resolution,
                                      geo_crs=wbd_crs
                                    )
                break
            
            except Exception as e:
                
                print(f'{e} - idx: {idx} | retries: {retries}')
                retries += 1
                
                if retries < max_retries:
                    continue
                else:
                    print(f'{e} - idx: {idx} | retries: {retries} - Using NHD')
                    failed_3dep = True
                    try:
                        dem = __get_tile_from_nhd(geometry,huc)
                    except Exception as e:
                        nhd_failed = True
                        dem = None
                        print(f'{e} - idx: {idx} | retries: {retries} - NHD Failed')
                        #breakpoint()
                    break
            
        log_dict = {
                    'idx' : idx, 'huc' : huc,
                    'retries' : retries, 'NHD Failed' : nhd_failed, '3DEP Failed' : failed_3dep,
                    'Both failed' : all([nhd_failed,failed_3dep])
                   }

        if (log_dict['NHD Failed']) | (log_dict['3DEP Failed']):
            log_dict['geometry'] = geometry
            log_dict['dem_resolution'] = dem_resolution
            log_dict['wbd_crs'] = wbd_crs
            log_dict['ndv'] = ndv
            log_dict['target_output_folder_path'] = target_output_folder_path
            log_dict['huc'] = huc
            
        
        if dem is None:
            return(log_dict)

        # reproject and resample
        #dem = dask.delayed(dem.rio.reproject)( wbd_crs,
        dem = dem.rio.reproject( wbd_crs,
                                 resolution=dem_resolution,
                                 resampling=Resampling.bilinear
                               )

        # reset ndv to project value
        #print(f'{huc} ndv')
        #dem = dask.delayed(dem.rio.write_nodata)(ndv)
        dem = dem.rio.write_nodata(ndv)

        # write out
        #print(f'{huc} writing')
        if target_output_folder_path:
        
            # make file name
            #dem_file_name = dask.delayed(os.path.join)(target_output_folder_path,f'dem_3dep_{huc}_{i}.tif')
            dem_file_name = os.path.join(target_output_folder_path,f'dem_3dep_{huc}_{int(dem_resolution)}m_{idx}.tif')
            
            # write file
            #dem = dem.rio.to_raster(dem_file_name,windowed=True,compute=False)
            dem = dem.rio.to_raster(dem_file_name,windowed=True,compute=True)

            return(log_dict)
    
    if retry:
        log_file_paths = [ os.path.join(target_output_folder_path,f'{huc}_{int(dem_resolution)}m','log_file.csv') for huc in hucs ]
        input_logs = [ pd.read_csv(log_file_path,index_col=False) for log_file_path in log_file_paths ]
        input_log_save = pd.concat(input_logs)
        input_log_save = input_log_save.reset_index(drop=True)
        
        def convert_to_shape(string):
            try:
                return(shapely.wkt.loads(string))
            except TypeError:
                return(Polygon())
        
        input_log = input_log_save.copy()

        input_log.loc[:,'geometry'] = input_log.loc[:,'geometry'].apply(convert_to_shape)
        
        input_log = gpd.GeoDataFrame(input_log,geometry='geometry')
        
        input_log = input_log.loc[:,['geometry','dem_resolution','wbd_crs','ndv','target_output_folder_path','huc','idx']]
        input_log = input_log.loc[~input_log.isna().any(axis=1),:]
        input_log = input_log.astype({'dem_resolution':float,'wbd_crs':str,'ndv':float,'target_output_folder_path':str,'huc':str,'idx':int})

        input_dict = input_log.T.values.tolist()

    # make list of operations to complete
    operations = [ dask.delayed(__retrieve_and_process_single_3dep_dem)(*inputs) for inputs in zip(*input_dict) ]
    
    retrieve_message = ' Retrieve and process tiles'
    
    if retry:
        retrieve_message = ' RETRY:' + retrieve_message
    
    with TqdmCallback(desc=retrieve_message):
        res = dask.compute(*operations)
    
    output_log = pd.DataFrame(res)

    # impute new log entries into original log files
    if retry:
        input_log_save = input_log_save.loc[~input_log_save.loc[:,'idx'].isin(output_log.loc[:,'idx']),:]
        output_log = pd.concat( (input_log_save,output_log))
        output_log.sort_values('idx',inplace=True,axis=0,ignore_index=True)
        output_log = output_log.reset_index(drop=True)
    
    failed_3dep = output_log.loc[:,'3DEP Failed'].sum()
    failed_nhd = output_log.loc[:,'NHD Failed'].sum()
    failed_both = output_log.loc[:,'Both failed'].sum()
    
    print(f'3DEP failed tiles: {failed_3dep} | NHD failed tiles: {failed_nhd} | Both failed tiles: {failed_both}')

    # save new log files
    for huc in hucs:
        log_file_path = os.path.join(target_output_folder_path,f'{huc}_{int(dem_resolution)}m','log_file.csv')
        
        huc_output_log = output_log.loc[output_log.loc[:,'huc'] == huc,:]
        huc_output_log.to_csv(log_file_path,index=False)

    # merge into vrt and then tiff
    huc_directories = [os.path.basename(f) for f in input_dict[4] ]
    huc_directories = list(set(huc_directories))
    
    for huc_dir in tqdm(huc_directories,desc=' Merging tiles'):
        
        huc_dir = os.path.join(target_output_folder_path,huc_dir)
        
        if not os.path.isdir(huc_dir):
            continue
        
        huc = os.path.basename(huc_dir)

        opts = BuildVRTOptions( xRes=dem_resolution,
                                yRes=dem_resolution,
                                srcNodata='nan',
                                VRTNodata=ndv,
                                resampleAlg='bilinear'
                              )
        
        sourceFiles = glob(os.path.join(huc_dir,'*.tif'))
        destVRT = os.path.join(target_output_folder_path,f'dem_3dep_{huc}.vrt')
        
        if os.path.exists(destVRT):
            os.remove(destVRT)
        
        vrt = BuildVRT(destName=destVRT, srcDSOrSrcDSTab=sourceFiles,options=opts)
        vrt = None
        
        # for some reason gdal_merge won't work
        merge_tiff = False
        if merge_tiff:
            
            destTiff = os.path.join(target_output_folder_path,f'dem_3dep_{huc}.tif')
            
            if os.path.exists(destTiff):
                os.remove(destTiff)

            subprocess.call( [ 'gdal_merge.py','-o',destTiff, '-ot', 'Float32',
                               '-co', 'BLOCKXSIZE=512','-co', 'BLOCKYSIZE=512',
                               '-co','TILED=YES', '-co', 'COMPRESS=LZW','-q','-co',
                               'BIGTIFF=YES', '-ps', f'{dem_resolution}',f'{dem_resolution}',
                               '-n',str(ndv), '-a_nodata',str(ndv),'-init',str(ndv),
                               destVRT
                           ])
            #gdal_merge([ '','-o',destTiff, '-ot', 'Float32', '-co', 'BLOCKXSIZE=512','-co', 'BLOCKYSIZE=512','-co','TILED=YES', '-co', 'COMPRESS=LZW','-co','BIGTIFF=YES', '-ps', f'{dem_resolution}', f'{dem_resolution}',destVRT])


def __download_usgs_dems(extent_files, dem_resolution, tile_size, wbd_buffer, output_folder_path, number_of_jobs, retry):
    
    '''
    Process:
    ----------
    download the actual raw (non reprojected files) from the USGS
    based on stated embedded arguments
    
    Parameters
    ----------
        - fl (object of fim_logger (must have been created))
        - remaining params are defined in acquire_and_preprocess_3dep_dems
        
    Notes
    ----------
        - pixel size set to 10 x 10 (m)
        - block size (256) (sometimes we use 512)
        - cblend 6 adds a small buffer when pulling down the tif (ensuring seamless
          overlap at the borders.)    
    
    '''

    print(f"==========================================================")
    print(f"-- Downloading USGS DEMs Starting")
    
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:

        executor_dict = {}
        
        for idx, extent_file in enumerate(extent_files):
            download_dem_args = {
                                 'extent_file': extent_file,
                                 'tile_size': tile_size,
                                 'dem_resolution': dem_resolution,
                                 'target_output_folder_path': output_folder_path,
                                 'ndv': sv.elev_raster_ndv,
                                 'number_of_jobs': number_of_jobs,
                                 'wbd_buffer': wbd_buffer,
                                 'retry': retry
                                }
        
            try:
                # future = executor.submit(download_usgs_dem_file, **download_dem_args)
                future = executor.submit(retrieve_and_reproject_3dep_for_huc, **download_dem_args)
                executor_dict[future] = extent_file
            except Exception as ex:
                
                summary = traceback.StackSummary.extract(
                        traceback.walk_stack(None))
                print(f"*** {ex}")                
                print(''.join(summary.format()))    
                
                logging.critical(f"*** {ex}")
                logging.critical(''.join(summary.format()))

                sys.exit(1)
            
        # Send the executor to the progress bar and wait for all tasks to finish
        sf.progress_bar_handler(executor_dict, f"Downloading USGG 3Dep Dems")

    print(f"-- Downloading USGS DEMs Completed")
    logging.info(f"-- Downloading USGS DEMs Completed")
    print(f"==========================================================")    
    
        

def polygonize(target_output_folder_path):
    """
    Create a polygon of 3DEP domain from individual HUC6 DEMS which are then dissolved into a single polygon
    """
    dem_domain_file = os.path.join(target_output_folder_path, 'HUC6_dem_domain.gpkg')

    msg = f" - Polygonizing -- {dem_domain_file} - Started"
    print(msg)
    logging.info(msg)
            
    dem_files = glob.glob(os.path.join(target_output_folder_path, '*_dem.tif'))
    dem_gpkgs = gpd.GeoDataFrame()

    for n, dem_file in enumerate(dem_files):
        edge_tif = f'{os.path.splitext(dem_file)[0]}_edge.tif'
        edge_gpkg = f'{os.path.splitext(edge_tif)[0]}.gpkg'

        # Calculate a constant valued raster from valid DEM cells
        if not os.path.exists(edge_tif):
            subprocess.run(['gdal_calc.py', '-A', dem_file, f'--outfile={edge_tif}', '--calc=where(A > -900, 1, 0)', '--co', 'BIGTIFF=YES', '--co', 'NUM_THREADS=ALL_CPUS', '--co', 'TILED=YES', '--co', 'COMPRESS=LZW', '--co', 'SPARSE_OK=TRUE', '--type=Byte', '--quiet'])

        # Polygonize constant valued raster
        subprocess.run(['gdal_polygonize.py', '-8', edge_tif, '-q', '-f', 'GPKG', edge_gpkg])

        gdf = gpd.read_file(edge_gpkg)

        if n == 0:
            dem_gpkgs = gdf
        else:
            dem_gpkgs = dem_gpkgs.append(gdf)

        os.remove(edge_tif)
        
    dem_gpkgs['DN'] = 1
    dem_dissolved = dem_gpkgs.dissolve(by='DN')
    dem_dissolved.to_file(dem_domain_file, driver='GPKG')

    if not os.path.exists(dem_domain_file):
        msg = f" - Polygonizing -- {dem_domain_file} - Failed"
        print(msg)
        logging.error(msg)
    else:
        msg = f" - Polygonizing -- {dem_domain_file} - Complete"
        print(msg)
        logging.info(msg)


def __setup_logger(output_folder_path):

    start_time = datetime.now()
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"3Dep_downloaded-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)    
    
    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == '__main__':

    # Parse arguments.
    
    # sample usage (min params):
    # - python3 /foss_fim/data/usgs/acquire_and_preprocess_3dep_dems.py -e /data/inputs/wbd/HUC6_5070/ -t /data/inputs/3dep_dems/10m_5070/ -r -j 20 -d 10
    
    # Notes:
    #   - This is a very low use tool. So for now, this only can load 10m (1/3 arc second) and is using
    #     hardcoded paths for the wbd gpkg to be used for clipping (no buffer for now).
    #     Also hardcoded usgs 3dep urls, etc.  Minor
    #     upgrades can easily be made for different urls, output folder paths, huc units, etc
    #     as/if needed (command line params)
    #   - The output path can be adjusted in case of a test reload of newer data for 3dep.
    #     The default is /data/input/usgs/3dep_dems/10m/
    #   - While you can (and should use more than one job number (if manageable by your server)),
    #     this tool is memory intensive and needs more RAM then it needs cores / cpus. Go ahead and 
    #     anyways and increase the job number so you are getting the most out of your RAM. Or
    #     depending on your machine performance, maybe half of your cpus / cores. This tool will
    #     not fail or freeze depending on the number of jobs / cores you select.
        
        
    # IMPORTANT: 
    # (Sept 2022): we do not process HUC2 of 22 (misc US pacific islands).
    # We left in HUC2 of 19 (alaska) as we hope to get there in the semi near future
    # They need to be removed from the input src clip directory in the first place.
    # They can not be reliably removed in code.
       
    parser = argparse.ArgumentParser(description='Acquires and preprocesses USGS 3Dep dems')

    parser.add_argument('-e','--extent_file_path', help='location the gpkg files that will'\
                        ' are being used as clip regions (aka.. huc_*.gpkg or whatever).'\
                        ' All gpkgs in this folder will be used.', required=True)

    parser.add_argument('-j','--number_of_jobs', help='Number of (jobs) cores/processes to used.', 
                        required=False, default=1, type=int)

    parser.add_argument('-r','--retry', help='If included, it will skip files that already exist.'\
                        ' Default is all will be loaded/reloaded.', 
                        required=False, action='store_true', default=False)

    parser.add_argument('-t','--target_output_folder_path', help='location of where the 3dep files'\
                        ' will be saved', required=False, default='')
    
    parser.add_argument('-d', '--dem_resolution', help='DEM resolution in meters', type=int, default=10, required=False)


    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    acquire_and_preprocess_3dep_dems(**args)

