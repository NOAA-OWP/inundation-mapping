#!/usr/bin/env python3

import argparse
import glob
import os
import subprocess
import sys
import traceback

from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime
from tqdm import tqdm

sys.path.append('/foss_fim/src')
import utils.shared_variables as sv


# local constants (until changed to input param)
__USGS_3DEP_10M_VRT_URL = r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'  # 10m = 13 (1/3 arc second)


def acquire_and_preprocess_3dep_dems(extent_file_path,
                                     target_output_folder_path = '', 
                                     number_of_jobs = 1, 
                                     retry = False):
    
    '''
    Overview
    ----------
    This will download 3dep rasters from USGS using USGS vrts.
    By default USGS 3Dep stores all their rasters in lat/long (northing and easting).
    By us downloading the rasters using WBD HUC4 clips and gdal, we an accomplish a few extra
    steps.
        1) Ensure the projection types that are downloaded are consistant and controlled.
           We are going to download them as NAD83 basic (espg: 4269) which is consistant
           with other data sources, even though FIM defaults to ESRI:102039. We will
           change that as we add the clipped version per HUC8.
        2) ensure we are adjusting blocksizes, compression and other raster params
        3) Create the 3dep rasters in the size we want (default at HUC4 for now)
        
    Notes:
        - As this is a very low use tool, all values such as the USGS vrt path, output
          folder paths, huc unit level (huc4), etc are all hardcoded
         
        
    Parameters
    ----------
        - extent_file_path (str):
            Location of where the extent files that are to be used as clip extent against
            the USGS 3Dep vrt url.
            ie) \data\inputs\wbd\HUC4
            
        - target_output_folder_path (str):
            The output location of the new 3dep dem files. When the param is not submitted,
            it will be sent to /data/input/usgs/3dep_dems/10m/.
    
        - number_of_jobs (int):
            This program supports multiple procs if multiple procs/cores are available.
            
        - retry (True / False):
            If retry is True and the file exists (either the raw downloaded DEM and/or)
            the projected one, then skip it
    '''
    
    # TODO Change to shared_functions as header (wait until s3 merged)

    total_cpus_available = os.cpu_count() - 1
    if number_of_jobs > total_cpus_available:
        raise ValueError('The number of jobs {number_of_jobs}'\
                          'exceeds your machine\'s available CPU count minus one. '\
                          'Please lower the number of jobs '\
                          'values accordingly.'.format(number_of_jobs)
                        )
    
    start_time = datetime.now()
    print_start_header('Loading 3dep dems', start_time)

    if (not os.path.exists(extent_file_path)):
        raise ValueError(f'extent_file_path value of {extent_file_path}'\
                          ' not set to a valid path')
    
    if (target_output_folder_path is None) or (target_output_folder_path == ""):
        target_output_folder_path = sv.INPUT_DEMS_3DEP_10M_DIR
    
    if (not os.path.exists(target_output_folder_path)):
        raise ValueError(f"Output folder path {target_output_folder_path} does not exist" )
   
    print(f"Downloading to {target_output_folder_path}")    
    
    # Get the WBD .gpkg files (or clip extent)
    extent_file_names = get_extent_file_names(extent_file_path)
   
    # download dems, setting projection, block size, etc
    __download_usgs_dems(extent_file_names, target_output_folder_path, number_of_jobs, retry)
    
    
    # TODO:  Save an log file with date stamp so we know the last
    # time it was loaded. It shoudl include all outputs plus input parameters.
    # Note: I don't see versions on USGS, so this will
    # have to do.
    
    end_time = datetime.now()
    print_end_header('Loading 3dep dems', start_time, end_time)


def __download_usgs_dems(extent_files, output_folder_path, number_of_jobs, retry):
    
    '''
    Process:
    ----------
    download the actual raw (non reprojected files) from the USGS
    based on stated embedded arguments
    
    '''

    print(f"==========================================================")
    print(f"-- Downloading USGS DEMs Starting")
    
    base_cmd =  'gdalwarp {0} {1}'
    base_cmd += ' -cutline {2} -crop_to_cutline -ot Float32 -r bilinear'
    base_cmd += ' -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"'
    base_cmd += ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES"'
    base_cmd += ' -t_srs EPSG:4269'
   
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:

        executor_dict = {}
        
        for idx, extent_file in enumerate(extent_files):
            
            download_dem_args = { 
                                'extent_file': extent_file,
                                'output_folder_path': output_folder_path,
                                'download_url': __USGS_3DEP_10M_VRT_URL,
                                'base_cmd':base_cmd,
                                'retry': retry
                                }
        
            try:
                future = executor.submit(download_usgs_dem_file, **download_dem_args)
                executor_dict[future] = extent_file
            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)
            
        # Send the executor to the progress bar and wait for all FR tasks to finish
        progress_bar_handler(executor_dict, f"Downloading USGG 3Dep Dems")
       


    print(f"-- Downloading USGS DEMs Completed")
    print(f"==========================================================")    
    
        
def download_usgs_dem_file(extent_file, 
                           output_folder_path, 
                           download_url,
                           base_cmd,
                           retry):

    '''
    Process:
    ----------    
        Downloads just one dem file from USGS. This is setup as a method
        to allow for multi-processing.

        
    Parameters:
    ----------
        - extent_file (str)
             When the dem is downloaded, it is clipped against this extent (.gkpg) file.
        - output_folder_path (str)
             Location of where the output file will be stored
        - download_url (str)
             URL for the USGS download site (note: Should include '/vsicurl/' at the 
             front of the URL)
        - base_cmd (str)
             The basic GDAL command with string formatting wholes for key values.
             See the cmd variable below.
             ie)
                base_cmd =  'gdalwarp {0} {1}'
                base_cmd += ' -cutline {2} -crop_to_cutline -ot Float32 -r bilinear'
                base_cmd +=  ' -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"'
                base_cmd +=  ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES"'
                base_cmd +=  ' -t_srs EPSG:4269'
        - retry (bool)
             If True, and the file exists (and is over 0k), downloading will be skipped.
        
    '''
    
    basic_file_name = os.path.basename(extent_file).split('.')[0]
    target_file_name_raw = f"{basic_file_name}_dem.tif" # as downloaded
    target_path_raw = os.path.join(output_folder_path,
                                    target_file_name_raw)
    
    # File might exist from a previous failed run. If it was aborted or failed
    # on a previous attempt, it's size less than 1mg, so delete it. Note:
    # it might be compromised on a previous run but not less than 1mg (part written).
    # That scenerio is not handled as we can not tell if it completed.
    if (os.path.exists(target_path_raw)) and (os.stat(target_path_raw).st_size < 1000):
        os.remove(target_path_raw)
    
    if (not retry) or (retry and not os.path.exists(target_path_raw)):
        
        print(f"Downloading -- {target_file_name_raw} - Started")       
                
        cmd = base_cmd.format(download_url,
                              target_path_raw,
                              extent_file)
        #PREP_PROJECTION_EPSG
        #fh.vprint(f"cmd is {cmd}", self.is_verbose, True)
        #print(f"cmd is {cmd}")
        
        # didn't use Popen becuase of how it interacts with multi proc
        # was creating some issues. Run worked much better.
        process = subprocess.run(cmd, shell = True,
                                    stdout = subprocess.PIPE, 
                                    stderr = subprocess.PIPE,
                                    check = True,
                                    universal_newlines=True) 

        print(process.stdout)
        print(process.stderr)
                    
        print(f"Downloading -- {target_file_name_raw} - Complete")

    else:
        print(f"Downloading -- {target_file_name_raw} - Skipped (already exists (see retry flag))")    
    

def get_extent_file_names(extent_src_folder):
    '''
    Process
    ----------
    Get a list of file names and paths from wbd huc gpkgs (or other files
    can be used for extents.
    
    Notes:
        - The files need to be .gkpg files
        - all files in this directory will be used and should be cut as HUC4, 6, 8 or 
          whatever. Remember.. by using vrt files later, size is not relavent so 4's are fine.
    
    Parameters
    ----------
        - extent_src_folder (str)
             Location of the .gkpg files defining the extent of each 
             downloaded DEM.
        - file_pattern  (str) 
             All files in the folder will likely follow a pattern, such as
             HUC4_xxxx.pkg  or HUC_8_xxxxxxxx.gkpg, so just use the * wildcard
             character for file name portions that are changeable. ie) HUC4_*.gkpg
             or HUC8_8.gpkg or whatever.
    
    Returns
    ----------
    A list of gkpgs
    '''
    
    # test that folder exists
    if (not os.path.exists(extent_src_folder)):
        raise ValueError(f"Extent src folder of {extent_src_folder} not found")

    print(f"Extent files coming from {extent_src_folder}")
    
    if (not extent_src_folder.endswith("/")):
        extent_src_folder += "/"
    
    extent_files = glob.glob(extent_src_folder + "*.*")
 
    if (len(extent_files) == 0):
        raise Exception("extent files not loaded or do not exist")
    
    extent_files.sort()
    
    return extent_files

    
def print_start_header(friendly_program_name, start_time):
    
    print("================================")
    print(f"Start {friendly_program_name}")
    dt_string = start_time.strftime("%m/%d/%Y %H:%M:%S")
    print (f"started: {dt_string}")
    print()


def print_end_header(friendly_program_name, start_time, end_time):
    
    print("================================")
    print(f"End {friendly_program_name}")

    dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
    print (f"ended: {dt_string}")

    # calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()

def progress_bar_handler(executor_dict, desc):

    for future in tqdm(as_completed(executor_dict),
                    total=len(executor_dict),
                    desc=desc
                    ):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future],exc.__class__.__name__,exc))


if __name__ == '__main__':

    # Parse arguments.
    
    # sample usage (min params):
    # - python3 /foss_fim/data/usgs/acquire_and_preprocess_3dep_dems.py -e /data/inputs/wbd/HUC4
    
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
    # (Sept 2022): we do not process HUC2 of 19 (alaska) or 22 (misc US pacific islands).
    # They need to be removed from the input src clip directory in the first place.
    # They can not be reliably removed in code.
       
    parser = argparse.ArgumentParser(description='Acquires and preprocesses USGS 3Dep dems')

    parser.add_argument('-e','--extent_file_path', help='location the gpkg files that will'\
                        ' are being used as clip regions (aka.. huc4_*.gpkg or whatever).'\
                        ' All gpkgs in this folder will be used.', required=True)

    parser.add_argument('-j','--number_of_jobs', help='Number of (jobs) cores/processes to used.', 
                        required=False, default=1, type=int)

    parser.add_argument('-r','--retry', help='If included, it will skip files that already exist.'\
                        ' Default is all will be loaded/reloaded.', 
                        required=False, action='store_true', default=False)

    parser.add_argument('-t','--target_output_folder_path', help='location of where the 3dep files'\
                        ' will be saved', required=False, default='')


    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    acquire_and_preprocess_3dep_dems(**args)

