import argparse
import os

from inundation import inundate
from multiprocessing import Pool

INUN_REVIEW_DIR = r'/data/inundation_review/inundation_nwm_recurr/'
INPUTS_DIR = r'/data/inputs'


def run_inundation(args):
    """
    This script is basically a wrapper for the inundate function and is designed for multiprocessing.
    
    Args:
        args (list): [fim_run_dir (str), huc (str), magnitude (str), magnitude_output_dir (str), config (str)]
    
    """
    
    fim_run_dir = args[0]
    huc = args[1]
    magnitude = args[2]
    magnitude_output_dir = args[3]
    config = args[4]
    
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
    forecast = os.path.join(INUN_REVIEW_DIR, 'nwm_recurr_flow_data', 'recurr_' + magnitude + '_cms.csv')
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    # Run inundate() once for depth and once for extent.
    if not os.path.exists(depth_raster):
        print("Running the NWM recurrence intervals for HUC: " + huc + ", " + magnitude + "...")
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=None,inundation_polygon=None,
                 depths=depth_raster,out_raster_profile=None,out_vector_profile=None,quiet=True
                )
        
    if not os.path.exists(inundation_raster):
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                 depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
                )
        

if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using streamflow recurrence interflow data. Inundation outputs are stored in the /inundation_review/inundation_nwm_recurr/ directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test)',required=True)
    parser.add_argument('-o', '--output-dir',help='The path to a directory to write the outputs. If not used, the inundation_review directory is used by default -> type=str',required=False, default="")
    parser.add_argument('-j', '--job-number',help='The number of jobs',required=False,default=1)
        
    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    output_dir = args['output_dir']
    magnitude_list = ['1_5']
    
    job_number = int(args['job_number'])

    huc_list = os.listdir(fim_run_dir)
        
    fim_version = os.path.split(fim_run_dir)[1]
    
    if output_dir == "":
        output_dir = os.path.join(INUN_REVIEW_DIR, fim_version)
            
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    if 'ms' in fim_version:
        config = 'ms'
    if 'fr' in fim_version:
        config = 'fr'
        
    procs_list = []
    
    for huc in huc_list:
        if huc != 'logs':
            for magnitude in magnitude_list:
                magnitude_output_dir = os.path.join(output_dir, magnitude + '_' + config)
                if not os.path.exists(magnitude_output_dir):
                    os.mkdir(magnitude_output_dir)
                    print(magnitude_output_dir)
                procs_list.append([fim_run_dir, huc, magnitude, magnitude_output_dir, config])
            
    # Multiprocess.
    if job_number > 1:
        with Pool(processes=job_number) as pool:
            pool.map(run_inundation, procs_list)

