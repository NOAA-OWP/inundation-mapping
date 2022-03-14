import argparse
import os

from inundation import inundate
from multiprocessing import Pool

INUN_REVIEW_DIR = r'/data/inundation_review/inundation_nwm_recurr/'
INUN_OUTPUT_DIR = r'/data/inundation_review/inundate_nation/'
INPUTS_DIR = r'/data/inputs'


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
        print("Running the NWM recurrence intervals for HUC inundation (depth): " + huc + ", " + magnitude + "...")
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
        

if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using streamflow recurrence interflow data. Inundation outputs are stored in the /inundation_review/inundation_nwm_recurr/ directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test)',required=True)
    parser.add_argument('-o', '--output-dir',help='Optional: The path to a directory to write the outputs. If not used, the inundation_nation directory is used by default -> type=str',required=False, default="")
    parser.add_argument('-m', '--magnitude-list', help = 'List of NWM recurr flow intervals to process (Default: 100_0) (Other options: 2_0 5_0 10_0 25_0 50_0 100_0)', nargs = '+', default = ['100_0'], required = False)
    parser.add_argument('-d', '--depth',help='Optional flag to produce inundation depth rasters (extent raster created by default)', action='store_true')
    parser.add_argument('-j', '--job-number',help='The number of jobs',required=False,default=1)
        
    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    output_dir = args['output_dir']
    depth_option = args['depth']
    magnitude_list = args['magnitude_list']
    job_number = int(args['job_number'])

    assert os.path.isdir(fim_run_dir), 'ERROR: could not find the input fim_dir location: ' + str(fim_run_dir)
    print("Input FIM Directory: " + str(fim_run_dir))
    huc_list = os.listdir(fim_run_dir)
    fim_version = os.path.split(fim_run_dir)[1]
    print("Using fim version: " + str(fim_version))

    for magnitude in magnitude_list:
        print("Preparing to generate inundation outputs for magnitude: " + str(magnitude))
        nwm_recurr_file = os.path.join(INUN_REVIEW_DIR, 'nwm_recurr_flow_data', 'nwm21_17C_recurr_' + magnitude + '_cms.csv')
        assert os.path.isfile(nwm_recurr_file), 'ERROR: could not find the input NWM recurr flow file: ' + str(nwm_recurr_file)
        print("Input flow file: " + str(nwm_recurr_file))
        
        if 'ms' in fim_version:
            config = 'ms'
        if 'fr' in fim_version:
            config = 'fr'

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

