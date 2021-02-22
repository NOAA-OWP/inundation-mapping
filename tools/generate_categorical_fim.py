import os
from multiprocessing import Pool
import argparse
import traceback
import sys

sys.path.insert(1, 'foss_fim/tests')
from inundation import inundate

INPUTS_DIR = r'/data/inputs'

# Define necessary variables for inundation().
hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'
mask_type, catchment_poly = 'huc', ''
    

def generate_categorical_fim(fim_run_dir, source_flow_dir, output_cat_fim_dir, job_number, gpkg, extif, depthtif):
    
    # Create output directory and log directory.
    if not os.path.exists(output_cat_fim_dir):
        os.mkdir(output_cat_fim_dir)
    log_dir = os.path.join(output_cat_fim_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    
    no_data_list = []
    procs_list = []
       
    # Loop through huc directories in the source_flow directory.
    source_flow_dir_list = os.listdir(source_flow_dir)
    for huc in source_flow_dir_list:
        if "." not in huc:
            
            # Get list of AHPS site directories.
            ahps_site_dir = os.path.join(source_flow_dir, huc)
            ahps_site_dir_list = os.listdir(ahps_site_dir)
            
            # Map paths to HAND files needed for inundation().
            fim_run_huc_dir = os.path.join(fim_run_dir, huc)
            rem = os.path.join(fim_run_huc_dir, 'rem_zeroed_masked.tif')
            catchments = os.path.join(fim_run_huc_dir, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            hydroTable =  os.path.join(fim_run_huc_dir, 'hydroTable.csv')
            
            exit_flag = False  # Default to False.
            
            # Check if necessary data exist; set exit_flag to True if they don't exist.
            for f in [rem, catchments, hydroTable]:
                if not os.path.exists(f):
                    print(f)
                    no_data_list.append(f)
                    exit_flag = True
                    
            # Log "Missing data" if missing TODO improve this.
            if exit_flag == True:
                f = open(os.path.join(log_dir, huc + '.txt'), 'w')
                f.write("Missing data")
                continue
            
            # Map path to huc directory inside out output_cat_fim_dir.
            cat_fim_huc_dir = os.path.join(output_cat_fim_dir, huc)
            if not os.path.exists(cat_fim_huc_dir):
                os.mkdir(cat_fim_huc_dir)
            
            # Loop through AHPS sites.
            for ahps_site in ahps_site_dir_list:
                # Map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj).
                ahps_site_parent = os.path.join(ahps_site_dir, ahps_site)
                thresholds_dir_list = os.listdir(ahps_site_parent)
                
                # Map parent directory for all inundation output filesoutput files.
                cat_fim_huc_ahps_dir = os.path.join(cat_fim_huc_dir, ahps_site)
                if not os.path.exists(cat_fim_huc_ahps_dir):
                    os.mkdir(cat_fim_huc_ahps_dir)
                        
                # Loop through thresholds/magnitudes and define inundation output files paths
                for magnitude in thresholds_dir_list:
                    if "." not in magnitude:
                        magnitude_flows_csv = os.path.join(ahps_site_parent, magnitude, 'ahps_' + ahps_site + '_huc_' + huc + '_flows_' + magnitude + '.csv')
                        if os.path.exists(magnitude_flows_csv):
                            if gpkg:
                                output_extent_gpkg = os.path.join(cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_extent.gpkg')
                            else:
                                output_extent_gpkg = None
                            if extif:
                                output_extent_grid = os.path.join(cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_extent.tif')
                            else:
                                output_extent_grid = None
                            if depthtif:
                                output_depth_grid = os.path.join(cat_fim_huc_ahps_dir, ahps_site + '_' + magnitude + '_depth.tif')
                            else:
                                output_depth_grid = None
                            
                            # Append necessary variables to list for multiprocessing.
                            procs_list.append([rem, catchments, catchment_poly, magnitude_flows_csv, huc, hydroTable, output_extent_gpkg, output_extent_grid, output_depth_grid, ahps_site, magnitude, log_dir])
    # Initiate multiprocessing.                                    
    pool = Pool(job_number)
    pool.map(run_inundation, procs_list)


def run_inundation(args):
    
    # Parse args.
    rem = args[0]
    catchments = args[1]
    catchment_poly = args[2]
    magnitude_flows_csv = args[3]
    huc = args[4]
    hydroTable = args[5]
    output_extent_gpkg = args[6]
    output_extent_grid = args[7]
    output_depth_grid = args[8]
    ahps_site = args[9]
    magnitude = args[10]
    log_dir = args[11]
    
    print("Running inundation for " + str(os.path.split(os.path.split(output_extent_gpkg)[0])[0]))
    try:
        inundate(
                 rem,catchments,catchment_poly,hydroTable,magnitude_flows_csv,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=huc,num_workers=1,aggregate=False,inundation_raster=output_extent_grid,inundation_polygon=output_extent_gpkg,
                 depths=output_depth_grid,out_raster_profile=None,out_vector_profile=None,quiet=True
                )
    except Exception:
        # Log errors and their tracebacks.
        f = open(os.path.join(log_dir, huc + "_" + ahps_site + "_" + magnitude + '.txt'), 'w')
        f.write(traceback.format_exc())
        f.close()
        
        
if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping and regression analysis for FOSS FIM. Regression analysis results are stored in the test directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-s', '--source-flow-dir',help='Path to directory containing flow CSVs to use to generate categorical FIM.',required=True, default="")
    parser.add_argument('-o', '--output-cat-fim-dir',help='Path to directory where categorical FIM outputs will be written.',required=True, default="")
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    parser.add_argument('-gpkg','--write-geopackage',help='Using this option will write a geopackage.',required=False, action='store_true')
    parser.add_argument('-extif','--write-extent-tiff',help='Using this option will write extent TIFFs. This is the default.',required=False, action='store_true')
    parser.add_argument('-depthtif','--write-depth-tiff',help='Using this option will write depth TIFFs.',required=False, action='store_true')
    
    args = vars(parser.parse_args())
    
    fim_run_dir = args['fim_run_dir']
    source_flow_dir = args['source_flow_dir']
    output_cat_fim_dir = args['output_cat_fim_dir']
    job_number = int(args['job_number'])
    gpkg = args['write_geopackage']
    extif = args['write_extent_tiff']
    depthtif = args['write_depth_tiff']
    
    generate_categorical_fim(fim_run_dir, source_flow_dir, output_cat_fim_dir, job_number, gpkg, extif, depthtif)
    
    
    
