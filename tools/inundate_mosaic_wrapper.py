import os, sys
import argparse
from timeit import default_timer as timer
sys.path.append('/foss_fim/tools')
from mosaic_inundation import Mosaic_inundation
from inundate_gms import Inundate_gms


def main(hydrofabric_dir, huc, forecast, inundation_raster, inundation_polygon, depths_raster, log_file, output_fileNames, num_workers, keep_intermediate, verbose):

    
    huc_dir = os.path.join(hydrofabric_dir, huc)
    print("Running inundate for " + huc + "...")
    map_file = Inundate_gms(  hydrofabric_dir = hydrofabric_dir, 
                                     forecast = forecast, 
                                     num_workers = num_workers,
                                     hucs = huc,
                                     inundation_raster = inundation_raster,
                                     inundation_polygon = None,
                                     depths_raster = None,
                                     verbose = False,
                                     log_file = None,
                                     output_fileNames = None )
    
    print("Mosaicking for " + huc)
    
    Mosaic_inundation( map_file,
                        mosaic_attribute = 'inundation_rasters',
                        mosaic_output = inundation_raster,
                        mask = os.path.join(huc_dir,'wbd.gpkg'),
                        unit_attribute_name = 'huc8',
                        nodata = -9999,
                        workers = 1,
                        remove_inputs = False,
                        subset = None,
                        verbose = False )
    
    
if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Inundate GMS')
    parser.add_argument('-y','--hydrofabric_dir', help='Directory path to FIM hydrofabric by processing unit', required=True)
    parser.add_argument('-u','--huc',help='List of HUCS to run',required=False,default="",type=str,)
    parser.add_argument('-f','--forecast',help='Forecast discharges in CMS as CSV file',required=True)
    parser.add_argument('-i','--inundation-raster',help='Inundation Raster output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-p','--inundation-polygon',help='Inundation polygon output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-d','--depths-raster',help='Depths raster output. Only writes if designated. Appends HUC code in batch mode.',required=False,default=None)
    parser.add_argument('-l','--log-file',help='Log-file to store level-path exceptions',required=False,default=None)
    parser.add_argument('-o','--output-fileNames',help='Output CSV file with filenames for inundation rasters, inundation polygons, and depth rasters',required=False,default=None)
    parser.add_argument('-w','--num-workers', help='Number of Workers', required=False,default=1)
    parser.add_argument('-k','--keep-intermediate',help='Keep intermediate products, i.e. individual branch inundation',required=False,default=False,action='store_true')
    parser.add_argument('-v','--verbose',help='Verbose printing',required=False,default=False,action='store_true')
    
    start = timer()

    # extract to dictionary and run
    main( **vars(parser.parse_args()) )

    print(f'{round(timer() - start, 2)}')
