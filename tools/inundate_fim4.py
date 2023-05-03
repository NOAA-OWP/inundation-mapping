#!/usr/bin/env python3

import os
import argparse

from mosaic_inundation import Mosaic_inundation
from inundate_gms import Inundate_gms

def Inundate_fim4(
    hydrofabric_dir,
    hucs,
    forecast,
    inundation_raster = None,
    inundation_polygon = None,
    depths_raster = None,
    log_file = None,
    output_fileNames = None,
    num_workers = 1,
    verbose = False
):
    
    """
    Inundate FIM4
    -------------
    This function is a wrapper for the inundate_gms and mosaic_inundation functions.
    It is designed to run the inundate_gms function and then mosaic the inundation
    rasters and depths rasters into a single raster for each HUC. It can also
    write the inundation rasters, inundation polygons, and depths rasters to a CSV
    file for later use.

    Parameters
    ----------
    hydrofabric_dir : str
        Directory path to FIM hydrofabric by processing unit
        
    """

    """
    TODO:
    - Add docstring for Inundate_fim4 function
    - Finish this function
    - Add fim_inputs.csv to repo
    - Return xarray object
    - Test additional arguments such as depths and polygons
    - Jupyter notebook for visualization
    """

    map_file = Inundate_gms( hydrofabric_dir = os.path.dirname(self.fim_dir), 
                                            forecast = benchmark_flows, 
                                            num_workers = gms_workers,
                                            hucs = self.huc,
                                            inundation_raster = predicted_raster_path,
                                            inundation_polygon = None,
                                            depths_raster = None,
                                            verbose = verbose,
                                            log_file = log_file,
                                            output_fileNames = None )

    mosaiced_inundation = Mosaic_inundation( map_file,
                        mosaic_attribute = 'inundation_rasters',
                        mosaic_output = predicted_raster_path,
                        mask = os.path.join(self.fim_dir,'wbd.gpkg'),
                        unit_attribute_name = 'huc8',
                        nodata = elev_raster_ndv,
                        workers = 1,
                        remove_inputs = True,
                        subset = None,
                        verbose = verbose )
    
    return mosaiced_inundation


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Inundate FIM')
    parser.add_argument('-y','--hydrofabric_dir', help='Directory path to FIM hydrofabric by processing unit', required=True)
    parser.add_argument('-u','--hucs',help='List of HUCS to run',required=False,default=None,type=str,nargs='+')
    parser.add_argument('-f','--forecast',help='Forecast discharges in CMS as CSV file',required=True)
    parser.add_argument('-i','--inundation-raster',help='Inundation Raster output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-p','--inundation-polygon',help='Inundation polygon output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-d','--depths-raster',help='Depths raster output. Only writes if designated. Appends HUC code in batch mode.',required=False,default=None)
    parser.add_argument('-l','--log-file',help='Log-file to store level-path exceptions',required=False,default=None)
    parser.add_argument('-o','--output-fileNames',help='Output CSV file with filenames for inundation rasters, inundation polygons, and depth rasters',required=False,default=None)
    parser.add_argument('-w','--num-workers', help='Number of Workers', required=False,default=1)
    parser.add_argument('-v','--verbose',help='Verbose printing',required=False,default=None,action='store_true')
    
    
    # extract to dictionary and run
    Inundate_gms( **vars(parser.parse_args()) )
