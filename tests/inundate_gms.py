#!/usr/bin/env python3

import numpy as np
from inundation import inundate
import os
from tqdm import tqdm
import argparse

def Inundate_gms(
                 hydrofabric_dir, forecast, inundation_raster=None,
                 inundation_polygon=None, depths_raster=None,
                 quiet=False
                 ):

    # define directories and files
    gms_dir = os.path.join(hydrofabric_dir,'gms')
    rem = os.path.join(gms_dir,'{}','rem_zeroed_masked_{}.tif')
    catchments = os.path.join(gms_dir,'{}','gw_catchments_reaches_{}.tif')
    hydroTable = os.path.join(gms_dir,'{}','hydroTable_{}.csv')
    catchment_poly = os.path.join(hydrofabric_dir,'gw_catchments_reaches_filtered.gpkg')
    
    # get available branch ids
    branch_ids = [i for i in os.listdir(gms_dir) if os.path.isdir(os.path.join(gms_dir,i))]
    
    if not quiet:
        print("Inundating branches ...")
    
    # iterate over branches
    for branch_id in tqdm(branch_ids,disable=quiet):
       
        # define branch specific files
        rem_branch = rem.format(branch_id,branch_id)
        catchments_branch = catchments.format(branch_id,branch_id)
        hydroTable_branch = hydroTable.format(branch_id,branch_id)
        inundation_polygon_file_name, inundation_polygon_extension = inundation_polygon.split('.')
        inundation_branch_polygon = inundation_polygon_file_name + "_{}.".format(branch_id) + inundation_polygon_extension

        try:
            inundate(
                     rem= rem_branch,catchments = catchments_branch,catchment_poly=catchment_poly,
                     hydro_table=hydroTable_branch,forecast=forecast,
                     mask_type="filter",hucs=None,hucs_layerName=None,
                     subset_hucs=None,num_workers=1,aggregate=False,inundation_raster=inundation_raster,
                     inundation_polygon=inundation_branch_polygon,
                     depths=depths_raster,out_raster_profile=None,out_vector_profile=None,quiet=True
                    )
        except Exception as e:
            print("Error on BranchID: {}".format(branch_id))
            print(e)
if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-y','--hydrofabric_dir', help='Directory path to FIM hydrofabric by processing unit', required=True)
    parser.add_argument('-f','--forecast',help='Forecast discharges in CMS as CSV file',required=True)
    parser.add_argument('-i','--inundation-raster',help='Inundation Raster output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-p','--inundation-polygon',help='Inundation polygon output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-d','--depths-raster',help='Depths raster output. Only writes if designated. Appends HUC code in batch mode.',required=False,default=None)
    
    
    # extract to dictionary and run
    Inundate_gms( **vars(parser.parse_args()) )
    
    
