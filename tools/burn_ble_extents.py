#!/usr/bin/env python3

import os
from functools import partial
from itertools import product

import numpy as np
import geopandas as gpd
import rioxarray as rxr
import rasterio
from geocube.api.core import make_geocube 
from geocube.rasterize import rasterize_image
from tqdm import tqdm

from utils.shared_variables import PREP_PROJECTION

root_dir = os.path.abspath(os.sep)
data_dir = os.path.join(root_dir,'data','misc','lidar_manuscript_data','reprocessed_ble')
huc8s_vector_fn = os.path.join(data_dir,'..','huc8s_1202.gpkg')
ble_huc_files_template = os.path.join(data_dir,"{}.gpkg")
ble_huc_files_layer_name = 'FLD_HAZ_AR'
extents_feature_name = 'EST_Risk'
magnitude_encodings_ble_to_yrs = {'High': 100,'Moderate': 500,'H':100,'M':500}

hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
#hucs = ['12020001']
years = [100,500]
#years = [500]
resolution = 3
#resolution = 10

def build_output_fim_file(huc,year):
    
    return os.path.join(root_dir, "data", "test_cases","ble_test_cases",
                        "validation_data_ble", huc,f"{year}yr",
                        f"ble_huc_{huc}_extent_{year}yr.tif")

def burn_ble_extents():
    
    huc8s_df = gpd.read_file(huc8s_vector_fn)

    # loop through files
    prev_huc = None
    for huc,y in tqdm(list(product(hucs,years)),desc='Burning BLE FIMs'):
        
        if huc != prev_huc:
            ble_gpd = gpd.read_file(ble_huc_files_template.format(huc),layer=ble_huc_files_layer_name) \
                                    .to_crs(PREP_PROJECTION)

            # create magnitudes
            ble_gpd["magnitudes"] = ble_gpd[extents_feature_name].apply(lambda x : magnitude_encodings_ble_to_yrs[x])
            ble_gpd["magnitudes_100yr"] = np.where(ble_gpd['magnitudes'] == 100,1,0)
            ble_gpd["magnitudes_500yr"] = np.where(np.isin(ble_gpd['magnitudes'],[100,500]),1,0)

            # remove empties
            ble_gpd = ble_gpd.loc[~ble_gpd.is_empty,:] 

        prev_huc = huc
        
        current_huc8 = huc8s_df.loc[huc8s_df.loc[:,'huc8'] == huc,:]
        current_huc8.insert(0,'burnvalue',1)

        huc8_xr = make_geocube(current_huc8,["burnvalue"],
                               fill=0,
                               resolution=resolution,
                               rasterize_function=partial(rasterize_image,
                                                          filter_nan=True,
                                                          all_touched=True)) \
                                    .to_array(dim='band',name=f'{huc}') \
                                    .chunk(256*4) \
                                    .rio.set_nodata(0) \
                                    .rio.write_nodata(-9999,encoded=True) \
                                    .sel(band="burnvalue") \
                                    .drop('band') 
        
        ble_xr = make_geocube(ble_gpd,[f"magnitudes_{y}yr"],
                              fill=0,
                              like=huc8_xr,
                              #resolution=resolution,
                              rasterize_function=partial(rasterize_image,
                                                         filter_nan=True,
                                                         all_touched=False)) \
                                   .to_array(dim='band',name=f'{y}yr') \
                                   .chunk(256*4) \
                                   .rio.set_nodata(np.nan) \
                                   .rio.write_nodata(-10,encoded=True) \
                                   .sel(band=f"magnitudes_{y}yr") \
                                   .drop('band') 
        
        ble_xr = ble_xr.where(huc8_xr != 0,np.nan) \
                       .rio.set_nodata(np.nan) \
                       .rio.write_nodata(-10,encoded=True)

        output_fim_fn = build_output_fim_file(huc,y)
        #output_fim_fn = os.path.join(data_dir,"..",f"TEMP_BLE_FIM_{huc}_{y}yr.tif")

        try:
            os.remove(output_fim_fn)
        except OSError:
            pass

        ble_xr.rio.to_raster(output_fim_fn,
                             tiled=True,windowed=True,lock=False,dtype=rasterio.int32,
                             compress='lzw')

        

if __name__ == '__main__':

    burn_ble_extents()
