
import geopandas as gpd
import pandas as pd
import os
from tools_shared_functions import get_metadata
from dotenv import load_dotenv
load_dotenv()
import argparse
import rasterio
#from rasterio import features
import affine
import rasterio.mask

def fimr_to_benchmark(fimr_path, output_path):
    '''
    This function converts USFIMR shapefiles from this website (https://waterserv.ua.edu/datasets/usfimr/) to rasters that c an 
    be used for FIM Alpha evaluations and calibration. Note only USFIMR data with flow values (q) can be used here. 

    Inputs: 1. FIMR Path - Path where th FIMR Shapefile was downloaded from the aformentioned website.
            2. Output Path_flow file - Path the flow file will be sent to. 
            3. Output path raster - path the raster will be sent too. There will be a file for each huc with the huc number in it.

    Outputs: 1. flow_file.csv - This is a flow file for the river. The flow values are from the most upstream usgs gage.
             2. rasterized{huc number}.tif - This is the fimr data that has been converted to a raster. It is on the huc8 scale so it provides
                each huc as a separate file. The inundated areas are assigned 1 and the dry areas are assigned 0.

    Other Files Used: 1. wbd - The huc 8 boundaries throughout the country
                      2. flowlines - the flowlines along the all the rivers in the country
    '''
    # input validation
    if (not os.path.exists(fimr_path)):
        raise Exception(f"Sorry. {fimr_path} does not exist")

    if (not os.path.exists(output_path)):
        raise Exception(f"Sorry. {output_path} does not exist")

    API_BASE_URL = os.getenv("API_BASE_URL")

    # Path for the wbd data and nwm flowlines
    wbd_path = '/data/inputs/wbd/WBD_National.gpkg'
    flowlines_path = '/data/inputs/nwm_hydrofabric/nwm_flows_ms_wrds.gpkg'

    # Saving wbd, flowlines, and fimr data as variables
    wbd = gpd.read_file(wbd_path, layer = 'WBDHU8')
    flowlines = gpd.read_file(flowlines_path)
    fimr = gpd.read_file(fimr_path)

    # Joining the flowlines and fimr to output only the flowlines in the inundated area
    joined_flowlines_fimr = gpd.sjoin(flowlines, fimr)
    
    # Renaming index_right to prevent it from causing an error in the next spatial join
    joined_flowlines_fimr = joined_flowlines_fimr.rename(columns = {'index_right':'right_index'})
    # Getting a list of all the unique feature Ids from the joined flowlines
    unique_feature_ids = joined_flowlines_fimr.drop_duplicates('ID')
    # Changing cfs to cms
    unique_feature_ids['Q_at_Image_cms'] = unique_feature_ids['Q_at_Image']*0.028317
    # Make a discharge dataframe with only the feture ID and the Q values
    feature_discharge = unique_feature_ids[['ID','Q_at_Image_cms']]

    #flow_file_path = os.path.join(output_path, 'flow_file.csv')
    #feature_discharge.to_csv(flow_file_path, header = ['feature_id', 'discharge'],index = False)
    #print('flow file created at ',flow_file_path)

    # Takes the huc 8s that overlap the flowlines for the river 
    huc_joined = gpd.sjoin(wbd, joined_flowlines_fimr)
    huc_list = huc_joined.HUC8.unique()
    # Overlay the hucs
    fimr_hucs = wbd.loc[wbd.HUC8.isin(huc_list)]
    fimr_huc_union = gpd.overlay(joined_flowlines_fimr, fimr_hucs, how = 'union')
    fimr_huc_union = fimr_huc_union.rename(columns = {'Shape_Length':'Shapelength'})
    # Renames Shape_Length as Shapelength to eliminate a duplicate column and let it be exported
    huc_joined = huc_joined.rename(columns = {'Shape_Length':'Shapelength'})

    # Takes the usgs location ids 
    gages = fimr['USGS_Gage']
    split_gages = gages.iloc[0].split( ", ")
    # Takes the first usgs gauge idea that has the streamflow data for the whole fimr area
    first_gage = (split_gages[0])
 
    # Get metadata for all usgs_site_codes that are active in the U.S.
    metadata_url = f'{API_BASE_URL}/metadata' 
    # Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = [first_gage] #change to usgs location id from fimr dataframe
    must_include = 'usgs_data.active'
    metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = None )
   
    # Takes the stream order at the gauge
    stream_order = metadata_df['nwm_feature_data_stream_order']
    stream_order_float = stream_order[0]

    # Gets flow lines where the streasm order is equal to the magnitude of the mainstem
    correct_flow = joined_flowlines_fimr.loc[joined_flowlines_fimr['order_'] == stream_order_float]
    
    # Exploding the fimr inundaton polygon
    exploded_fimr = fimr.explode()
    # Taking only the fimr polygons that overlap the flowlines to minimize noise
    clean_fimr = gpd.sjoin(exploded_fimr, correct_flow)
    # Renaming columns to prevent duplicate column names in overlay
    clean_fimr = clean_fimr.rename(columns = {'index_right':'index_from_right'})

    # Overlaying the cleaned fimr data and the hucs it is in
    fimr_huc_union = gpd.overlay(clean_fimr, fimr_hucs, how = 'union')
    fimr_huc_union = fimr_huc_union.rename(columns = {'Shape_Length':'Shapelength'})
    # Renames Shape_Length as Shapelength to eliminate a duplicate column and let it be exported
    huc_joined = huc_joined.rename(columns = {'Shape_Length':'Shapelength'})
    
    # Looping through the three huc8s, getting the fimr data in each 
    for huc in huc_list:
      
        fimr_huc = fimr_huc_union.loc[fimr_huc_union.HUC8 == huc]
        shapes = []

        for index,row in fimr_huc.iterrows():

            if pd.isna(row.right_index):
                value = 0
            else:
                value = 1
            
            shapes.append((row.geometry, value))

        #Outputting Flow File
    
        flow_file_path = os.path.join(output_path, 'fimr_huc_' + huc + '_flows.csv')
        feature_discharge.to_csv(flow_file_path, header = ['feature_id', 'discharge'],index = False)
        print('flow file created at',flow_file_path)
        # output path for the rasters, float causes it to keep all the digits 
        out_path_raster = os.path.join(output_path,'fimr_huc_' + huc + '_extent.tif')

        #Rasterizing

        minx, miny, maxx, maxy = fimr_huc.geometry.total_bounds
        transform = affine.Affine(10, 0, minx, 0, -10, maxy)

        # Makeing the empty raster
        raster_shape = (int((maxy - miny)//10), int((maxx - minx)//10))
        raster = rasterio.features.rasterize(shapes,
                                     out_shape = raster_shape,
                                     transform = transform,
                                     fill = -999)
    
        with rasterio.open(out_path_raster, 'w', driver = 'GTiff', height = raster_shape[0], width = raster_shape[1], count = 1, dtype = raster.dtype, nodata = -999,
            crs = '+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs',transform = transform) as dst_dataset:
            # writing array into the empty raster
            dst_dataset.write_band(1, raster)
            print('Rasterized fimr file created at ', out_path_raster)

     # Parse Arguments   
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Get FIMR data for the different rivers from the different files')
    
    parser.add_argument('-f','--fimr_path', type = str, help = 'the path for the shapefile inputed from the FIMR dastabase', required = True) 
    parser.add_argument('-o','--output_path', type = str, help = 'output path for the flow file and FIMR rasters.', required = True)
    args = vars(parser.parse_args())

    fimr_to_benchmark(**args)


