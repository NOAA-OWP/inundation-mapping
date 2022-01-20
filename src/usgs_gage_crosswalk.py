#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import rasterio
import argparse
import pygeos
from shapely.wkb import dumps, loads
import warnings
from utils.shared_functions import mem_profile
warnings.simplefilter("ignore")


''' Get elevation at adjusted USGS gages locations

    Parameters
    ----------
    usgs_gages_filename : str
        File name of USGS stations layer.
    dem_filename : str
        File name of original DEM.
    input_flows_filename : str
        File name of FIM streams layer.
    input_catchment_filename : str
        File name of FIM catchment layer.
    wbd_buffer_filename : str
        File name of buffered wbd.
    dem_adj_filename : str
        File name of thalweg adjusted DEM.
    output_table_filename : str
        File name of output table.
'''


@mem_profile
def crosswalk_usgs_gage(usgs_gages_filename,nws_lid_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename,dem_adj_filename,output_table_filename,extent,huc8):

    wbd_buffer = gpd.read_file(wbd_buffer_filename)
    usgs_gages = gpd.read_file(usgs_gages_filename, mask=wbd_buffer, dtype={'huc': object})
    ahps_sites = gpd.read_file(nws_lid_filename, mask=wbd_buffer)
    dem_m = rasterio.open(dem_filename,'r')
    input_flows = gpd.read_file(input_flows_filename)
    input_catchment = gpd.read_file(input_catchment_filename)
    dem_adj = rasterio.open(dem_adj_filename,'r')

    #MS extent use gages that are mainstem & match huc8 id
    if extent == "MS":
        usgs_gages = usgs_gages.query('curve == "yes" & mainstem == "yes"')
        usgs_gages = usgs_gages[usgs_gages.HUC8 == huc8]
    #FR extent use gages that are not mainstem & match huc8 id
    if extent == "FR":
        usgs_gages = usgs_gages.query('curve == "yes" & mainstem == "no"')
        usgs_gages = usgs_gages[usgs_gages.HUC8 == huc8]

    if input_flows.HydroID.dtype != 'int': input_flows.HydroID = input_flows.HydroID.astype(int)

     # Get AHPS sites within the HUC and add them to the USGS dataset
    ahps_sites = ahps_sites[ahps_sites.HUC8 == huc8] # filter to HUC8
    ahps_sites.rename(columns={'nwm_feature_id':'feature_id',
                          'usgs_site_code':'location_id'}, inplace=True)
    ahps_sites = ahps_sites[ahps_sites.location_id.isna()] # Filter sites that are already in the USGS dataset
    usgs_gages = usgs_gages.append(ahps_sites[['feature_id', 'nws_lid', 'location_id', 'HUC8', 'name', 'states','geometry']])
    usgs_gages.location_id.fillna(usgs_gages.nws_lid, inplace=True) 

    # Identify closest HydroID
    usgs_gages.rename(columns={'feature_id':'feature_id_wrds'}, inplace=True)# rename feature_id attribute from USGS gages (obtained from WRDS api)
    closest_catchment = gpd.sjoin(usgs_gages, input_catchment, how='left', op='within').reset_index(drop=True)
    closest_hydro_id = closest_catchment.filter(items=['location_id','HydroID','min_thal_elev','med_thal_elev','max_thal_elev', 'order_','feature_id_wrds','feature_id'])
    closest_hydro_id = closest_hydro_id.dropna()

    # Get USGS gages that are within catchment boundaries
    usgs_gages = usgs_gages.loc[usgs_gages.location_id.isin(list(closest_hydro_id.location_id))]

    columns = ['location_id','nws_lid', 'HydroID','dem_elevation','dem_adj_elevation','min_thal_elev', 'med_thal_elev','max_thal_elev','str_order','feature_id_wrds','feature_id','gage_distance_to_line']
    gage_data = []

    # Move USGS gage to stream
    for index, gage in usgs_gages.iterrows():

        # Get stream attributes
        hydro_id = closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].HydroID.item()
        str_order = str(int(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].order_.item()))
        feat_id = str(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].feature_id.item())
        feat_id_wrds = str(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].feature_id_wrds.item())
        min_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].min_thal_elev.item(),2)
        med_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].med_thal_elev.item(),2)
        max_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.location_id==gage.location_id].max_thal_elev.item(),2)

        # Convert headwater point geometries to WKB representation
        wkb_gages = dumps(gage.geometry)

        # Create pygeos headwater point geometries from WKB representation
        gage_bin_geom = pygeos.io.from_wkb(wkb_gages)

        # Closest segment to headwater
        closest_stream = input_flows.loc[input_flows.HydroID==hydro_id]
        wkb_closest_stream = dumps(closest_stream.geometry.item())
        stream_bin_geom = pygeos.io.from_wkb(wkb_closest_stream)

        # Linear reference headwater to closest stream segment
        gage_distance_to_line = pygeos.linear.line_locate_point(stream_bin_geom, gage_bin_geom)
        referenced_gage = pygeos.linear.line_interpolate_point(stream_bin_geom, gage_distance_to_line)

        # Convert geometries to wkb representation
        bin_referenced_gage = pygeos.io.to_wkb(referenced_gage)

        # Convert to shapely geometries
        shply_referenced_gage = loads(bin_referenced_gage)

        # Sample rasters at adjusted gage
        dem_m_elev = round(list(rasterio.sample.sample_gen(dem_m,shply_referenced_gage.coords))[0].item(),2)
        dem_adj_elev = round(list(rasterio.sample.sample_gen(dem_adj,shply_referenced_gage.coords))[0].item(),2)

        # Append dem_m_elev, dem_adj_elev, hydro_id, and gage number to table
        site_elevations = [str(gage.location_id), str(gage.nws_lid), str(hydro_id), dem_m_elev, dem_adj_elev, min_thal_elev, med_thal_elev, max_thal_elev,str(str_order),str(feat_id_wrds),str(feat_id),gage_distance_to_line]
        gage_data.append(site_elevations)

    elev_table = pd.DataFrame(gage_data, columns=columns)
    elev_table.loc[elev_table['location_id'] == elev_table['nws_lid'], 'location_id'] = None # set location_id to None where there isn't a gage
    elev_table.loc[elev_table['nws_lid'] == 'Bogus_ID', 'nws_lid'] = None  # set bogus nws_lids to None

    if not elev_table.empty:
        elev_table.to_csv(output_table_filename,index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and get elevations')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-ahps','--nws-lid-filename', help='USGS gages', required=True)
    parser.add_argument('-dem','--dem-filename',help='DEM',required=True)
    parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
    parser.add_argument('-cat','--input-catchment-filename', help='DEM derived catchments', required=True)
    parser.add_argument('-wbd','--wbd-buffer-filename', help='WBD buffer', required=True)
    parser.add_argument('-dem_adj','--dem-adj-filename', help='Thalweg adjusted DEM', required=True)
    parser.add_argument('-outtable','--output-table-filename', help='Table to append data', required=True)
    parser.add_argument('-e', '--extent', help="extent configuration entered by user when running fim_run.sh", required = True)
    parser.add_argument('-huc','--huc8-id', help='HUC8 ID (to verify gage location huc)', type=str, required=True)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    nws_lid_filename = args['nws_lid_filename']
    dem_filename = args['dem_filename']
    input_flows_filename = args['input_flows_filename']
    input_catchment_filename = args['input_catchment_filename']
    wbd_buffer_filename = args['wbd_buffer_filename']
    dem_adj_filename = args['dem_adj_filename']
    output_table_filename = args['output_table_filename']
    extent = args['extent']
    huc8 = args['huc8_id']

    crosswalk_usgs_gage(usgs_gages_filename,nws_lid_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename, dem_adj_filename,output_table_filename, extent,huc8)
