#!/usr/bin/env python3

import os
import geopandas as gpd
import pandas as pd
import rasterio
import argparse
import pygeos
from shapely.wkb import dumps, loads


''' Get elevation at adjusted USGS gages locations'''


def crosswalk_usgs_gage(usgs_gages_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename,dem_adj_filename,ref_table_filename,output_table_filename):

    wbd_buffer = gpd.read_file(wbd_buffer_filename)
    usgs_gages = gpd.read_file(usgs_gages_filename, mask=wbd_buffer)
    dem_m = rasterio.open(dem_filename,'r')
    input_flows = gpd.read_file(input_flows_filename)
    input_catchment = gpd.read_file(input_catchment_filename)
    dem_adj = rasterio.open(dem_adj_filename,'r')
    ref_table = pd.read_csv(ref_table_filename)


    # Identify closest HydroID
    closest_catchment = gpd.sjoin(usgs_gages, input_catchment, how='left', op='within').reset_index(drop=True)
    closest_hydro_id = closest_catchment.filter(items=['site_no','HydroID','Min_Thal_Elev_m','Median_Thal_Elev_m','Max_Thal_Elev_m'])

    if input_flows.HydroID.dtype != 'int': input_flows.HydroID = input_flows.HydroID.astype(int)

    columns = ['usgs_gage_id','HydroID','dem_elevation','dem_adj_elevation','min_thal_elev', 'med_thal_elev','max_thal_elev']
    gage_data = []

    # Move USGS gage to stream
    for index, gage in usgs_gages.iterrows():

        print (f"usgs gage: {gage.site_no}")
        # Get stream attributes
        hydro_id = closest_hydro_id.loc[closest_hydro_id.site_no==gage.site_no].HydroID.item()
        min_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.site_no==gage.site_no].Min_Thal_Elev_m.item(),2)
        med_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.site_no==gage.site_no].Median_Thal_Elev_m.item(),2)
        max_thal_elev = round(closest_hydro_id.loc[closest_hydro_id.site_no==gage.site_no].Max_Thal_Elev_m.item(),2)

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
        dem_m_elev = list(rasterio.sample.sample_gen(dem_m,shply_referenced_gage.coords))[0].item().astype(float).round(2)
        dem_adj_elev = list(rasterio.sample.sample_gen(dem_adj,shply_referenced_gage.coords))[0].item().astype(float).round(2)

        # Print elevations to log file
        print(f"post adjusted catchment pixel ID: {dem_m_elev}")
        print(f"post adjusted elevation: {dem_adj_elev}")

        # Append dem_m_elev, dem_adj_elev, hydro_id, and gage number to table
        site_elevations = [gage.site_no, hydro_id, dem_m_elev, dem_adj_elev, min_thal_elev, med_thal_elev, max_thal_elev]
        gage_data.append(site_elevations)


    elev_table = pd.DataFrame(gage_data, columns=columns)
    # elev_table = elev_table.merge(ref_table, on='HydroID')

    if not elev_table.empty:
        elev_table.to_csv(output_table_filename,index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and get elevations')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-dem_m','--dem-filename',help='Catchment pixel raster',required=True)
    parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
    parser.add_argument('-cat','--input-catchment-filename', help='DEM derived catchments', required=True)
    parser.add_argument('-wbd','--wbd-buffer-filename', help='WBD buffer', required=True)
    parser.add_argument('-dem_adj','--dem-adj-filename', help='Thalweg adjusted DEM', required=True)
    parser.add_argument('-reftable','--ref-table-filename', help='Hand reference table', required=True)
    parser.add_argument('-outtable','--output-table-filename', help='Table to append data', required=True)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    dem_filename = args['dem_filename']
    input_flows_filename = args['input_flows_filename']
    input_catchment_filename = args['input_catchment_filename']
    wbd_buffer_filename = args['wbd_buffer_filename']
    dem_adj_filename = args['dem_adj_filename']
    ref_table_filename = args['ref_table_filename']
    output_table_filename = args['output_table_filename']

    crosswalk_usgs_gage(usgs_gages_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename, dem_adj_filename,ref_table_filename,output_table_filename)
