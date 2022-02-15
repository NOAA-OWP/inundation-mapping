#!/usr/bin/env python3

import os
import geopandas as gpd
import rasterio
import argparse
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

class GageCrosswalk(object):

    def __init__(self, usgs_subset_gages_filename, branch_id):
        
        self.branch_id = branch_id
        self.gages = self._load_gages(usgs_subset_gages_filename)

    def _load_gages(self, gages_filename):
        '''Reads gage geopackage from huc level and filters based on current branch id'''

        usgs_gages = gpd.read_file(gages_filename)
        return  usgs_gages[usgs_gages.levpa_id == self.branch_id]

    def catchment_sjoin(self, input_catchment_filename):
        '''Spatial joins gages to FIM catchments'''

        input_catchments = gpd.read_file(input_catchment_filename, dtype={'HydroID':int})
        self.gages = gpd.sjoin(self.gages, input_catchments[['HydroID', 'LakeID', 'geometry']], how='left')

    def snap_to_dem_derived_flows(self, input_flows_filename):
        '''Joins to dem derived flow line and produces snap_distance and geometry_snapped for sampling DEMs on the thalweg'''

        input_flows = gpd.read_file(input_flows_filename)
        input_flows['geometry_ln'] = input_flows.geometry
        self.gages = self.gages.merge(input_flows[['HydroID', 'geometry_ln']], on='HydroID')
        
        # Snap each point to its feature_id line
        self.gages['geometry_snapped'], self.gages['snap_distance'] = self.gages.apply(self.snap_to_line, axis=1,result_type='expand').T.values

    def sample_dem(self, dem_filename, column_name):
        '''Sample an input DEM at snapped points. Make sure to run self.gages.set_geometry("geometry_snapped") before runnig
        this method, otherwise the DEM will be sampled at the actual gage locations.'''

        coord_list = [(x,y) for x,y in zip(self.gages['geometry'].x , self.gages['geometry'].y)]
        
        with rasterio.open(dem_filename) as dem:
            self.gages[column_name] = [x[0] for x in dem.sample(coord_list)]

    def write(self, output_table_filename):
        '''Write to csv file'''

        elev_table = self.gages.copy()
        # Elev table cleanup
        elev_table.loc[elev_table['location_id'] == elev_table['nws_lid'], 'location_id'] = None # set location_id to None where there isn't a gage
        elev_table = elev_table[['location_id', 'nws_lid', 'feature_id', 'HydroID', 'levpa_id', 'dem_elevation', 'dem_adj_elevation', 'order_', 'LakeID', 'HUC8', 'snap_distance']]

        if not elev_table.empty:
            elev_table.to_csv(output_table_filename, index=False)

    @staticmethod
    def snap_to_line(row):
        if not row.geometry_ln:
            return (None, None)
        snap_geom = row.geometry_ln.interpolate(row.geometry_ln.project(row.geometry))
        return (snap_geom, snap_geom.distance(row.geometry))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and get elevations')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gage subset at the huc level', required=True)
    parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
    parser.add_argument('-cat','--input-catchment-filename', help='DEM derived catchments', required=True)
    parser.add_argument('-dem','--dem-filename',help='DEM',required=True)
    parser.add_argument('-dem_adj','--dem-adj-filename', help='Thalweg adjusted DEM', required=True)
    parser.add_argument('-outtable','--output-table-filename', help='Table to append data', required=True)
    parser.add_argument('-b','--branch-id', help='Branch ID used to filter the gages', type=str, required=True)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    input_flows_filename = args['input_flows_filename']
    input_catchment_filename = args['input_catchment_filename']
    dem_filename = args['dem_filename']
    dem_adj_filename = args['dem_adj_filename']
    output_table_filename = args['output_table_filename']
    branch_id = args['branch_id']

    assert os.path.isfile(usgs_gages_filename), f"The input file {usgs_gages_filename} does not exist."

    # Instantiate class
    gage_crosswalk = GageCrosswalk(usgs_gages_filename, branch_id)
    if gage_crosswalk.gages.empty:
        print(f'There are no gages for branch {branch_id}')
        os._exit(0)
    # Spatial join to fim catchments
    gage_crosswalk.catchment_sjoin(input_catchment_filename)
    # Snap to dem derived flow lines
    gage_crosswalk.snap_to_dem_derived_flows(input_flows_filename)
    # Set gage geometry to the snapped points
    gage_crosswalk.gages.set_geometry('geometry_snapped')
    # Sample DEM and thalweg adjusted DEM
    gage_crosswalk.sample_dem(dem_filename, 'dem_elevation')
    gage_crosswalk.sample_dem(dem_adj_filename, 'dem_adj_elevation')
    # Write to csv
    num_gages = len(gage_crosswalk.gages)
    print(f"{num_gages} gage{'' if num_gages == 1 else 's'} in branch {branch_id}")
    gage_crosswalk.write(output_table_filename)

"""
python /foss_fim/src/usgs_gage_crosswalk.py -gages /data/outputs/carson_gms_bogus/02020005/usgs_subset_gages.gpkg -flows /data/outputs/carson_gms_bogus/02020005/branches/3246000305/demDerived_reaches_split_filtered_3246000305.gpkg -cat /data/outputs/carson_gms_bogus/02020005/branches/3246000305/gw_catchments_reaches_filtered_addedAttributes_3246000305.gpkg -dem /data/outputs/carson_gms_bogus/02020005/branches/3246000305/dem_meters_3246000305.tif -dem_adj /data/outputs/carson_gms_bogus/02020005/branches/3246000305/dem_thalwegCond_3246000305.tif -outtable /data/outputs/carson_gms_bogus/02020005/branches/3246000305/usgs_elev_table.csv -b 3246000305

python /foss_fim/src/usgs_gage_crosswalk.py -gages /data/outputs/carson_gms_bogus/02020005/usgs_subset_gages.gpkg -flows /data/outputs/carson_gms_bogus/02020005/branches/3246000257/demDerived_reaches_split_filtered_3246000257.gpkg -cat /data/outputs/carson_gms_bogus/02020005/branches/3246000257/gw_catchments_reaches_filtered_addedAttributes_3246000257.gpkg -dem /data/outputs/carson_gms_bogus/02020005/branches/3246000257/dem_meters_3246000257.tif -dem_adj /data/outputs/carson_gms_bogus/02020005/branches/3246000257/dem_thalwegCond_3246000257.tif -outtable /data/outputs/carson_gms_bogus/02020005/branches/3246000257/usgs_elev_table.csv -b 3246000257
"""