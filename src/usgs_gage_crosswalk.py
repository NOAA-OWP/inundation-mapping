#!/usr/bin/env python3

import argparse
import os
import warnings
from os.path import join

import geopandas as gpd
import rasterio


warnings.simplefilter("ignore")


''' Get elevation at adjusted USGS gages locations

    Parameters
    ----------
    usgs_gages_filename : str
        File path of USGS stations subset layer. i.e. '/data/path/usgs_subset_gages.gpkg'
    input_flows_filename : str
        File path of FIM streams layer. i.e. '/data/path/demDerived_reaches_split_filtered_3246000257.gpkg'
    input_catchment_filename : str
        File path of FIM catchment layer. i.e.
            '/data/path/gw_catchments_reaches_filtered_addedAttributes_3246000257.gpkg'
    dem_filename : str
        File path of original DEM. i.e. '/data/path/dem_meters_3246000257.tif'
    dem_adj_filename : str
        File path of thalweg adjusted DEM. i.e. '/data/path/dem_thalwegCond_3246000257.tif'
    output_directory : str
        Directory to create output table. i.e. '/data/path/'
    branch_id: str
        ID of the current branch i.e. '3246000257'
'''


class GageCrosswalk(object):
    def __init__(self, usgs_subset_gages_filename, branch_id):
        self.branch_id = branch_id
        self.gages = self._load_gages(usgs_subset_gages_filename)

    def run_crosswalk(
        self, input_catchment_filename, input_flows_filename, dem_filename, dem_adj_filename, output_directory
    ):
        '''Run the gage crosswalk steps: 1) spatial join to branch catchments layer 2) snap sites to
        the dem-derived flows 3) sample both dems at the snapped points 4) write the crosswalked points
        to usgs_elev_table.csv
        '''
        if self.gages.empty:
            print(f'There are no gages for branch {branch_id}')
            os._exit(0)
        # Spatial join to fim catchments
        self.catchment_sjoin(input_catchment_filename)
        if self.gages.empty:
            print(f'There are no gages for branch {branch_id}')
            os._exit(0)

        # Snap to dem derived flow lines
        self.snap_to_dem_derived_flows(input_flows_filename)
        # Sample DEM and thalweg adjusted DEM
        self.sample_dem(dem_filename, 'dem_elevation')
        self.sample_dem(dem_adj_filename, 'dem_adj_elevation')
        # Write to csv
        num_gages = len(self.gages)
        print(f"{num_gages} gage{'' if num_gages == 1 else 's'} in branch {self.branch_id}")
        self.write(output_directory)

    def _load_gages(self, gages_filename):
        '''Reads gage geopackage from huc level and filters based on current branch id'''

        usgs_gages = gpd.read_file(gages_filename)
        return usgs_gages[(usgs_gages.levpa_id == self.branch_id)]

    def catchment_sjoin(self, input_catchment_filename):
        '''Spatial joins gages to FIM catchments'''

        input_catchments = gpd.read_file(input_catchment_filename, dtype={'HydroID': int})
        self.gages = gpd.sjoin(self.gages, input_catchments[['HydroID', 'LakeID', 'geometry']], how='inner')

    def snap_to_dem_derived_flows(self, input_flows_filename):
        '''
        Joins to dem derived flow line and produces snap_distance and
        geometry_snapped for sampling DEMs on the thalweg
        '''

        input_flows = gpd.read_file(input_flows_filename)
        input_flows['geometry_ln'] = input_flows.geometry
        self.gages = self.gages.merge(input_flows[['HydroID', 'geometry_ln']], on='HydroID')

        # Snap each point to its feature_id line
        self.gages['geometry_snapped'], self.gages['snap_distance'] = self.gages.apply(
            self.snap_to_line, axis=1, result_type='expand'
        ).T.values
        self.gages.geometry_snapped = self.gages.geometry_snapped.astype('geometry')

    def sample_dem(self, dem_filename, column_name):
        '''
        Sample an input DEM at snapped points. Make sure to run self.gages.set_geometry("geometry_snapped")
        before running this method, otherwise the DEM will be sampled at the actual gage locations.
        '''

        coord_list = [
            (x, y) for x, y in zip(self.gages['geometry_snapped'].x, self.gages['geometry_snapped'].y)
        ]

        with rasterio.open(dem_filename) as dem:
            self.gages[column_name] = [x[0] for x in dem.sample(coord_list)]

    def write(self, output_directory):
        '''Write to csv file'''

        # Prep and write out file
        elev_table = self.gages.copy()
        elev_table.loc[
            elev_table['location_id'] == elev_table['nws_lid'], 'location_id'
        ] = None  # set location_id to None where there isn't a gage
        elev_table = elev_table[elev_table['location_id'].notna()]
        elev_table.source = elev_table.source.apply(str.lower)

        # filter for just ras2fim entries (note that source column includes suffix with version number)
        ras_elev_table = elev_table[elev_table['source'].str.contains('ras2fim')]
        ras_elev_table = ras_elev_table[
            [
                "location_id",
                "HydroID",
                "feature_id",
                "levpa_id",
                "HUC8",
                "dem_elevation",
                "dem_adj_elevation",
                "source",
                "stream_stn",
            ]
        ]
        if not ras_elev_table.empty:
            ras_elev_table.to_csv(join(output_directory, 'ras_elev_table.csv'), index=False)
        else:
            print(
                'INFO: there were no ras2fim points located in this huc'
                ' (note that most hucs do not have ras2fim data)'
            )

        # filter for just usgs entries
        # look for source attributes that do not contain "ras2fim"
        usgs_elev_table = elev_table[~elev_table['source'].str.contains('ras2fim')]
        if not usgs_elev_table.empty:
            usgs_elev_table.to_csv(join(output_directory, 'usgs_elev_table.csv'), index=False)

    @staticmethod
    def snap_to_line(row):
        if not row.geometry_ln:
            return (None, None)
        snap_geom = row.geometry_ln.interpolate(row.geometry_ln.project(row.geometry))
        return (snap_geom, snap_geom.distance(row.geometry))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and get elevations')
    parser.add_argument(
        '-gages', '--usgs-gages-filename', help='USGS gage subset at the huc level', required=True
    )
    parser.add_argument('-flows', '--input-flows-filename', help='DEM derived streams', required=True)
    parser.add_argument('-cat', '--input-catchment-filename', help='DEM derived catchments', required=True)
    parser.add_argument('-dem', '--dem-filename', help='DEM', required=True)
    parser.add_argument('-dem_adj', '--dem-adj-filename', help='Thalweg adjusted DEM', required=True)
    parser.add_argument(
        '-out', '--output-directory', help='Directory where output tables created', required=True
    )
    parser.add_argument(
        '-b', '--branch-id', help='Branch ID used to filter the gages', type=str, required=True
    )

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    input_flows_filename = args['input_flows_filename']
    input_catchment_filename = args['input_catchment_filename']
    dem_filename = args['dem_filename']
    dem_adj_filename = args['dem_adj_filename']
    output_directory = args['output_directory']
    branch_id = args['branch_id']

    assert os.path.isfile(usgs_gages_filename), f"The input file {usgs_gages_filename} does not exist."

    # Instantiate class
    gage_crosswalk = GageCrosswalk(usgs_gages_filename, branch_id)
    gage_crosswalk.run_crosswalk(
        input_catchment_filename, input_flows_filename, dem_filename, dem_adj_filename, output_directory
    )

"""
Examples:

python /foss_fim/src/usgs_gage_crosswalk.py -gages /outputs/carson_gms_bogus/02020005/usgs_subset_gages.gpkg
    -flows /outputs/carson_gms_bogus/02020005/branches/3246000305/demDerived_reaches_split_filtered_3246000305.gpkg
    -cat /outputs/carson_gms_bogus/02020005/branches/3246000305/gw_catchments_reaches_filtered_addedAttributes_3246000305.gpkg
    -dem /outputs/carson_gms_bogus/02020005/branches/3246000305/dem_meters_3246000305.tif
    -dem_adj /outputs/carson_gms_bogus/02020005/branches/3246000305/dem_thalwegCond_3246000305.tif
    -outtable /outputs/carson_gms_bogus/02020005/branches/3246000305/usgs_elev_table.csv
    -b 32460003 05

python /foss_fim/src/usgs_gage_crosswalk.py -gages /outputs/carson_gms_bogus/02020005/usgs_subset_gages.gpkg
    -flows /outputs/carson_gms_bogus/02020005/branches/3246000257/demDerived_reaches_split_filtered_3246000257.gpkg
    -cat /outputs/carson_gms_bogus/02020005/branches/3246000257/gw_catchments_reaches_filtered_addedAttributes_3246000257.gpkg
    -dem /outputs/carson_gms_bogus/02020005/branches/3246000257/dem_meters_3246000257.tif
    -dem_adj /outputs/carson_gms_bogus/02020005/branches/3246000257/dem_thalwegCond_3246000257.tif
    -outtable /outputs/carson_gms_bogus/02020005/branches/3246000257/usgs_elev_table.csv
    -b 32460002 57

python /foss_fim/src/usgs_gage_crosswalk.py -gages /outputs/carson_gage_test/04130001/usgs_subset_gages.gpkg
    -flows /outputs/carson_gage_test/04130001/branches/9041000030/demDerived_reaches_split_filtered_9041000030.gpkg
    -cat /outputs/carson_gage_test/04130001/branches/9041000030/gw_catchments_reaches_filtered_addedAttributes_9041000030.gpkg
    -dem /outputs/carson_gage_test/04130001/branches/9041000030/dem_meters_9041000030.tif
    -dem_adj /outputs/carson_gage_test/04130001/branches/904100030/dem_thalwegCond_0941000030.tif
    -outtable /outputs/carson_gage_test/04130001/branches/9041000030/usgs_elev_table.csv
    -b 90410000 30
"""
