import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterstats import zonal_stats


def min_hand_excluding_zero(values):
    # Convert to unmasked array and drop 0 and masked/nodata
    # return np.nan if on NoData Hand to be able to filter them later
    data = np.ma.filled(values.astype(float), np.nan)  # Convert masked to nan
    valid = data[(data != 0) & (~np.isnan(data))]
    return float(np.min(valid)) if valid.size > 0 else np.nan


def process_roads_fimpact(
    hand_grid_raster: str, osm_road_vector: str, catchments_path: str, output_path: str
) -> None:
    """
    Processes road impacts within a HUC region using the FIMpact framework and  saves the result to a csv file.

    Parameters:
    - source_hand_raster (str): REQUIRED. Path to the source HAND raster file
    - osm_road_vector (str): REQUIRED. Path to a GeoPackage (GPKG) file containing the road segments.
    - catchments (srr): REQUIRED. Path to HAND catchment
    - output_path (str): REQUIRED. Path where the output CSV file will be saved.

    """
    # get branch id from output file passed from fim pipeline
    branch_id = Path(output_path).parent.name

    # read hand grid
    with rasterio.open(hand_grid_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # read roads data
    roads_gdf = gpd.read_file(osm_road_vector)

    # remove this extra id
    if 'catchment_id' in roads_gdf.columns:
        roads_gdf = roads_gdf.drop(columns=['catchment_id'])

    # read HAND catchments to split the roads segments for each intersected HYDROIDs/feature_ids.
    # this is different than bridges, because a road can exists within multiple HydroID/hydroTable and
    # we need to consider threshold hand for all intersected HydroID.
    catchments_df = gpd.read_file(catchments_path, columns=['HydroID', 'feature_id', 'order_', 'geometry'])

    # possible that feature id and hydro id be as type float. first make them int and then str
    catchments_df['feature_id'] = catchments_df['feature_id'].astype(int).astype(str)
    catchments_df['HydroID'] = catchments_df['HydroID'].astype(int).astype(str)

    # further split the roads based on HAND catchments
    roads_gdf_splitted = gpd.overlay(roads_gdf, catchments_df, how="intersection")

    # zonal stats does not like the lines input if it is jagged (can happenen because
    # of overlaying with catchment boundaries) and can yield wrong results.
    # threfore, we explode the lines to make sure all segments are single linestring.
    roads_gdf_splitted = roads_gdf_splitted.explode(index_parts=True).reset_index(drop=True)

    if not roads_gdf_splitted.empty:
        roads_gdf_splitted['branch'] = branch_id

        # Call zonal_stats with the custom stat
        stats = zonal_stats(
            roads_gdf_splitted['geometry'],
            hand_grid_array,
            affine=hand_grid_profile['transform'],
            nodata=hand_grid_profile["nodata"],
            all_touched=True,
            stats=[],  # No built-in stats needed
            add_stats={"min_ex0": min_hand_excluding_zero},
        )

        # we do not care about the length of inundated roads... just the min hand anywhere along the length
        roads_gdf_splitted.loc[:, 'threshold_hand'] = [x.get('min_ex0') for x in stats]

        # it is possible that roads cross areas of a HAND with nan data (levee), so make sure to remove those Nan threshold hands
        roads_gdf_splitted = roads_gdf_splitted.dropna(subset=['threshold_hand'])

        # no need to save geometry --helpful to save disc size
        roads_gdf_splitted = roads_gdf_splitted.drop(columns='geometry')

        # group by segment id, hydroid, and report the min of threshold hand to remove extra exploded road segments in each hydroid
        min_idx = roads_gdf_splitted.groupby(['osmid_catchid', 'HydroID'])['threshold_hand'].idxmin()
        roads_gdf_splitted = roads_gdf_splitted.loc[min_idx]

        # make sure to record ids as str for csv output file
        cols_to_str = ['osmid', 'huc8', 'HydroID', 'feature_id', 'order_', 'branch']
        roads_gdf_splitted[cols_to_str] = roads_gdf_splitted[cols_to_str].astype(str)

        roads_gdf_splitted.to_csv(output_path, index=False)
    else:
        print(f'no splitted roads for {branch_id}')

    del catchments_df


if __name__ == "__main__":
    '''
    Sample usage :
        python foss_fim/src/process_roads_fimpact.py
        -g outputs/roads/02050206/branches/0/rem_zeroed_masked_0.tif
        -c outputs/roads/02050206/branches/0/gw_catchments_reaches_filtered_addedAttributes_crosswalked_0.gpkg
        -r outputs/roads/02050206/osm_roads_subset.gpkg
        -o outputs/roads/02050206/branches/0/osm_roads_fimpact_0.csv

    '''

    parser = argparse.ArgumentParser(description='Process roads FIMpacts')

    parser.add_argument(
        '-g', '--hand_grid_raster', help='REQUIRED: Path for HAND grid raster file', required=True
    )

    parser.add_argument(
        '-r',
        '--osm_road_vector',
        help='REQUIRED: Path to a GPKG file containing the osm roads centerline ',
        required=True,
    )

    parser.add_argument(
        '-c',
        '--catchments_path',
        help='REQUIRED: Path and file name of the HAND catchments geopackage',
        required=True,
    )

    parser.add_argument(
        '-o', '--output_path', help='REQUIRED: Path where the output csv file will be saved', required=True
    )

    args = vars(parser.parse_args())

    process_roads_fimpact(**args)
