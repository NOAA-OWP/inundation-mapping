
import argparse
import glob
import os
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio import features
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats
from src.process_roads_fimpact import min_hand_excluding_zero
from src.heal_bridges_osm import flow_lookup
from tools.road_inundation import stage_lookup
import traceback
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger

import geopandas as gpd
import pandas as pd
'''
step0: Ideantify which HUCs intersect with provided geometries. so d=for each geometry ad a column of which hucs must be processed. 

step 1: make a for loop for all branches:

if  [ -f $tempHucDataDir/osm_roads_subset.gpkg ]; then
    echo -e $startDiv"Process roads FIMpact $hucNumber $current_branch_id"
    python3 $srcDir/process_roads_fimpact.py \
        -g $tempCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif \
        -r $tempHucDataDir/osm_roads_subset.gpkg \
        -c $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg \
        -o $tempCurrentBranchDataDir/osm_roads_fimpact_$current_branch_id.csv

Three new columns are added to the road dataset: threshold_hand, HydroID, and feature_id for each branch.


Step2: get discharge value corresponding to each threshold_hand from the branch’s HydroTable (per HydroID) and assigns it as threshold_discharge.
Any record with a threshold_hand value greater than 25m (the maximum stage listed in the HydroTables) is removed entirely.

Step3: get the flow file, find the discharge for that feature_id, flag as flooded and compute depth as shown in tools/road_inundation.py.


'''

def get_evaluated_stage(fim_path,huc,branch,fimpact_df):
    # Ensure the column exists before assignment
    fimpact_df['evaluated_stage'] = np.nan

    # Loop over each unique branch
    for branch_id in fimpact_df['branch'].unique():
        # Load hydrotable once for the branch
        hydrotable_filename = os.path.join(fim_path,huc,'branches',branch ,f'hydroTable_{branch_id}.csv' )
        hydrotable_df = pd.read_csv(
            hydrotable_filename,
            dtype={'HydroID': str, 'stage': float, 'discharge_cms': float},
            usecols=['HydroID', 'discharge_cms', 'stage'],
        )

        # Subset fimpact_df for this branch
        branch_df = fimpact_df[(fimpact_df['branch'] == branch_id)&(fimpact_df['HUC'] == huc)]

        # Interpolate for each row in this subset
        for _, row in branch_df.iterrows():
            single_hydro = hydrotable_df[hydrotable_df.HydroID == row.HydroID]
            evaluated_stage = stage_lookup(
                row.evaluated_discharge, single_hydro['discharge_cms'], single_hydro['stage']
            )
            fimpact_df.at[row.name, 'evaluated_stage'] = evaluated_stage
    return fimpact_df

def compute_flood_depth(fim_path,huc,branch,fimpact_df):
    # Read the flow_file. make sure feaure id is str

    # read hydroTable and find the stage corresponding to given discharge
    # subtract that from threshold_hand...this is called flood_depth
    # so when we report the inundated roads, we also report the case with maximum flood depth
    # add given discharge
    fimpact_df = fimpact_df.merge(flow_file_data, on='feature_id')

    # change the name of the given flow to evaluated discharge
    fimpact_df.rename(columns={'discharge': 'evaluated_discharge'}, inplace=True)

    # selected the inundated records
    fimpact_df = fimpact_df[fimpact_df['evaluated_discharge'] > fimpact_df['threshold_discharge']]

    # add evaluated stage. For performance, read hydrotable of each branch once for all records in that branch
    get_evaluated_stage(fim_path,huc,branch,fimpact_df)

    fimpact_df['flood_depth'] = fimpact_df['evaluated_stage'] - fimpact_df['threshold_hand']

    # for now, remove any record with negative flood depth. these may happen due to non-monotonic src especially in branch zero.
    fimpact_df = fimpact_df[fimpact_df['flood_depth'] >= 0]

    return fimpact_df


def get_threshold_flow(fim_path,huc,branch,fimpact_df):
    hydrotable_path = os.path.join(fim_path,huc,'branches',branch, f'hydroTable_{branch}.csv')
    hydrotable = pd.read_csv(hydrotable_path, dtype={'HydroID': str,'stage': float,'discharge_cms': float,}, usecols=['HydroID','stage','discharge_cms'])

    # make sure to remove any road with threshold hand greater than 25m (the max available in HydroTable)
    # these roads are assumed to be non-inundated. so no need to process them further.
    fimpact_df = fimpact_df[fimpact_df['threshold_hand'] < 25].copy()

    if fimpact_df.empty:
        return None
    
    fimpact_df.loc[:,'threshold_discharge'] = fimpact_df.apply(
        lambda row: flow_lookup(row.threshold_hand, row.HydroID, hydrotable), axis=1
    )

    # Convert stages and dischrages to ft and cfs respectively
    fimpact_df.loc[:,'threshold_hand_ft'] = fimpact_df['threshold_hand'] * 3.28084
    fimpact_df.loc[:,'threshold_discharge_cfs'] = fimpact_df['threshold_discharge'] * 35.3147
    return fimpact_df

def get_threshold_hand(fim_path,huc,branch,roads_gdf):
    # read hand grid
    hand_grid_path=os.path.join(fim_path,huc,'branches',branch,'rem_zeroed_masked_%s.tif'%branch)
    catchments_path=os.path.join(fim_path,huc,'branches',branch,'gw_catchments_reaches_filtered_addedAttributes_crosswalked_%s.gpkg'%branch)
    with rasterio.open(hand_grid_path, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # remove interfering id from input geometry, if available
    if 'catchment_id' in roads_gdf.columns:
        roads_gdf = roads_gdf.drop(columns=['catchment_id'])

    # read HAND catchments to split the roads/geometry segments for each intersected HYDROIDs/feature_ids.
    # because a road can exists within multiple HydroID/hydroTable and
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

    if roads_gdf_splitted.empty:
        print(f'no splitted roads for {branch}')
        return None

    # tag the processed branch
    roads_gdf_splitted['branch'] = branch

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

    #the REM unit after fim pipeline changes to mm.so need to convert to meter first.
    # this unit conversion might provide slight discrepencies compared to results in 'osm_roads_fimpact_xxx.csv' cretaed during a fim pileline 
    roads_gdf_splitted['threshold_hand']=roads_gdf_splitted['threshold_hand']/1000 

    # it is possible that roads cross areas of a HAND with nan data (levee), so make sure to remove those Nan threshold hands
    roads_gdf_splitted = roads_gdf_splitted.dropna(subset=['threshold_hand'])

    # no need to save geometry since we want to report the final result for the full lenght of roads and we need to merge to initial roads at the very end again
    roads_gdf_splitted = roads_gdf_splitted.drop(columns='geometry')

    # group by segment id, hydroid, and report the min of threshold hand to remove extra exploded road segments in each hydroid
    min_idx = roads_gdf_splitted.groupby(['osmid_catchid', 'HydroID'])['threshold_hand'].idxmin()
    fimpact_df = roads_gdf_splitted.loc[min_idx]

    # make sure to record ids as str for csv output file
    cols_to_str = ['osmid', 'huc8', 'HydroID', 'feature_id', 'branch']
    fimpact_df[cols_to_str] = fimpact_df[cols_to_str].astype(str)

    return fimpact_df


def find_intersecting_hucs(fim_path,roads_gdf):
    '''
    for each input geometry file, it finds the intersecting HUCs available in the provide fim run path. 
    The boundary of each huc is read from wbd8_clp.gpkg file within each huc outputs. 
    If an input geometry intersects two neighboring HUCs, two records are created for thatgeometry (one for each huc)
    and the tool will work on each huc independently and the more conservative result (higher flood depth) is reported.
    '''

    print("identifying intersected HUCs for each road segment")
    # all input features must be in 4326
    #TODO raise error ?
    roads_gdf = roads_gdf.to_crs(4326)

    #now process hucs and make a gdf for their boundary in 4326
    hucs = [huc for huc in os.listdir(fim_path) if re.match(r'\d{8}', huc)]

    hucs_boundaries_geoms = []
    for huc in hucs:
        huc_boundary_gdf = gpd.read_file(os.path.join(fim_path,huc,'wbd8_clp.gpkg'))
        huc_boundary_gdf["HUC"] = huc

        huc_boundary_gdf["original_crs"] = huc_boundary_gdf.crs.to_string()
        huc_boundary_gdf = huc_boundary_gdf.to_crs(4326)
        # if applicable, merge all polygons in wbd (which often contains multiple polygon pieces or sub-basins) into one unified geometry
        huc_boundary_gdf = huc_boundary_gdf.dissolve().reset_index(drop=True)
        
        hucs_boundaries_geoms.append(huc_boundary_gdf[["HUC", "geometry","original_crs"]])

    hucs_boundaries_gdf = gpd.GeoDataFrame(pd.concat(hucs_boundaries_geoms, ignore_index=True), crs="EPSG:4326")

    roads_in_hucs_gdf = gpd.sjoin(roads_gdf, hucs_boundaries_gdf, how="inner", predicate="intersects")
    # Drop join index column added by sjoin
    roads_in_hucs_gdf = roads_in_hucs_gdf.drop(columns=["index_right"], errors="ignore")
    return roads_in_hucs_gdf

def task_fn(  fim_path, unique_id, this_huc_roads_gdf,  file_logger, screen_queue, task_id):
    file_logger.info(f"Started processing {task_id}")
    try:
        huc,branch=unique_id.split("_")
        fimpact_df=get_threshold_hand(fim_path,huc,branch,this_huc_roads_gdf)
        if fimpact_df is not None:
            fimpact_df=get_threshold_flow(fim_path,huc,branch,fimpact_df)
            if fimpact_df is not None:
                fimpact_df=compute_flood_depth(fim_path,huc,branch,fimpact_df)
        return 1,[fimpact_df, True]
    except Exception as e:
        file_logger.error(f"❌ Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0,[None, False]


flow_file='data/inputs/rating_curve/nwm_recur_flows/nwm3_17C_recurr_50_0_cms.csv'

# output_dir= os.path.join ('outputs/flood_depth/run_05030104/','flood_depth_output')
output_dir= os.path.join ('outputs/flood_depth/run_4_hucs/','flood_depth_output')
os.makedirs(output_dir, exist_ok=True)

# Create the logger
log_file_path = os.path.join(output_dir, "sample.log")
file_logger = setup_mp_file_logger(log_file_path, logger_name='depth_logger')
print('started the process')
file_logger.info('started the process')

# fim_path='outputs/flood_depth/run_05030104/fim_run/'
fim_path='outputs/post_to_huc/my_branch/New/pipeline/'

# roads_gdf=gpd.read_file(r"outputs/flood_depth/run_05030104/fim_run/05030104/osm_roads_subset.gpkg")
roads_gdf=gpd.read_file("outputs/flood_depth/run_4_hucs/combined.gpkg")


roads_in_hucs_gdf=find_intersecting_hucs(fim_path,roads_gdf)

'''
#get the hucs to process HAND
# final = []
# hucs=test['HUC'].unique().tolist()
# for huc in hucs:
#     print(f'working on huc{huc}')
#     huc_dir=os.path.join(fim_path,huc)
#     this_huc_roads_gdf=test[test['HUC']==huc]
#     # bring it back to its original crs
#     original_crs = this_huc_roads_gdf["original_crs"].iloc[0]  # all rows should have same CRS
#     this_huc_roads_gdf = this_huc_roads_gdf.to_crs(original_crs)
#     #now ready to process for each huc
#     # find all branches inside branch folder
#     branched_dfs=[]
#     branches = [branch for branch in os.listdir(os.path.join(huc_dir,'branches'))]
#     for branch_id in branches:
#         print('    branch %s'%branch_id)
#         threshold_gdf=get_threshold_hand(huc_dir,branch_id,this_huc_roads_gdf)
#         if threshold_gdf is not None:
#             fimpact_df=get_threshold_flow(huc_dir,branch_id,threshold_gdf)
#             branched_dfs.append(fimpact_df)
            
#     # aggregare results of all branches
#     aggregated_branched_dfs= pd.concat(branched_dfs, ignore_index=True)

#     fimpact_gdf=compute_flood_depth(huc_dir,aggregated_branched_dfs,flow_file)
#     final.append(fimpact_gdf)

# final_gdf = gpd.GeoDataFrame(pd.concat(final, ignore_index=True), crs="EPSG:4326")
# final_gdf.to_file(output_path)

#heavy lifting is during processing HAND. so it is goo to include get_threshold_hand in MP
# final = []
'''

flow_file_data = pd.read_csv(flow_file, dtype={'feature_id': str})

tasks_args_list = []
available_hucs=roads_in_hucs_gdf['HUC'].unique().tolist()
for huc in available_hucs:
    huc_dir=os.path.join(fim_path,huc)

    this_huc_roads_gdf=roads_in_hucs_gdf[roads_in_hucs_gdf['HUC']==huc]
    original_crs = this_huc_roads_gdf["original_crs"].iloc[0]  # all rows should have same CRS
    this_huc_roads_gdf = this_huc_roads_gdf.to_crs(original_crs)

    # Prepare tasks_args_list as shown earlier
    # need to process all branches of a huc since the input objects can be anywhere across the huc 
    branches = [branch for branch in os.listdir(os.path.join(huc_dir,'branches'))]
    for branch in branches:
        tasks_args_list.append({
        "fim_path":fim_path,
        "unique_id":f"{huc}_{branch}",
        "this_huc_roads_gdf": this_huc_roads_gdf, #this is input features with huc numbers
        })


# Run multiprocessing
mp_results = run_with_mp(
    task_function=task_fn,
    tasks_args_list=tasks_args_list,
    file_logger=file_logger,
    max_workers=8,
    task_id_key="unique_id",  # Must match a key inside task arguments
    show_progress=True
)

all_fimpacts_dfs=[]
for task_id, this_task_results in mp_results.items():
    #the task function returns a single item (a gdf for each HUC) in a list
    this_fimpact_df=this_task_results[0]
    all_fimpacts_dfs.append(this_fimpact_df)

all_fimpacts_dfs= pd.concat(all_fimpacts_dfs, ignore_index=True)
final_fimpact_df = all_fimpacts_dfs.loc[all_fimpacts_dfs.groupby(['osmid_catchid'])['flood_depth'].idxmax()]
#now merge with full lentgh of roads to warn the entire road to be closed 
final_fimpact_gdf = final_fimpact_df.merge(roads_gdf[['geometry','osmid_catchid']], on='osmid_catchid', how='left')
final_fimpact_gdf = gpd.GeoDataFrame(final_fimpact_gdf, geometry='geometry', crs=roads_gdf.crs)

final_fimpact_gdf = final_fimpact_gdf.to_crs('epsg:4326')
final_fimpact_gdf.to_file(os.path.join(output_dir, 'flood_depth.gpkg'))










