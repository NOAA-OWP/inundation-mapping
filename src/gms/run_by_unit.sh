#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

echo -e $startDiv"Parameter Values"
echo -e "extent=$extent"
echo -e "agree_DEM_buffer=$agree_DEM_buffer"
echo -e "wbd_buffer=$wbd_buffer"
echo -e "ms_buffer_dist=$ms_buffer_dist"
echo -e "lakes_buffer_dist_meters=$lakes_buffer_dist_meters"
echo -e "negative_burn_value=$negative_burn_value"
echo -e "max_split_distance_meters=$max_split_distance_meters"
echo -e "mannings_n=$manning_n"
echo -e "stage_min_meters=$stage_min_meters"
echo -e "stage_interval_meters=$stage_interval_meters"
echo -e "stage_max_meters=$stage_max_meters"
echo -e "slope_min=$slope_min"
echo -e "ms_buffer_dist=$ms_buffer_dist"
echo -e "ncores_gw=$ncores_gw"
echo -e "ncores_fd=$ncores_fd"
echo -e "default_max_jobs=$default_max_jobs"
echo -e "memfree=$memfree"$stopDiv

## SET OUTPUT DIRECTORY FOR UNIT ##
hucNumber="$1"
current_branch_id="$2"
outputHucDataDir=$outputRunDataDir/$hucNumber
outputGmsDataDir=$outputHucDataDir/gms

# make outputs directory
if [ ! -d "$outputGmsDataDir" ]; then
    mkdir -p $outputGmsDataDir
fi

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/derive_level_paths.py -i $outputHucDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -b $branch_id_attribute -o $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg -d $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -v
Tcount

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/buffer_stream_branches.py -s $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputGmsDataDir/polygons.gpkg -v 
Tcount

##### EDIT DEM DERIVED POINTS TO ADD BRANCH IDS ######
echo -e $startDiv"EDITING DEM DERIVED POINTS for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/edit_points.py -i $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg -b $branch_id_attribute -r $outputHucDataDir/demDerived_reaches_split_points.gpkg -o $outputGmsDataDir/demDerived_reaches_points.gpkg -p $outputGmsDataDir/demDerived_pixels_points.gpkg
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create file of branch ids for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/generate_branch_list.py -t $outputHucDataDir/hydroTable.csv -c $outputGmsDataDir/branch_id.lst -d $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -b $branch_id_attribute
Tcount

