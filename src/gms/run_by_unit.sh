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
outputHucDataDir=$outputRunDataDir/$hucNumber
outputBranchDataDir=$outputHucDataDir/branches

# make outputs directory
if [ ! -d "$outputHucDataDir" ]; then
    mkdir -p $outputHucDataDir
fi

# make branches outputs directory
if [ ! -d "$outputBranchDataDir" ]; then
    mkdir -p $outputBranchDataDir
fi

## SET VARIABLES AND FILE INPUTS ##
hucUnitLength=${#hucNumber}
huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}
input_NHD_WBHD_layer=WBDHU$hucUnitLength
input_DEM=$inputDataDir/nhdplus_rasters/HRNHDPlusRasters"$huc4Identifier"/elev_m.tif
input_NLD=$inputDataDir/nld_vectors/huc2_levee_lines/nld_preprocessed_"$huc2Identifier".gpkg
input_bathy_bankfull=$inputDataDir/$bankfull_input_table

# Define the landsea water body mask using either Great Lakes or Ocean polygon input #
if [[ $huc2Identifier == "04" ]] ; then
  input_LANDSEA=$input_GL_boundaries
  echo -e "Using $input_LANDSEA for water body mask (Great Lakes)"
else
  input_LANDSEA=$inputDataDir/landsea/water_polygons_us.gpkg
fi

## GET WBD ##
echo -e $startDiv"Get WBD $hucNumber"$stopDiv
date -u
Tstart
ogr2ogr -f GPKG $outputHucDataDir/wbd.gpkg $input_WBD_gdb $input_NHD_WBHD_layer -where "HUC$hucUnitLength='$hucNumber'"
Tcount

## Subset Vector Layers ##
echo -e $startDiv"Get Vector Layers and Subset $hucNumber"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/clip_vectors_to_wbd.py -d $hucNumber -w $input_nwm_flows -s $input_nhd_flowlines -l $input_nwm_lakes -r $input_NLD -g $outputHucDataDir/wbd.gpkg -f $outputHucDataDir/wbd_buffered.gpkg -m $input_nwm_catchments -y $input_nhd_headwaters -v $input_LANDSEA -c $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg -z $outputHucDataDir/nld_subset_levees.gpkg -a $outputHucDataDir/nwm_lakes_proj_subset.gpkg -n $outputHucDataDir/nwm_catchments_proj_subset.gpkg -e $outputHucDataDir/nhd_headwater_points_subset.gpkg -b $outputHucDataDir/nwm_subset_streams.gpkg -x $outputHucDataDir/LandSea_subset.gpkg -extent GMS -gl $input_GL_boundaries -lb $lakes_buffer_dist_meters -wb $wbd_buffer
Tcount

## Clip WBD8 ##
echo -e $startDiv"Clip WBD8"$stopDiv
date -u
Tstart
ogr2ogr -f GPKG -clipsrc $outputHucDataDir/wbd_buffered.gpkg $outputHucDataDir/wbd8_clp.gpkg $inputDataDir/wbd/WBD_National.gpkg WBDHU8
Tcount

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/derive_level_paths.py -i $outputHucDataDir/nwm_subset_streams.gpkg -b $branch_id_attribute -r "ID" -o $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -e $outputHucDataDir/nwm_headwaters.gpkg -c $outputHucDataDir/nwm_catchments_proj_subset.gpkg -t $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -n $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg -v
Tcount

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/buffer_stream_branches.py -s $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputHucDataDir/branch_polygons.gpkg -v 
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create file of branch ids for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/generate_branch_list.py -o $outputHucDataDir/branch_id.lst -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute
Tcount

