#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SOURCE BASH FUNCTIONS
source $srcDir/bash_functions.env

## SET VARIABLES AND FILE INPUTS ##
hucNumber="$1"
current_branch_id="$2"
hucUnitLength=${#hucNumber}
huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}

## SET CRS
if [ $huc2Identifier -eq 19 ]; then
    huc_CRS=$ALASKA_CRS
else
    huc_CRS=$DEFAULT_FIM_PROJECTION_CRS
fi

# Skip branch zero
if [ $current_branch_id = $branch_zero_id ]; then
    exit 0
fi

tempCurrentBranchDataDir=$tempBranchDataDir/$current_branch_id

## OVERWRITE
if [ -d "$tempCurrentBranchDataDir" ]; then
    rm -rf $tempCurrentBranchDataDir
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $tempCurrentBranchDataDir

## START MESSAGE ##
echo -e $startDiv"Processing HUC: $hucNumber - branch_id: $current_branch_id"

## INITIALIZE TOTAL BRANCH TIMER ##
T_total_start
branch_start_time=`date +%s`
date -u

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches $hucNumber $current_branch_id"
echo -e "Querying NWM streams ..."
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_extended_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved_extended.gpkg
echo -e "Querying NWM catchments ..."
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg
echo -e "Querying NWM Dissolved Levelpaths headwaters ..."
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg
#echo -e "Querying NWM headwaters ..."
# ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
#     $tempCurrentBranchDataDir/nwm_headwaters_$current_branch_id.gpkg \
#      $tempHucDataDir/nwm_headwaters.gpkg

## GET RASTERS FROM ROOT HUC DIRECTORY AND CLIP TO CURRENT BRANCH BUFFER ##
echo -e $startDiv"Clipping rasters to branches $hucNumber $current_branch_id"
$srcDir/clip_rasters_to_branches.py -d $current_branch_id \
    -b $tempHucDataDir/branch_polygons.gpkg \
    -i $branch_id_attribute \
    -r $tempHucDataDir/dem_meters.tif $tempHucDataDir/bridge_elev_diff_meters.tif \
    -c $tempCurrentBranchDataDir/dem_meters.tif $tempCurrentBranchDataDir/bridge_elev_diff_meters.tif

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $current_branch_id"
read ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy\
<<<$($srcDir/getRasterInfoNative.py -r $tempCurrentBranchDataDir/dem_meters_$current_branch_id.tif)

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $current_branch_id"
gdal_rasterize -q -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
    -te $xmin $ymin $xmax $ymax \
    -ts $ncols $nrows $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_extended_$current_branch_id.gpkg \
    $tempCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif

## ADJUST FLOODPLAINS ##
echo -e $startDiv"Adjust floodplains $hucNumber $current_branch_id"
$srcDir/adjust_floodplains.py -d $current_branch_id \
    -i $tempCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif \
    -e $tempCurrentBranchDataDir/flows_grid_boolean_euclidean_distance_$current_branch_id.tif \
    -d $tempCurrentBranchDataDir/dem_meters_$current_branch_id.tif \
    -p $tempHucDataDir/branch_polygons.gpkg \
    -b $current_branch_id \
    -o $tempCurrentBranchDataDir/dem_burned_$current_branch_id.tif \
    -z 50

## PIT REMOVE BURNED DEM - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $current_branch_id"
rd_depression_filling $tempCurrentBranchDataDir/dem_burned_$current_branch_id.tif \
    $tempCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif

## D8 FLOW DIR - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $current_branch_id"
mpiexec -n $ncores_fd $taudemDir2/d8flowdir \
    -fel $tempCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif \
    -p $tempCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters $hucNumber $current_branch_id"
gdal_rasterize -q -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
    -te $xmin $ymin $xmax $ymax \
    -ts $ncols $nrows \
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg \
    $tempCurrentBranchDataDir/headwaters_$current_branch_id.tif

## PRODUCE THE REM AND OTHER HAND FILE OUTPUTS ##
export hucNumber=$hucNumber
export current_branch_id=$current_branch_id
export tempCurrentBranchDataDir=$tempCurrentBranchDataDir
export tempHucDataDir=$tempHucDataDir
export ndv=$ndv
export xmin=$xmin
export ymin=$ymin
export xmax=$xmax
export ymax=$ymax
export ncols=$ncols
export nrows=$nrows
$srcDir/delineate_hydros_and_produce_HAND.sh "branch"

## USGS CROSSWALK ##
if [ -f $tempHucDataDir/usgs_subset_gages.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $current_branch_id"
    python3 $srcDir/usgs_gage_crosswalk.py \
        -gages $tempHucDataDir/usgs_subset_gages.gpkg \
        -flows $tempCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg \
        -cat $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg \
        -dem $tempCurrentBranchDataDir/dem_meters_$current_branch_id.tif \
        -dem_adj $tempCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif \
        -out $tempCurrentBranchDataDir \
        -b $current_branch_id \
        -huc_CRS $huc_CRS
fi

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_branches_list ]; then
    echo -e $startDiv"Remove files $hucNumber $current_branch_id"
    $srcDir/outputs_cleanup.py -d $tempCurrentBranchDataDir -l $deny_branches_list -b $current_branch_id
fi

echo -e $startDiv"End Branch Processing $hucNumber $current_branch_id ..."
date -u
Calc_Duration "Duration : " $branch_start_time
echo
