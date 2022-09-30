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

# Skip branch zero
if [ $current_branch_id = $branch_zero_id ]; then
    exit 0
fi

outputHucDataDir=$outputRunDataDir/$hucNumber
outputBranchDataDir=$outputHucDataDir/branches
outputCurrentBranchDataDir=$outputBranchDataDir/$current_branch_id

# set input files
input_DEM=$inputDataDir/nhdplus_rasters/HRNHDPlusRasters"$huc4Identifier"/elev_m.tif
input_NLD=$inputDataDir/nld_vectors/huc2_levee_lines/nld_preprocessed_"$huc2Identifier".gpkg
input_bathy_bankfull=$inputDataDir/$bankfull_input_table
input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg

## OVERWRITE
if [ -d "$outputCurrentBranchDataDir" ];then
    if [ $overwrite -eq 1 ]; then
        rm -rf $outputCurrentBranchDataDir
    else
        echo "GMS branch data directories for $hucNumber - $current_branch_id already exist. Use -o/--overwrite to continue"
        exit 1
    fi
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $outputCurrentBranchDataDir

## START MESSAGE ##
echo -e $startDiv"Processing branch_id: $current_branch_id in HUC: $hucNumber ..."$stopDiv

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
echo -e "Querying NWM streams ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg
echo -e "Querying NWM catchments ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg
echo -e "Querying NWM Dissolved Levelpaths headwaters ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg
#echo -e "Querying NWM headwaters ..."
#ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_headwaters_$current_branch_id.gpkg $outputHucDataDir/nwm_headwaters.gpkg
Tcount

## GET RASTERS FROM BRANCH ZERO AND CLIP TO CURRENT BRANCH BUFFER ##
echo -e $startDiv"Clipping rasters to branches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/gms/clip_rasters_to_branches.py -d $current_branch_id -b $outputHucDataDir/branch_polygons.gpkg -i $branch_id_attribute -r $outputBranchDataDir/$branch_zero_id/dem_meters_$branch_zero_id.tif $outputBranchDataDir/$branch_zero_id/flowdir_d8_burned_filled_$branch_zero_id.tif -c $outputCurrentBranchDataDir/dem_meters.tif $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -v
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif)
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg $outputCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg $outputCurrentBranchDataDir/headwaters_$current_branch_id.tif
Tcount

## PRODUCE THE REM AND OTHER HAND FILE OUTPUTS ##
export hucNumber=$hucNumber
export current_branch_id=$current_branch_id
export outputCurrentBranchDataDir=$outputCurrentBranchDataDir
export outputHucDataDir=$outputHucDataDir
export ndv=$ndv
export xmin=$xmin
export ymin=$ymin
export xmax=$xmax
export ymax=$ymax
export ncols=$ncols
export nrows=$nrows
$srcDir/gms/delineate_hydros_and_produce_HAND.sh "branch"

## USGS CROSSWALK ##
if [ -f $outputHucDataDir/usgs_subset_gages.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    python3 $srcDir/usgs_gage_crosswalk.py -gages $outputHucDataDir/usgs_subset_gages.gpkg -flows $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -cat $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg -dem $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif -dem_adj $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif -outtable $outputCurrentBranchDataDir/usgs_elev_table.csv -b $current_branch_id
    Tcount
fi

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_gms_branches_list ]; then
    echo -e $startDiv"Remove files $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    $srcDir/gms/outputs_cleanup.py -d $outputCurrentBranchDataDir -l $deny_gms_branches_list -b $current_branch_id
    Tcount
fi

echo -e $startDiv"End Processing $hucNumber $current_branch_id ..."
echo