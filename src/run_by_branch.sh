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

huc_CRS=$(get_crs_for_huc "$hucNumber")

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
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_extended_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_subset_streams_levelPaths_extended.gpkg
echo -e "Querying NWM catchments ..."
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg
echo -e "Querying NWM Dissolved Levelpaths headwaters ..."
ogr2ogr -f GPKG -t_srs $huc_CRS -where $branch_id_attribute="$current_branch_id" \
    $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg \
    $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg

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
gdal_rasterize -q -at -ot Int32 -burn 1 -init 0 -a_nodata -9999 \
    -co "BIGTIFF=YES" \
    -te $xmin $ymin $xmax $ymax \
    -ts $ncols $nrows $tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_extended_$current_branch_id.gpkg \
    $tempCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif

## DEM Reconditioning - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber (Branches)"
python3 $srcDir/agreedem.py -r $tempCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif \
    -d $tempCurrentBranchDataDir/dem_meters_$current_branch_id.tif \
    -w $tempCurrentBranchDataDir \
    -o $tempCurrentBranchDataDir/dem_burned_$current_branch_id.tif \
    -b $agree_DEM_buffer \
    -sm 10 \
    -sh 1000

## ADJUST FLOODPLAINS ##
echo -e $startDiv"Adjust floodplains $hucNumber $current_branch_id"
echo -e "Using FEMA floodplain layer: $fema_floodplain_layer"
python3 $srcDir/adjust_floodplains.py \
    -i $tempCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif \
    -e $tempCurrentBranchDataDir/flows_grid_boolean_euclidean_distance_$current_branch_id.tif \
    -d $tempCurrentBranchDataDir/dem_burned_$current_branch_id.tif \
    -o $tempCurrentBranchDataDir/dem_burned_adjusted_$current_branch_id.tif \
    -t $floodplain_distance_threshold \
    -s $floodplain_slope_exponent \
    -z $floodplain_z_factor \
    -p $tempHucDataDir/branch_polygons.gpkg \
    -b $current_branch_id \
    -f $input_fema_flood_hazard_zones/nfhl_$hucNumber.gpkg \
    -l $fema_floodplain_layer \
    -c $tempHucDataDir/nwm_catchments_proj_subset.gpkg \
    -n $tempHucDataDir/nwm_subset_streams.gpkg \
    -lp $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg

## PIT REMOVE BURNED DEM - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $current_branch_id"
if [ -f $tempCurrentBranchDataDir/dem_burned_adjusted_$current_branch_id.tif ]; then
    rd_depression_filling $tempCurrentBranchDataDir/dem_burned_adjusted_$current_branch_id.tif \
        $tempCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif
else
    rd_depression_filling $tempCurrentBranchDataDir/dem_burned_$current_branch_id.tif \
        $tempCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif
fi

## D8 FLOW DIR - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $current_branch_id"
mpiexec -n $ncores_fd $taudemDir2/d8flowdir \
    -fel $tempCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif \
    -p $tempCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif \
    2> >(while read -r line; do
        # Check if BOTH strings are present in the error line
        if [[ "$line" == *"ERROR 6:"* && "$line" == *"Dataset does not support the AddBand() method."* ]]; then
            # Do nothing (ignore the error)
            :
        else
            # Print the line to the standard error stream (screen)
            echo "$line" >&2
        fi
    done)
    # May 1, 2026: Merge config between Ryan and Matt gdal PR. commented out Ryans. Can we marry the two? do we want too?    
    # 2>&1 | sed -e 's/.*no output sd8 file specified.*/INFO: TauDEM d8flowdir running without optional sd8 slope output./I' \
    #            -e 's/.*no output p file specified.*/INFO: TauDEM d8flowdir running without optional sd8 slope output./I'

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NWM Headwaters $hucNumber $current_branch_id"
gdal_rasterize -q -ot Int32 -burn 1 -init 0 -a_nodata -9999 \
     -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
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
