#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SOURCE BASH FUNCTIONS
source $srcDir/bash_functions.env

## SET VARIABLES AND FILE INPUTS ##
hucNumber="$1"
current_branch_id="$2"

outputHucDataDir=$outputRunDataDir/$hucNumber
outputGmsDataDir=$outputHucDataDir/gms

input_demThal=$outputHucDataDir/dem_thalwegCond.tif
input_flowdir=$outputHucDataDir/flowdir_d8_burned_filled.tif
input_slopes=$outputHucDataDir/slopes_d8_dem_meters.tif
input_demDerived_raster=$outputHucDataDir/demDerived_streamPixels.tif
input_demDerived_reaches=$outputHucDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg
input_demDerived_reaches_points=$outputHucDataDir/demDerived_reaches_split_points.gpkg
input_demDerived_pixel_points=$outputHucDataDir/flows_points_pixels.gpkg
input_stage_list=$outputHucDataDir/stage.txt
input_hydroTable=$outputHucDataDir/hydroTable.csv
input_src_full=$outputHucDataDir/src_full_crosswalked.csv


## ECHO PARAMETERS
echo -e $startDiv"Parameter Values"
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
echo -e "memfree=$memfree"
echo -e "branch_id_attribute=$branch_id_attribute"
echo -e "branch_buffer_distance_meters=$branch_buffer_distance_meters"$stopDiv


## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputHucDataDir/dem.tif)
Tcount

## LOOP OVER EACH STREAM BRANCH TO DERIVE BRANCH LEVEL HYDROFABRIC ##
#for current_branch_id in $(cat $outputGmsDataDir/branch_id.lst);
#do

#[ "$current_branch_id" -ne "15080120" ] && continue
echo -e $startDiv$startDiv"Processing branch_id: $current_branch_id in HUC: $hucNumber ..."$stopDiv$stopDiv

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches for branch id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/clip_rasters_to_branches.py -d $current_branch_id -b $outputGmsDataDir/polygons.gpkg -i $branch_id_attribute -r $input_demThal $input_flowdir $input_slopes $input_demDerived_raster -c $outputGmsDataDir/dem_thalwegCond.tif $outputGmsDataDir/flowdir.tif $outputGmsDataDir/slopes.tif $outputGmsDataDir/demDerived.tif -v 
Tcount

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches for $current_branch_id in HUC $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/query_vectors_by_branch_polygons.py -a $outputGmsDataDir/polygons.gpkg -d $current_branch_id -i $branch_id_attribute -s $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputGmsDataDir/demDerived_reaches_points.gpkg $outputGmsDataDir/demDerived_pixels_points.gpkg -o $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputGmsDataDir/demDerived_reaches_points.gpkg $outputGmsDataDir/demDerived_pixels_points.gpkg -v
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches $hucNumber"$stopDiv
date -u
Tstart
$srcDir/split_flows.py $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved_$current_branch_id.gpkg $outputGmsDataDir/dem_thalwegCond_$current_branch_id.tif $outputGmsDataDir/demDerived_reaches_split_$current_branch_id.gpkg $outputGmsDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg $outputHucDataDir/wbd8_clp.gpkg $outputHucDataDir/nwm_lakes_proj_subset.gpkg
Tcount

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputGmsDataDir/flowdir_"$current_branch_id".tif -gw $outputGmsDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputGmsDataDir/demDerived_pixels_points_$current_branch_id.gpkg -id $outputGmsDataDir/idFile_$current_branch_id.txt
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputGmsDataDir/flowdir_$current_branch_id.tif -gw $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.tif -o $outputGmsDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg -id $outputGmsDataDir/idFile_$current_branch_id.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/rem.py -d $outputGmsDataDir/dem_thalwegCond_"$current_branch_id".tif -w $outputGmsDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputGmsDataDir/rem_$current_branch_id.tif -t $outputGmsDataDir/demDerived_$current_branch_id.tif
Tcount

## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS##
echo -e $startDiv"Bring negative values in REM to zero and mask to catchments for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputGmsDataDir/rem_$current_branch_id.tif -B $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.tif --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv --outfile=$outputGmsDataDir/"rem_zeroed_masked_$current_branch_id.tif"
Tcount

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
gdal_polygonize.py -8 -f GPKG $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.tif $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.gpkg catchments HydroID
Tcount

## MASK SLOPE TO CATCHMENTS ##
echo -e $startDiv"Mask to slopes to catchments for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputGmsDataDir/slopes_$current_branch_id.tif -B $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.tif --calc="A*(B>0)" --NoDataValue=$ndv --outfile=$outputGmsDataDir/slopes_masked_$current_branch_id.tif
Tcount

## MAKE CATCHMENT AND STAGE FILES ##
echo -e $startDiv"Generate Catchment List and Stage List Files $hucNumber"$stopDiv
date -u
Tstart
$srcDir/make_stages_and_catchlist.py $outputGmsDataDir/demDerived_reaches_split_$current_branch_id.gpkg $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.gpkg $outputGmsDataDir/stage_$current_branch_id.txt $outputGmsDataDir/catch_list_$current_branch_id.txt $stage_min_meters $stage_interval_meters $stage_max_meters
Tcount


## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
$taudemDir/catchhydrogeo -hand $outputGmsDataDir/rem_zeroed_masked_$current_branch_id.tif -catch $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.tif -catchlist $outputGmsDataDir/catch_list_$current_branch_id.txt -slp $outputGmsDataDir/slopes_masked_$current_branch_id.tif -h $outputGmsDataDir/stage_$current_branch_id.txt -table $outputGmsDataDir/src_base_$current_branch_id.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
date -u
Tstart
$srcDir/add_crosswalk.py -d $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.gpkg -a $outputGmsDataDir/demDerived_reaches_split_$current_branch_id.gpkg -s $outputGmsDataDir/src_base_$current_branch_id.csv -u $inputDataDir/bathymetry/BANKFULL_CONUS.txt -v $outputGmsDataDir/bathy_crosswalk_calcs_$current_branch_id.csv -e $outputGmsDataDir/bathy_stream_order_calcs_$current_branch_id.csv -g $outputGmsDataDir/bathy_thalweg_flag_$current_branch_id.csv -i $outputGmsDataDir/bathy_xs_area_hydroid_lookup_$current_branch_id.csv -l $outputGmsDataDir/gw_catchments_reaches_crosswalked_$current_branch_id.gpkg -f $outputGmsDataDir/demDerived_reaches_split_crosswalked_$current_branch_id.gpkg -r $outputGmsDataDir/src_full_$current_branch_id.csv -j $outputGmsDataDir/src_$current_branch_id.json -x $outputGmsDataDir/crosswalk_table_$current_branch_id.csv -t $outputGmsDataDir/hydroTable_$current_branch_id.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputHucDataDir/nwm_subset_streams.gpkg -y $outputHucDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputHucDataDir/nwm_catchments_proj_subset.gpkg -p MS -k $outputGmsDataDir/small_segments.csv
Tcount

# make branch output directory and mv files to
branchOutputDir=$outputGmsDataDir/$current_branch_id
if [ ! -d "$branchOutputDir" ]; then
    mkdir -p $branchOutputDir
fi

if [ "$production" -eq 1 ]; then
    echo -e $startDiv"Remove files for branch_id: $current_branch_id in HUC: $hucNumber"$stopDiv
    
    cd $outputGmsDataDir
    rm -f flowdir_$current_branch_id.tif stage_$current_branch_id.txt src_base_$current_branch_id.csv src_$current_branch_id.json demDerived_reaches_split_points_$current_branch_id.gpkg dem_thalwegCond_$current_branch_id.tif demDerived_pixels_points_$current_branch_id.gpkg demDerived_reaches_levelPaths_$current_branch_id.gpkg demDerived_reaches_levelPaths_dissolved_$current_branch_id.gpkg demDerived_reaches_$current_branch_id.gpkg idFile_$current_branch_id.txt demDerived_$current_branch_id.tif crosswalk_table_$current_branch_id.csv catch_list_$current_branch_id.txt gw_catchments_pixels_$current_branch_id.tif slopes_$current_branch_id.tif slopes_masked_$current_branch_id.tif demDerived_reaches_$current_branch_id.gpkg bathy_crosswalk_calcs_$current_branch_id.csv bathy_stream_order_calcs_$current_branch_id.csv bathy_thalweg_flag_$current_branch_id.csv bathy_xs_area_hydroid_lookup_$current_branch_id.csv demDerived_reaches_points_$current_branch_id.gpkg demDerived_reaches_split_$current_branch_id.gpkg rem_$current_branch_id.tif demDerived_reaches_points_$current_branch_id.gpkg
    cd $OLDPWD
fi

# mv files to branch output directory
find $outputGmsDataDir -maxdepth 1 -type f -iname "*_$current_branch_id.*" -exec mv {} $branchOutputDir \;


