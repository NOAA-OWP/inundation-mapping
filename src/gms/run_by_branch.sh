#!/bin/bash
#######!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SOURCE BASH FUNCTIONS
source $srcDir/bash_functions.env

# make outputs directory
if [ ! -d "$outputGmsDataDir" ]; then
    mkdir -p $outputGmsDataDir
else # remove contents if already exists
    rm -rf $outputGmsDataDir
    mkdir -p $outputGmsDataDir
fi

## TEMP ##
## SET VARIABLES AND FILE INPUTS ##
source /foss_fim/config/params_calibrated.env
hucNumber="$1"
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

## SET OUTPUT DIRECTORY FOR UNIT ##
outputHucDataDir=$outputRunDataDir/$hucNumber
outputGmsDataDir=$outputHucDataDir/gms

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

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/buffer_stream_branches.py -s $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputGmsDataDir/polygons.gpkg -v 
Tcount

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/clip_rasters_to_branches.py -b $outputGmsDataDir/polygons.gpkg -i $branch_id_attribute -r $input_demThal $input_flowdir $input_slopes $input_demDerived_raster -c $outputGmsDataDir/dem_thalwegCond.tif $outputGmsDataDir/flowdir.tif $outputGmsDataDir/slopes.tif $outputGmsDataDir/demDerived.tif -v 
Tcount

##### EDIT DEM DERIVED POINTS TO ADD BRANCH IDS ######
echo -e $startDiv"EDITING DEM DERIVED POINTS for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/edit_points.py -i $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg -b $branch_id_attribute -r $input_demDerived_reaches_points -o $outputGmsDataDir/demDerived_reaches_points.gpkg -p $outputGmsDataDir/demDerived_pixels_points.gpkg
Tcount

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/query_vectors_by_branch_polygons.py -a $outputGmsDataDir/polygons.gpkg -i $branch_id_attribute -s $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputGmsDataDir/demDerived_reaches_points.gpkg $outputGmsDataDir/demDerived_pixels_points.gpkg -o $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg $outputGmsDataDir/demDerived_reaches_points.gpkg $outputGmsDataDir/demDerived_pixels_points.gpkg -v
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create file of branch ids for $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/generate_branch_list.py -t $input_hydroTable -c $outputGmsDataDir/branch_id.lst -d $outputGmsDataDir/demDerived_reaches_levelPaths_dissolved.gpkg -b $branch_id_attribute
Tcount

## CREATE BRANCH LEVEL CATCH LISTS ##
echo -e $startDiv"Create branch level catch lists in HUC: $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/subset_catch_list_by_branch_id.py -c $outputHucDataDir/catchment_list.txt -s $outputGmsDataDir/demDerived_reaches_levelPaths.gpkg -b $branch_id_attribute -l $outputGmsDataDir/branch_id.lst -v
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputHucDataDir/dem.tif)

## LOOP OVER EACH STREAM BRANCH TO DERIVE BRANCH LEVEL HYDROFABRIC ##
for current_branch_id in $(cat $outputGmsDataDir/branch_id.lst);
do

    #[ "$current_branch_id" -ne "15080120" ] && continue
    echo -e $startDiv$startDiv"Processing branch_id: $current_branch_id in HUC: $hucNumber ..."$stopDiv$stopDiv

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
    #$srcDir/gms/finalize_srcs.py -b $outputGmsDataDir/src_base_$current_branch_id.csv -w $input_hydroTable -r $outputGmsDataDir/src_full_$current_branch_id.csv -f $input_src_full -t $outputGmsDataDir/hydroTable_$current_branch_id.csv
    $srcDir/add_crosswalk.py -d $outputGmsDataDir/gw_catchments_reaches_$current_branch_id.gpkg -a $outputGmsDataDir/demDerived_reaches_split_$current_branch_id.gpkg -s $outputGmsDataDir/src_base_$current_branch_id.csv -l $outputGmsDataDir/gw_catchments_reaches_crosswalked_$current_branch_id.gpkg -f $outputGmsDataDir/demDerived_reaches_split_crosswalked_$current_branch_id.gpkg -r $outputGmsDataDir/src_full_$current_branch_id.csv -j $outputGmsDataDir/src_$current_branch_id.json -x $outputGmsDataDir/crosswalk_table_$current_branch_id.csv -t $outputGmsDataDir/hydroTable_$current_branch_id.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputHucDataDir/nwm_subset_streams.gpkg -y $outputHucDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputHucDataDir/nwm_catchments_proj_subset.gpkg -p MS -k $outputGmsDataDir/small_segments.csv

    Tcount

    # make branch output directory and mv files to
    branchOutputDir=$outputGmsDataDir/$current_branch_id
    if [ ! -d "$branchOutputDir" ]; then
        mkdir -p $branchOutputDir
    fi
    
    # mv files to branch output directory
    find $outputGmsDataDir -maxdepth 1 -type f -iname "*_$current_branch_id.*" -exec mv {} $branchOutputDir \;

done

# TEMP: Remove files that are not in catchlist
# rasters
find $outputGmsDataDir -maxdepth 1 -type f -iname "dem_thalwegCond_*.tif" -delete
find $outputGmsDataDir -maxdepth 1 -type f -iname "flowdir_*.tif" -delete
find $outputGmsDataDir -maxdepth 1 -type f -iname "slopes_*.tif" -delete
find $outputGmsDataDir -maxdepth 1 -type f -iname "demDerived_*.tif" -delete

# vectors
find $outputGmsDataDir -maxdepth 1 -type f -iname "demDerived_reaches_*.gpkg" -delete
find $outputGmsDataDir -maxdepth 1 -type f -iname "demDerived_reaches_points_*.gpkg" -delete
find $outputGmsDataDir -maxdepth 1 -type f -iname "demDerived_pixels_points_*.gpkg" -delete

# other
find $outputGmsDataDir -maxdepth 1 -type f -iname "idFile_*.txt" -delete
