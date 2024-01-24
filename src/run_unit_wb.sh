#!/bin/bash -e

# Do not call this file directly. Call fim_process_unit_wb.sh which calls
# this file.

## SOURCE FILE AND FUNCTIONS ##
# load the various enviro files
args_file=$outputDestDir/runtime_args.env

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

branch_list_csv_file=$tempHucDataDir/branch_ids.csv
branch_list_lst_file=$tempHucDataDir/branch_ids.lst

branchSummaryLogFile=$outputDestDir/logs/branch/"$hucNumber"_summary_branch.log

## INITIALIZE TOTAL UNIT AND IT'S BRANCHES TIMER ##
T_total_start
huc_start_time=`date +%s`
date -u

## Copy HUC's pre-clipped .gpkg files from $pre_clip_huc_dir (use -a & /. -- only copies folder's contents)
echo -e $startDiv"Copying staged wbd and .gpkg files from $pre_clip_huc_dir/$hucNumber"
cp -a $pre_clip_huc_dir/$hucNumber/. $tempHucDataDir

# Copy necessary files from $inputsDir into $tempHucDataDir to avoid File System Collisions
# For buffer_stream_branches.py
cp $input_DEM_domain $tempHucDataDir
# For usgs_gage_unit_setup.py
cp $inputsDir/usgs_gages/usgs_gages.gpkg $tempHucDataDir
cp $ras_rating_curve_points_gpkg $tempHucDataDir
cp $inputsDir/ahps_sites/nws_lid.gpkg $tempHucDataDir

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"
$srcDir/derive_level_paths.py -i $tempHucDataDir/nwm_subset_streams.gpkg \
    -s $tempHucDataDir/wbd_buffered_streams.gpkg \
    -b $branch_id_attribute \
    -r "ID" \
    -o $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg \
    -d $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg \
    -e $tempHucDataDir/nwm_headwaters.gpkg \
    -c $tempHucDataDir/nwm_catchments_proj_subset.gpkg \
    -t $tempHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg \
    -n $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg \
    -w $tempHucDataDir/nwm_lakes_proj_subset.gpkg \
    -wbd $tempHucDataDir/wbd.gpkg

# test if we received a non-zero code back from derive_level_paths.py
#subscript_exit_code=$?

# we have to retrow it if it is not a zero (but it will stop further execution in this script)
# if [ $subscript_exit_code -ne 0 ] && [ $subscript_exit_code -ne 62 ] && [ $subscript_exit_code -eq 63 ]; then
#     exit $subscript_exit_code
# fi

# check if level paths exists
levelpaths_exist=1
if [ ! -f $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg ]; then levelpaths_exist=0; fi

## ASSOCIATE LEVEL PATHS WITH LEVEES
echo -e $startDiv"Associate level paths with levees"
[ -f $tempHucDataDir/nld_subset_levees.gpkg ] && \
python3 $srcDir/associate_levelpaths_with_levees.py -nld $tempHucDataDir/nld_subset_levees.gpkg \
    -s $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg \
    -lpa $tempHucDataDir/LeveeProtectedAreas_subset.gpkg \
    -out $tempHucDataDir/levee_levelpaths.csv \
    -w $levee_buffer \
    -b $branch_id_attribute \
    -l $levee_id_attribute

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"
$srcDir/buffer_stream_branches.py -a $tempHucDataDir/HUC6_dem_domain.gpkg \
    -s $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg \
    -i $branch_id_attribute \
    -d $branch_buffer_distance_meters \
    -b $tempHucDataDir/branch_polygons.gpkg

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create list file of branch ids for $hucNumber"
$srcDir/generate_branch_list.py -d $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg \
    -b $branch_id_attribute \
    -o $branch_list_lst_file

## CREATE BRANCH ZERO ##
echo -e $startDiv"Creating branch zero for $hucNumber"
tempCurrentBranchDataDir=$tempBranchDataDir/$branch_zero_id

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $tempCurrentBranchDataDir

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $branch_zero_id"
# Note: don't need to use gdalwarp -cblend as we are using a buffered wbd
[ ! -f $tempCurrentBranchDataDir/dem_meters.tif ] && \
gdalwarp -cutline $tempHucDataDir/wbd_buffered.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" \
    -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" \
    -co "BIGTIFF=YES" -t_srs $DEFAULT_FIM_PROJECTION_CRS $input_DEM $tempHucDataDir/dem_meters.tif -q

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $branch_zero_id"
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy\
    <<<$($srcDir/getRasterInfoNative.py $tempHucDataDir/dem_meters.tif)

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $branch_zero_id"
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $tempHucDataDir/3d_nld_subset_levees_burned.gpkg ] && \
gdal_rasterize -q -l 3d_nld_subset_levees_burned -3d -at -a_nodata $ndv \
    -te $xmin $ymin $xmax $ymax -ts $ncols $nrows \
    -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" \
    -co "TILED=YES" $tempHucDataDir/3d_nld_subset_levees_burned.gpkg \
    $tempCurrentBranchDataDir/nld_rasterized_elev_$branch_zero_id.tif

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters"
echo -e "(*Overwrite dem_meters.tif output) $hucNumber $branch_zero_id"
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $tempCurrentBranchDataDir/nld_rasterized_elev_$branch_zero_id.tif ] && \
python3 $srcDir/burn_in_levees.py \
    -dem $tempHucDataDir/dem_meters.tif \
    -nld $tempCurrentBranchDataDir/nld_rasterized_elev_$branch_zero_id.tif \
    -out $tempHucDataDir/dem_meters.tif

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $branch_zero_id"
gdal_rasterize -q -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
    -te $xmin $ymin $xmax $ymax -ts $ncols $nrows \
    $tempHucDataDir/nwm_subset_streams.gpkg $tempCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCHES (Not 0) (NWM levelpath streams) ##
if [ "$levelpaths_exist" = "1" ]; then
    echo -e $startDiv"Rasterize Reach Boolean $hucNumber (Branches)"
    gdal_rasterize -q -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
        -te $xmin $ymin $xmax $ymax -ts $ncols $nrows \
        $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg $tempHucDataDir/flows_grid_boolean.tif
fi

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NWM Headwaters $hucNumber $branch_zero_id"
gdal_rasterize -q -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" \
    -te $xmin $ymin $xmax $ymax -ts $ncols $nrows \
    $tempHucDataDir/nwm_headwater_points_subset.gpkg $tempCurrentBranchDataDir/headwaters_$branch_zero_id.tif

## DEM Reconditioning - BRANCH 0 (include all NWM streams) ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $branch_zero_id"
python3 $srcDir/agreedem.py \
    -r $tempCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif \
    -d $tempHucDataDir/dem_meters.tif \
    -w $tempCurrentBranchDataDir \
    -o $tempCurrentBranchDataDir/dem_burned_$branch_zero_id.tif \
    -b $agree_DEM_buffer \
    -sm 10 \
    -sh 1000

## DEM Reconditioning - BRANCHES (NOT 0) (NWM levelpath streams) ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
if [ "$levelpaths_exist" = "1" ]; then
    echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber (Branches)"
    python3 $srcDir/agreedem.py -r $tempHucDataDir/flows_grid_boolean.tif \
        -d $tempHucDataDir/dem_meters.tif \
        -w $tempHucDataDir \
        -o $tempHucDataDir/dem_burned.tif \
        -b $agree_DEM_buffer \
        -sm 10 \
        -sh 1000
fi

## PIT REMOVE BURNED DEM - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $branch_zero_id"
rd_depression_filling $tempCurrentBranchDataDir/dem_burned_$branch_zero_id.tif \
    $tempCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif

## PIT REMOVE BURNED DEM - BRANCHES (NOT 0) (NWM levelpath streams) ##
if [ "$levelpaths_exist" = "1" ]; then
    echo -e $startDiv"Pit remove Burned DEM $hucNumber (Branches)"
    rd_depression_filling $tempHucDataDir/dem_burned.tif $tempHucDataDir/dem_burned_filled.tif
fi

## D8 FLOW DIR - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $branch_zero_id"
mpiexec -n $ncores_fd $taudemDir2/d8flowdir \
    -fel $tempCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif \
    -p $tempCurrentBranchDataDir/flowdir_d8_burned_filled_$branch_zero_id.tif

## D8 FLOW DIR - BRANCHES (NOT 0) (NWM levelpath streams) ##
if [ "$levelpaths_exist" = "1" ]; then
    echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber (Branches)"
    mpiexec -n $ncores_fd $taudemDir2/d8flowdir \
        -fel $tempHucDataDir/dem_burned_filled.tif \
        -p $tempHucDataDir/flowdir_d8_burned_filled.tif
fi

## MAKE A COPY OF THE DEM FOR BRANCH 0
echo -e $startDiv"Copying DEM to Branch 0"
cp $tempHucDataDir/dem_meters.tif $tempCurrentBranchDataDir/dem_meters_$branch_zero_id.tif


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

## PRODUCE BRANCH ZERO HAND
$srcDir/delineate_hydros_and_produce_HAND.sh "unit"

## CREATE USGS GAGES FILE
if [ -f $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg ]; then
    echo -e $startDiv"Assigning USGS gages to branches for $hucNumber"
    python3 $srcDir/usgs_gage_unit_setup.py \
        -gages $tempHucDataDir/usgs_gages.gpkg \
        -nwm $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg \
        -ras $tempHucDataDir/$ras_rating_curve_gpkg_filename \
        -o $tempHucDataDir/usgs_subset_gages.gpkg \
        -huc $hucNumber \
        -ahps $tempHucDataDir/nws_lid.gpkg \
        -bzero_id $branch_zero_id
fi

## USGS CROSSWALK ##
if [ -f $tempHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $branch_zero_id"
    python3 $srcDir/usgs_gage_crosswalk.py \
        -gages $tempHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg \
        -flows $tempCurrentBranchDataDir/demDerived_reaches_split_filtered_$branch_zero_id.gpkg \
        -cat $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$branch_zero_id.gpkg \
        -dem $tempCurrentBranchDataDir/dem_meters_$branch_zero_id.tif \
        -dem_adj $tempCurrentBranchDataDir/dem_thalwegCond_$branch_zero_id.tif \
        -out $tempCurrentBranchDataDir -b $branch_zero_id
fi

## CLEANUP BRANCH ZERO OUTPUTS ##
echo -e $startDiv"Cleaning up outputs in branch zero $hucNumber"
$srcDir/outputs_cleanup.py -d $tempCurrentBranchDataDir -l $deny_branch_zero_list -b $branch_zero_id


# -------------------
## Start the local csv branch list
$srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber -b $branch_zero_id

# -------------------
## Processing Branches ##
echo
echo "---- Start of branch processing for $hucNumber"
branch_processing_start_time=`date +%s`

if [ -f $branch_list_lst_file ]; then
    date -u
    Tstart
    # There may not be a branch_ids.lst if there were no level paths (no stream orders 3+)
    # but there will still be a branch zero
    parallel --timeout $branch_timeout -j $jobBranchLimit --joblog $branchSummaryLogFile --colsep ',' \
    -- $srcDir/process_branch.sh $runName $hucNumber :::: $branch_list_lst_file
    Tcount
else
    echo "No level paths exist with this HUC. Processing branch zero only."
fi

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_unit_list ]; then
    echo -e $startDiv"Remove files $hucNumber"
    date -u
    Tstart
    $srcDir/outputs_cleanup.py -d $tempHucDataDir -l $deny_unit_list -b $hucNumber
    Tcount
fi

echo "---- HUC $hucNumber - branches have now been processed"
Calc_Duration $branch_processing_start_time
echo

# WRITE TO LOG FILE CONTAINING ALL HUC PROCESSING TIMES
total_duration_display="$hucNumber,$(Calc_Time $huc_start_time),$(Calc_Time_Minutes_in_Percent $huc_start_time)"
echo "$total_duration_display" >> "$outputDestDir/logs/unit/total_duration_run_by_unit_all_HUCs.csv"

date -u
echo "---- HUC processing for $hucNumber is complete"
Calc_Duration $huc_start_time
echo
