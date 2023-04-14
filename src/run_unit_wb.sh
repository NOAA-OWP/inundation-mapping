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

## INITIALIZE TOTAL TIME TIMER ##
T_total_start
huc_start_time=`date +%s`

## SET VARIABLES AND FILE INPUTS ##
hucUnitLength=${#hucNumber}
huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}
input_NHD_WBHD_layer=WBDHU$hucUnitLength

# Define the landsea water body mask using either Great Lakes or Ocean polygon input #
if [[ $huc2Identifier == "04" ]] ; then
  input_LANDSEA=$input_GL_boundaries
  #echo -e "Using $input_LANDSEA for water body mask (Great Lakes)"
else
  input_LANDSEA=$inputsDir/landsea/water_polygons_us.gpkg
fi

## GET WBD ##
echo -e $startDiv"Get WBD $hucNumber"
date -u
Tstart
ogr2ogr -f GPKG -t_srs $DEFAULT_FIM_PROJECTION_CRS $tempHucDataDir/wbd.gpkg $input_WBD_gdb $input_NHD_WBHD_layer -where "HUC$hucUnitLength='$hucNumber'"
Tcount

## Subset Vector Layers ##
echo -e $startDiv"Get Vector Layers and Subset $hucNumber"
date -u
Tstart

cmd_args=" -a $tempHucDataDir/nwm_lakes_proj_subset.gpkg"
cmd_args+=" -b $tempHucDataDir/nwm_subset_streams.gpkg"
cmd_args+=" -d $hucNumber"
cmd_args+=" -e $tempHucDataDir/nwm_headwater_points_subset.gpkg"
cmd_args+=" -f $tempHucDataDir/wbd_buffered.gpkg"
cmd_args+=" -g $tempHucDataDir/wbd.gpkg"
cmd_args+=" -i $input_DEM"
cmd_args+=" -j $input_DEM_domain"
cmd_args+=" -l $input_nwm_lakes"
cmd_args+=" -m $input_nwm_catchments"
cmd_args+=" -n $tempHucDataDir/nwm_catchments_proj_subset.gpkg"
cmd_args+=" -r $input_NLD"
cmd_args+=" -rp $input_levees_preprocessed"
cmd_args+=" -v $input_LANDSEA"
cmd_args+=" -w $input_nwm_flows"
cmd_args+=" -x $tempHucDataDir/LandSea_subset.gpkg"
cmd_args+=" -y $input_nwm_headwaters"
cmd_args+=" -z $tempHucDataDir/nld_subset_levees.gpkg"
cmd_args+=" -zp $tempHucDataDir/3d_nld_subset_levees_burned.gpkg"
cmd_args+=" -wb $wbd_buffer"
cmd_args+=" -lpf $input_nld_levee_protected_areas"
cmd_args+=" -lps $tempHucDataDir/LeveeProtectedAreas_subset.gpkg"

#echo "$cmd_args"
python3 $srcDir/clip_vectors_to_wbd.py $cmd_args
Tcount

## Clip WBD8 ##
echo -e $startDiv"Clip WBD8"
date -u
Tstart
ogr2ogr -f GPKG -t_srs $DEFAULT_FIM_PROJECTION_CRS -clipsrc $tempHucDataDir/wbd_buffered.gpkg $tempHucDataDir/wbd8_clp.gpkg $inputsDir/wbd/WBD_National.gpkg WBDHU8
Tcount

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"
date -u
Tstart
$srcDir/derive_level_paths.py -i $tempHucDataDir/nwm_subset_streams.gpkg -b $branch_id_attribute -r "ID" -o $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg -d $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -e $tempHucDataDir/nwm_headwaters.gpkg -c $tempHucDataDir/nwm_catchments_proj_subset.gpkg -t $tempHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -n $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg -w $tempHucDataDir/nwm_lakes_proj_subset.gpkg

# test if we received a non-zero code back from derive_level_paths.py
subscript_exit_code=$?
# we have to retrow it if it is not a zero (but it will stop further execution in this script)
if [ $subscript_exit_code -ne 0 ]; then exit $subscript_exit_code; fi
Tcount

## ASSOCIATE LEVEL PATHS WITH LEVEES
echo -e $startDiv"Associate level paths with levees"
date -u
Tstart
[ -f $tempHucDataDir/nld_subset_levees.gpkg ] && \
python3 $srcDir/associate_levelpaths_with_levees.py -nld $tempHucDataDir/nld_subset_levees.gpkg -s $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -lpa $tempHucDataDir/LeveeProtectedAreas_subset.gpkg -out $tempHucDataDir/levee_levelpaths.csv -w $levee_buffer -b $branch_id_attribute -l $levee_id_attribute
Tcount

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"
date -u
Tstart
$srcDir/buffer_stream_branches.py -a $input_DEM_domain -s $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $tempHucDataDir/branch_polygons.gpkg
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create list file of branch ids for $hucNumber"
date -u
Tstart
$srcDir/generate_branch_list.py -d $tempHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute -o $branch_list_lst_file
Tcount

## CREATE BRANCH ZERO ##
echo -e $startDiv"Creating branch zero for $hucNumber"
tempCurrentBranchDataDir=$tempBranchDataDir/$branch_zero_id

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $tempCurrentBranchDataDir

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $branch_zero_id"
# Note: don't need to use gdalwarp -cblend as we are using a buffered wbd
date -u
Tstart

[ ! -f $tempCurrentBranchDataDir/dem_meters.tif ] && \
gdalwarp -cutline $tempHucDataDir/wbd_buffered.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -t_srs $DEFAULT_FIM_PROJECTION_CRS $input_DEM $tempHucDataDir/dem_meters.tif

Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $branch_zero_id"
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $tempHucDataDir/dem_meters.tif)

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $branch_zero_id"
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $tempHucDataDir/3d_nld_subset_levees_burned.gpkg ] && \
gdal_rasterize -l 3d_nld_subset_levees_burned -3d -at -a_nodata $ndv -te $xmin $ymin $xmax $ymax -ts $ncols $nrows -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" $tempHucDataDir/3d_nld_subset_levees_burned.gpkg $tempCurrentBranchDataDir/nld_rasterized_elev_$branch_zero_id.tif
Tcount

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber $branch_zero_id"
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $tempCurrentBranchDataDir/nld_subset_levees.tif ] && \
python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $tempHucDataDir/dem_meters.tif -nld $tempCurrentBranchDataDir/nld_rasterized_elev_$branch_zero_id.tif -out $tempHucDataDir/dem_meters.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $branch_zero_id"
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $tempHucDataDir/nwm_subset_streams.gpkg $tempCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCHES > 0 (NWM levelpath streams) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $branch_zero_id"
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg $tempHucDataDir/flows_grid_boolean.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NWM Headwaters $hucNumber $branch_zero_id"
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $tempHucDataDir/nwm_headwater_points_subset.gpkg $tempCurrentBranchDataDir/headwaters_$branch_zero_id.tif
Tcount

## DEM Reconditioning - BRANCH 0 (include all NWM streams) ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $branch_zero_id"
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $tempCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif -d $tempHucDataDir/dem_meters.tif -w $tempCurrentBranchDataDir -o $tempCurrentBranchDataDir/dem_burned_$branch_zero_id.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## DEM Reconditioning - BRANCHES > 0 (NWM levelpath streams) ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $branch_zero_id"
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $tempHucDataDir/flows_grid_boolean.tif -d $tempHucDataDir/dem_meters.tif -w $tempHucDataDir -o $tempHucDataDir/dem_burned.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## PIT REMOVE BURNED DEM - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
rd_depression_filling $tempCurrentBranchDataDir/dem_burned_$branch_zero_id.tif $tempCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif
Tcount

## PIT REMOVE BURNED DEM - BRANCHES > 0 (NWM levelpath streams) ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
rd_depression_filling $tempHucDataDir/dem_burned.tif $tempHucDataDir/dem_burned_filled.tif
Tcount

## D8 FLOW DIR - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $tempCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif -p $tempCurrentBranchDataDir/flowdir_d8_burned_filled_$branch_zero_id.tif
Tcount

## D8 FLOW DIR - BRANCHES > 0 (NWM levelpath streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $tempHucDataDir/dem_burned_filled.tif -p $tempHucDataDir/flowdir_d8_burned_filled.tif
Tcount

## MAKE A COPY OF THE DEM FOR BRANCH 0
echo -e $startDiv"Copying DEM to Branch 0"
date -u
Tstart
cp $tempHucDataDir/dem_meters.tif $tempCurrentBranchDataDir/dem_meters_$branch_zero_id.tif
Tcount

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
    date -u
    Tstart
    python3 -m memory_profiler $srcDir/usgs_gage_unit_setup.py -gages $inputsDir/usgs_gages/usgs_gages.gpkg -nwm $tempHucDataDir/nwm_subset_streams_levelPaths.gpkg -o $tempHucDataDir/usgs_subset_gages.gpkg -huc $hucNumber -ahps $inputsDir/ahps_sites/nws_lid.gpkg -bzero_id $branch_zero_id
    Tcount
fi

## USGS CROSSWALK ##
if [ -f $tempHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $branch_zero_id"
    date -u
    Tstart
    python3 $srcDir/usgs_gage_crosswalk.py -gages $tempHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg -flows $tempCurrentBranchDataDir/demDerived_reaches_split_filtered_$branch_zero_id.gpkg -cat $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$branch_zero_id.gpkg -dem $tempCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -dem_adj $tempCurrentBranchDataDir/dem_thalwegCond_$branch_zero_id.tif -outtable $tempCurrentBranchDataDir/usgs_elev_table.csv -b $branch_zero_id
    Tcount
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
    # There may not be a branch_ids.lst if there were no level paths (no stream orders 3+)
    # but there will still be a branch zero
    parallel --eta --timeout $branch_timeout -j $jobBranchLimit --joblog $branchSummaryLogFile --colsep ',' -- $srcDir/process_branch.sh $runName $hucNumber :::: $branch_list_lst_file
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

# -------------------
## REMOVE FILES FROM DENY LIST FOR BRANCH ZERO (but using normal branch deny) ##
#if [ "$has_deny_branch_zero_override" == "1" ]
#then
#    echo -e $startDiv"Second cleanup of files for branch zero (none default)"
#    $srcDir/outputs_cleanup.py -d $tempHucDataDir -l $deny_branch_zero_list -b 0

#else 
#    echo -e $startDiv"Second cleanup of files for branch zero using the default branch deny list"
#    $srcDir/outputs_cleanup.py -d $tempHucDataDir -l $deny_branches_list -b 0
#fi

echo "---- HUC $hucNumber - branches have now been processed"
Calc_Duration $branch_processing_start_time
echo

date -u
echo "---- HUC processing for $hucNumber is complete"
Calc_Duration $huc_start_time
echo

