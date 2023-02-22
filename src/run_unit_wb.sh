#!/bin/bash -e

# Do not call this file directly. Call fim_process_unit_wb.sh which calls
# this file.

## SOURCE FILE AND FUNCTIONS ##
# load the various enviro files
args_file=$outputRunDataDir/runtime_args.env

source $args_file
source $outputRunDataDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

branch_list_csv_file=$outputHucDataDir/branch_ids.csv
branch_list_lst_file=$outputHucDataDir/branch_ids.lst

branchSummaryLogFile=$outputRunDataDir/logs/branch/"$hucNumber"_summary_branch.log

## INITIALIZE TOTAL TIME TIMER ##
T_total_start
huc_start_time=`date +%s`

## SET VARIABLES AND FILE INPUTS ##
hucUnitLength=${#hucNumber}
huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}
input_NHD_WBHD_layer=WBDHU$hucUnitLength

input_NLD=$inputDataDir/nld_vectors/huc2_levee_lines/nld_preprocessed_"$huc2Identifier".gpkg

# Define the landsea water body mask using either Great Lakes or Ocean polygon input #
if [[ $huc2Identifier == "04" ]] ; then
  input_LANDSEA=$input_GL_boundaries
  #echo -e "Using $input_LANDSEA for water body mask (Great Lakes)"
else
  input_LANDSEA=$inputDataDir/landsea/water_polygons_us.gpkg
fi

## GET WBD ##
echo -e $startDiv"Get WBD $hucNumber"
date -u
Tstart
ogr2ogr -f GPKG -t_srs $DEFAULT_FIM_PROJECTION_CRS $outputHucDataDir/wbd.gpkg $input_WBD_gdb $input_NHD_WBHD_layer -where "HUC$hucUnitLength='$hucNumber'"
Tcount

## Subset Vector Layers ##
echo -e $startDiv"Get Vector Layers and Subset $hucNumber"
date -u
Tstart

cmd_args=" -a $outputHucDataDir/nwm_lakes_proj_subset.gpkg"
cmd_args+=" -b $outputHucDataDir/nwm_subset_streams.gpkg"
cmd_args+=" -d $hucNumber"
cmd_args+=" -e $outputHucDataDir/nwm_headwater_points_subset.gpkg"
cmd_args+=" -f $outputHucDataDir/wbd_buffered.gpkg"
cmd_args+=" -g $outputHucDataDir/wbd.gpkg"
cmd_args+=" -i $input_DEM"
cmd_args+=" -j $input_DEM_domain"
cmd_args+=" -l $input_nwm_lakes"
cmd_args+=" -m $input_nwm_catchments"
cmd_args+=" -n $outputHucDataDir/nwm_catchments_proj_subset.gpkg"
cmd_args+=" -r $input_NLD"
cmd_args+=" -v $input_LANDSEA"
cmd_args+=" -w $input_nwm_flows"
cmd_args+=" -x $outputHucDataDir/LandSea_subset.gpkg"
cmd_args+=" -y $input_nwm_headwaters"
cmd_args+=" -z $outputHucDataDir/nld_subset_levees.gpkg"
cmd_args+=" -wb $wbd_buffer"
cmd_args+=" -lpf $input_nld_levee_protected_areas"
cmd_args+=" -lps $outputHucDataDir/LeveeProtectedAreas_subset.gpkg"

#echo "$cmd_args"
python3 $srcDir/clip_vectors_to_wbd.py $cmd_args
Tcount

## Clip WBD8 ##
echo -e $startDiv"Clip WBD8"
date -u
Tstart
ogr2ogr -f GPKG -t_srs $DEFAULT_FIM_PROJECTION_CRS -clipsrc $outputHucDataDir/wbd_buffered.gpkg $outputHucDataDir/wbd8_clp.gpkg $inputDataDir/wbd/WBD_National.gpkg WBDHU8
Tcount

## DERIVE LEVELPATH  ##
echo -e $startDiv"Generating Level Paths for $hucNumber"
date -u
Tstart
$srcDir/derive_level_paths.py -i $outputHucDataDir/nwm_subset_streams.gpkg -b $branch_id_attribute -r "ID" -o $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -e $outputHucDataDir/nwm_headwaters.gpkg -c $outputHucDataDir/nwm_catchments_proj_subset.gpkg -t $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -n $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg -w $outputHucDataDir/nwm_lakes_proj_subset.gpkg

# test if we received a non-zero code back from derive_level_paths.py
subscript_exit_code=$?
# we have to retrow it if it is not a zero (but it will stop further execution in this script)
if [ $subscript_exit_code -ne 0 ]; then exit $subscript_exit_code; fi
Tcount

## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons for $hucNumber"
date -u
Tstart
$srcDir/buffer_stream_branches.py -a $input_DEM_domain -s $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputHucDataDir/branch_polygons.gpkg
Tcount

## CREATE BRANCHID LIST FILE
echo -e $startDiv"Create list file of branch ids for $hucNumber"
date -u
Tstart
$srcDir/generate_branch_list.py -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute -o $branch_list_lst_file
Tcount

## CREATE BRANCH ZERO ##
echo -e $startDiv"Creating branch zero for $hucNumber"
outputCurrentBranchDataDir=$outputBranchDataDir/$branch_zero_id

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $outputCurrentBranchDataDir

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $branch_zero_id"
# Note: don't need to use gdalwarp -cblend as we are using a buffered wbd
date -u
Tstart
[ ! -f $outputCurrentBranchDataDir/dem_meters.tif ] && \
gdalwarp -cutline $outputHucDataDir/wbd_buffered.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -t_srs $DEFAULT_FIM_PROJECTION_CRS $input_DEM $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $branch_zero_id"
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif)

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $branch_zero_id"
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputHucDataDir/nld_subset_levees.gpkg ] && \
gdal_rasterize -l nld_subset_levees -3d -at -a_nodata $ndv -te $xmin $ymin $xmax $ymax -ts $ncols $nrows -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" $outputHucDataDir/nld_subset_levees.gpkg $outputCurrentBranchDataDir/nld_subset_levees_$branch_zero_id.tif
Tcount

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber $branch_zero_id"
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputCurrentBranchDataDir/nld_subset_levees.tif ] && \
python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -nld $outputCurrentBranchDataDir/nld_subset_levees_$branch_zero_id.tif -out $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $branch_zero_id"
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nwm_subset_streams.gpkg $outputCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NWM Headwaters $hucNumber $branch_zero_id"
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nwm_headwater_points_subset.gpkg $outputCurrentBranchDataDir/headwaters_$branch_zero_id.tif
Tcount

## DEM Reconditioning ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $branch_zero_id"
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $outputCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif -d $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -w $outputCurrentBranchDataDir -o $outputCurrentBranchDataDir/dem_burned_$branch_zero_id.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## PIT REMOVE BURNED DEM ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
rd_depression_filling $outputCurrentBranchDataDir/dem_burned_$branch_zero_id.tif $outputCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif
Tcount

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $branch_zero_id"
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$branch_zero_id.tif
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

## PRODUCE BRANCH ZERO HAND
$srcDir/delineate_hydros_and_produce_HAND.sh "unit"

## CREATE USGS GAGES FILE
if [ -f $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg ]; then
    echo -e $startDiv"Assigning USGS gages to branches for $hucNumber"
    date -u
    Tstart
    python3 -m memory_profiler $srcDir/usgs_gage_unit_setup.py -gages $inputDataDir/usgs_gages/usgs_gages.gpkg -nwm $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -o $outputHucDataDir/usgs_subset_gages.gpkg -huc $hucNumber -ahps $inputDataDir/ahps_sites/nws_lid.gpkg -bzero_id $branch_zero_id
    Tcount
fi

## USGS CROSSWALK ##
if [ -f $outputHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $branch_zero_id"
    date -u
    Tstart
    python3 $srcDir/usgs_gage_crosswalk.py -gages $outputHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg -flows $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$branch_zero_id.gpkg -cat $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$branch_zero_id.gpkg -dem $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -dem_adj $outputCurrentBranchDataDir/dem_thalwegCond_$branch_zero_id.tif -outtable $outputCurrentBranchDataDir/usgs_elev_table.csv -b $branch_zero_id
    Tcount
fi

## CLEANUP BRANCH ZERO OUTPUTS ##
echo -e $startDiv"Cleaning up outputs in branch zero $hucNumber"
$srcDir/outputs_cleanup.py -d $outputCurrentBranchDataDir -l $deny_branch_zero_list -b $branch_zero_id


## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_unit_list ]; then
    echo -e $startDiv"Remove files $hucNumber"
    date -u
    Tstart
    $srcDir/outputs_cleanup.py -d $outputHucDataDir -l $deny_unit_list -b $hucNumber
    Tcount
fi

# -------------------
## Start the local csv branch list
$srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber -b $branch_zero_id

# -------------------
## Processing Branches ##
echo
echo "---- Start of branch processing for $hucNumber"
branch_processing_start_time=`date +%s`

parallel --eta --timeout $branch_timeout -j $jobBranchLimit --joblog $branchSummaryLogFile --colsep ',' -- $srcDir/process_branch.sh $runName $hucNumber :::: $branch_list_lst_file

# -------------------
## REMOVE FILES FROM DENY LIST FOR BRANCH ZERO (but using normal branch deny) ##
if [ "$has_deny_branch_zero_override" == "1" ]
then
    echo -e $startDiv"Second cleanup of files for branch zero (none default)"
    $srcDir/outputs_cleanup.py -d $outputHucDataDir -l $deny_branch_zero_list -b 0

else 
    echo -e $startDiv"Second cleanup of files for branch zero using the default branch deny list"
    $srcDir/outputs_cleanup.py -d $outputHucDataDir -l $deny_branches_list -b 0
fi

echo "---- All huc for $hucNumber branches have been now processed"
Calc_Duration $branch_processing_start_time
echo

date -u
echo "---- HUC processing for $hucNumber is complete"
Calc_Duration $huc_start_time
echo


