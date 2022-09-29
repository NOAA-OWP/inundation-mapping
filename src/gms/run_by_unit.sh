#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SET OUTPUT DIRECTORY FOR UNIT ##
hucNumber="$1"

# Even though check_input_hucs at gms_run_unit validate that all values
# are numbers, sometimes the huc list can come in as windows incoded and not
# unix encoded. It can get missed but tee and time can parse it wrong.
# so, we will strip a slash of the end if it exists, the re-validat that the
# value is a number.  (Note: doesn't seem to work all of the time for encoding
# issues (??))
re='^[0-9]+$'
if ! [[ $hucNumber =~ $re ]] ; then
   echo "Error: hucNumber is not a number" >&2; exit 1
fi

outputHucDataDir=$outputRunDataDir/$hucNumber
outputBranchDataDir=$outputHucDataDir/branches
current_branch_id=$branch_zero_id

## huc data
if [ -d "$outputHucDataDir" ]; then
    if [ $overwrite -eq 1 ]; then
        rm -rf $outputHucDataDir
    else
        echo "Output dir $outputHucDataDir exists. Use overwrite -o to run."
    fi
fi

# make outputs directory
mkdir -p $outputHucDataDir

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

## START MESSAGE ##
echo -e $startDiv"Processing HUC: $hucNumber ..."$stopDiv

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
python3 -m memory_profiler $srcDir/clip_vectors_to_wbd.py -d $hucNumber -w $input_nwm_flows -s $input_nhd_flowlines -l $input_nwm_lakes -r $input_NLD -g $outputHucDataDir/wbd.gpkg -f $outputHucDataDir/wbd_buffered.gpkg -m $input_nwm_catchments -y $input_nhd_headwaters -v $input_LANDSEA -lpf $input_nld_leveeprotectedareas -c $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg -z $outputHucDataDir/nld_subset_levees.gpkg -a $outputHucDataDir/nwm_lakes_proj_subset.gpkg -n $outputHucDataDir/nwm_catchments_proj_subset.gpkg -e $outputHucDataDir/nhd_headwater_points_subset.gpkg -b $outputHucDataDir/nwm_subset_streams.gpkg -x $outputHucDataDir/LandSea_subset.gpkg -lps $outputHucDataDir/LeveeProtectedAreas_subset.gpkg -extent $extent -gl $input_GL_boundaries -lb $lakes_buffer_dist_meters -wb $wbd_buffer
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
$($srcDir/gms/derive_level_paths.py -i $outputHucDataDir/nwm_subset_streams.gpkg -b $branch_id_attribute -r "ID" -o $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -e $outputHucDataDir/nwm_headwaters.gpkg -c $outputHucDataDir/nwm_catchments_proj_subset.gpkg -t $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -n $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg -v -s $dropLowStreamOrders -w $outputHucDataDir/nwm_lakes_proj_subset.gpkg)

# test if we received a non-zero code back from derive_level_paths.py
subscript_exit_code=$?
# we have to retrow it if it is not a zero (but it will stop further execution in this script)
if [ $subscript_exit_code -ne 0 ]; then exit $subscript_exit_code; fi
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
if [ $dropLowStreamOrders != 0 ]; then # only add branch zero to branch list if low stream orders are dropped
    $srcDir/gms/generate_branch_list.py -o $outputHucDataDir/branch_id.lst -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute -z $branch_zero_id
else
    $srcDir/gms/generate_branch_list.py -o $outputHucDataDir/branch_id.lst -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute
fi

Tcount

## CREATE BRANCH ZERO ##
echo -e $startDiv"Creating branch zero for $hucNumber"$stopDiv
outputCurrentBranchDataDir=$outputBranchDataDir/$branch_zero_id

## OVERWRITE
if [ -d "$outputCurrentBranchDataDir" ];then
    if [ $overwrite -eq 1 ]; then
        rm -rf $outputCurrentBranchDataDir
    else
        echo "GMS branch data directories for $hucNumber - $branch_zero_id already exist. Use -o/--overwrite to continue"
        exit 1
    fi
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $outputCurrentBranchDataDir

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
[ ! -f $outputCurrentBranchDataDir/dem_meters.tif ] && \
gdalwarp -cutline $outputHucDataDir/wbd_buffered.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $input_DEM $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif)
Tcount

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputHucDataDir/nld_subset_levees.gpkg ] && \
gdal_rasterize -l nld_subset_levees -3d -at -a_nodata $ndv -te $xmin $ymin $xmax $ymax -ts $ncols $nrows -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" $outputHucDataDir/nld_subset_levees.gpkg $outputCurrentBranchDataDir/nld_subset_levees_$branch_zero_id.tif
Tcount

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputCurrentBranchDataDir/nld_subset_levees.tif ] && \
python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -nld $outputCurrentBranchDataDir/nld_subset_levees_$branch_zero_id.tif -out $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nwm_subset_streams.gpkg $outputCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nhd_headwater_points_subset.gpkg $outputCurrentBranchDataDir/headwaters_$branch_zero_id.tif
Tcount

## DEM Reconditioning ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $outputCurrentBranchDataDir/flows_grid_boolean_$branch_zero_id.tif -d $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -w $outputCurrentBranchDataDir -g $outputCurrentBranchDataDir/temp_work -o $outputCurrentBranchDataDir/dem_burned_$branch_zero_id.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## PIT REMOVE BURNED DEM ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $branch_zero_id"$stopDiv
date -u
Tstart
rd_depression_filling $outputCurrentBranchDataDir/dem_burned_$branch_zero_id.tif $outputCurrentBranchDataDir/dem_burned_filled_$branch_zero_id.tif
Tcount

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $branch_zero_id"$stopDiv
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
export max_order_branch=$max_order_branch
export max_order=$max_order
if [ $dropLowStreamOrders != 0 ]; then # only produce branch zero HAND if low stream orders are dropped
    $srcDir/gms/delineate_hydros_and_produce_HAND.sh "unit"
else
    echo -e $startDiv"Skipping branch zero processing because there are no stream orders being dropped $hucNumber"$stopDiv
fi

## CREATE USGS GAGES FILE
echo -e $startDiv"Assigning USGS gages to branches for $hucNumber"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/usgs_gage_unit_setup.py -gages $inputDataDir/usgs_gages/usgs_gages.gpkg -nwm $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -o $outputHucDataDir/usgs_subset_gages.gpkg -huc $hucNumber -ahps $inputDataDir/ahps_sites/nws_lid.gpkg -bzero_id $branch_zero_id -bzero $dropLowStreamOrders
Tcount

## USGS CROSSWALK ##
if [ -f $outputHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg ]; then
    echo -e $startDiv"USGS Crosswalk $hucNumber $branch_zero_id"$stopDiv
    date -u
    Tstart
    python3 $srcDir/usgs_gage_crosswalk.py -gages $outputHucDataDir/usgs_subset_gages_$branch_zero_id.gpkg -flows $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$branch_zero_id.gpkg -cat $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$branch_zero_id.gpkg -dem $outputCurrentBranchDataDir/dem_meters_$branch_zero_id.tif -dem_adj $outputCurrentBranchDataDir/dem_thalwegCond_$branch_zero_id.tif -outtable $outputCurrentBranchDataDir/usgs_elev_table.csv -b $branch_zero_id
    Tcount
fi

## CLEANUP BRANCH ZERO OUTPUTS ##
echo -e $startDiv"Cleaning up outputs in branch zero $hucNumber"$stopDiv
date -u
Tstart
$srcDir/gms/outputs_cleanup.py -d $outputCurrentBranchDataDir -l $srcDir/../config/deny_gms_branch_zero.lst -v -b
Tcount

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_gms_unit_list ]; then
    echo -e $startDiv"Remove files $hucNumber"$stopDiv
    date -u
    Tstart
    $srcDir/gms/outputs_cleanup.py -d $outputHucDataDir -l $deny_gms_unit_list -v
    Tcount
fi
