#!/bin/bash

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## DELETING ALL FILES BUT INPUTS ##
rm -rf $outputDataDir/*

## GET WBD ##
echo -e $startDiv"Get WBD"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/wbd.gpkg ] && \
ogr2ogr -f GPKG $outputDataDir/wbd.gpkg $inputDataDir/NHD_H_1209_HU4_Shape/WBDHU6.shp -where "HUC6='120903'"
Tcount

## REPROJECT WBD ##
echo -e $startDiv"Reproject WBD"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/wbd_projected.gpkg ] && \
ogr2ogr -t_srs "$PROJ" -f GPKG $outputDataDir/wbd_projected.gpkg $outputDataDir/wbd.gpkg
Tcount

## BUFFER WBD ##
echo -e $startDiv"Buffer WBD"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/wbd_projected_buffered.gpkg ] && \
ogr2ogr -f GPKG -dialect sqlite -sql "select ST_buffer(geom, $bufferDistance) from 'WBDHU6'" $outputDataDir/wbd_projected_buffered.gpkg $outputDataDir/wbd_projected.gpkg
Tcount

## GET STREAMS ##
echo -e $startDiv"Get Vector Layers and Subset"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/demDerived_reaches.gpkg ] && \
$libDir/snap_and_clip_to_nhd.py -d 120903 -p "$PROJ" -w $nwmDir/nwm_headwaters_proj.gpkg -s $inputDataDir/NHDPlusBurnLineEvent_1209.gpkg  -v $inputDataDir/NHDPlusFlowlineVAA_1209.gpkg -l $nwmDir/nwm_lakes_proj.gpkg -u $outputDataDir/wbd_projected.gpkg -c $outputDataDir/NHDPlusBurnLineEvent_clipped.gpkg -a $outputDataDir/nwm_lakes_proj_120903.gpkg -t $outputDataDir/nwm_headwaters_proj_120903.gpkg -m $nwmDir/nwm_catchments_proj.gpkg -n $outputDataDir/nwm_catchments_proj_120903.gpkg -e $outputDataDir/nhd_headwater_points.gpkg
Tcount

## CLIP DEM ##
echo -e $startDiv"Clip DEM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem.tif ] && \
gdalwarp -cutline $outputDataDir/wbd_projected_buffered.gpkg -crop_to_cutline -ot Int32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $inputDataDir/mosaic.vrt $outputDataDir/dem.tif
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($libDir/getRasterInfoNative.py $outputDataDir/dem.tif)
echo "Complete"
Tcount

## CONVERT TO METERS ##
echo -e $startDiv"Convert DEM to Meters"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem_meters.tif ] && \
gdal_calc.py --type=Float32 --co "BLOCKXSIZE=512" --co "BLOCKYSIZE=512" --co "TILED=YES" --co "COMPRESS=LZW" --co "BIGTIFF=YES" -A $outputDataDir/dem.tif --outfile="$outputDataDir/dem_meters.tif" --calc="((float32(A)*(float32(A)>$ndv))/100)+((float32(A)<=$ndv)*$ndv)" --NoDataValue=$ndv
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_grid_boolean.tif ] && \
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/NHDPlusBurnLineEvent_clipped.gpkg $outputDataDir/flows_grid_boolean.tif
Tcount

## RASTERIZE NHD HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/headwaters.tif ] && \
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/nhd_headwater_points.gpkg $outputDataDir/headwaters.tif
Tcount

## RASTERIZE NWM CATCHMENTS ##
echo -e $startDiv"Raster NWM Catchments"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/nwm_catchments_proj_120903.tif ] && \
gdal_rasterize -ot Int32 -a feature_id -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/nwm_catchments_proj_120903.gpkg $outputDataDir/nwm_catchments_proj_120903.tif
Tcount

## BURN NEGATIVE ELEVATIONS STREAMS ##
echo -e $startDiv"Drop thalweg elevations by "$negativeBurnValue" units"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem_burned.tif ] && \
gdal_calc.py --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/dem_meters.tif -B $outputDataDir/flows_grid_boolean.tif --calc="A-$negativeBurnValue*B" --outfile="$outputDataDir/dem_burned.tif" --NoDataValue=$ndv
Tcount

## PIT REMOVE BURNED DEM ##
echo -e $startDiv"Pit remove Burned DEM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem_burned_filled.tif ] && \
rd_depression_filling $outputDataDir/dem_burned.tif $outputDataDir/dem_burned_filled.tif
Tcount

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Burned DEM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flowdir_d8_burned_filled.tif ] && [ ! -f $outputDataDir/slopes_d8_burned_filled.tif ] && \
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputDataDir/dem_burned_filled.tif -p $outputDataDir/flowdir_d8_burned_filled.tif -sd8 $outputDataDir/slopes_d8_burned_filled.tif
Tcount

## MASK BURNED DEM FOR STREAMS ONLY ###
echo -e $startDiv"Mask Burned DEM for Thalweg Only"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flowdir_d8_burned_filled_flows.tif ] && \
gdal_calc.py --type=Int32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/flowdir_d8_burned_filled.tif -B $outputDataDir/flows_grid_boolean.tif --calc="A/B" --outfile="$outputDataDir/flowdir_d8_burned_filled_flows.tif" --NoDataValue=0
Tcount

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem_thalwegCond.tif ] && \
$taudemDir/flowdircond -p $outputDataDir/flowdir_d8_burned_filled_flows.tif -z $outputDataDir/dem_meters.tif -zfdc $outputDataDir/dem_thalwegCond.tif
Tcount

## FLOW CONDITION STREAMS ##
# echo -e $startDiv"Flow Condition Thalweg - 2"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/dem_thalwegCond_filled.tif ] && \
# $taudemDir/flowdircond -p $outputDataDir/flowdir_d8_burned_filled_flows.tif -z $outputDataDir/dem_burned_filled.tif -zfdc $outputDataDir/dem_thalwegCond_filled.tif
# Tcount

# ## PIT FILL THALWEG COND DEM ##
# echo -e $startDiv"Pit remove thalweg conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/dem_thalwegCond_filled.tif ] && \
# rd_depression_filling $outputDataDir/dem_thalwegCond.tif $outputDataDir/dem_thalwegCond_filled.tif
# Tcount

# # D8 FLOW DIR THALWEG COND DEM ##
# echo -e $startDiv"D8 on Filled Flows"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/flowdir_d8_thalwegCond_filled.tif ] && [ ! -f $outputDataDir/slopes_d8_burned_filled.tif ] && \
# mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputDataDir/dem_thalwegCond_filled.tif -p $outputDataDir/flowdir_d8_thalwegCond_filled.tif -sd8 $outputDataDir/slopes_d8_thalwegCond_filled.tif
# Tcount

## DINF FLOW DIR ##
# echo -e $startDiv"DINF on Filled Thalweg Conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/flowdir_dinf_thalwegCond.tif ] && \
# mpiexec -n $ncores_fd $taudemDir2/dinfflowdir -fel $outputDataDir/dem_thalwegCond_filled.tif -ang $outputDataDir/flowdir_dinf_thalwegCond.tif -slp $outputDataDir/slopes_dinf.tif
# Tcount

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations"$stopDiv
date -u
Tstart
$taudemDir/aread8 -p $outputDataDir/flowdir_d8_burned_filled.tif -ad8  $outputDataDir/flowaccum_d8_burned_filled.tif -wg  $outputDataDir/headwaters.tif -nc
Tcount

# THRESHOLD ACCUMULATIONS ##
echo -e $startDiv"Threshold Accumulations"$stopDiv
date -u
Tstart
$taudemDir/threshold -ssa $outputDataDir/flowaccum_d8_burned_filled.tif -src  $outputDataDir/demDerived_streamPixels.tif -thresh 1
Tcount

# STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/demDerived_reaches.gpkg ] && \
$taudemDir/streamnet -p $outputDataDir/flowdir_d8_burned_filled.tif  -fel $outputDataDir/dem_thalwegCond.tif -ad8 $outputDataDir/flowaccum_d8_burned_filled.tif -src $outputDataDir/demDerived_streamPixels.tif -ord $outputDataDir/streamOrder.tif -tree $outputDataDir/treeFile.txt -coord $outputDataDir/coordFile.txt -w $outputDataDir/sn_catchments_reaches.tif -net $outputDataDir/demDerived_reaches.gpkg
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/demDerived_reaches_split.gpkg ] && \
$libDir/split_flows.py $outputDataDir/demDerived_reaches.gpkg "$PROJ" $outputDataDir/dem_thalwegCond.tif $outputDataDir/demDerived_reaches_split.gpkg $outputDataDir/demDerived_reaches_split_points.gpkg $maxSplitDistance_meters $manning_n $slope_min
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches.tif ] && \
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_d8_burned_filled.tif -gw $outputDataDir/gw_catchments_reaches.tif -o $outputDataDir/demDerived_reaches_split_points.gpkg -id $outputDataDir/idFile.txt
Tcount

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_points_pixels.gpkg ] && \
$libDir/reachID_grid_to_vector_points.py $outputDataDir/flows_grid_boolean.tif $outputDataDir/flows_points_pixels.gpkg featureID
Tcount

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_pixels.tif ] && \
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_d8_burned_filled.tif -gw $outputDataDir/gw_catchments_pixels.tif -o $outputDataDir/flows_points_pixels.gpkg -id $outputDataDir/idFile.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem.tif ] && \
$libDir/rem.py -d $outputDataDir/dem_thalwegCond.tif -w $outputDataDir/gw_catchments_pixels.tif -o $outputDataDir/rem.tif -n $ndv
Tcount

## DINF DISTANCE DOWN ##
# echo -e $startDiv"DINF Distance Down on Filled Thalweg Conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/flowdir_dinf_thalwegCond.tif ] && \
# mpiexec -n $ncores_fd $taudemDir/dinfdistdown -ang $outputDataDir/flowdir_dinf_thalwegCond.tif -fel $outputDataDir/dem_thalwegCond_filled.tif -src $outputDataDir/demDerived_streamPixels.tif -dd $outputDataDir/rem.tif -m ave h
# Tcount

## CLIP REM TO HUC ##
echo -e $startDiv"Clip REM to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem_clipped.tif ] && \
gdalwarp -cutline $outputDataDir/wbd_projected.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $outputDataDir/rem.tif $outputDataDir/rem_clipped.tif
Tcount

## BRING DISTANCE DOWN TO ZERO ##
echo -e $startDiv"Zero out negative values in distance down grid"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem_clipped_zeroed.tif ] && \
gdal_calc.py --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/rem_clipped.tif --calc="(A*(A>=0))+((A<=$ndv)*$ndv)" --NoDataValue=$ndv --outfile=$outputDataDir/"rem_clipped_zeroed.tif"
Tcount

## CLIP CM TO HUC ##
echo -e $startDiv"Clip Catchmask to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_clipped.tif ] && \
gdalwarp -r near -cutline $outputDataDir/wbd_projected.gpkg -crop_to_cutline -ot Int32 -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $outputDataDir/gw_catchments_reaches.tif $outputDataDir/gw_catchments_reaches_clipped.tif
Tcount

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_clipped.gpkg ] && \
gdal_polygonize.py -8 -f GPKG $outputDataDir/gw_catchments_reaches_clipped.tif $outputDataDir/gw_catchments_reaches_clipped.gpkg catchments HydroID
Tcount

## CLIP MODEL STREAMS TO HUC ##
echo -e $startDiv"Clipping Model Streams to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/demDerived_reaches_split_clipped.gpkg ] && \
ogr2ogr -progress -f GPKG -clipsrc $outputDataDir/wbd_projected.gpkg $outputDataDir/demDerived_reaches_split_clipped.gpkg $outputDataDir/demDerived_reaches_split.gpkg
Tcount

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##
echo -e $startDiv"Process catchments and model streams step 1"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg ] && \
$libDir/filter_catchments_and_add_attributes.py $outputDataDir/gw_catchments_reaches_clipped.gpkg $outputDataDir/demDerived_reaches_split_clipped.gpkg $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg
Tcount

## GET RASTER METADATA ##
echo -e $startDiv"Get Clipped Raster Metadata"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv_clipped xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($libDir/getRasterInfoNative.py $outputDataDir/gw_catchments_reaches_clipped.tif)
echo "Complete"
Tcount

## RASTERIZE NEW CATCHMENTS AGAIN ##
echo -e $startDiv"Rasterize filtered catchments"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.tif ] && \
gdal_rasterize -ot Int32 -a HydroID -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.tif
Tcount

## CLIP SLOPE RASTER ##
echo -e $startDiv"Clipping Slope Raster to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/slopes_d8_burned_filled_clipped.tif ] && \
gdalwarp -r near -cutline $outputDataDir/wbd_projected.gpkg -crop_to_cutline -ot Float32 -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $outputDataDir/slopes_d8_burned_filled.tif $outputDataDir/slopes_d8_burned_filled_clipped.tif
Tcount

## MASK SLOPE RASTER ##
echo -e $startDiv"Masking Slope Raster to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/slopes_d8_burned_filled_clipped_masked.tif ] && \
gdal_calc.py --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/slopes_d8_burned_filled_clipped.tif -B $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.tif --calc="(A*(B>0))+((B<=0)*-1)" --NoDataValue=-1 --outfile=$outputDataDir/"slopes_d8_burned_filled_clipped_masked.tif"
Tcount

## MASK REM RASTER ##
echo -e $startDiv"Masking REM Raster to HUC"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem_clipped_zeroed_masked.tif ] && \
gdal_calc.py --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/rem_clipped_zeroed.tif -B $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.tif --calc="(A*(B>0))+((B<=0)*$ndv)" --NoDataValue=$ndv --outfile=$outputDataDir/"rem_clipped_zeroed_masked.tif"
Tcount

## MAKE CATCHMENT AND STAGE FILES ##
echo -e $startDiv"Generate Catchment List and Stage List Files"$stopDiv
date -u
Tstart
$libDir/make_stages_and_catchlist.py $outputDataDir/demDerived_reaches_split_clipped.gpkg $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg $outputDataDir/stage.txt $outputDataDir/catchment_list.txt $stage_min_meters $stage_interval_meters $stage_max_meters
Tcount

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Hydraulic Properties"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/src_base.csv ] && \
$taudemDir/catchhydrogeo -hand $outputDataDir/rem_clipped_zeroed_masked.tif -catch $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.tif -catchlist $outputDataDir/catchment_list.txt -slp $outputDataDir/slopes_d8_burned_filled_clipped_masked.tif -h $outputDataDir/stage.txt -table $outputDataDir/src_base.csv
Tcount

## GET MAJORITY COUNTS ##
echo -e $startDiv"Getting majority counts"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/majority.geojson ] && \
fio cat $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg | rio zonalstats -r $outputDataDir/nwm_catchments_proj_120903.tif --stats majority > $outputDataDir/majority.geojson
Tcount

## POST PROCESS HYDRAULIC PROPERTIES ##
echo -e $startDiv"Post Process Hydraulic Properties"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/src_full.csv ] && \
$libDir/hydraulic_property_postprocess.py $outputDataDir/src_base.csv $manning_n $outputDataDir/src_full.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_clipped_addedAttributes_crosswalked.gpkg ] && \
$libDir/add_crosswalk.py $outputDataDir/gw_catchments_reaches_clipped_addedAttributes.gpkg $outputDataDir/demDerived_reaches_split_clipped.gpkg $outputDataDir/src_full.csv $outputDataDir/majority.geojson $outputDataDir/gw_catchments_reaches_clipped_addedAttributes_crosswalked.gpkg $outputDataDir/demDerived_reaches_split_clipped_addedAttributes_crosswalked.gpkg $outputDataDir/src_full_crosswalked.csv $outputDataDir/src.json $outputDataDir/crosswalk_table.csv
Tcount

## INUNDATION LIBRARIES ##
# echo -e $startDiv"Inundation Libraries"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/inundation_library.gpkg ] && \
# echo "inundation library"
# Tcount
