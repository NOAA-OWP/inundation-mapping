#!/bin/bash

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## DELETING ALL FILES BUT INPUTS ##
# find $outputDataDir -type f -not -name $logFile -delete
# find $outputDataDir -type d -exec rm -rf "{}" \
rm -rf $outputDataDir/*
# cp -a $inputDataDir/dem.tif $outputDataDir/

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
echo -e $startDiv"Get Streams"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows.gpkg ] && \
ogr2ogr -f GPKG $outputDataDir/flows.gpkg $inputFlows -where "HUC6 = '120903'"
# ogr2ogr -progress -f GPKG -clipsrc $outputDataDir/wbd_projected.gpkg $outputDataDir/flows.gpkg $inputDataDir/nwm_flows_proj_huc6.gpkg
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

## RASTERIZE REACH IDENTIFIERS ##
echo -e $startDiv"Rasterize Reach Identifiers"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_grid_reaches.tif ] && \
gdal_rasterize -ot Int32 -a 'ID' -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -a_nodata 0 -init 0 -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/flows.gpkg $outputDataDir/flows_grid_reaches.tif
Tcount

## VECTORIZE REACH ID CENTROIDS ##
echo -e $startDiv"Vectorize ReachID Centroids"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_points_reachid.gpkg ] && \
$libDir/reachID_grid_to_vector_points.py $outputDataDir/flows_grid_reaches.tif $outputDataDir/flows_points_reachid.gpkg reachID
Tcount

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_points_pixels.gpkg ] && \
$libDir/reachID_grid_to_vector_points.py $outputDataDir/flows_grid_reaches.tif $outputDataDir/flows_points_pixels.gpkg featureID
Tcount

## RASTERIZE PIXEL IDENTIFIERS ##
echo -e $startDiv"Rasterize Pixel Identifiers"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_grid_pixels.tif ] && \
gdal_rasterize -ot Int32 -a 'id' -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/flows_points_pixels.gpkg $outputDataDir/flows_grid_pixels.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flows_grid_boolean.tif ] && \
gdal_calc.py --type=Int32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/flows_grid_reaches.tif --calc="A>0" --outfile="$outputDataDir/flows_grid_boolean.tif" --NoDataValue=0
gdal_edit.py -unsetnodata $outputDataDir/flows_grid_boolean.tif
Tcount

## GET HEADWATER VECTOR POINTS ##
# echo -e $startDiv"Get Headwater Vector Points"$stopDiv
# date -u
# Tstart
# $libDir/find_inlets/find_inlets_mr -flow $inputFlows -dangle $outputDataDir/headwater_points.gpkg \
# && [ $? -ne 0 ] && echo "ERROR Getting Headwater Vector Points" && exit 1
# echo "Complete"
# Tcount

# ## RASTERIZE HEADWATER VECTOR POINTS ##
# echo -e $startDiv"Rasterizeing Headwater Vector Points"$stopDiv
# date -u
# Tstart
# gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputDataDir/headwater_points.gpkg $outputDataDir/headwater_points.tif \
# && [ $? -ne 0 ] && echo "ERROR Rasterizeing Headwater Vector Points" && exit 1
# Tcount

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
# mpiexec -n $ncores_pr $taudemDir/pitremove -z $outputDataDir/dem_burned.tif -fel $outputDataDir/dem_burned_filled.tif
# $libDir/fill_and_resolve_flats.py $outputDataDir/dem_burned.tif $outputDataDir/dem_burned_filled.tif False
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
# "(A*(B>0))+($ndv*(B<1))"
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

## PIT FILL THALWEG COND DEM ##
echo -e $startDiv"Pit remove thalweg conditioned DEM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/dem_thalwegCond_filled.tif ] && \
rd_depression_filling $outputDataDir/dem_thalwegCond.tif $outputDataDir/dem_thalwegCond_filled.tif
# mpiexec -n $ncores_pr $taudemDir/pitremove -z $outputDataDir/dem_thalwegCond.tif -fel $outputDataDir/dem_thalwegCond_filled.tif
# $libDir/fill_and_resolve_flats.py $outputDataDir/dem_thalwegCond.tif $outputDataDir/dem_thalwegCond_filled.tif False
Tcount

# D8 FLOW DIR THALWEG COND DEM ##
echo -e $startDiv"D8 on Filled Flows"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/flowdir_d8_thalwegCond_filled.tif ] && [ ! -f $outputDataDir/slopes_d8_thalwegCond_filled.tif ] && \
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputDataDir/dem_thalwegCond_filled.tif -p $outputDataDir/flowdir_d8_thalwegCond_filled.tif -sd8 $outputDataDir/slopes_d8_thalwegCond_filled.tif
Tcount

# ## DINF FLOW DIR ##
# echo -e $startDiv"DINF on Filled Thalweg Conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/INSERTFILE ] && \
# mpiexec -n $ncores_fd $taudemDir/dinfflowdir -fel $outputDataDir/dem_thalwegCond_filled.tif -ang $outputDataDir/flowdir_dinf_thalwegCond.tif -slp $outputDataDir/slopes_dinf.tif
# Tcount

## D8 FLOW ACCUMULATIONS ##
# echo -e $startDiv"D8 Flow Accumulations"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/INSERTFILE ] && \
# mpiexec -n $ncores $taudemDir/aread8 -p $outputDataDir/flowdir_d8_thalwegCond_filled.tif -ad8  $outputDataDir/flowaccum_d8_thalwegCond.tif -wg  $outputDataDir/headwater_points.tif -nc
# Tcount

## THRESHOLD ACCUMULATIONS ##
# echo -e $startDiv"Threshold Accumulations"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/INSERTFILE ] && \
# mpirun -n $ncores $taudemDir/threshold -ssa  $outputDataDir/flowaccum_d8_thalwegCond.tif -src  $outputDataDir/demDerived_streamPixels.tif -thresh 1
# Tcount

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations"$stopDiv
date -u
Tstart
cp -a data/test2/inputs/headwaters.tif data/test2/outputs/
$taudemDir/aread8 -p $outputDataDir/flowdir_d8_burned_filled.tif -ad8  $outputDataDir/flowaccum_d8_burned_filled.tif -wg  $outputDataDir/headwaters.tif -nc
Tcount

## Streamnet ##
echo -e $startDiv"Stream Net for Reaches"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/streamnet -p $outputDataDir/flowdir_d8_burned_filled.tif  -fel $outputDataDir/dem_thalwegCond_filled.tif -ad8 $outputDataDir/flowaccum_d8_burned_filled.tif -src $outputDataDir/flows_grid_boolean.tif -ord $outputDataDir/streamOrder_reaches.tif -tree $outputDataDir/treeFile_reaches.txt -coord $outputDataDir/coordFile_reaches.txt -w $outputDataDir/gw_catchments_reaches.tif -net $outputDataDir/outputStreams_reaches.shp
Tcount

## Streamnet ##
echo -e $startDiv"Stream Net for Pixels"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/streamnet -o $outputDataDir/flows_points_pixels.gpkg -p $outputDataDir/flowdir_d8_burned_filled.tif  -fel $outputDataDir/dem_thalwegCond_filled.tif -ad8 $outputDataDir/flowaccum_d8_burned_filled.tif -src $outputDataDir/flows_grid_boolean.tif -ord $outputDataDir/streamOrder_pixels.tif -tree $outputDataDir/treeFile_pixels.txt -coord $outputDataDir/coordFile_pixels.txt -w $outputDataDir/gw_catchments_pixels.tif -net $outputDataDir/outputStreams_pixels.shp
Tcount

# ## GAGE WATERSHED FOR REACHES ##
# echo -e $startDiv"Gage Watershed for Reaches"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/gw_catchments_reaches.tif ] && \
# mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_d8_thalwegCond_filled.tif -gw $outputDataDir/gw_catchments_reaches.tif -o $outputDataDir/flows_points_reachid.gpkg -id $outputDataDir/idFile.txt
# Tcount
#
# ## GAGE WATERSHED FOR PIXELS ##
# echo -e $startDiv"Gage Watershed for Pixels"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/gw_catchments_pixels.tif ] && \
# mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_d8_thalwegCond_filled.tif -gw $outputDataDir/gw_catchments_pixels.tif -o $outputDataDir/flows_points_pixels.gpkg -id $outputDataDir/idFile.txt
# Tcount

## POLYGONIZE PIXEL WATERSHEDS ##
# echo -e $startDiv"Polygonize Pixel Watersheds"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/INSERTFILE ] && \
# gdal_polygonize.py -8 -f GPKG $outputDataDir/gw_catchments_pixels.tif $outputDataDir/gw_catchments_pixels.gpkg catchments pixelID
# Tcount

## D8 REM ##
echo -e $startDiv"D8 REM"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem.tif ] && \
$libDir/rem.py -d $outputDataDir/dem_thalwegCond.tif -w $outputDataDir/gw_catchments_pixels.tif -o $outputDataDir/rem.tif -n $ndv
Tcount

# ## DINF DISTANCE DOWN ##
# echo -e $startDiv"DINF Distance Down on Filled Thalweg Conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputDataDir/INSERTFILE ] && \
# mpiexec -n $ncores $taudemDir/dinfdistdown -ang $outputDataDir/flowdir_dinf_thalwegCond.tif -fel $outputDataDir/dem_thalwegCond_filled.tif -src $outputDataDir/flows_grid_boolean.tif -dd $outputDataDir/rem.tif -m ave h
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
gdal_polygonize.py -8 -f GPKG $outputDataDir/gw_catchments_reaches_clipped.tif $outputDataDir/gw_catchments_reaches_clipped.gpkg catchments FeatureID
Tcount

## CROSS WALKING ##
# echo -e $startDiv"Cross-walking"$stopDiv
# date -u
# Tstart
# fio cat $outputDataDir/gw_catchments_pixels.gpkg | rio zonalstats -r $inputCatchmask --stats majority > $outputDataDir/majority.geojson \
# && [ $? -ne 0 ] && echo "Zero out negatives" && exit 1
# Tcount

## HYDRAULIC PROPERTIES ##
# echo -e $startDiv"Hydraulic Properties"$stopDiv
# date -u
# Tstart
# python2 $libDir/Hydraulic_Property_V2.2.py -catchment $outputDataDir/gw_catchments_reaches.gpkg -flowline $inputFlows -HAND $outputDataDir/rem_zeroed.tif -netcdf $outputDataDir/hydroProperties.nc -catchRaster $outputDataDir/gw_catchments_reaches.tif \
# && [ $? -ne 0 ] && echo "ERROR Hydraulic Properties" && exit 1
# Tcount

## ALTERNATIVE HYDROPROP ##
# mpiexec -n $ncores $taudemDir/catchhydrogeo -hand $outputDataDir/rem.tif -catch $outputDataDir/gw_catchments_reaches.tif -catchlist $outputDataDir/${n}_comid.txt -slp $outputDataDir/slopes_d8.tif -h $stageconf -table $outputDataDir/hydroprop-basetable-${n}.csv
