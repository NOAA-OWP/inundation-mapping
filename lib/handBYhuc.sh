#!/bin/bash

# set variables
# PROJ="+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
projectDir=$HOME/Documents/research/nwc/foss_fim
dataDir=$projectDir/data/test3
libDir=$projectDir/lib
taudemDir=/usr/local/taudem
negativeBurnValue=10000
ncores=3
startDiv="\n#######################\n"
stopDiv="\n#######################"
inputDEM=dem.tif
inputFlows=flows
inputWeights=weights.tif

# ## DELETING ALL FILES BUT INPUTS ##
find $dataDir -type f -not -name $inputDEM -not -name $inputFlows.shp -not -name $inputFlows.shx -not -name $inputFlows.prj -not -name $inputFlows.dbf -not -name $inputWeights -delete

## GET RASTER METADATA
echo -e $startDiv"Get Raster Metadata"$stopDiv
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($projectDir/lib/getRasterInfoNative.py $dataDir/dem.tif)

## BURN REACH IDENTIFIERS ##
echo -e $startDiv"Burn Reach Identifiers"$stopDiv
rm -f $dataDir/flows_grid_reaches.tif
gdal_rasterize -ot Int32 -l 'flows' -a 'COMID' -a_nodata 0 -init 0 -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $dataDir/flows.shp $dataDir/flows_grid_reaches.tif \
&& [ $? -ne 0 ] && echo "ERROR burning reach identifiers" && exit 1

## VECTORIZE REACH ID CENTROIDS ##
echo -e $startDiv"Vectorize ReachID Centroids"$stopDiv
$libDir/reachID_grid_to_vector_points.py $dataDir/flows_grid_reaches.tif $dataDir/flows_points_reachid.shp reachID \
&& [ $? -ne 0 ] && echo "ERROR Vectorizing ReachID Centroids" && exit 1

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids"$stopDiv
$libDir/reachID_grid_to_vector_points.py $dataDir/flows_grid_reaches.tif $dataDir/flows_points_pixels.shp featureID \
&& [ $? -ne 0 ] && echo "ERROR Vectorizing Pixel Centroids" && exit 1

## BURN PIXEL IDENTIFIERS ##
echo -e $startDiv"Burn Pixel Identifiers"$stopDiv
rm -f $dataDir/flows_grid_pixels.tif
gdal_rasterize -ot Int32 -a 'id' -a_nodata 0 -init 0 -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $dataDir/flows_points_pixels.shp $dataDir/flows_grid_pixels.tif \
&& [ $? -ne 0 ] && echo "ERROR burning pixel identifiers" && exit 1

## BURN REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Burn Reach Boolean"$stopDiv
gdal_calc.py --overwrite -A $dataDir/flows_grid_reaches.tif --calc="A>0" --outfile="$dataDir/flows_grid_boolean.tif" --NoDataValue=0 \
&& [ $? -ne 0 ] && echo "ERROR Burning Reach Booleans" && exit 1
gdal_edit.py -unsetnodata $dataDir/flows_grid_boolean.tif

## BURN NEGATIVE ELEVATIONS STREAMS ##
echo -e $startDiv"Drop thalweg elevations by "$negativeBurnValue" units"$stopDiv
gdal_calc.py --overwrite -A $dataDir/dem.tif -B $dataDir/flows_grid_boolean.tif --calc="A-$negativeBurnValue*B" --outfile="$dataDir/dem_burned.tif" --NoDataValue=$ndv \
&& [ $? -ne 0 ] && echo "ERROR Dropping Thalweg Elevations" && exit 1

## PIT REMOVE DEM ##
echo -e $startDiv"Pit remove burned DEM"$stopDiv
mpiexec -n $ncores $taudemDir/pitremove -z $dataDir/dem_burned.tif -fel $dataDir/dem_burned_filled.tif \
&& [ $? -ne 0 ] && echo "ERROR Pit removing burned DEM" && exit 1

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Filled Burned DEM"$stopDiv
mpiexec -n $ncores $taudemDir/d8flowdir -fel $dataDir/dem_burned_filled.tif -p $dataDir/flowdir_d8_burned.tif \
&& [ $? -ne 0 ] && echo "ERROR D8 Flow Directions on Filled Burned DEM" && exit 1

## MASK D8 FLOW DIR FOR STREAMS ONLY ###
echo -e $startDiv"Mask D8 Flow Directions for Thalweg Only"$stopDiv
gdal_calc.py --overwrite -A $dataDir/flowdir_d8_burned.tif -B $dataDir/flows_grid_boolean.tif --calc="A/B" --outfile="$dataDir/flowdir_d8_burned_flows.tif" --NoDataValue=0 \
&& [ $? -ne 0 ] && echo "ERROR Masking D8 Flow Directions for Thalweg Only" && exit 1

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg"$stopDiv
$taudemDir/flowdircond -p $dataDir/flowdir_d8_burned_flows.tif -z $dataDir/dem.tif -zfdc $dataDir/dem_thalwegCond.tif \
&& [ $? -ne 0 ] && echo "ERROR Flow Conditioning Thalweg" && exit 1

## PIT FILL THALWEG COND DEM ##
echo -e $startDiv"Pit remove thalweg conditioned DEM"$stopDiv
mpiexec -n $ncores $taudemDir/pitremove -z $dataDir/dem_thalwegCond.tif -fel $dataDir/dem_thalwegCond_filled.tif \
&& [ $? -ne 0 ] && echo "ERROR Pit removing thalweg conditioned DEM" && exit 1

## D8 FLOW DIR THALWEG COND DEM ##
echo -e $startDiv"D8 on Filled Thalweg Conditioned DEM"$stopDiv
mpiexec -n $ncores $taudemDir/d8flowdir -fel $dataDir/dem_thalwegCond_filled.tif -p $dataDir/flowdir_d8_thalwegCond_filled.tif \
&& [ $? -ne 0 ] && echo "ERROR D8 on Filled Thalweg Conditioned DEM" && exit 1

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations"$stopDiv
mpiexec -n $ncores $taudemDir/aread8 -p $dataDir/flowdir_d8_thalwegCond_filled.tif -ad8  $dataDir/flowaccum_d8_thalwegCond.tif -wg  $dataDir/weights.tif -nc \
&& [ $? -ne 0 ] && echo "ERROR D8 Flow Accumulations" && exit 1

## THRESHOLD ACCUMULATIONS ##
# echo -e $startDiv"Threshold Accumulations"$stopDiv
# mpirun -n $ncores $taudemDir/threshold -ssa  $dataDir/flowaccum_d8_thalwegCond.tif -src  $dataDir/demDerived_streamPixels.tif -thresh 1 \
# && [ $? -ne 0 ] && echo "ERROR Threshold Accumulations" && exit 1

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches"$stopDiv
mpiexec -n $ncores $taudemDir/gagewatershed -p $dataDir/flowdir_d8_thalwegCond_filled.tif -gw $dataDir/gw_catchments_reaches.tif -o $dataDir/flows_points_reachid.shp -id $dataDir/idFile.txt \
&& [ $? -ne 0 ] && echo "ERROR Gage Watershed for Reaches" && exit 1

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels"$stopDiv
mpiexec -n $ncores $taudemDir/gagewatershed -p $dataDir/flowdir_d8_thalwegCond_filled.tif -gw $dataDir/gw_catchments_pixels.tif -o $dataDir/flows_points_pixels.shp -id $dataDir/idFile.txt \
&& [ $? -ne 0 ] && echo "ERROR Gage Watershed for Pixels" && exit 1

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds"$stopDiv
gdal_polygonize.py -8 -f "ESRI Shapefile" $dataDir/gw_catchments_reaches.tif $dataDir/gw_catchments_reaches.shp catchments FeatureID \
&& [ $? -ne 0 ] && echo "ERROR Polygonize Reach Watersheds" && exit 1

## POLYGONIZE PIXEL WATERSHEDS ##
echo -e $startDiv"Polygonize Pixel Watersheds"$stopDiv
gdal_polygonize.py -8 -f "ESRI Shapefile" $dataDir/gw_catchments_pixels.tif $dataDir/gw_catchments_pixels.shp catchments FeatureID \
&& [ $? -ne 0 ] && echo "ERROR Polygonize Pixel Watersheds" && exit 1

## D8 REM ##
echo -e $startDiv"D8 REM"$stopDiv
$libDir/d8_rem.py $dataDir/dem_thalwegCond.tif $dataDir/flows_grid_pixels.tif $dataDir/gw_catchments_pixels.tif $dataDir/distDown_d8.tif \
&& [ $? -ne 0 ] && echo "D8 REM" && exit 1

## DINF FLOW DIR ##
# echo -e $startDiv"DINF on Filled Thalweg Conditioned DEM"$stopDiv
# mpiexec -n $ncores $taudemDir/dinfflowdir -fel $dataDir/dem_thalwegCond_filled.tif -ang $dataDir/flowdir_dinf_thalwegCond.tif -slp $dataDir/slopes_d8_thalwegCond.tif \
# && [ $? -ne 0 ] && echo "ERROR DINF on Filled Thalweg Conditioned DEM" && exit 1

## DINF DISTANCE DOWN ##
# echo -e $startDiv"DINF Distance Down on Filled Thalweg Conditioned DEM"$stopDiv
# mpiexec -n $ncores $taudemDir/dinfdistdown -ang $dataDir/flowdir_dinf_thalwegCond.tif -fel $dataDir/dem_thalwegCond.tif -src $dataDir/demDerived_streamPixels.tif -dd $dataDir/distDown_dinf.tif -m ave h \
# && [ $? -ne 0 ] && echo "ERROR DINF Distance Down on Filled Thalweg Conditioned DEM" && exit 1
#mpiexec -n $ncores $taudemDir/dinfdistdown -ang $dataDir/flowdir_dinf_thalwegCond.tif -fel $dataDir/dem_thalwegCond_filled.tif -src $dataDir/demDerived_streamPixels.tif -dd $dataDir/distDown_dinf_2.tif -m ave h

## BRING DISTANCE DOWN TO ZERO ##
echo -e $startDiv"Zero out negative values in distance down grid"$stopDiv
gdal_calc.py --overwrite -A $dataDir/distDown_d8.tif --calc="A*(A>=0)" --outfile=$dataDir/"distDown_d8_zeroed.tif" \
&& [ $? -ne 0 ] && echo "Zero out negatives" && exit 1

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Hydraulic Properties"$stopDiv
python2 Hydraulic_Property_V2.2.py -catchment $dataDir/gw_catchments_reaches.shp -flowline $dataDir/flows.shp -HAND $dataDir/distDown_d8_zeroed.tif -netcdf $dataDir/hydroProperties.nc -catchRaster $dataDir/gw_catchments_reaches.tif \
&& [ $? -ne 0 ] && echo "ERROR Hydraulic Properties" && exit 1
# RUN CPROFILE TO FIND BOTTLENECK
