#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

echo -e $startDiv"Parameter Values"
echo -e "extent=$extent"
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
echo -e "step_start_number=$step_start_number"
echo -e "step_end_number=$step_end_number"
echo -e $stopDiv

## SET OUTPUT DIRECTORY FOR UNIT ##
#hucNumber="$1"
hucNumber=$(echo "$1" | xargs)  # strip extra chars off either end
outputHucDataDir=$outputRunDataDir/$hucNumber
mkdir $outputHucDataDir

## SET VARIABLES AND FILE INPUTS ##
hucUnitLength=${#hucNumber}

huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}
input_NHD_WBHD_layer=WBDHU$hucUnitLength
input_DEM=$inputDataDir/nhdplus_rasters/HRNHDPlusRasters"$huc4Identifier"/elev_m.tif
input_NLD=$inputDataDir/nld_vectors/huc2_levee_lines/nld_preprocessed_"$huc2Identifier".gpkg
input_bathy_bankfull=$inputDataDir/$bankfull_input_table

# Define the landsea water body mask using either Great Lakes or Ocean polygon input #
if [[ $huc2Identifier == "04" ]] ; then
  input_LANDSEA=$input_GL_boundaries
  echo -e "Using $input_LANDSEA for water body mask (Great Lakes)"
else
  input_LANDSEA=$inputDataDir/landsea/water_polygons_us.gpkg
fi


# This will jump to the current step and keep incrementing by one automatically
currentStepNumber=0

# =================================
# Step 1

## GET WBD ##

let "currentStepNumber+=1"
echo 'current='$currentStepNumber
if [ $currentStepNumber -ge $step_start_number ]; then
	echo -e $startDiv"Step "$currentStepNumber": Get WBD $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/wbd.gpkg ] && \
	ogr2ogr -f GPKG $outputHucDataDir/wbd.gpkg $input_WBD_gdb $input_NHD_WBHD_layer -where "HUC$hucUnitLength='$hucNumber'"
	Tcount
fi


# =================================
# Step 2

## Subset Vector Layers ##

let "currentStepNumber+=1"
echo 'current='$currentStepNumber
if [ $currentStepNumber -ge $step_start_number ] && 
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Get Vector Layers and Subset $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg ] && \
	python3 -m memory_profiler $srcDir/clip_vectors_to_wbd.py -d $hucNumber \
															  -w $input_nwm_flows \
															  -s $input_nhd_flowlines \
															  -l $input_nwm_lakes \
															  -r $input_NLD \
															  -g $outputHucDataDir/wbd.gpkg \
															  -f $outputHucDataDir/wbd_buffered.gpkg \
															  -m $input_nwm_catchments \
															  -y $input_nhd_headwaters \
															  -v $input_LANDSEA \
															  -c $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg \
															  -z $outputHucDataDir/nld_subset_levees.gpkg \
															  -a $outputHucDataDir/nwm_lakes_proj_subset.gpkg \
															  -n $outputHucDataDir/nwm_catchments_proj_subset.gpkg \
															  -e $outputHucDataDir/nhd_headwater_points_subset.gpkg \
															  -b $outputHucDataDir/nwm_subset_streams.gpkg \
															  -x $outputHucDataDir/LandSea_subset.gpkg \
															  -extent $extent \
															  -gl $input_GL_boundaries \
															  -lb $lakes_buffer_dist_meters \
															  -wb $wbd_buffer
	Tcount

	if [ "$extent" = "MS" ]; then
		if [[ ! -f $outputHucDataDir/nhd_headwater_points_subset.gpkg ]] ; then
			echo "No AHPs point(s) within HUC $hucNumber boundaries. Aborting run_by_unit.sh"
			rm -rf $outputHucDataDir
			exit 55
		fi
	fi
fi


# =================================
# Step 3

## Clip WBD8 ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then
   
	echo -e $startDiv"Step "$currentStepNumber": Clip WBD8"$stopDiv
	date -u
	Tstart
	ogr2ogr -f GPKG -clipsrc $outputHucDataDir/wbd_buffered.gpkg $outputHucDataDir/wbd8_clp.gpkg $inputDataDir/wbd/WBD_National.gpkg WBDHU8
	Tcount
fi


# =================================
# Step 4

## CLIP DEM ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Clip DEM $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/dem_meters.tif ] && \
	gdalwarp -cutline $outputHucDataDir/wbd_buffered.gpkg -crop_to_cutline \
														  -ot Float32 \
														  -r bilinear \
														  -of "GTiff" \
														  -overwrite \
														  -co "BLOCKXSIZE=512" \
														  -co "BLOCKYSIZE=512" \
														  -co "TILED=YES" \
														  -co "COMPRESS=LZW" \
														  -co "BIGTIFF=YES" $input_DEM $outputHucDataDir/dem_meters.tif
	Tcount
fi


# =================================
# Step 5

## GET RASTER METADATA

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then
   
	echo -e $startDiv"Step "$currentStepNumber": Get DEM Metadata $hucNumber"$stopDiv
	date -u
	Tstart
	read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputHucDataDir/dem_meters.tif)
fi


# =================================
# Step 6

## RASTERIZE NLD MULTILINES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Rasterize all NLD multilines using zelev vertices"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/nld_rasterized_elev.tif ] && [ -f $outputHucDataDir/nld_subset_levees.gpkg ] && \
	gdal_rasterize -l nld_subset_levees -3d -at -a_nodata $ndv \
										-te $xmin $ymin $xmax $ymax \
										-ts $ncols $nrows \
										-ot Float32 \
										-of GTiff \
										-co "BLOCKXSIZE=512" \
										-co "BLOCKYSIZE=512" \
										-co "COMPRESS=LZW" \
										-co "BIGTIFF=YES" \
										-co "TILED=YES" $outputHucDataDir/nld_subset_levees.gpkg $outputHucDataDir/nld_rasterized_elev.tif
	Tcount
fi


# =================================
# Step 7

## RASTERIZE REACH BOOLEAN (1 & 0) ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Rasterize Reach Boolean $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/flows_grid_boolean.tif ] && \
	gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" \
											 -co "BIGTIFF=YES" \
											 -co "TILED=YES" \
											 -te $xmin $ymin $xmax $ymax \
											 -ts $ncols $nrows $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg $outputHucDataDir/flows_grid_boolean.tif
	Tcount
fi


# =================================
# Step 8

## RASTERIZE NHD HEADWATERS (1 & 0) ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then
   
	echo -e $startDiv"Step "$currentStepNumber": Rasterize NHD Headwaters $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/headwaters.tif ] && \
	gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" \
										     -co "BIGTIFF=YES" \
											 -co "TILED=YES" \
											 -te $xmin $ymin $xmax $ymax \
											 -ts $ncols $nrows $outputHucDataDir/nhd_headwater_points_subset.gpkg $outputHucDataDir/headwaters.tif
	Tcount
fi


# =================================
# Step 9

## Raster NWM Catchments ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then
	   
	if [ "$extent" = "FR" ]; then
		# RASTERIZE NWM CATCHMENTS ##
		echo -e $startDiv"Step "$currentStepNumber": Raster NWM Catchments $hucNumber"$stopDiv
		date -u
		Tstart
		[ ! -f $outputHucDataDir/nwm_catchments_proj_subset.tif ] && \
		gdal_rasterize -ot Int32 -a ID -a_nodata 0 -init 0 -co "COMPRESS=LZW" \
														   -co "BIGTIFF=YES" \
														   -co "TILED=YES" \
														   -te $xmin $ymin $xmax $ymax \
														   -ts $ncols $nrows $outputHucDataDir/nwm_catchments_proj_subset.gpkg $outputHucDataDir/nwm_catchments_proj_subset.tif
		Tcount
	else
		echo -e $startDiv"Step "$currentStepNumber": Raster NWM Catchments $hucNumber - Skipped"$stopDiv
	fi
fi


# =================================
# Step 10

## BURN LEVEES INTO DEM ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber"$stopDiv
	date -u
	Tstart
	[ -f $outputHucDataDir/nld_rasterized_elev.tif ] && \
	python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $outputHucDataDir/dem_meters.tif \
														 -nld $outputHucDataDir/nld_rasterized_elev.tif \
														 -out $outputHucDataDir/dem_meters.tif
	Tcount
fi


# =================================
# Step 11

## DEM Reconditioning ##

# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Creating AGREE DEM using $agree_DEM_buffer meter buffer"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/dem_burned.tif ] && \
	python3 -m memory_profiler $srcDir/agreedem.py -r $outputHucDataDir/flows_grid_boolean.tif \
											       -d $outputHucDataDir/dem_meters.tif \
												   -w $outputHucDataDir \
												   -g $outputHucDataDir/temp_work \
												   -o $outputHucDataDir/dem_burned.tif \
												   -b $agree_DEM_buffer \
												   -sm 10 \
												   -sh 1000
	Tcount
fi


# =================================
# Step 12

## PIT REMOVE BURNED DEM ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Pit remove Burned DEM $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/dem_burned_filled.tif ] && \
	rd_depression_filling $outputHucDataDir/dem_burned.tif $outputHucDataDir/dem_burned_filled.tif
	Tcount
fi


# =================================
# Step 13

## D8 FLOW DIR ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": D8 Flow Directions on Burned DEM $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/flowdir_d8_burned_filled.tif ] && \
	mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputHucDataDir/dem_burned_filled.tif \
												-p $outputHucDataDir/flowdir_d8_burned_filled.tif
	Tcount
fi

## DINF FLOW DIR ##
# echo -e $startDiv"DINF on Filled Thalweg Conditioned DEM"$stopDiv
# date -u
# Tstart
# [ ! -f $outputHucDataDir/flowdir_dinf_thalwegCond.tif] && \
# mpiexec -n $ncores_fd $taudemDir2/dinfflowdir -fel $outputHucDataDir/dem_thalwegCond_filled.tif -ang $outputHucDataDir/flowdir_dinf_thalwegCond.tif -slp $outputHucDataDir/slopes_dinf.tif
# Tcount


# =================================
# Step 14

## D8 FLOW ACCUMULATIONS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": D8 Flow Accumulations $hucNumber"$stopDiv
	date -u
	Tstart
	$taudemDir/aread8 -p $outputHucDataDir/flowdir_d8_burned_filled.tif -ad8  $outputHucDataDir/flowaccum_d8_burned_filled.tif \
																		-wg  $outputHucDataDir/headwaters.tif -nc
	Tcount
fi


# =================================
# Step 15

## THRESHOLD ACCUMULATIONS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Threshold Accumulations $hucNumber"$stopDiv
	date -u
	Tstart
	$taudemDir/threshold -ssa $outputHucDataDir/flowaccum_d8_burned_filled.tif -src  $outputHucDataDir/demDerived_streamPixels.tif -thresh 1
	Tcount
fi


# =================================
# Step 16

## PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Preprocessing for lateral thalweg adjustment $hucNumber"$stopDiv
	date -u
	Tstart
	python3 -m memory_profiler $srcDir/unique_pixel_and_allocation.py -s $outputHucDataDir/demDerived_streamPixels.tif \
																	  -o $outputHucDataDir/demDerived_streamPixels_ids.tif \
																	  -g $outputHucDataDir/temp_grass
	Tcount
fi


# =================================
# Step 17

## ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Performing lateral thalweg adjustment $hucNumber"$stopDiv
	date -u
	Tstart
	python3 -m memory_profiler $srcDir/adjust_thalweg_lateral.py -e $outputHucDataDir/dem_meters.tif \
																 -s $outputHucDataDir/demDerived_streamPixels.tif \
																 -a $outputHucDataDir/demDerived_streamPixels_ids_allo.tif \
																 -d $outputHucDataDir/demDerived_streamPixels_ids_dist.tif \
																 -t 50 \
																 -o $outputHucDataDir/dem_lateral_thalweg_adj.tif \
																 -th $thalweg_lateral_elev_threshold
	Tcount
fi


# =================================
# Step 18

## MASK BURNED DEM FOR STREAMS ONLY ###

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Mask Burned DEM for Thalweg Only $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/flowdir_d8_burned_filled_flows.tif ] && \
	gdal_calc.py --quiet --type=Int32 --overwrite --co "COMPRESS=LZW" \
												  --co "BIGTIFF=YES" \
												  --co "TILED=YES" \
												  -A $outputHucDataDir/flowdir_d8_burned_filled.tif \
												  -B $outputHucDataDir/demDerived_streamPixels.tif \
												  --calc="A/B" \
												  --outfile="$outputHucDataDir/flowdir_d8_burned_filled_flows.tif" \
												  --NoDataValue=0
	Tcount
fi


# =================================
# Step 19

## FLOW CONDITION STREAMS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Flow Condition Thalweg $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/dem_thalwegCond.tif ] && \
	$taudemDir/flowdircond -p $outputHucDataDir/flowdir_d8_burned_filled_flows.tif -z $outputHucDataDir/dem_lateral_thalweg_adj.tif \
																				   -zfdc $outputHucDataDir/dem_thalwegCond.tif
	Tcount
fi


# =================================
# Step 20

## D8 SLOPES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": D8 Slopes from DEM $hucNumber"$stopDiv
	date -u
	Tstart
	mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputHucDataDir/dem_lateral_thalweg_adj.tif -sd8 $outputHucDataDir/slopes_d8_dem_meters.tif
	Tcount
fi


# =================================
# Step 21

## STREAMNET FOR REACHES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Stream Net for Reaches $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/demDerived_reaches.shp ] && \
	$taudemDir/streamnet -p $outputHucDataDir/flowdir_d8_burned_filled.tif -fel $outputHucDataDir/dem_thalwegCond.tif \
																		   -ad8 $outputHucDataDir/flowaccum_d8_burned_filled.tif \
																		   -src $outputHucDataDir/demDerived_streamPixels.tif \
																		   -ord $outputHucDataDir/streamOrder.tif \
																		   -tree $outputHucDataDir/treeFile.txt \
																		   -coord $outputHucDataDir/coordFile.txt \
																		   -w $outputHucDataDir/sn_catchments_reaches.tif \
																		   -net $outputHucDataDir/demDerived_reaches.shp
	Tcount
fi


# =================================
# Step 22

## SPLIT DERIVED REACHES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Split Derived Reaches $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/demDerived_reaches_split.gpkg ] && \
	python3 -m memory_profiler $srcDir/split_flows.py -f $outputHucDataDir/demDerived_reaches.shp \
													  -d $outputHucDataDir/dem_thalwegCond.tif \
													  -s $outputHucDataDir/demDerived_reaches_split.gpkg \
													  -p $outputHucDataDir/demDerived_reaches_split_points.gpkg \
													  -w $outputHucDataDir/wbd8_clp.gpkg \
													  -l $outputHucDataDir/nwm_lakes_proj_subset.gpkg
	Tcount

	if [[ ! -f $outputHucDataDir/demDerived_reaches_split.gpkg ]] ; then
		echo "No AHPs point(s) within HUC $hucNumber boundaries. Aborting run_by_unit.sh"
		rm -rf $outputHucDataDir
		exit 56
	fi
fi


# =================================
# Step 23

## MASK RASTERS BY MS BUFFER (if applicable)##
	  
let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	if [ "$extent" = "MS" ]; then

		echo -e $startDiv"Step "$currentStepNumber": Mask Rasters with Stream Buffer (Extent=MS) $hucNumber"$stopDiv
		date -u
		Tstart
		python3 -m memory_profiler $srcDir/fr_to_ms_raster_mask.py -s $outputHucDataDir/demDerived_reaches_split.gpkg \
																   -f $outputHucDataDir/flowdir_d8_burned_filled.tif \
																   -d $outputHucDataDir/dem_thalwegCond.tif \
																   -r $outputHucDataDir/slopes_d8_dem_meters.tif \
																   -m $outputHucDataDir/flowdir_d8_MS.tif \
																   -n $outputHucDataDir/dem_thalwegCond_MS.tif \
																   -o $outputHucDataDir/slopes_d8_dem_metersMS.tif \
																   -p $outputHucDataDir/demDerived_streamPixels.tif \
																   -q $outputHucDataDir/demDerived_streamPixelsMS.tif
		Tcount

		if [[ ! -f $outputHucDataDir/dem_thalwegCond_MS.tif ]] ; then
			echo "No AHPs point(s) within HUC $hucNumber boundaries. Aborting run_by_unit.sh"
			rm -rf $outputHucDataDir
			exit 57
		fi

		dem_thalwegCond=$outputHucDataDir/dem_thalwegCond_MS.tif
		slopes_d8_dem_meters=$outputHucDataDir/slopes_d8_dem_metersMS.tif
		flowdir_d8_burned_filled=$outputHucDataDir/flowdir_d8_MS.tif
		demDerived_streamPixels=$outputHucDataDir/demDerived_streamPixelsMS.tif
	else
		echo -e $startDiv"Step "$currentStepNumber": Mask Rasters with Stream Buffer (Extent=FS) - no processing performed, just setting values"$stopDiv
		dem_thalwegCond=$outputHucDataDir/dem_thalwegCond.tif
		slopes_d8_dem_meters=$outputHucDataDir/slopes_d8_dem_meters.tif
		flowdir_d8_burned_filled=$outputHucDataDir/flowdir_d8_burned_filled.tif
		demDerived_streamPixels=$outputHucDataDir/demDerived_streamPixels.tif
	fi
fi


# =================================
# Step 24

## GAGE WATERSHED FOR REACHES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Gage Watershed for Reaches $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_reaches.tif ] && \
	mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $flowdir_d8_burned_filled \
												   -gw $outputHucDataDir/gw_catchments_reaches.tif \
												   -o $outputHucDataDir/demDerived_reaches_split_points.gpkg \
												   -id $outputHucDataDir/idFile.txt
	Tcount
fi


# =================================
# Step 25

## VECTORIZE FEATURE ID CENTROIDS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Vectorize Pixel Centroids $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/flows_points_pixels.gpkg ] && \
	python3 -m memory_profiler $srcDir/reachID_grid_to_vector_points.py -r $demDerived_streamPixels \
																		-i featureID \
																		-p $outputHucDataDir/flows_points_pixels.gpkg
	Tcount
fi


# =================================
# Step 26

## GAGE WATERSHED FOR PIXELS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Gage Watershed for Pixels $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_pixels.tif ] && \
	mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $flowdir_d8_burned_filled \
												   -gw $outputHucDataDir/gw_catchments_pixels.tif \
												   -o $outputHucDataDir/flows_points_pixels.gpkg \
												   -id $outputHucDataDir/idFile.txt
	Tcount
fi


# =================================
# Step 27

## D8 REM ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": D8 REM $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/rem.tif ] && \
	python3 -m memory_profiler $srcDir/rem.py -d $dem_thalwegCond \
											  -w $outputHucDataDir/gw_catchments_pixels.tif \
											  -o $outputHucDataDir/rem.tif \
											  -t $demDerived_streamPixels \
											  -i $outputHucDataDir/gw_catchments_reaches.tif \
											  -s $outputHucDataDir/demDerived_reaches_split.gpkg
	Tcount
fi

## DINF DISTANCE DOWN ##
# echo -e $startDiv"DINF Distance Down on Filled Thalweg Conditioned DEM $hucNumber"$stopDiv
# date -u
# Tstart
# [ ! -f $outputHucDataDir/flowdir_dinf_thalwegCond.tif] && \
# mpiexec -n $ncores_fd $taudemDir/dinfdistdown -ang $outputHucDataDir/flowdir_dinf_thalwegCond.tif -fel $outputHucDataDir/dem_thalwegCond_filled.tif -src $demDerived_streamPixels -dd $outputHucDataDir/rem.tif -m ave h
# Tcount


# =================================
# Step 28

## BRING DISTANCE DOWN TO ZERO ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Zero out negative values in distance down grid $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/rem_zeroed.tif ] && \
	gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" \
													--co "BIGTIFF=YES" \
													--co "TILED=YES" \
													-A $outputHucDataDir/rem.tif \
													--calc="(A*(A>=0))" \
													--NoDataValue=$ndv \
													--outfile=$outputHucDataDir/"rem_zeroed.tif"
	Tcount
fi


# =================================
# Step 29

## POLYGONIZE REACH WATERSHEDS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Polygonize Reach Watersheds $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_reaches.gpkg ] && \
	gdal_polygonize.py -8 -f GPKG $outputHucDataDir/gw_catchments_reaches.tif $outputHucDataDir/gw_catchments_reaches.gpkg catchments HydroID
	Tcount
fi


# =================================
# Step 30

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Process catchments and model streams step 1 $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg ] && \
	python3 -m memory_profiler $srcDir/filter_catchments_and_add_attributes.py -i $outputHucDataDir/gw_catchments_reaches.gpkg \
																			   -f $outputHucDataDir/demDerived_reaches_split.gpkg \
																			   -c $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg \
																			   -o $outputHucDataDir/demDerived_reaches_split_filtered.gpkg \
																			   -w $outputHucDataDir/wbd8_clp.gpkg \
																			   -u $hucNumber

	if [[ ! -f $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg ]] ; then
		echo "No relevant streams within HUC $hucNumber boundaries. Aborting run_by_unit.sh"
		rm -rf $outputHucDataDir
		exit 65
	fi
	Tcount
fi


# =================================
# Step 31

## GET RASTER METADATA ## *****

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Get Clipped Raster Metadata $hucNumber"$stopDiv
	date -u
	Tstart
	read fsize ncols nrows ndv_clipped xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputHucDataDir/gw_catchments_reaches.tif)
	Tcount
fi


# =================================
# Step 32

## RASTERIZE NEW CATCHMENTS AGAIN ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Rasterize filtered catchments $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.tif ] && \
	gdal_rasterize -ot Int32 -a HydroID -a_nodata 0 -init 0 -co "COMPRESS=LZW" \
															-co "BIGTIFF=YES" \
															-co "TILED=YES" \
															-te $xmin $ymin $xmax $ymax \
															-ts $ncols $nrows $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg \
															$outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.tif
	Tcount
fi


# =================================
# Step 33

## RASTERIZE LANDSEA (OCEAN AREA) POLYGON (IF APPLICABLE) ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Rasterize filtered/dissolved ocean/Glake polygon $hucNumber"$stopDiv
	date -u
	Tstart
	[ -f $outputHucDataDir/LandSea_subset.gpkg ] && [ ! -f $outputHucDataDir/LandSea_subset.tif ] && \
	gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" \
															   -co "BIGTIFF=YES" \
															   -co "TILED=YES" \
															   -te $xmin $ymin $xmax $ymax \
															   -ts $ncols \
															   $nrows \
															   $outputHucDataDir/LandSea_subset.gpkg \
															   $outputHucDataDir/LandSea_subset.tif
	Tcount
fi


# =================================
# Step 34

## MASK SLOPE RASTER ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Masking Slope Raster to HUC $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/slopes_d8_dem_meters_masked.tif ] && \
	gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" \
													--co "BIGTIFF=YES" \
													--co "TILED=YES" \
													-A $slopes_d8_dem_meters \
													-B $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.tif \
													--calc="(A*(B>0))+((B<=0)*-1)" \
													--NoDataValue=-1 \
													--outfile=$outputHucDataDir/"slopes_d8_dem_meters_masked.tif"
	Tcount
fi


# =================================
# Step 35

## MASK REM RASTER ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Masking REM Raster to HUC $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/rem_zeroed_masked.tif ] && \
	gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" \
													--co "BIGTIFF=YES" \
													--co "TILED=YES" \
													-A $outputHucDataDir/rem_zeroed.tif \
													-B $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.tif \
													--calc="(A*(B>0))" \
													--NoDataValue=$ndv \
													--outfile=$outputHucDataDir/"rem_zeroed_masked.tif"
	Tcount
fi


# =================================
# Step 36

## MASK REM RASTER TO REMOVE OCEAN AREAS ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Additional masking to REM raster to remove ocean/Glake areas in HUC $hucNumber"$stopDiv
	date -u
	Tstart
	[ -f $outputHucDataDir/LandSea_subset.tif ] && \
	gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" \
													--co "BIGTIFF=YES" \
													--co "TILED=YES" \
													-A $outputHucDataDir/rem_zeroed_masked.tif \
													-B $outputHucDataDir/LandSea_subset.tif \
													--calc="(A*B)" \
													--NoDataValue=$ndv \
													--outfile=$outputHucDataDir/"rem_zeroed_masked.tif"
	Tcount
fi


# =================================
# Step 37

## MAKE CATCHMENT AND STAGE FILES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Generate Catchment List and Stage List Files $hucNumber"$stopDiv
	date -u
	Tstart
	python3 -m memory_profiler $srcDir/make_stages_and_catchlist.py -f $outputHucDataDir/demDerived_reaches_split_filtered.gpkg \
																	-c $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg \
																	-s $outputHucDataDir/stage.txt \
																	-a $outputHucDataDir/catchment_list.txt \
																	-m $stage_min_meters \
																	-i $stage_interval_meters \
																	-t $stage_max_meters
	Tcount
fi


# =================================
# Step 38

## HYDRAULIC PROPERTIES ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Hydraulic Properties $hucNumber"$stopDiv
	date -u
	Tstart
	[ ! -f $outputHucDataDir/src_base.csv ] && \
	$taudemDir/catchhydrogeo -hand $outputHucDataDir/rem_zeroed_masked.tif \
							 -catch $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.tif \
							 -catchlist $outputHucDataDir/catchment_list.txt \
							 -slp $outputHucDataDir/slopes_d8_dem_meters_masked.tif \
							 -h $outputHucDataDir/stage.txt \
							 -table $outputHucDataDir/src_base.csv
	Tcount
fi


# =================================
# Step 39

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Finalize catchments and model streams $hucNumber"$stopDiv output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName,
	date -u
	Tstart
	[ ! -f $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg ] && \
	echo python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg \
															 -a $outputHucDataDir/demDerived_reaches_split_filtered.gpkg \
															 -s $outputHucDataDir/src_base.csv \
															 -u $input_bathy_bankfull \
															 -v $outputHucDataDir/bathy_crosswalk_calcs.csv \
															 -e $outputHucDataDir/bathy_stream_order_calcs.csv \
															 -g $outputHucDataDir/bathy_thalweg_flag.csv \
															 -i $outputHucDataDir/bathy_xs_area_hydroid_lookup.csv \
															 -l $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg \
															 -f $outputHucDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg \
															 -r $outputHucDataDir/src_full_crosswalked.csv -j $outputHucDataDir/src.json \
															 -x $outputHucDataDir/crosswalk_table.csv \
															 -t $outputHucDataDir/hydroTable.csv \
															 -w $outputHucDataDir/wbd8_clp.gpkg \
															 -b $outputHucDataDir/nwm_subset_streams.gpkg \
															 -y $outputHucDataDir/nwm_catchments_proj_subset.tif \
															 -m $manning_n \
															 -z $input_nwm_catchments \
															 -p $extent \
															 -k $outputHucDataDir/small_segments.csv
															 
	python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg \
														-a $outputHucDataDir/demDerived_reaches_split_filtered.gpkg \
														-s $outputHucDataDir/src_base.csv \
														-u $input_bathy_bankfull \
														-v $outputHucDataDir/bathy_crosswalk_calcs.csv \
														-e $outputHucDataDir/bathy_stream_order_calcs.csv \
														-g $outputHucDataDir/bathy_thalweg_flag.csv \
														-i $outputHucDataDir/bathy_xs_area_hydroid_lookup.csv \
														-l $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg \
														-f $outputHucDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg \
														-r $outputHucDataDir/src_full_crosswalked.csv \
														-j $outputHucDataDir/src.json \
														-x $outputHucDataDir/crosswalk_table.csv \
														-t $outputHucDataDir/hydroTable.csv \
														-w $outputHucDataDir/wbd8_clp.gpkg \
														-b $outputHucDataDir/nwm_subset_streams.gpkg \
														-y $outputHucDataDir/nwm_catchments_proj_subset.tif \
														-m $manning_n \
														-z $input_nwm_catchments \
														-p $extent \
														-k $outputHucDataDir/small_segments.csv
	Tcount
fi


# =================================
# Step 40

## USGS CROSSWALK ##

let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": USGS Crosswalk $hucNumber"$stopDiv
	date -u
	Tstart
	python3 -m memory_profiler $srcDir/usgs_gage_crosswalk.py -gages $inputDataDir/usgs_gages/usgs_gages.gpkg \
															  -dem $outputHucDataDir/dem_meters.tif \
															  -flows $outputHucDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg \
															  -cat $outputHucDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg \
															  -wbd $outputHucDataDir/wbd_buffered.gpkg \
															  -dem_adj $dem_thalwegCond \
															  -outtable $outputHucDataDir/usgs_elev_table.csv \
															  -e $extent
	Tcount
fi


# =================================
# Step 41

## CLEANUP OUTPUTS ##
let "currentStepNumber+=1"
if [ $currentStepNumber -ge $step_start_number ] &&
   [ $currentStepNumber -le $step_end_number ]; then

	echo -e $startDiv"Step "$currentStepNumber": Cleaning up outputs $hucNumber"$stopDiv
	args=()
	[[ ! -z "$whitelist" ]] && args+=( "-w$whitelist" )
	(( production == 1 )) && args+=( '-p' )
	(( viz == 1 )) && args+=( '-v' )
	date -u
	Tstart
	python3 -m memory_profiler $srcDir/output_cleanup.py $hucNumber $outputHucDataDir "${args[@]}"
	Tcount
fi
