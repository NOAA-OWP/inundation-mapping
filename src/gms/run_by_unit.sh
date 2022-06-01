#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SET OUTPUT DIRECTORY FOR UNIT ##
hucNumber="$1"
outputHucDataDir=$outputRunDataDir/$hucNumber
outputBranchDataDir=$outputHucDataDir/branches


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
python3 -m memory_profiler $srcDir/clip_vectors_to_wbd.py -d $hucNumber -w $input_nwm_flows -s $input_nhd_flowlines -l $input_nwm_lakes -r $input_NLD -g $outputHucDataDir/wbd.gpkg -f $outputHucDataDir/wbd_buffered.gpkg -m $input_nwm_catchments -y $input_nhd_headwaters -v $input_LANDSEA -c $outputHucDataDir/NHDPlusBurnLineEvent_subset.gpkg -z $outputHucDataDir/nld_subset_levees.gpkg -a $outputHucDataDir/nwm_lakes_proj_subset.gpkg -n $outputHucDataDir/nwm_catchments_proj_subset.gpkg -e $outputHucDataDir/nhd_headwater_points_subset.gpkg -b $outputHucDataDir/nwm_subset_streams.gpkg -x $outputHucDataDir/LandSea_subset.gpkg -extent $extent -gl $input_GL_boundaries -lb $lakes_buffer_dist_meters -wb $wbd_buffer
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
$srcDir/gms/derive_level_paths.py -i $outputHucDataDir/nwm_subset_streams.gpkg -b $branch_id_attribute -r "ID" -o $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -e $outputHucDataDir/nwm_headwaters.gpkg -c $outputHucDataDir/nwm_catchments_proj_subset.gpkg -t $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -n $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg -v -s $dropLowStreamOrders

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
$srcDir/gms/generate_branch_list.py -o $outputHucDataDir/branch_id.lst -d $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved.gpkg -b $branch_id_attribute
Tcount

## CREATE BRANCH ZERO ##
echo -e $startDiv"Creating branch zero for $hucNumber"$stopDiv
outputBranchDataDir=$outputHucDataDir/branches
outputCurrentBranchDataDir=$outputBranchDataDir/$zero_branch_id

# set input files
input_DEM=$inputDataDir/nhdplus_rasters/HRNHDPlusRasters"$huc4Identifier"/elev_m.tif
input_NLD=$inputDataDir/nld_vectors/huc2_levee_lines/nld_preprocessed_"$huc2Identifier".gpkg
input_bathy_bankfull=$inputDataDir/$bankfull_input_table
input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg

## OVERWRITE
if [ -d "$outputCurrentBranchDataDir" ];then
    if [ $overwrite -eq 1 ]; then
        rm -rf $outputCurrentBranchDataDir
    else
        echo "GMS branch data directories for $hucNumber - $zero_branch_id already exist. Use -o/--overwrite to continue"
        exit 1
    fi
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $outputCurrentBranchDataDir

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
[ ! -f $outputCurrentBranchDataDir/dem_meters.tif ] && \
gdalwarp -cutline $outputHucDataDir/wbd_buffered.gpkg -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" $input_DEM $outputCurrentBranchDataDir/dem_meters.tif
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputCurrentBranchDataDir/dem_meters.tif)

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputHucDataDir/nld_subset_levees.gpkg ] && \
gdal_rasterize -l nld_subset_levees -3d -at -a_nodata $ndv -te $xmin $ymin $xmax $ymax -ts $ncols $nrows -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" $outputHucDataDir/nld_subset_levees.gpkg $outputCurrentBranchDataDir/nld_subset_levees.tif
Tcount

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputCurrentBranchDataDir/nld_subset_levees.tif ] && \
python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $outputCurrentBranchDataDir/dem_meters.tif -nld $outputCurrentBranchDataDir/nld_subset_levees.tif -out $outputCurrentBranchDataDir/dem_meters.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nwm_subset_streams.gpkg $outputCurrentBranchDataDir/flows_grid_boolean.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/nhd_headwater_points_subset.gpkg $outputCurrentBranchDataDir/headwaters.tif
Tcount

## DEM Reconditioning ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $outputCurrentBranchDataDir/flows_grid_boolean.tif -d $outputCurrentBranchDataDir/dem_meters.tif -w $outputCurrentBranchDataDir -g $outputCurrentBranchDataDir/temp_work -o $outputCurrentBranchDataDir/dem_burned.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## PIT REMOVE BURNED DEM ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
rd_depression_filling $outputCurrentBranchDataDir/dem_burned.tif $outputCurrentBranchDataDir/dem_burned_filled.tif
Tcount

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_burned_filled.tif -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif
Tcount

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$taudemDir/aread8 -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -ad8  $outputCurrentBranchDataDir/flowaccum_d8_burned_filled.tif -wg  $outputCurrentBranchDataDir/headwaters.tif -nc
Tcount

# THRESHOLD ACCUMULATIONS ##
echo -e $startDiv"Threshold Accumulations $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$taudemDir/threshold -ssa $outputCurrentBranchDataDir/flowaccum_d8_burned_filled.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels.tif -thresh 1
Tcount

## PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ###
echo -e $startDiv"Preprocessing for lateral thalweg adjustment $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/unique_pixel_and_allocation.py -s $outputCurrentBranchDataDir/demDerived_streamPixels.tif -o $outputCurrentBranchDataDir/demDerived_streamPixels_ids.tif -g $outputCurrentBranchDataDir/temp_grass
Tcount

## ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ##
echo -e $startDiv"Performing lateral thalweg adjustment $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/adjust_thalweg_lateral.py -e $outputCurrentBranchDataDir/dem_meters.tif -s $outputCurrentBranchDataDir/demDerived_streamPixels.tif -a $outputCurrentBranchDataDir/demDerived_streamPixels_ids_allo.tif -d $outputCurrentBranchDataDir/demDerived_streamPixels_ids_dist.tif -t 50 -o $outputCurrentBranchDataDir/dem_lateral_thalweg_adj.tif -th $thalweg_lateral_elev_threshold
Tcount

## MASK BURNED DEM FOR STREAMS ONLY ###
echo -e $startDiv"Mask Burned DEM for Thalweg Only $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
gdal_calc.py --quiet --type=Int32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -B $outputCurrentBranchDataDir/demDerived_streamPixels.tif --calc="A/B" --outfile="$outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows.tif" --NoDataValue=0
Tcount

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$taudemDir/flowdircond -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows.tif -z $outputCurrentBranchDataDir/dem_lateral_thalweg_adj.tif -zfdc $outputCurrentBranchDataDir/dem_thalwegCond.tif
Tcount

## D8 SLOPES ##
echo -e $startDiv"D8 Slopes from DEM $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_lateral_thalweg_adj.tif -sd8 $outputCurrentBranchDataDir/slopes_d8_dem_meters.tif
Tcount

# STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$taudemDir/streamnet -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -fel $outputCurrentBranchDataDir/dem_thalwegCond.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels.tif -ord $outputCurrentBranchDataDir/streamOrder.tif -tree $outputCurrentBranchDataDir/treeFile.txt -coord $outputCurrentBranchDataDir/coordFile.txt -w $outputCurrentBranchDataDir/sn_catchments_reaches.tif -net $outputCurrentBranchDataDir/demDerived_reaches.shp
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$srcDir/split_flows.py -f $outputCurrentBranchDataDir/demDerived_reaches.shp -d $outputCurrentBranchDataDir/dem_thalwegCond.tif -s $outputCurrentBranchDataDir/demDerived_reaches_split.gpkg -p $outputCurrentBranchDataDir/demDerived_reaches_split_points.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -l $outputHucDataDir/nwm_lakes_proj_subset.gpkg -ds $dropLowStreamOrders
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -gw $outputCurrentBranchDataDir/gw_catchments_reaches.tif -o $outputCurrentBranchDataDir/demDerived_reaches_split_points.gpkg -id $outputCurrentBranchDataDir/idFile.txt
Tcount

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$srcDir/reachID_grid_to_vector_points.py -r $outputCurrentBranchDataDir/demDerived_streamPixels.tif -i featureID -p $outputCurrentBranchDataDir/flows_points_pixels.gpkg
Tcount

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled.tif -gw $outputCurrentBranchDataDir/gw_catchments_pixels.tif -o $outputCurrentBranchDataDir/flows_points_pixels.gpkg -id $outputCurrentBranchDataDir/idFile.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$srcDir/gms/rem.py -d $outputCurrentBranchDataDir/dem_thalwegCond.tif -w $outputCurrentBranchDataDir/gw_catchments_pixels.tif -o $outputCurrentBranchDataDir/rem.tif -t $outputCurrentBranchDataDir/demDerived_streamPixels.tif
Tcount

## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS ##
echo -e $startDiv"Bring negative values in REM to zero and mask to catchments $hucNumber $zero_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches.tif --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked.tif"
Tcount

## RASTERIZE LANDSEA (OCEAN AREA) POLYGON (IF APPLICABLE) ##
if [ -f $outputHucDataDir/LandSea_subset.gpkg ]; then
    echo -e $startDiv"Rasterize filtered/dissolved ocean/lake polygon $hucNumber $zero_branch_id"$stopDiv
    date -u
    Tstart

    gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/LandSea_subset.gpkg $outputCurrentBranchDataDir/LandSea_subset.tif
    Tcount
fi

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
gdal_polygonize.py -8 -f GPKG $outputCurrentBranchDataDir/gw_catchments_reaches.tif $outputCurrentBranchDataDir/gw_catchments_reaches.gpkg catchments HydroID
Tcount

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##
echo -e $startDiv"Process catchments and model streams $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/filter_catchments_and_add_attributes.py -i $outputCurrentBranchDataDir/gw_catchments_reaches.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg -o $outputCurrentBranchDataDir/demDerived_reaches_split_filtered.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -u $hucNumber -s $dropLowStreamOrders


## RASTERIZE NEW CATCHMENTS AGAIN ##
echo -e $startDiv"Rasterize filtered catchments $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -a HydroID -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.tif
Tcount

## MASK SLOPE TO CATCHMENTS ##
echo -e $startDiv"Mask to slopes to catchments $hucNumber $zero_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/slopes_d8_dem_meters.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.tif --calc="A*(B>0)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/slopes_d8_dem_meters_masked.tif
Tcount

## MAKE CATCHMENT AND STAGE FILES ##
echo -e $startDiv"Generate Catchment List and Stage List Files $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$srcDir/make_stages_and_catchlist.py -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg -s $outputCurrentBranchDataDir/stage.txt -a $outputCurrentBranchDataDir/catch_list.txt -m $stage_min_meters -i $stage_interval_meters -t $stage_max_meters
Tcount

## MASK REM RASTER TO REMOVE OCEAN AREAS ##
echo -e $startDiv"Additional masking to REM raster to remove ocean/lake areas $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
[ -f $outputCurrentBranchDataDir/LandSea_subset.tif ] && \
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_zeroed_masked.tif -B $outputCurrentBranchDataDir/LandSea_subset_$current_node_id.tif --calc="(A*B)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked.tif"
Tcount

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
$taudemDir/catchhydrogeo -hand $outputCurrentBranchDataDir/rem_zeroed_masked.tif -catch $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.tif -catchlist $outputCurrentBranchDataDir/catch_list.txt -slp $outputCurrentBranchDataDir/slopes_d8_dem_meters_masked.tif -h $outputCurrentBranchDataDir/stage.txt -table $outputCurrentBranchDataDir/src_base.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams $hucNumber $zero_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $outputCurrentBranchDataDir/demDerived_reaches_split_filtered.gpkg -s $outputCurrentBranchDataDir/src_base.csv -l $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $outputCurrentBranchDataDir/src_full_crosswalked.csv -j $outputCurrentBranchDataDir/src.json -x $outputCurrentBranchDataDir/crosswalk_table.csv -t $outputCurrentBranchDataDir/hydroTable.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -y $outputCurrentBranchDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -p $extent -k $outputCurrentBranchDataDir/small_segments.csv
Tcount

## CLEANUP BRANCH ZERO OUTPUTS ##
echo -e $startDiv"Cleaning up outputs in zero branch $hucNumber"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/output_cleanup.py $hucNumber $outputCurrentBranchDataDir -v -w dem_meters.tif slopes_d8_dem_meters.tif 
Tcount

## CREATE USGS GAGES FILE
echo -e $startDiv"Assigning USGS gages to branches for $hucNumber"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/usgs_gage_unit_setup.py -gages $inputDataDir/usgs_gages/usgs_gages.gpkg -nwm $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -o $outputHucDataDir/usgs_subset_gages.gpkg -huc $hucNumber -ahps $inputDataDir/ahps_sites/nws_lid.gpkg
Tcount

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_gms_unit_list ]; then
    echo -e $startDiv"Remove files $hucNumber"$stopDiv
    date -u
    Tstart
    $srcDir/gms/outputs_cleanup.py -d $outputHucDataDir -l $deny_gms_unit_list -v
    Tcount
fi
