#!/bin/bash -e

level=$1
if [ "$level" = "unit" ]; then
    file_suffix=""
elif [ "$level" = "branch" ]; then
    file_suffix="_$current_branch_id"
fi

T_total_start

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/aread8 -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled$file_suffix.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled$file_suffix.tif -wg $outputCurrentBranchDataDir/headwaters$file_suffix.tif -nc
Tcount

# THRESHOLD ACCUMULATIONS ##
echo -e $startDiv"Threshold Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/threshold -ssa $outputCurrentBranchDataDir/flowaccum_d8_burned_filled$file_suffix.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif -thresh 1
Tcount

## PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ###
echo -e $startDiv"Preprocessing for lateral thalweg adjustment $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/unique_pixel_and_allocation.py -s $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif -o $outputCurrentBranchDataDir/demDerived_streamPixels_ids$file_suffix.tif -g $outputCurrentBranchDataDir/temp_grass
Tcount

## ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ##
echo -e $startDiv"Performing lateral thalweg adjustment $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/adjust_thalweg_lateral.py -e $outputCurrentBranchDataDir/dem_meters$file_suffix.tif -s $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif -a $outputCurrentBranchDataDir/demDerived_streamPixels_ids"$file_suffix"_allo.tif -d $outputCurrentBranchDataDir/demDerived_streamPixels_ids"$file_suffix"_dist.tif -t 50 -o $outputCurrentBranchDataDir/dem_lateral_thalweg_adj$file_suffix.tif -th $thalweg_lateral_elev_threshold
Tcount

## MASK BURNED DEM FOR STREAMS ONLY ###
echo -e $startDiv"Mask Burned DEM for Thalweg Only $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_calc.py --quiet --type=Int32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/flowdir_d8_burned_filled$file_suffix.tif -B $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif --calc="A/B" --outfile="$outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows$file_suffix.tif" --NoDataValue=0
Tcount

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/flowdircond -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows$file_suffix.tif -z $outputCurrentBranchDataDir/dem_lateral_thalweg_adj$file_suffix.tif -zfdc $outputCurrentBranchDataDir/dem_thalwegCond$file_suffix.tif
Tcount

## D8 SLOPES ##
echo -e $startDiv"D8 Slopes from DEM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_lateral_thalweg_adj$file_suffix.tif -sd8 $outputCurrentBranchDataDir/slopes_d8_dem_meters$file_suffix.tif
Tcount

# STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/streamnet -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled$file_suffix.tif -fel $outputCurrentBranchDataDir/dem_thalwegCond$file_suffix.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled$file_suffix.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif -ord $outputCurrentBranchDataDir/streamOrder$file_suffix.tif -tree $outputCurrentBranchDataDir/treeFile$file_suffix.txt -coord $outputCurrentBranchDataDir/coordFile$file_suffix.txt -w $outputCurrentBranchDataDir/sn_catchments_reaches$file_suffix.tif -net $outputCurrentBranchDataDir/demDerived_reaches$file_suffix.shp
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/split_flows.py -f $outputCurrentBranchDataDir/demDerived_reaches$file_suffix.shp -d $outputCurrentBranchDataDir/dem_thalwegCond$file_suffix.tif -s $outputCurrentBranchDataDir/demDerived_reaches_split$file_suffix.gpkg -p $outputCurrentBranchDataDir/demDerived_reaches_split_points$file_suffix.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -l $outputHucDataDir/nwm_lakes_proj_subset.gpkg -ds $dropLowStreamOrders
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled$file_suffix.tif -gw $outputCurrentBranchDataDir/gw_catchments_reaches$file_suffix.tif -o $outputCurrentBranchDataDir/demDerived_reaches_split_points$file_suffix.gpkg -id $outputCurrentBranchDataDir/idFile$file_suffix.txt
Tcount

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/reachID_grid_to_vector_points.py -r $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif -i featureID -p $outputCurrentBranchDataDir/flows_points_pixels$file_suffix.gpkg
Tcount

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled"$file_suffix".tif -gw $outputCurrentBranchDataDir/gw_catchments_pixels$file_suffix.tif -o $outputCurrentBranchDataDir/flows_points_pixels$file_suffix.gpkg -id $outputCurrentBranchDataDir/idFile$file_suffix.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/gms/rem.py -d $outputCurrentBranchDataDir/dem_thalwegCond"$file_suffix".tif -w $outputCurrentBranchDataDir/gw_catchments_pixels$file_suffix.tif -o $outputCurrentBranchDataDir/rem$file_suffix.tif -t $outputCurrentBranchDataDir/demDerived_streamPixels$file_suffix.tif
Tcount

## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS##
echo -e $startDiv"Bring negative values in REM to zero and mask to catchments $hucNumber $current_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem$file_suffix.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches$file_suffix.tif --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked$file_suffix.tif"
Tcount

## RASTERIZE LANDSEA (OCEAN AREA) POLYGON (IF APPLICABLE) ##
if [ -f $outputHucDataDir/LandSea_subset.gpkg ]; then
    echo -e $startDiv"Rasterize filtered/dissolved ocean/Glake polygon $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart

    gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/LandSea_subset.gpkg $outputCurrentBranchDataDir/LandSea_subset$file_suffix.tif
    Tcount
fi

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_polygonize.py -8 -f GPKG $outputCurrentBranchDataDir/gw_catchments_reaches$file_suffix.tif $outputCurrentBranchDataDir/gw_catchments_reaches$file_suffix.gpkg catchments HydroID
Tcount

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##
echo -e $startDiv"Process catchments and model streams $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/filter_catchments_and_add_attributes.py -i $outputCurrentBranchDataDir/gw_catchments_reaches$file_suffix.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split$file_suffix.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.gpkg -o $outputCurrentBranchDataDir/demDerived_reaches_split_filtered$file_suffix.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -u $hucNumber -s $dropLowStreamOrders
Tcount

## RASTERIZE NEW CATCHMENTS AGAIN ##
echo -e $startDiv"Rasterize filtered catchments $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -a HydroID -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.gpkg $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.tif
Tcount

## MASK SLOPE TO CATCHMENTS ##
echo -e $startDiv"Mask to slopes to catchments $hucNumber $current_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/slopes_d8_dem_meters$file_suffix.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.tif --calc="A*(B>0)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/slopes_d8_dem_meters_masked$file_suffix.tif
Tcount

## MAKE CATCHMENT AND STAGE FILES ##
echo -e $startDiv"Generate Catchment List and Stage List Files $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/make_stages_and_catchlist.py -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered$file_suffix.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.gpkg -s $outputCurrentBranchDataDir/stage$file_suffix.txt -a $outputCurrentBranchDataDir/catch_list$file_suffix.txt -m $stage_min_meters -i $stage_interval_meters -t $stage_max_meters
Tcount

## MASK REM RASTER TO REMOVE OCEAN AREAS ##
echo -e $startDiv"Additional masking to REM raster to remove ocean/Glake areas $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
[ -f $outputCurrentBranchDataDir/LandSea_subset.tif ] && \
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_zeroed_masked$file_suffix.tif -B $outputCurrentBranchDataDir/LandSea_subset_$current_node_id.tif --calc="(A*B)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked$file_suffix.tif"
Tcount

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/catchhydrogeo -hand $outputCurrentBranchDataDir/rem_zeroed_masked$file_suffix.tif -catch $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.tif -catchlist $outputCurrentBranchDataDir/catch_list$file_suffix.txt -slp $outputCurrentBranchDataDir/slopes_d8_dem_meters_masked$file_suffix.tif -h $outputCurrentBranchDataDir/stage$file_suffix.txt -table $outputCurrentBranchDataDir/src_base$file_suffix.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
if [ "$level" = "branch" ]; then
    python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes$file_suffix.gpkg -a $outputCurrentBranchDataDir/demDerived_reaches_split_filtered$file_suffix.gpkg -s $outputCurrentBranchDataDir/src_base$file_suffix.csv -l $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked$file_suffix.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked$file_suffix.gpkg -r $outputCurrentBranchDataDir/src_full_crosswalked$file_suffix.csv -j $outputCurrentBranchDataDir/src$file_suffix.json -x $outputCurrentBranchDataDir/crosswalk_table$file_suffix.csv -t $outputCurrentBranchDataDir/hydroTable$file_suffix.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths$file_suffix.gpkg -y $outputCurrentBranchDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths$file_suffix.gpkg -p $extent -k $outputCurrentBranchDataDir/small_segments$file_suffix.csv
elif [ "$level" = "unit" ]; then
    # Branch zero has a different source for -b and -z arguments
    python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $outputCurrentBranchDataDir/demDerived_reaches_split_filtered.gpkg -s $outputCurrentBranchDataDir/src_base.csv -l $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $outputCurrentBranchDataDir/src_full_crosswalked.csv -j $outputCurrentBranchDataDir/src.json -x $outputCurrentBranchDataDir/crosswalk_table.csv -t $outputCurrentBranchDataDir/hydroTable.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -y $outputCurrentBranchDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg -p $extent -k $outputCurrentBranchDataDir/small_segments.csv
fi
Tcount
