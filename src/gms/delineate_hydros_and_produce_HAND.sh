#!/bin/bash -e

## Level is equal to the parent script: 'unit' or 'branch'
level=$1

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/aread8 -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -wg $outputCurrentBranchDataDir/headwaters_$current_branch_id.tif -nc
Tcount

# THRESHOLD ACCUMULATIONS ##
echo -e $startDiv"Threshold Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/threshold -ssa $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -thresh 1
Tcount

## PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ###
echo -e $startDiv"Preprocessing for lateral thalweg adjustment $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/unique_pixel_and_allocation.py -s $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -o $outputCurrentBranchDataDir/demDerived_streamPixels_ids_$current_branch_id.tif -g $outputCurrentBranchDataDir/temp_grass
Tcount

## ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ##
echo -e $startDiv"Performing lateral thalweg adjustment $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/adjust_thalweg_lateral.py -e $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif -s $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -a $outputCurrentBranchDataDir/demDerived_streamPixels_ids_"$current_branch_id"_allo.tif -d $outputCurrentBranchDataDir/demDerived_streamPixels_ids_"$current_branch_id"_dist.tif -t 50 -o $outputCurrentBranchDataDir/dem_lateral_thalweg_adj_$current_branch_id.tif -th $thalweg_lateral_elev_threshold
Tcount

## MASK BURNED DEM FOR STREAMS ONLY ###
echo -e $startDiv"Mask Burned DEM for Thalweg Only $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_calc.py --quiet --type=Int32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -B $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif --calc="A/B" --outfile="$outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows_$current_branch_id.tif" --NoDataValue=0
Tcount

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/flowdircond -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_flows_$current_branch_id.tif -z $outputCurrentBranchDataDir/dem_lateral_thalweg_adj_$current_branch_id.tif -zfdc $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif
Tcount

## D8 SLOPES ##
echo -e $startDiv"D8 Slopes from DEM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_lateral_thalweg_adj_$current_branch_id.tif -sd8 $outputCurrentBranchDataDir/slopes_d8_dem_meters_$current_branch_id.tif
Tcount

## STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/streamnet -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -fel $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -ord $outputCurrentBranchDataDir/streamOrder_$current_branch_id.tif -tree $outputCurrentBranchDataDir/treeFile_$current_branch_id.txt -coord $outputCurrentBranchDataDir/coordFile_$current_branch_id.txt -w $outputCurrentBranchDataDir/sn_catchments_reaches_$current_branch_id.tif -net $outputCurrentBranchDataDir/demDerived_reaches_$current_branch_id.shp
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/split_flows.py -f $outputCurrentBranchDataDir/demDerived_reaches_$current_branch_id.shp -d $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif -s $outputCurrentBranchDataDir/demDerived_reaches_split_$current_branch_id.gpkg -p $outputCurrentBranchDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -l $outputHucDataDir/nwm_lakes_proj_subset.gpkg -n $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg -ds $dropLowStreamOrders
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -gw $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.tif -o $outputCurrentBranchDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg -id $outputCurrentBranchDataDir/idFile_$current_branch_id.txt
Tcount

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e $startDiv"Vectorize Pixel Centroids $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/reachID_grid_to_vector_points.py -r $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -i featureID -p $outputCurrentBranchDataDir/flows_points_pixels_$current_branch_id.gpkg
Tcount

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_"$current_branch_id".tif -gw $outputCurrentBranchDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputCurrentBranchDataDir/flows_points_pixels_$current_branch_id.gpkg -id $outputCurrentBranchDataDir/idFile_$current_branch_id.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/gms/rem.py -d $outputCurrentBranchDataDir/dem_thalwegCond_"$current_branch_id".tif -w $outputCurrentBranchDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputCurrentBranchDataDir/rem_$current_branch_id.tif -t $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif
Tcount

## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS##
echo -e $startDiv"Bring negative values in REM to zero and mask to catchments $hucNumber $current_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_$current_branch_id.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.tif --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked_$current_branch_id.tif"
Tcount

## RASTERIZE LANDSEA (OCEAN AREA) POLYGON (IF APPLICABLE) ##
if [ -f $outputHucDataDir/LandSea_subset.gpkg ]; then
    echo -e $startDiv"Rasterize filtered/dissolved ocean/Glake polygon $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/LandSea_subset.gpkg $outputCurrentBranchDataDir/LandSea_subset_$current_branch_id.tif
    Tcount
fi

## RASTERIZE LEVEE-PROTECTED AREAS POLYGON (IF APPLICABLE) ##
if [ "$mask_leveed_area_toggle" = "True" ] && [ -f $outputHucDataDir/LeveeProtectedAreas_subset.gpkg ]; then
    echo -e $startDiv"Rasterize levee-protected areas polygons $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    if [ $current_branch_id = $branch_zero_id ]; then 
        gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/LeveeProtectedAreas_subset.gpkg $outputCurrentBranchDataDir/LeveeProtectedAreas_subset_$current_branch_id.tif
    else
        $srcDir/gms/rasterize_by_order.py -c $ncols -r $nrows -n $ndv --x-min $xmin --y-min $ymin --x-max $xmax --y-max $ymax -v $outputHucDataDir/LeveeProtectedAreas_subset.gpkg -f $outputCurrentBranchDataDir/LeveeProtectedAreas_subset_$current_branch_id.tif -s $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg -i $current_branch_id
    fi
    Tcount
fi

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_polygonize.py -8 -f GPKG $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.tif $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.gpkg catchments HydroID
Tcount

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##
echo -e $startDiv"Process catchments and model streams $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/filter_catchments_and_add_attributes.py -i $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_$current_branch_id.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg -o $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -u $hucNumber -s $dropLowStreamOrders
Tcount

## RASTERIZE NEW CATCHMENTS AGAIN ##
echo -e $startDiv"Rasterize filtered catchments $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -a HydroID -a_nodata 0 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.tif
Tcount

## MASK SLOPE TO CATCHMENTS ##
echo -e $startDiv"Mask to slopes to catchments $hucNumber $current_branch_id"$stopDiv
date -u
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/slopes_d8_dem_meters_$current_branch_id.tif -B $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.tif --calc="A*(B>0)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/slopes_d8_dem_meters_masked_$current_branch_id.tif
Tcount

## MAKE CATCHMENT AND STAGE FILES ##
echo -e $startDiv"Generate Catchment List and Stage List Files $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/make_stages_and_catchlist.py -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg -s $outputCurrentBranchDataDir/stage_$current_branch_id.txt -a $outputCurrentBranchDataDir/catch_list_$current_branch_id.txt -m $stage_min_meters -i $stage_interval_meters -t $stage_max_meters
Tcount

## MASK REM RASTER TO REMOVE OCEAN AREAS ##
if  [ -f $outputCurrentBranchDataDir/LandSea_subset_$current_branch_id.tif ]; then
    echo -e $startDiv"Additional masking to REM raster to remove ocean/Glake areas $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif -B $outputCurrentBranchDataDir/LandSea_subset_$current_branch_id.tif --calc="(A*B)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked_$current_branch_id.tif"
    Tcount
fi

## MASK REM RASTER TO REMOVE LEVEE-PROTECTED AREAS ##
if [ -f $outputCurrentBranchDataDir/LeveeProtectedAreas_subset_$current_branch_id.tif ]; then
    echo -e $startDiv"Additional masking to REM raster to remove levee-protected areas $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif -B $outputCurrentBranchDataDir/LeveeProtectedAreas_subset_$current_branch_id.tif --calc="(A*B)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked_$current_branch_id.tif"
    Tcount
fi

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/catchhydrogeo -hand $outputCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif -catch $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.tif -catchlist $outputCurrentBranchDataDir/catch_list_$current_branch_id.txt -slp $outputCurrentBranchDataDir/slopes_d8_dem_meters_masked_$current_branch_id.tif -h $outputCurrentBranchDataDir/stage_$current_branch_id.txt -table $outputCurrentBranchDataDir/src_base_$current_branch_id.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
if [ "$level" = "branch" ]; then
    b_arg=$outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg
    z_arg=$outputCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg
elif [ "$level" = "unit" ]; then
    # Branch zero has a different source for -b and -z arguments
    b_arg=$outputHucDataDir/nwm_subset_streams.gpkg
    z_arg=$outputHucDataDir/nwm_catchments_proj_subset.gpkg
fi
python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg -a $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -s $outputCurrentBranchDataDir/src_base_$current_branch_id.csv -l $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg -r $outputCurrentBranchDataDir/src_full_crosswalked_$current_branch_id.csv -j $outputCurrentBranchDataDir/src_$current_branch_id.json -x $outputCurrentBranchDataDir/crosswalk_table_$current_branch_id.csv -t $outputCurrentBranchDataDir/hydroTable_$current_branch_id.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $b_arg -y $outputCurrentBranchDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $z_arg -p $extent -k $outputCurrentBranchDataDir/small_segments_$current_branch_id.csv
Tcount
