#!/bin/bash -e

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

## SOURCE BASH FUNCTIONS
source $srcDir/bash_functions.env

## SET VARIABLES AND FILE INPUTS ##
hucNumber="$1"
current_branch_id="$2"
hucUnitLength=${#hucNumber}
huc4Identifier=${hucNumber:0:4}
huc2Identifier=${hucNumber:0:2}

outputHucDataDir=$outputRunDataDir/$hucNumber
outputBranchDataDir=$outputHucDataDir/branches
outputCurrentBranchDataDir=$outputBranchDataDir/$current_branch_id

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
        echo "GMS branch data directories for $hucNumber - $current_branch_id already exist. Use -o/--overwrite to continue"
        exit 1
    fi
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p $outputCurrentBranchDataDir

## START MESSAGE ##
echo -e $startDiv$startDiv"Processing branch_id: $current_branch_id in HUC: $hucNumber ..."$stopDiv$stopDiv

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/gms/clip_rasters_to_branches.py -d $current_branch_id -b $outputHucDataDir/branch_polygons.gpkg -i $branch_id_attribute -r $input_DEM -c $outputCurrentBranchDataDir/dem_meters.tif -v 
Tcount

## GET RASTER METADATA
echo -e $startDiv"Get DEM Metadata $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy<<<$($srcDir/getRasterInfoNative.py $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif)

## SUBSET VECTORS
echo -e $startDiv"Subsetting vectors to branches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
echo -e "Querying NWM streams ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg $outputHucDataDir/nwm_subset_streams_levelPaths.gpkg
echo -e "Querying NWM catchments ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg $outputHucDataDir/nwm_catchments_proj_subset_levelPaths.gpkg
echo -e "Querying NWM Dissolved Levelpaths headwaters ..."
ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg $outputHucDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg
#echo -e "Querying NWM headwaters ..."
#ogr2ogr -f GPKG -where $branch_id_attribute="$current_branch_id" $outputCurrentBranchDataDir/nwm_headwaters_$current_branch_id.gpkg $outputHucDataDir/nwm_headwaters.gpkg
Tcount

## RASTERIZE NLD MULTILINES ##
echo -e $startDiv"Rasterize all NLD multilines using zelev vertices $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputHucDataDir/nld_subset_levees.gpkg ] && \
gdal_rasterize -l nld_subset_levees -3d -at -a_nodata $ndv -te $xmin $ymin $xmax $ymax -ts $ncols $nrows -ot Float32 -of GTiff -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" $outputHucDataDir/nld_subset_levees.gpkg $outputCurrentBranchDataDir/nld_rasterized_elev_$current_branch_id.tif
Tcount

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e $startDiv"Rasterize Reach Boolean $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg $outputCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif
Tcount

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e $startDiv"Rasterize NHD Headwaters $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
gdal_rasterize -ot Int32 -burn 1 -init 0 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_dissolved_headwaters_$current_branch_id.gpkg $outputCurrentBranchDataDir/headwaters_$current_branch_id.tif
Tcount

## BURN LEVEES INTO DEM ##
echo -e $startDiv"Burn nld levees into dem & convert nld elev to meters (*Overwrite dem_meters.tif output) $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
# REMAINS UNTESTED FOR AREAS WITH LEVEES
[ -f $outputCurrentBranchDataDir/nld_rasterized_elev_$current_branch_id.tif ] && \
python3 -m memory_profiler $srcDir/burn_in_levees.py -dem $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif -nld $outputCurrentBranchDataDir/nld_rasterized_elev_$current_branch_id.tif -out $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif
Tcount

## DEM Reconditioning ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e $startDiv"Creating AGREE DEM using $agree_DEM_buffer meter buffer $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
python3 -m memory_profiler $srcDir/agreedem.py -r $outputCurrentBranchDataDir/flows_grid_boolean_$current_branch_id.tif -d $outputCurrentBranchDataDir/dem_meters_$current_branch_id.tif -w $outputCurrentBranchDataDir -g $outputCurrentBranchDataDir/temp_work -o $outputCurrentBranchDataDir/dem_burned_$current_branch_id.tif -b $agree_DEM_buffer -sm 10 -sh 1000
Tcount

## PIT REMOVE BURNED DEM ##
echo -e $startDiv"Pit remove Burned DEM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
rd_depression_filling $outputCurrentBranchDataDir/dem_burned_$current_branch_id.tif $outputCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif
Tcount

## D8 FLOW DIR ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
mpiexec -n $ncores_fd $taudemDir2/d8flowdir -fel $outputCurrentBranchDataDir/dem_burned_filled_$current_branch_id.tif -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif
Tcount

## D8 FLOW ACCUMULATIONS ##
echo -e $startDiv"D8 Flow Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/aread8 -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -ad8  $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -wg  $outputCurrentBranchDataDir/headwaters_$current_branch_id.tif -nc
Tcount

# THRESHOLD ACCUMULATIONS ##
echo -e $startDiv"Threshold Accumulations $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/threshold -ssa $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -src  $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -thresh 1
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

# STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$taudemDir/streamnet -p $outputCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif -fel $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif -ad8 $outputCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif -src $outputCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif -ord $outputCurrentBranchDataDir/streamOrder_$current_branch_id.tif -tree $outputCurrentBranchDataDir/treeFile_$current_branch_id.txt -coord $outputCurrentBranchDataDir/coordFile_$current_branch_id.txt -w $outputCurrentBranchDataDir/sn_catchments_reaches_$current_branch_id.tif -net $outputCurrentBranchDataDir/demDerived_reaches_$current_branch_id.shp
Tcount

## SPLIT DERIVED REACHES ##
echo -e $startDiv"Split Derived Reaches $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
$srcDir/split_flows.py -f $outputCurrentBranchDataDir/demDerived_reaches_$current_branch_id.shp -d $outputCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif -s $outputCurrentBranchDataDir/demDerived_reaches_split_$current_branch_id.gpkg -p $outputCurrentBranchDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -l $outputHucDataDir/nwm_lakes_proj_subset.gpkg
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
echo -e $startDiv"Rasterize filtered/dissolved ocean/Glake polygon $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
[ -f $outputHucDataDir/LandSea_subset.gpkg ] && \
gdal_rasterize -ot Int32 -burn $ndv -a_nodata $ndv -init 1 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES" -te $xmin $ymin $xmax $ymax -ts $ncols $nrows $outputHucDataDir/LandSea_subset.gpkg $outputCurrentBranchDataDir/LandSea_subset_$current_branch_id.tif
Tcount

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
python3 -m memory_profiler $srcDir/filter_catchments_and_add_attributes.py -i $outputCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_$current_branch_id.gpkg -c $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg -o $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -w $outputHucDataDir/wbd8_clp.gpkg -u $hucNumber
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
echo -e $startDiv"Additional masking to REM raster to remove ocean/Glake areas $hucNumber $current_branch_id"$stopDiv
date -u
Tstart
[ -f $outputCurrentBranchDataDir/LandSea_subset.tif ] && \
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif -B $outputCurrentBranchDataDir/LandSea_subset_$current_node_id.tif --calc="(A*B)" --NoDataValue=$ndv --outfile=$outputCurrentBranchDataDir/"rem_zeroed_masked_$current_branch_id.tif"
Tcount

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
python3 -m memory_profiler $srcDir/add_crosswalk.py -d $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg -a $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg -s $outputCurrentBranchDataDir/src_base_$current_branch_id.csv -l $outputCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg -f $outputCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg -r $outputCurrentBranchDataDir/src_full_crosswalked_$current_branch_id.csv -j $outputCurrentBranchDataDir/src_$current_branch_id.json -x $outputCurrentBranchDataDir/crosswalk_table_$current_branch_id.csv -t $outputCurrentBranchDataDir/hydroTable_$current_branch_id.csv -w $outputHucDataDir/wbd8_clp.gpkg -b $outputCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg -y $outputCurrentBranchDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $outputCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg -p $extent -k $outputCurrentBranchDataDir/small_segments_$current_branch_id.csv
Tcount

## REMOVE FILES FROM DENY LIST ##
if [ -f $deny_gms_branches_list ]; then
    echo -e $startDiv"Remove files $hucNumber $current_branch_id"$stopDiv
    date -u
    Tstart
    $srcDir/gms/outputs_cleanup.py -d $outputCurrentBranchDataDir -l $deny_gms_branches_list -v -b
    Tcount
fi


