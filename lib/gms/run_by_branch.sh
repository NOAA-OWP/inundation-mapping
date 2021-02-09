#!/bin/bash -e

source $libDir/bash_functions.env

## INITIALIZE TOTAL TIME TIMER ##
T_total_start

# TEMP
export startDiv="\n##########################################################################\n"
export stopDiv="\n##########################################################################"
ndv=-2147483648

## ECHO PARAMETERS
# echo -e "memfree=$memfree"$stopDiv

## SET OUTPUT DIRECTORY FOR UNIT ##
outputDataDir=/data/temp/gms/test1
inputDataDir=/data/outputs/default_12090301/12090301

if [ -d "$outputDataDir" ]; then
    mkdir -p $outputDataDir
fi

## SET VARIABLES AND FILE INPUTS ##
branch_id_attribute=HydroID
branch_buffer_distance_meters=2000
ncores_gw=1
input_demThal=$inputDataDir/dem_thalwegCond.tif
input_flowdir=$inputDataDir/flowdir_d8_burned_filled.tif
input_slopes=$inputDataDir/slopes_d8_dem_meters.tif
input_demDerived_raster=$inputDataDir/demDerived_streamPixels.tif
input_demDerived_reaches=$inputDataDir/demDerived_reaches_split.gpkg
input_demDerived_reaches_points=$inputDataDir/demDerived_reaches_split_points.gpkg
input_demDerived_pixel_points=$inputDataDir/flows_points_pixels.gpkg
input_catchment_list=$inputDataDir/catchment_list.txt
input_stage_list=$inputDataDir/stage.txt
input_hydroTable=$inputDataDir/hydroTable.csv


## DERIVE LEVELPATH 
# derive_level_paths.py


## STREAM BRANCH POLYGONS
echo -e $startDiv"Generating Stream Branch Polygons"$stopDiv
date -u
Tstart
$libDir/gms/buffer_stream_branches.py -s $input_demDerived_reaches -i $branch_id_attribute -d $branch_buffer_distance_meters -b $outputDataDir/polygons.gpkg -v 
Tcount

## CLIP RASTERS
echo -e $startDiv"Clipping rasters to branches"$stopDiv
date -u
Tstart
$libDir/gms/clip_rasters_to_branches.py -b $outputDataDir/polygons.gpkg -i $branch_id_attribute -r $input_demThal $input_flowdir $input_slopes $input_demDerived_raster -c $outputDataDir/dem_thalwegCond.tif $outputDataDir/flowdir.tif $outputDataDir/slopes.tif $outputDataDir/demDerived.tif -v 
Tcount

## SUBSET VECTORS
#echo -e $startDiv"Subsetting vectors to branches"$stopDiv
#date -u
#Tstart
#$libDir/gms/clip_rasters_to_branches.py -b $outputDataDir/polygons.gpkg -i $branch_id_attribute -r $input_demDerived_reaches_points $input_demDerived_pixel_points -c $outputDataDir/demDerived_reaches_points.gpkg $outputDataDir/demDerived_pixel_points.gpkg -v 
# not clipping "$input_demDerived_reaches" to "$outputDataDir/demDerived_reaches.gpkg"
#Tcount

current_branch_id=15080001

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels $hucNumber"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_pixels.tif ] && \
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_"$current_branch_id".tif -gw $outputDataDir/gw_catchments_pixels_$current_branch_id.tif -o $inputDataDir/flows_points_pixels.gpkg -id $outputDataDir/idFile.txt
Tcount

# D8 REM ##
echo -e $startDiv"D8 REM $hucNumber"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/rem.tif ] && \
$libDir/rem.py -d $outputDataDir/dem_thalwegCond_"$current_branch_id".tif -w $outputDataDir/gw_catchments_pixels_$current_branch_id.tif -o $outputDataDir/rem_$current_branch_id.tif -t $outputDataDir/demDerived_$current_branch_id.tif
Tcount

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches $hucNumber"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches.tif ] && \
mpiexec -n $ncores_gw $taudemDir/gagewatershed -p $outputDataDir/flowdir_$current_branch_id.tif -gw $outputDataDir/gw_catchments_reaches_$current_branch_id.tif -o $inputDataDir/demDerived_reaches_split_points.gpkg -id $outputDataDir/idFile.txt
Tcount

## BRING DISTANCE DOWN TO ZERO ##
echo -e $startDiv"Bring negative values in REM to zero"$stopDiv
date -u
[ ! -f $outputHucDataDir/rem_zeroed.tif ] && \
gdal_calc.py --quiet --type=Float32 --overwrite --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES" -A $outputDataDir/rem_$current_branch_id.tif --calc="(A*(A>=0))" --NoDataValue=$ndv --outfile=$outputDataDir/"rem_zeroed_$current_branch_id.tif"
Tcount

## POLYGONIZE REACH WATERSHEDS ##
echo -e $startDiv"Polygonize Reach Watersheds $hucNumber"$stopDiv
date -u
Tstart
[ ! -f $outputHucDataDir/gw_catchments_reaches.gpkg ] && \
gdal_polygonize.py -8 -f GPKG $outputDataDir/gw_catchments_reaches_$current_branch_id.tif $outputDataDir/gw_catchments_reaches_$current_branch_id.gpkg catchments HydroID
Tcount

echo "1" > $outputDataDir/catch_list_$current_branch_id.txt
grep "$current_branch_id" $input_catchment_list >> $outputDataDir/catch_list_$current_branch_id.txt

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/src_base.csv ] && \
$taudemDir/catchhydrogeo -hand $outputDataDir/rem_zeroed_$current_branch_id.tif -catch $outputDataDir/gw_catchments_reaches_$current_branch_id.tif -catchlist $outputDataDir/catch_list_$current_branch_id.txt -slp $outputDataDir/slopes_$current_branch_id.tif -h $input_stage_list -table $outputDataDir/src_base_$current_branch_id.csv
Tcount

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams $hucNumber"$stopDiv
date -u
Tstart
[ ! -f $outputDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg ] && \
$libDir/add_crosswalk.py -d $outputDataDir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $outputDataDir/demDerived_reaches_split_filtered.gpkg -s $outputDataDir/src_base.csv -l $outputDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $outputDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $outputDataDir/src_full_crosswalked.csv -j $outputDataDir/src.json -x $outputDataDir/crosswalk_table.csv -t $outputDataDir/hydroTable.csv -w $outputDataDir/wbd8_clp.gpkg -b $outputDataDir/nwm_subset_streams.gpkg -y $outputDataDir/nwm_catchments_proj_subset.tif -m $manning_n -z $input_NWM_Catchments -p $extent
Tcount

## CLEANUP OUTPUTS ##
echo -e $startDiv"Cleaning up outputs $hucNumber"$stopDiv
args=()
[[ ! -z "$whitelist" ]] && args+=( "-w$whitelist" )
(( production == 1 )) && args+=( '-p' )
(( viz == 1 )) && args+=( '-v' )
date -u
Tstart
$libDir/output_cleanup.py $hucNumber $outputDataDir "${args[@]}"
Tcount
