#!/bin/bash -e
#### always leave the -e above as it means exit on fail (critical to our system)
### set -eo pipefail  (debugging)

source $srcDir/bash_functions.env

## Level is equal to the parent script: 'unit' or 'branch'
level=$1
huc2Identifier=${hucNumber:0:2}

if [[ "$level" == "branch" ]]; then
    b_arg="${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_${current_branch_id}.parquet"
    z_arg="${tempCurrentBranchDataDir}/nwm_catchments_proj_subset_levelPaths_${current_branch_id}.parquet"
elif [[ "$level" == "unit" ]]; then
    # Branch zero has a different source for -b and -z arguments
    b_arg="${tempHucDataDir}/nwm_subset_streams.gpkg"
    z_arg="${tempHucDataDir}/nwm_catchments_proj_subset.gpkg"
fi

## MASK LEVEE-PROTECTED AREAS FROM DEM ##
if [[ "$mask_leveed_area_toggle" == "True"  &&  -f "${tempHucDataDir}/LeveeProtectedAreas_subset.gpkg" ]]; then
    echo -e "${startDiv}Mask levee-protected areas from DEM (*Overwrite dem_meters.tif output) ${hucNumber} ${current_branch_id}"
    args=(
        -dem "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
        -nld "${tempHucDataDir}/LeveeProtectedAreas_subset.gpkg"
        -catchments "${z_arg}"
        -out "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
        -b "${branch_id_attribute}"
        -i "${current_branch_id}"
        -b0 "${branch_zero_id}"
        -csv "${tempHucDataDir}/levee_levelpaths.csv"
        -l "${levee_id_attribute}"
    )
    python3 "${srcDir}/mask_dem.py" "${args[@]}"
fi

## D8 FLOW ACCUMULATIONS ##
echo -e "${startDiv}D8 Flow Accumulations ${hucNumber} ${current_branch_id}"
args=(
    -fd "${tempCurrentBranchDataDir}/flowdir_d8_burned_filled_${current_branch_id}.tif"
    -fa "${tempCurrentBranchDataDir}/flowaccum_d8_burned_filled_${current_branch_id}.tif"
    -wg "${tempCurrentBranchDataDir}/headwaters_${current_branch_id}.tif"
    -stream "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
    -thresh "1"
)
python3 "${srcDir}/accumulate_headwaters.py" "${args[@]}"

## PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ###
echo -e "${startDiv}Preprocessing for lateral thalweg adjustment ${hucNumber} ${current_branch_id}"
args=(
    -s "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
    -o "${tempCurrentBranchDataDir}/demDerived_streamPixels_ids_${current_branch_id}.tif"
)
python3 "${srcDir}/unique_pixel_and_allocation.py" "${args[@]}"

## ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ##
echo -e "${startDiv}Performing lateral thalweg adjustment ${hucNumber} ${current_branch_id}"
args=(
    -e "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
    -s "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
    -a "${tempCurrentBranchDataDir}/demDerived_streamPixels_ids_"${current_branch_id}"_allo.tif"
    -d "${tempCurrentBranchDataDir}/demDerived_streamPixels_ids_"${current_branch_id}"_dist.tif"
    -t "50"
    -o "${tempCurrentBranchDataDir}/dem_lateral_thalweg_adj_${current_branch_id}.tif"
    -th "${thalweg_lateral_elev_threshold}"
)
python3 "${srcDir}/adjust_thalweg_lateral.py" "${args[@]}"

## MASK BURNED DEM FOR STREAMS ONLY ###
echo -e "${startDiv}Mask Burned DEM for Thalweg Only ${hucNumber} ${current_branch_id}"
args=(
    --quiet --type=Int32 --overwrite
    --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
    -A "${tempCurrentBranchDataDir}/flowdir_d8_burned_filled_${current_branch_id}.tif"
    -B "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
    --calc="A*B"
    --outfile="${tempCurrentBranchDataDir}/flowdir_d8_burned_filled_flows_${current_branch_id}.tif"
    --NoDataValue="0"
)
gdal_calc.py "${args[@]}"

## FLOW CONDITION STREAMS ##
echo -e $startDiv"Flow Condition Thalweg $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py flowdircond \
    -t "$taudemDir" \
    -p "$tempCurrentBranchDataDir/flowdir_d8_burned_filled_flows_$current_branch_id.tif" \
    -z "$tempCurrentBranchDataDir/dem_lateral_thalweg_adj_$current_branch_id.tif" \
    -zfdc "$tempCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif"

## D8 SLOPES ##
echo -e $startDiv"D8 Slopes from DEM $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py d8flowdir \
    -n $ncores_fd \
    -t $taudemDir2 \
    -fel $tempCurrentBranchDataDir/dem_lateral_thalweg_adj_$current_branch_id.tif \
    -sd8 $tempCurrentBranchDataDir/slopes_d8_dem_meters_$current_branch_id.tif

## STREAMNET FOR REACHES ##
echo -e $startDiv"Stream Net for Reaches $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py streamnet \
    -t "$taudemDir" \
    -p "$tempCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif" \
    -fel "$tempCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif" \
    -ad8 "$tempCurrentBranchDataDir/flowaccum_d8_burned_filled_$current_branch_id.tif" \
    -src "$tempCurrentBranchDataDir/demDerived_streamPixels_$current_branch_id.tif" \
    -ord "$tempCurrentBranchDataDir/streamOrder_$current_branch_id.tif" \
    -tree "$tempCurrentBranchDataDir/treeFile_$current_branch_id.txt" \
    -coord "$tempCurrentBranchDataDir/coordFile_$current_branch_id.txt" \
    -w "$tempCurrentBranchDataDir/sn_catchments_reaches_$current_branch_id.tif" \
    -net "$tempCurrentBranchDataDir/demDerived_reaches_$current_branch_id.shp"

## SPLIT DERIVED REACHES ##
echo -e "${startDiv}Split Derived Reaches ${hucNumber} ${current_branch_id}"
args=(
    -f "${tempCurrentBranchDataDir}/demDerived_reaches_${current_branch_id}.shp"
    -d "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
    -s "${tempCurrentBranchDataDir}/demDerived_reaches_split_${current_branch_id}.parquet"
    -p "${tempCurrentBranchDataDir}/demDerived_reaches_split_points_${current_branch_id}.shp"
    -w "${tempHucDataDir}/wbd8_clp.gpkg"
    -l "${tempHucDataDir}/nwm_lakes_proj_subset.gpkg"
    -n "${b_arg}"
    -m "${max_split_distance_meters}"
    -t "${slope_min}"
    -b "${lakes_buffer_dist_meters}"
)
python3 "${srcDir}/split_flows.py" "${args[@]}"

## GAGE WATERSHED FOR REACHES ##
echo -e $startDiv"Gage Watershed for Reaches $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py gagewatershed \
    -n $ncores_gw \
    -t "$taudemDir" \
    -p "$tempCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif" \
    -gw "$tempCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.tif" \
    -o "$tempCurrentBranchDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg" \
    -id "$tempCurrentBranchDataDir/idFile_$current_branch_id.txt"

## VECTORIZE FEATURE ID CENTROIDS ##
echo -e "${startDiv}Vectorize Pixel Centroids ${hucNumber} ${current_branch_id}"
args=(
    -r "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
    -i "featureID"
    -p "${tempCurrentBranchDataDir}/flows_points_pixels_${current_branch_id}.shp"
)
python3 "${srcDir}/reachID_grid_to_vector_points.py" "${args[@]}"

## GAGE WATERSHED FOR PIXELS ##
echo -e $startDiv"Gage Watershed for Pixels $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py gagewatershed \
    -n $ncores_gw \
    -t "$taudemDir" \
    -p "$tempCurrentBranchDataDir/flowdir_d8_burned_filled_$current_branch_id.tif" \
    -gw "$tempCurrentBranchDataDir/gw_catchments_pixels_$current_branch_id.tif" \
    -o "$tempCurrentBranchDataDir/flows_points_pixels_$current_branch_id.gpkg" \
    -id "$tempCurrentBranchDataDir/idFile_$current_branch_id.txt" \


## CATCH AND MITIGATE BRANCH OUTLET BACKPOOL ERROR ##
echo -e "${startDiv}Catching and mitigating branch outlet backpool issue ${hucNumber} ${current_branch_id}"
args=(
    -b "${tempCurrentBranchDataDir}"
    -cp "${tempCurrentBranchDataDir}/gw_catchments_pixels_${current_branch_id}.tif"
    -cpp "${tempCurrentBranchDataDir}/gw_catchments_pixels_${current_branch_id}.parquet"
    -cr "${tempCurrentBranchDataDir}/gw_catchments_reaches_${current_branch_id}.tif"
    -s "${tempCurrentBranchDataDir}/demDerived_reaches_split_${current_branch_id}.parquet"
    -p "${tempCurrentBranchDataDir}/demDerived_reaches_split_points_${current_branch_id}.shp"
    -n "${b_arg}"
    -d "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
    -t "${slope_min}"
    --calculate-stats
)
python3 "${srcDir}/mitigate_branch_outlet_backpool.py" "${args[@]}"

## D8 REM ##
echo -e "${startDiv}D8 REM ${hucNumber} ${current_branch_id}"
args=(
    -d "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
    -w "${tempCurrentBranchDataDir}/gw_catchments_pixels_${current_branch_id}.tif"
    -o "${tempCurrentBranchDataDir}/rem_${current_branch_id}.tif"
    -t "${tempCurrentBranchDataDir}/demDerived_streamPixels_${current_branch_id}.tif"
)
python3 "${srcDir}/make_rem.py" "${args[@]}"

## BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS ##
echo -e "${startDiv}Bring negative values in REM to zero and mask to catchments ${hucNumber} ${current_branch_id}"
args=(
    --quiet --type=Float32 --overwrite 
    --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
    -A "${tempCurrentBranchDataDir}/rem_${current_branch_id}.tif"
    -B "${tempCurrentBranchDataDir}/gw_catchments_reaches_${current_branch_id}.tif"
    --calc="(A*(A>=0)*(B>0))" --NoDataValue="${ndv}"
    --outfile="${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
)
gdal_calc.py "${args[@]}"

## RASTERIZE LANDSEA (OCEAN AREA) POLYGON (IF APPLICABLE) ##
if [[ -f "${tempHucDataDir}/LandSea_subset.gpkg" ]]; then
    echo -e "${startDiv}Rasterize filtered/dissolved ocean/Glake polygon ${hucNumber} ${current_branch_id}"
    args=(
        -q -ot "Int32" -burn "${ndv}" -init "1" -a_nodata "${ndv}"
        -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES"
        -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
        "${tempHucDataDir}/LandSea_subset.gpkg"
        "${tempCurrentBranchDataDir}/LandSea_subset_${current_branch_id}.tif"
    )
    gdal_rasterize "${args[@]}"
fi

## POLYGONIZE REACH WATERSHEDS ##
echo -e "${startDiv}Polygonize Reach Watersheds ${hucNumber} ${current_branch_id}"
args=(
    "${tempCurrentBranchDataDir}/gw_catchments_reaches_${current_branch_id}.tif"
    "${tempCurrentBranchDataDir}/gw_catchments_reaches_${current_branch_id}.parquet"
    "HydroID"
    -q -8
)
python3 "${srcDir}/polygonize_raster.py" "${args[@]}"

## PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ##
echo -e "${startDiv}Process catchments and model streams ${hucNumber} ${current_branch_id}"
args=(
    -i "${tempCurrentBranchDataDir}/gw_catchments_reaches_${current_branch_id}.parquet"
    -f "${tempCurrentBranchDataDir}/demDerived_reaches_split_${current_branch_id}.parquet"
    -c "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_${current_branch_id}.parquet"
    -o "${tempCurrentBranchDataDir}/demDerived_reaches_split_filtered_${current_branch_id}.parquet"
    -w "${tempHucDataDir}/wbd8_clp.gpkg"
    -u "${hucNumber}"
)
python3 "${srcDir}/filter_catchments_and_add_attributes.py" "${args[@]}"

## RASTERIZE NEW CATCHMENTS AGAIN ##
echo -e "${startDiv}Rasterize filtered catchments ${hucNumber} ${current_branch_id}"
args=(
    -q -ot Int32 -a HydroID -a_nodata "0" -init "0"
    -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES"
    -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
    "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_${current_branch_id}.parquet"
    "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_${current_branch_id}.tif"
)
python3 "${srcDir}/rasterize_parquet.py" "${args[@]}"

## MASK SLOPE TO CATCHMENTS ##
echo -e "${startDiv}Mask to slopes to catchments ${hucNumber} ${current_branch_id}"
args=(
    --quiet --type=Float32 --overwrite 
    --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
    -A "${tempCurrentBranchDataDir}/slopes_d8_dem_meters_${current_branch_id}.tif"
    -B "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_${current_branch_id}.tif"
    --calc="A*(B>0)" --NoDataValue="${ndv}"
    --outfile="${tempCurrentBranchDataDir}/slopes_d8_dem_meters_masked_${current_branch_id}.tif"
)
gdal_calc.py "${args[@]}"

## MAKE CATCHMENT AND STAGE FILES ##
echo -e "${startDiv}Generate Catchment List and Stage List Files ${hucNumber} ${current_branch_id}"
args=(
    -f "${tempCurrentBranchDataDir}/demDerived_reaches_split_filtered_${current_branch_id}.parquet"
    -c "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_${current_branch_id}.parquet"
    -s "${tempCurrentBranchDataDir}/stage_${current_branch_id}.txt"
    -a "${tempCurrentBranchDataDir}/catch_list_${current_branch_id}.txt"
    -m "${stage_min_meters}"
    -i "${stage_interval_meters}"
    -t "${stage_max_meters}"
)
python3 "${srcDir}/make_stages_and_catchlist.py" "${args[@]}"

## MASK REM RASTER TO REMOVE OCEAN AREAS ##
if  [[ -f "${tempCurrentBranchDataDir}/LandSea_subset_${current_branch_id}.tif" ]]; then
    echo -e "${startDiv}Additional masking to REM raster to remove ocean/Glake areas ${hucNumber} ${current_branch_id}"
    args=(
        --quiet --type=Float32 --overwrite
        --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
        -A "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -B "${tempCurrentBranchDataDir}/LandSea_subset_${current_branch_id}.tif"
        --calc="(A*B)" --NoDataValue="${ndv}"
        --outfile="${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
    )
    gdal_calc.py "${args[@]}"
fi

## HEAL HAND -- REMOVES HYDROCONDITIONING ARTIFACTS ##
if [[ "${healed_hand_hydrocondition}" == "true"  &&  "${current_branch_id}" != "${branch_zero_id}" ]]; then
    echo -e "${startDiv}Healed HAND to Remove Hydro-conditioning Artifacts ${hucNumber} ${current_branch_id}"
    args=(
        --quiet --type=Float32 --overwrite 
        --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
        -R "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -D "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
        -T "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
        --calc="R+(D-T)" --NoDataValue=${ndv}
        --outfile="${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
    )
    gdal_calc.py "${args[@]}"
fi

## HYDRAULIC PROPERTIES ##
echo -e $startDiv"Sample reach averaged parameters $hucNumber $current_branch_id"
python3 $srcDir/run_taudem_subprocess.py catchhydrogeo \
    -t "$taudemDir" \
    -hand "$tempCurrentBranchDataDir/rem_zeroed_masked_$current_branch_id.tif" \
    -catch "$tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.tif" \
    -catchlist "$tempCurrentBranchDataDir/catch_list_$current_branch_id.txt" \
    -slp "$tempCurrentBranchDataDir/slopes_d8_dem_meters_masked_$current_branch_id.tif" \
    -H "$tempCurrentBranchDataDir/stage_$current_branch_id.txt" \
    -table "$tempCurrentBranchDataDir/src_base_$current_branch_id.csv"

## FINALIZE CATCHMENTS AND MODEL STREAMS ##
echo -e $startDiv"Finalize catchments and model streams $hucNumber $current_branch_id"
python3 $srcDir/add_crosswalk.py \
    -d $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_$current_branch_id.gpkg \
    -a $tempCurrentBranchDataDir/demDerived_reaches_split_filtered_$current_branch_id.gpkg \
    -s $tempCurrentBranchDataDir/src_base_$current_branch_id.csv \
    -l $tempCurrentBranchDataDir/gw_catchments_reaches_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg \
    -f $tempCurrentBranchDataDir/demDerived_reaches_split_filtered_addedAttributes_crosswalked_$current_branch_id.gpkg \
    -r $tempCurrentBranchDataDir/src_full_crosswalked_$current_branch_id.csv \
    -j $tempCurrentBranchDataDir/src_$current_branch_id.json \
    -x $tempCurrentBranchDataDir/crosswalk_table_$current_branch_id.csv \
    -t $tempCurrentBranchDataDir/hydroTable_$current_branch_id.csv \
    -w $tempHucDataDir/wbd8_clp.gpkg \
    -b $b_arg \
    -u $hucNumber \
    -m $manning_n \
    -k $tempCurrentBranchDataDir/small_segments_$current_branch_id.csv \
    -e $min_catchment_area \
    -g $min_stream_length \
    -i $iris_sword_slope \
    -p $hfab_ransac_slope

## HEAL HAND -- REMOVES HYDROCONDITIONING ARTIFACTS ##
if [[ "${healed_hand_hydrocondition}" == "true" && "${current_branch_id}" == "${branch_zero_id}" ]]; then
    echo -e "${startDiv}Healed HAND to Remove Hydro-conditioning Artifacts ${hucNumber} ${current_branch_id}"
    args=(
        --quiet --type=Float32 --overwrite
        --co "COMPRESS=LZW" --co "BIGTIFF=YES" --co "TILED=YES"
        -R "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -D "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
        -T "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
        --calc="R+(D-T)" --NoDataValue="${ndv}"
        --outfile="${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
    )
    gdal_calc.py "${args[@]}"
fi

## HEAL HAND BRIDGES ##
if [[ -f "${tempHucDataDir}/osm_bridges_subset.gpkg" ]]; then
    echo -e "${startDiv}Burn in bridges ${hucNumber} ${current_branch_id}"
    date -u
    Tstart
    args=(
        -g "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -d "${tempCurrentBranchDataDir}/bridge_elev_diff_meters_${current_branch_id}.tif"
        -s "${tempHucDataDir}/osm_bridges_subset.gpkg"
        -b1 10
        -b2 1.5
        -p "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_${current_branch_id}.parquet"
        -c "${tempCurrentBranchDataDir}/osm_bridge_centroids_${current_branch_id}.parquet"
    )
    python3 "${srcDir}/heal_bridges_osm.py" "${args[@]}"
    Tcount


else
    echo -e "${startDiv}No applicable bridge data for ${hucNumber}"
fi

## Process roads FIMpact ##
if  [[ -f "${tempHucDataDir}/osm_roads_subset.gpkg" ]]; then
    echo -e "${startDiv}Process roads FIMpact ${hucNumber} ${current_branch_id}"
    date -u
    Tstart
    args=(
        -g "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -r "${tempHucDataDir}/osm_roads_subset.gpkg"
        -c "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_${current_branch_id}.parquet"
        -o "${tempCurrentBranchDataDir}/osm_roads_fimpact_${current_branch_id}.csv"
    )
    python3 "${srcDir}/process_roads_fimpact.py" "${args[@]}"
    Tcount
else
    echo -e "${startDiv}No osm roads data for ${hucNumber}"
fi

## Process buildings FIMpact ##
if  [[ -f "${tempHucDataDir}/buildings_subset.gpkg" ]]; then
    echo -e "${startDiv}Process buildings FIMpact ${hucNumber} ${current_branch_id}"
    date -u
    Tstart
    args=(
        -g "${tempCurrentBranchDataDir}/rem_zeroed_masked_${current_branch_id}.tif"
        -r "${tempHucDataDir}/buildings_subset.gpkg"
        -c "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_${current_branch_id}.parquet"
        -o "${tempCurrentBranchDataDir}/buildings_fimpact_${current_branch_id}.csv"
    )
    python3 "${srcDir}/process_buildings_fimpact.py" "${args[@]}"
    Tcount
else
    echo -e "${startDiv}No buildings data for ${hucNumber}"
fi

## EVALUATE CROSSWALK ##
if [[ "${current_branch_id}" == "${branch_zero_id}" && "${evaluateCrosswalk}" == "1" ]]; then
    echo -e "${startDiv}Evaluate crosswalk ${hucNumber} ${current_branch_id}"
    args=(
        -a "${tempCurrentBranchDataDir}/demDerived_reaches_split_filtered_addedAttributes_crosswalked_${current_branch_id}.parquet"
        -b "${b_arg}"
        -c "${tempHucDataDir}/crosswalk_evaluation_${current_branch_id}.csv"
        -d "${tempHucDataDir}/nwm_headwater_points_subset.gpkg"
        -u "${hucNumber}"
        -z "${current_branch_id}"
    )
    python3 "$toolsDir/evaluate_crosswalk.py" "${args[@]}"
fi

if [[ "${huc2Identifier}" == "19" ]]; then
    echo -e "Skipping Int16 Conversion for Alaska HUC"
else
    ## CONVERSION TO INT16 ##
    echo -e "${startDiv}Convert GW Catchments and REM to Int16 ${hucNumber} ${current_branch_id}"
    python3 "${toolsDir}/convert_to_int16.py" -b "${tempCurrentBranchDataDir}"
fi
