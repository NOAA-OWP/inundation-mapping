#!/bin/bash -e
# We ... DO ....  the -e here which means stop execution immediately on fail.
# We want this to auto fail as it is logging and error handling are done by its parent
# of process_huc.sh
### Yes.. not all of our .sh files are the same with the -e flag, be design.

## SOURCE BASH FUNCTIONS
source "${srcDir}/bash_functions.env"

## INITIALIZE TOTAL TIME TIMER ##
## Used by timers in sections below
## Overall page timer in process_branch.sh in case of errors
T_total_start

## SET VARIABLES AND FILE INPUTS ##
hucNumber="$1"
current_branch_id="$2"
hucUnitLength="${#hucNumber}"
huc4Identifier="${hucNumber:0:4}"
huc2Identifier="${hucNumber:0:2}"

huc_CRS=$(get_crs_for_huc "$hucNumber")

# Skip branch zero
if [[ "${current_branch_id}" == "${branch_zero_id}" ]]; then
    exit 0
fi

tempCurrentBranchDataDir="${tempBranchDataDir}/${current_branch_id}"

## OVERWRITE
if [[ -d "${tempCurrentBranchDataDir}" ]]; then
    rm -rf "${tempCurrentBranchDataDir}"
fi

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p "${tempCurrentBranchDataDir}"

## SUBSET VECTORS
echo -e "${startDiv}Subsetting vectors to branches ${hucNumber} ${current_branch_id}"
args=(
    --crs "${huc_CRS}"
    --where "${branch_id_attribute}='${current_branch_id}'"
    --files
        "${tempHucDataDir}/nwm_subset_streams_levelPaths.parquet" "${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_${current_branch_id}.parquet"
        "${tempHucDataDir}/nwm_subset_streams_levelPaths_extended.parquet" "${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_extended_${current_branch_id}.parquet"
        "${tempHucDataDir}/nwm_catchments_proj_subset_levelPaths.parquet" "${tempCurrentBranchDataDir}/nwm_catchments_proj_subset_levelPaths_${current_branch_id}.parquet"
        "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved_headwaters.parquet" "${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_dissolved_headwaters_${current_branch_id}.parquet"
)
python3 "${srcDir}/subset_vectors_to_branches.py" "${args[@]}"

## GET RASTERS FROM ROOT HUC DIRECTORY AND CLIP TO CURRENT BRANCH BUFFER ##
echo -e "${startDiv}Clipping rasters to branches ${hucNumber} ${current_branch_id}"
args=(
    -d "${current_branch_id}"
    -b "${tempHucDataDir}/branch_polygons.parquet"
    -i "${branch_id_attribute}"
    -r "${tempHucDataDir}/dem_meters.tif" "${tempHucDataDir}/bridge_elev_diff_meters.tif"
    -c "${tempCurrentBranchDataDir}/dem_meters.tif" "${tempCurrentBranchDataDir}/bridge_elev_diff_meters.tif"
)
python3 "${srcDir}/clip_rasters_to_branches.py" "${args[@]}" 

## GET RASTER METADATA
echo -e "${startDiv}Get DEM Metadata ${hucNumber} ${current_branch_id}"
read -r ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy \
    <<<"$("${srcDir}/getRasterInfoNative.py" -r "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif")"

## RASTERIZE REACH BOOLEAN (1 & 0) ##
echo -e "${startDiv}Rasterize Reach Boolean ${hucNumber} ${current_branch_id}"
args=(
    -q -at -ot "Int32" -burn "1" -init "0" -a_nodata "-9999"
    -co "BIGTIFF=YES"
    -co "TILED=YES"
    -co "BLOCKXSIZE=512"
    -co "BLOCKYSIZE=512"
    -co "COMPRESS=LZW"
    -co "COPY_SRC_OVERVIEWS=YES"
    -te "${xmin}" "${ymin}" ${xmax} "${ymax}"
    -ts "${ncols}" "${nrows}"
    "${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_extended_${current_branch_id}.parquet"
    "${tempCurrentBranchDataDir}/flows_grid_boolean_${current_branch_id}.tif"
)
python3 "${srcDir}/rasterize_parquet.py" "${args[@]}"

## DEM Reconditioning - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e "${startDiv}Creating AGREE DEM using ${agree_DEM_buffer} meter buffer ${hucNumber} (Branches)"
args=(
    -r "${tempCurrentBranchDataDir}/flows_grid_boolean_${current_branch_id}.tif"
    -d "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
    -w "${tempCurrentBranchDataDir}"
    -o "${tempCurrentBranchDataDir}/dem_burned_${current_branch_id}.tif"
    -b "${agree_DEM_buffer}"
    -sm "10"
    -sh "1000"
)
python3 "${srcDir}/agreedem.py" "${args[@]}"

## ADJUST FLOODPLAINS ##
echo -e "${startDiv}Adjust floodplains ${hucNumber} ${current_branch_id}"
echo -e "Using FEMA floodplain layer: ${fema_floodplain_layer}"
args=(
    -i "${tempCurrentBranchDataDir}/flows_grid_boolean_${current_branch_id}.tif"
    -e "${tempCurrentBranchDataDir}/flows_grid_boolean_euclidean_distance_${current_branch_id}.tif"
    -d "${tempCurrentBranchDataDir}/dem_burned_${current_branch_id}.tif"
    -o "${tempCurrentBranchDataDir}/dem_burned_adjusted_${current_branch_id}.tif"
    -t "${floodplain_distance_threshold}"
    -s "${floodplain_slope_exponent}"
    -z "${floodplain_z_factor}"
    -p "${tempHucDataDir}/branch_polygons.parquet"
    -b "${current_branch_id}"
    -f "${input_fema_flood_hazard_zones}/nfhl_${hucNumber}.gpkg"
    -l "${fema_floodplain_layer}"
    -c "${tempHucDataDir}/nwm_catchments_proj_subset.gpkg"
    -n "${tempHucDataDir}/nwm_subset_streams.gpkg"
    -lp "${tempHucDataDir}/nwm_subset_streams_levelPaths.parquet"
)
python3 "${srcDir}/adjust_floodplains.py" "${args[@]}"

## PIT REMOVE BURNED DEM - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e "${startDiv}Pit remove Burned DEM ${hucNumber} ${current_branch_id}"
if [[ -f "${tempCurrentBranchDataDir}/dem_burned_adjusted_${current_branch_id}.tif" ]]; then
    dem_burned="${tempCurrentBranchDataDir}/dem_burned_adjusted_${current_branch_id}.tif"
else
    dem_burned="${tempCurrentBranchDataDir}/dem_burned_${current_branch_id}.tif"
fi
args=(
    "${dem_burned}"
    "${tempCurrentBranchDataDir}/dem_burned_filled_${current_branch_id}.tif"
)
rd_depression_filling "${args[@]}"

## D8 FLOW DIR - BRANCHES (NOT 0) (NWM levelpath streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber ${current_branch_id}"
args=(
    -n "${ncores_fd}"
    -t "${taudemDir2}"
    -fel "${tempCurrentBranchDataDir}/dem_burned_filled_${current_branch_id}.tif"
    -p "${tempCurrentBranchDataDir}/flowdir_d8_burned_filled_${current_branch_id}.tif"
)
python3 "${srcDir}/run_taudem_subprocess.py" d8flowdir "${args[@]}"

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e "${startDiv}Rasterize NWM Headwaters ${hucNumber} ${current_branch_id}"
args=(
    -q -ot "Int32" -burn "1" -init "0" -a_nodata "-9999"
    -co "BIGTIFF=YES"
    -co "TILED=YES"
    -co "BLOCKXSIZE=512"
    -co "BLOCKYSIZE=512"
    -co "COMPRESS=LZW"
    -co "COPY_SRC_OVERVIEWS=YES"
    -te "${xmin}" "${ymin}" ${xmax} "${ymax}"
    -ts "${ncols}" "${nrows}"
    "${tempCurrentBranchDataDir}/nwm_subset_streams_levelPaths_dissolved_headwaters_${current_branch_id}.parquet"
    "${tempCurrentBranchDataDir}/headwaters_${current_branch_id}.tif"
)
python3 "${srcDir}/rasterize_parquet.py" "${args[@]}"

## PRODUCE THE REM AND OTHER HAND FILE OUTPUTS ##
export hucNumber current_branch_id tempCurrentBranchDataDir tempHucDataDir ndv xmin ymin xmax ymax ncols nrows

"${srcDir}/delineate_hydros_and_produce_HAND.sh" "branch"

## USGS CROSSWALK ##
if [[ -f "${tempHucDataDir}/usgs_subset_gages.gpkg" ]]; then
    echo -e "${startDiv}USGS Crosswalk ${hucNumber} ${current_branch_id}"
    args=(
        -gages "${tempHucDataDir}/usgs_subset_gages.gpkg"
        -flows "${tempCurrentBranchDataDir}/demDerived_reaches_split_filtered_${current_branch_id}.parquet"
        -cat "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_${current_branch_id}.parquet"
        -dem "${tempCurrentBranchDataDir}/dem_meters_${current_branch_id}.tif"
        -dem_adj "${tempCurrentBranchDataDir}/dem_thalwegCond_${current_branch_id}.tif"
        -out "${tempCurrentBranchDataDir}"
        -b "${current_branch_id}"
        -huc_CRS "${huc_CRS}"
    )
    python3 "${srcDir}/usgs_gage_crosswalk.py" "${args[@]}"
fi

## REMOVE FILES FROM DENY LIST ##
if [[ -f "${deny_branches_list}" ]]; then
    echo -e "${startDiv}Remove files ${hucNumber} ${current_branch_id}"
    args=(
        -d "${tempCurrentBranchDataDir}"
        -l "${deny_branches_list}"
        -b "${current_branch_id}"
    )
    python3 "${srcDir}/outputs_cleanup.py" "${args[@]}"
fi

echo
