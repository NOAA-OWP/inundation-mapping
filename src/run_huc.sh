#!/bin/bash -e
# The addition of the -e tells it to stop on fail and is critical
### Yes.. not all of our .sh files are the same with the -e flag, be design.

# Do not call this file directly. Call fim_process_unit_wb.sh which calls
# this file.

## SOURCE FILE AND FUNCTIONS ##
# load the various enviro files
source $srcDir/bash_functions.env

args_file=$outputDestDir/runtime_args.env
source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

export HYDRA_LAUNCHER="fork"
export DISPLAY=":0"

branch_list_csv_file=$tempHucDataDir/branch_ids.csv

# This file is used only to help run_huc know what branches to process
# Not all branches will be successful
branch_list_lst_file=$tempHucDataDir/branch_ids_for_huc_processing.lst

branchSummaryLogFile=$tempHucDataDir/logs/"$hucNumber"_summary_branch.log
# Version with cleaned up columns
branchSummaryLog_Adj_File=$tempHucDataDir/logs/"$hucNumber"_summary_branch_adj.csv
huc2Identifier=${hucNumber:0:2}

## SET input DEM domain
if [ $huc2Identifier -eq 19 ]; then
    huc_input_DEM_domain=$input_DEM_domain_Alaska
    input_DEM=$input_DEM_Alaska
    input_pit_fill=$input_DEM_pit_fills_Alaska
    input_bridge_elev_diff=$input_bridge_elev_diff_alaska

elif [ $hucNumber -eq 22010000 ]; then
    huc_input_DEM_domain=$input_DEM_domain_Guam
    input_DEM=$input_DEM_Guam
    input_pit_fill=$input_DEM_pit_fills_Guam
    input_bridge_elev_diff=$input_bridge_elev_diff_guam

elif [ $hucNumber -eq 22030001 ]; then
    huc_input_DEM_domain=$input_DEM_domain_AmericanSamoa
    input_DEM=$input_DEM_AmericanSamoa
    input_pit_fill=$input_DEM_pit_fills_AmericanSamoa
    input_bridge_elev_diff=$input_bridge_elev_diff_americansamoa

else
    huc_input_DEM_domain=$input_DEM_domain
    input_DEM=$input_DEM
    input_pit_fill=$input_DEM_pit_fills
    input_bridge_elev_diff=$input_bridge_elev_diff

fi

huc_CRS=$(get_crs_for_huc "$hucNumber")

echo -e ${startDiv}"Using CRS: ${huc_CRS}" ## debug

## INITIALIZE TOTAL TIME TIMER ##
T_total_start
huc_start_time="$(date +%s)"
date -u

## Copy HUC's pre-clipped .gpkg files from $pre_clip_huc_dir (use -a & /. -- only copies folder's contents)
echo -e "${startDiv}Copying staged wbd and .gpkg files from ${pre_clip_huc_dir}/${hucNumber}"
cp -R "${pre_clip_huc_dir}/${hucNumber}/." "${tempHucDataDir}"

# Copy necessary files from $inputsDir into ${tempHucDataDir} to avoid File System Collisions
# For buffer_stream_branches.py
cp "${huc_input_DEM_domain}" "${tempHucDataDir}"

# For usgs_gage_unit_setup.py
cp "${nws_lid}" "${tempHucDataDir}/nws_lid.gpkg"

# Renamed to usgs_gages.gpkg while being copied
cp "${usgs_gages_file}" "${tempHucDataDir}/usgs_gages.gpkg"

# Check if the ${hucNumber} directory exists in the ras2fim $inputsDir
if [[ -d "${ras2fim_input_dir}/${hucNumber}" ]]; then
    ras_rating_gpkg="${ras2fim_input_dir}/${hucNumber}/${ras_rating_curve_gpkg_filename}"
    ras_rating_csv="${ras2fim_input_dir}/${hucNumber}/${ras_rating_curve_csv_filename}"
    if [[ -f "${ras_rating_gpkg}" ]]; then
        cp "${ras_rating_gpkg}" "${tempHucDataDir}"
        echo -e "Copied ${ras_rating_gpkg} to ${tempHucDataDir}"
    else
        echo -e "File ${ras_rating_gpkg} does not exist. Skipping copy."
    fi
    if [[ -f "${ras_rating_csv}" ]]; then
        cp "${ras_rating_csv}" "${tempHucDataDir}"
        echo -e "Copied ${ras_rating_csv} to ${tempHucDataDir}"
    else
        echo -e "File ${ras_rating_csv} does not exist. Skipping copy."
    fi
fi

## DERIVE LEVELPATH  ##
echo -e "${startDiv}Generating Level Paths for ${hucNumber}"
args=(
    -i "${tempHucDataDir}/nwm_subset_streams.gpkg"
    -s "${tempHucDataDir}/wbd_buffered_streams.gpkg"
    -b "${branch_id_attribute}"
    -r "ID"
    -o "${tempHucDataDir}/nwm_subset_streams_levelPaths.parquet"
    -d "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved.parquet"
    -de "${tempHucDataDir}/nwm_subset_streams_levelPaths_extended.parquet"
    -e "${tempHucDataDir}/nwm_headwaters.gpkg"
    -c "${tempHucDataDir}/nwm_catchments_proj_subset.gpkg"
    -t "${tempHucDataDir}/nwm_catchments_proj_subset_levelPaths.parquet"
    -n "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved_headwaters.parquet"
    -w "${tempHucDataDir}/nwm_lakes_proj_subset.gpkg"
    -wbd "${tempHucDataDir}/wbd.gpkg"
    -u "${hucNumber}"
)
python3 "${srcDir}/derive_level_paths.py" "${args[@]}"

# check if level paths exists
levelpaths_exist=1
if [[ ! -f "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved.parquet" ]]; then levelpaths_exist=0; fi

## ASSOCIATE LEVEL PATHS WITH LEVEES
echo -e "${startDiv}Associate level paths with levees"
if [[ -f "${tempHucDataDir}/nld_subset_levees.gpkg" ]]; then
    args=(
        -nld "${tempHucDataDir}/nld_subset_levees.gpkg"
        -s "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved.parquet"
        -lpa "${tempHucDataDir}/LeveeProtectedAreas_subset.gpkg"
        -out "${tempHucDataDir}/levee_levelpaths.csv"
        -w "${levee_buffer}"
        -b "${branch_id_attribute}"
        -l "${levee_id_attribute}"
    )
    python3 "${srcDir}/associate_levelpaths_with_levees.py" "${args[@]}"
fi

## STREAM BRANCH POLYGONS
echo -e "${startDiv}Generating Stream Branch Polygons for ${hucNumber}"
args=(
    -s "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved.parquet"
    -i "${branch_id_attribute}"
    -d "${branch_buffer_distance_meters}"
    -b "${tempHucDataDir}/branch_polygons.parquet"
    -w "${tempHucDataDir}/wbd_buffered.gpkg"
)
python3 "${srcDir}/buffer_stream_branches.py" "${args[@]}"

## CREATE BRANCHID LIST FILE
echo -e "${startDiv}Create list file of branch ids for ${hucNumber}"
args=(
    -d "${tempHucDataDir}/nwm_subset_streams_levelPaths_dissolved.parquet"
    -b "${branch_id_attribute}"
    -o "${branch_list_lst_file}"
)
python3 "${srcDir}/generate_branch_list.py" "${args[@]}"

## CREATE BRANCH ZERO ##
branch0_start_time="$(date +%s)"

echo -e "${startDiv}Creating branch zero for ${hucNumber}"
tempCurrentBranchDataDir="${tempBranchDataDir}/${branch_zero_id}"

## MAKE OUTPUT BRANCH DIRECTORY
mkdir -p ${tempCurrentBranchDataDir}

## CLIP RASTERS
echo -e "${startDiv}Clipping rasters to branches ${hucNumber} ${branch_zero_id}"
gdal_opts=(
    -cutline "${tempHucDataDir}/wbd_buffered.gpkg"
    -crop_to_cutline
    -ot "Float32"
    -r near
    -of "GTiff"
    -overwrite
    -co "BLOCKXSIZE=512"
    -co "BLOCKYSIZE=512"
    -co "TILED=YES"
    -co "COMPRESS=LZW"
    -co "BIGTIFF=YES"
    -t_srs "${huc_CRS}"
    -tr "${res}" "${res}"
    -tap
)
gdalwarp "${gdal_opts[@]}" "${input_DEM}" "${tempHucDataDir}/dem_meters_orig.tif"
gdalwarp "${gdal_opts[@]}" "${input_pit_fill}" "${tempHucDataDir}/dem_meters_pit_fill.tif"
gdalwarp "${gdal_opts[@]}" "${input_bridge_elev_diff}" "${tempHucDataDir}/bridge_elev_diff_meters.tif"

## Combine Raw DEM with Pit Fill DEM (use pit fill elev)
gdal_opts=(
    -ot "Float32"
    -of "GTiff"
    -co "BLOCKXSIZE=512"
    -co "BLOCKYSIZE=512"
    -co "TILED=YES"
    -co "COMPRESS=LZW"
    -co "BIGTIFF=YES"
    -overwrite
)

input_tifs=(
    "${tempHucDataDir}/dem_meters_orig.tif"
    "${tempHucDataDir}/dem_meters_pit_fill.tif"
)

output_tif="${tempHucDataDir}/dem_meters.tif"

gdalwarp "${gdal_opts[@]}" "${input_tifs[@]}" "${output_tif}"

## GET RASTER METADATA
echo -e "${startDiv}Get DEM Metadata ${hucNumber} ${branch_zero_id}"
read -r ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy \
    <<<"$("${srcDir}/getRasterInfoNative.py" -r "${tempHucDataDir}/dem_meters.tif")"

## RASTERIZE NLD MULTILINES ##
echo -e "${startDiv}Rasterize all NLD multilines using zelev vertices ${hucNumber} ${branch_zero_id}"
# REMAINS UNTESTED FOR AREAS WITH LEVEES
if [[ -f "${tempHucDataDir}/3d_nld_subset_levees_burned.gpkg" ]]; then
    args=(-q -l "3d_nld_subset_levees_burned" -3d -at -a_nodata "${ndv}"
        -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
        -ot "Float32" -of "GTiff"
        -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" -co "COMPRESS=LZW" -co "BIGTIFF=YES"
        -co "TILED=YES"
        "${tempHucDataDir}/3d_nld_subset_levees_burned.gpkg"
        "${tempCurrentBranchDataDir}/nld_rasterized_elev_${branch_zero_id}.tif"
    )
    gdal_rasterize "${args[@]}"
fi

## BURN LEVEES INTO DEM ##
echo -e "${startDiv}Burn nld levees into dem & convert nld elev to meters"
echo -e "(*Overwrite dem_meters.tif output) ${hucNumber} ${branch_zero_id}"
# REMAINS UNTESTED FOR AREAS WITH LEVEES
if [[ -f "${tempCurrentBranchDataDir}/nld_rasterized_elev_${branch_zero_id}.tif" ]]; then
    args=(
        -dem "${tempHucDataDir}/dem_meters.tif"
        -nld "${tempCurrentBranchDataDir}/nld_rasterized_elev_${branch_zero_id}.tif"
        -out "${tempHucDataDir}/dem_meters.tif"
    )
    python3 "${srcDir}/burn_in_levees.py" "${args[@]}"
fi

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCH 0 (include all NWM streams) ##
echo -e "${startDiv}Rasterize Reach Boolean ${hucNumber} ${branch_zero_id}"
args=(
    -q -ot "Int32" -burn "1" -init "0" -a_nodata "-9999"
    -co "BIGTIFF=YES"
    -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
    "${tempHucDataDir}/nwm_subset_streams.gpkg"
    "${tempCurrentBranchDataDir}/flows_grid_boolean_${branch_zero_id}.tif"
)
gdal_rasterize "${args[@]}"

## RASTERIZE REACH BOOLEAN (1 & 0) - BRANCHES (Not 0) (NWM levelpath streams) ##
if [[ "${levelpaths_exist}" == "1" ]]; then
    echo -e "${startDiv}Rasterize Reach Boolean ${hucNumber} (Branches)"
    args=(
        -q -ot "Int32" -burn "1" -init "0" -a_nodata "-9999"
        -co "BIGTIFF=YES"
        -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
        "${tempHucDataDir}/nwm_subset_streams_levelPaths_extended.parquet"
        "${tempHucDataDir}/flows_grid_boolean.tif"
    )
    python3 "${srcDir}/rasterize_parquet.py" "${args[@]}"
fi

## RASTERIZE NWM Levelpath HEADWATERS (1 & 0) ##
echo -e "${startDiv}Rasterize NWM Headwaters ${hucNumber} ${branch_zero_id}"
args=(
    -q -at -ot "Int32" -burn "1" -init "0" -a_nodata "-9999"
    -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "TILED=YES"
    -te "${xmin}" "${ymin}" "${xmax}" "${ymax}" -ts "${ncols}" "${nrows}"
    "${tempHucDataDir}/nwm_headwater_points_subset.gpkg"
    "${tempCurrentBranchDataDir}/headwaters_${branch_zero_id}.tif"
)
gdal_rasterize "${args[@]}"

## DEM Reconditioning - BRANCH 0 (include all NWM streams) ##
# Using AGREE methodology, hydroenforce the DEM so that it is consistent with the supplied stream network.
# This allows for more realistic catchment delineation which is ultimately reflected in the output FIM mapping.
echo -e "${startDiv}Creating AGREE DEM using $agree_DEM_buffer meter buffer ${hucNumber} ${branch_zero_id}"
args=(
    -r "${tempCurrentBranchDataDir}/flows_grid_boolean_${branch_zero_id}.tif"
    -d "${tempHucDataDir}/dem_meters.tif"
    -w "${tempCurrentBranchDataDir}"
    -o "${tempCurrentBranchDataDir}/dem_burned_${branch_zero_id}.tif"
    -b "$agree_DEM_buffer"
    -sm "10"
    -sh "1000"
)
python3 "${srcDir}/agreedem.py" "${args[@]}"

## PIT REMOVE BURNED DEM - BRANCH 0 (include all NWM streams) ##
echo -e "${startDiv}Pit remove Burned DEM ${hucNumber} ${branch_zero_id}"
args=(
    "${tempCurrentBranchDataDir}/dem_burned_${branch_zero_id}.tif"
    "${tempCurrentBranchDataDir}/dem_burned_filled_${branch_zero_id}.tif"
)
rd_depression_filling "${args[@]}"

## D8 FLOW DIR - BRANCH 0 (include all NWM streams) ##
echo -e $startDiv"D8 Flow Directions on Burned DEM $hucNumber ${branch_zero_id}"
args=(
    -n "$ncores_fd"
    -t "${taudemDir2}"
    -fel "${tempCurrentBranchDataDir}/dem_burned_filled_${branch_zero_id}.tif"
    -p "${tempCurrentBranchDataDir}/flowdir_d8_burned_filled_${branch_zero_id}.tif"
)
python3 "${srcDir}/run_taudem_subprocess.py" d8flowdir "${args[@]}"

## MAKE A COPY OF THE DEM and DEM DIFF FOR BRANCH 0
echo -e "${startDiv}Copying DEM to Branch 0"
cp "${tempHucDataDir}/dem_meters.tif" "${tempCurrentBranchDataDir}/dem_meters_${branch_zero_id}.tif"
cp "${tempHucDataDir}/bridge_elev_diff_meters.tif" "${tempCurrentBranchDataDir}/bridge_elev_diff_meters_${branch_zero_id}.tif"


## PRODUCE THE REM AND OTHER HAND FILE OUTPUTS ##
export hucNumber current_branch_id tempCurrentBranchDataDir tempHucDataDir ndv xmin ymin xmax ymax ncols nrows

## PRODUCE BRANCH ZERO HAND
"${srcDir}/delineate_hydros_and_produce_HAND.sh" "unit"

## CREATE USGS GAGES FILE
## Note: the usgs_gages.gpkg was renamed during copying into the unit folder
if [[ -f "${tempHucDataDir}/nwm_subset_streams_levelPaths.parquet" ]]; then
    echo -e "${startDiv}Assigning USGS gages to branches for ${hucNumber}"
    args=(
        -gages "${tempHucDataDir}/usgs_gages.gpkg"
        -nwm "${tempHucDataDir}/nwm_subset_streams_levelPaths.parquet"
        -ras "${tempHucDataDir}/${ras_rating_curve_gpkg_filename}"
        -o "${tempHucDataDir}/usgs_subset_gages.gpkg"
        -huc "${hucNumber}"
        -ahps "${tempHucDataDir}/nws_lid.gpkg"
        -bzero_id "${branch_zero_id}"
        -huc_CRS "${huc_CRS}"
    )
    python3 "${srcDir}/usgs_gage_unit_setup.py" "${args[@]}"
fi

## USGS CROSSWALK ##
if [[ -f "${tempHucDataDir}/usgs_subset_gages_${branch_zero_id}.gpkg" ]]; then
    echo -e "${startDiv}USGS Crosswalk ${hucNumber} ${branch_zero_id}"
    args=(
        -gages "${tempHucDataDir}/usgs_subset_gages_${branch_zero_id}.gpkg"
        -flows "${tempCurrentBranchDataDir}/demDerived_reaches_split_filtered_${branch_zero_id}.parquet"
        -cat "${tempCurrentBranchDataDir}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_${branch_zero_id}.parquet"
        -dem "${tempCurrentBranchDataDir}/dem_meters_${branch_zero_id}.tif"
        -dem_adj "${tempCurrentBranchDataDir}/dem_thalwegCond_${branch_zero_id}.tif"
        -out "${tempCurrentBranchDataDir}"
        -b "${branch_zero_id}"
        -huc_CRS "${huc_CRS}"
    )
    python3 "${srcDir}/usgs_gage_crosswalk.py" "${args[@]}"
fi

## CLEANUP BRANCH ZERO OUTPUTS ##
echo -e "${startDiv}Cleaning up outputs in branch zero ${hucNumber}"
python3 "${srcDir}/outputs_cleanup.py" -d "${tempCurrentBranchDataDir}" -l "${deny_branch_zero_list}" -b "${branch_zero_id}"

branch0=$(Calc_Time $branch0_start_time)
branch0_percent=$(Calc_Time_Minutes_in_Percent $branch0_start_time)

# -------------------
## Processing Branches ##
echo "+++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of branch processing for $hucNumber using $jobBranchLimit workers for branch processing"
branch_processing_start_time=`date +%s`

# We do not want a branch to shut down the huc, so process_branch.sh always sends
# back an exit status of 0.
# We don't have an answer for what if all branches failed at this time.
# Each will return an independant exit code.
if [ -f $branch_list_lst_file ]; then
    date -u
    Tstart
    # There may not be a branch_ids.lst if there were no level paths (no stream orders 3+)
    # but there will still be a branch zero
    # Define parallel options in an array
    args=(
    --timeout "${branch_timeout}"
    -j "${jobBranchLimit}"
    --joblog "${branchSummaryLogFile}"
    --colsep ','
    --line-buffer
    )

    # Execute GNU Parallel using the array
    parallel "${args[@]}" -- "${srcDir}/process_branch.sh" "${runName}" "${hucNumber}" :::: "${branch_list_lst_file}" || true
    Tcount
else
    echo "Exit Status: 63 - No level paths exist with this HUC. Processing branch zero only."
fi

# We should have a summary file now
# but it is possible we do not have one if there are no non branch 0 branches left
if [ -f $branchSummaryLogFile ]; then

    # Adjust branch summary parallel log to more readable format
    # Changing the 3rd col (Starttime from epoch time to human readable d/t)
    # and 4th column from seconds and milliseconds to min
    awk 'BEGIN {
        FS="\t"
        OFS="\t"
    } 
    NR==1 {
        print
        next
    } 
    {
        $3=strftime("%m/%d/%Y..%H:%M:%S", $3)
        $4=sprintf("%.2fm", $4/60)
        print
    }' "$branchSummaryLogFile" > "$branchSummaryLog_Adj_File"
fi

# TODO: Jul 2026: Add a test to see if we have any valid completed branches so we can issues a special
# new status code of 6x (need a new one), that we can catch better. Low priority
# It is continuing on to the calibrate tools even though it does not need too.

# -------------------
branches="$(Calc_Time ${branch_processing_start_time})"
branches_percent="$(Calc_Time_Minutes_in_Percent ${branch_processing_start_time})"

## REMOVE FILES FROM DENY LIST ##
if [[ -f "${deny_unit_list}" ]]; then
    echo -e "${startDiv}Remove files ${hucNumber}"
    date -u
    Tstart  # TODO: Do we need a tstart and count on this
    $srcDir/outputs_cleanup.py -d $tempHucDataDir -l $deny_unit_list -b $hucNumber
    Tcount
fi

## ADJUST CALIBRATION
## call src adjustments..Pass False as an argument to flag it is not a rerun of calibration. 
$srcDir/calibrate_rating_curves.sh "False" $jobBranchLimit $hucNumber

## Start the local csv branch list
echo -e $startDiv"Generating Branch List that have successfully completed"
$srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber

echo "---- HUC $hucNumber - branches have now been processed"
Calc_Duration "Duration for processing branches : " $branch_processing_start_time
echo
total_branches=$(wc -l < $branch_list_csv_file)

# WRITE TO LOG FILE CONTAINING ALL HUC PROCESSING TIMES
total_duration_display="${hucNumber},$(Calc_Time "${huc_start_time}"),$(Calc_Time_Minutes_in_Percent "${huc_start_time}"),${total_branches},${branch0},${branch0_percent},${branches},${branches_percent}"
echo -e "${total_duration_display}" >> "${tempHucDataDir}/processing_time_${hucNumber}.txt"

# Yes.. we let this log to the hucLogFile so error seach tools can look only in this huc file.
date -u
echo "---- HUC processing for $hucNumber is complete"
Calc_Duration "Duration for huc processing : " $huc_start_time
echo

# let the script return whatever code it wants unless controlled exit like calibrate_rating_curves.sh