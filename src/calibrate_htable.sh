
## RUN UPDATE HYDROTABLE AND SRC ##
hucNumber=$1

echo -e ""
echo -e"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo -e "---- Start of HUC post_processing for $hucNumber"
echo -e "---- Started: `date -u`"
echo ""



# TODO  this may need to revise. This will be applied for branches parallel
if [ "$jobLimit" = "" ]; then jobLimit=1; fi

# Tstart
# COUNTER_FILE="${tempHucDataDir}/post_processing_attempt.txt"
# # Function to clean up
# cleanup() {
#     if [ "$SUCCESS" = true ]; then
#         if [ -f "$COUNTER_FILE" ]; then
#             COUNTER=$(cat "$COUNTER_FILE")
#             if [ "$COUNTER" -eq 1 ]; then
#                 echo -e "Counter is 1. Removing the counter file."
#                 rm "$COUNTER_FILE"
#             fi
#         fi
#     fi
# }

# # Set up trap to call cleanup on EXIT, ERR, and INT (interrupt signal)
# trap cleanup EXIT ERR INT
# # Initialize the counter file if it doesn't exist
# if [ ! -f "$COUNTER_FILE" ]; then
#     echo 0 > "$COUNTER_FILE"
# fi

# # Read the current counter value
# COUNTER=$(cat "$COUNTER_FILE")

# # Increment the counter
# COUNTER=$((COUNTER + 1))

# # Save the new counter value
# echo -e "$COUNTER" > "$COUNTER_FILE"

# Check if the counter is greater than one
# if [ "$COUNTER" -gt 1 ]; then
#     # Execute the Python file
#     echo -e $startDiv"Updating hydroTable & scr_full_crosswalked for branches"
#     python3 $srcDir/update_htable_src.py -d $tempHucDataDir
#     Tcount
# else
#     echo -e "Execution count is $COUNTER, not executing the update_htable_src.py file."
# fi


## RUN AGGREGATE BRANCH ELEV TABLES ##
echo -e $startDiv"Processing usgs & ras2fim elev table aggregation"
Tstart
python3 $srcDir/aggregate_by_huc.py -huc_dir $tempHucDataDir -elev -ras -j $jobLimit
Tcount

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    echo -e $startDiv"Performing Bathymetry Adjustment routine"
    Tstart
    # Run bathymetry adjustment routine
    aibathy_toggle=${ai_toggle} #:-0}
    python3 $srcDir/bathymetric_adjustment.py \
        -huc_dir $tempHucDataDir \
        -bathy_ehydro $bathy_file_ehydro \
        -bathy_aibased $bathy_file_aibased \
        -j $jobLimit \
        -ait $aibathy_toggle
    Tcount
fi

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Estimating bankfull stage in SRCs"
    Tstart
    # Run SRC bankfull estimation routine routine
    python3 $srcDir/identify_src_bankfull.py \
        -huc_dir $tempHucDataDir \
        -flows $bankfull_flows_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ] && [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    python3 $srcDir/subdiv_chan_obank_src.py \
        -huc_dir $tempHucDataDir \
        -mann $vmann_input_file \
        -j $jobLimit
    Tcount
fi

## RUN NONMONOTONIC SRC ADJUSTMENT ROUTINE ##
if [ "$nonmonotonic_src_adjustment" = "True" ]; then
    echo -e $startDiv"Performing Nonmonotonic SRC Adjustment routine"
    # Run Nonmonotonic SRCs Adjustment routine -flows $bankfull_flows_file \
    Tstart
    python3 $srcDir/nonmonotonic_src_adjustment.py \
        -huc_dir $tempHucDataDir \
        -j $jobLimit
    Tcount
fi

## RUN LONGITUDINAL FILTER ROUTINE ##
if [ "$logitudinal_filter" = "True" ]; then
    echo -e $startDiv"Performing longitudinal discharge adjustment routine"
    Tstart
    python3 $srcDir/longitudinal_flow_adjustment.py \
        -huc_dir $tempHucDataDir \
        -j $jobLimit \

    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ] && [ "$skipcal" = "0" ]; then
    Tstart
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_usgs_rating_trace.py \
        -huc_dir $tempHucDataDir \
        -usgs_rc $usgs_rating_curve_csv \
        -usgs_sites $usgs_acceptable_gages_path \
        -nwm_recur $nwm_recur_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ RAS2FIM CROSS SECTION RATING CURVES ##
if [ "$src_adjust_ras2fim" = "True" ] && [ "$src_subdiv_toggle" = "True" ] && [ "$skipcal" = "0" ]; then
    Tstart
    echo -e $startDiv"Performing SRC adjustments using ras2fim rating curve database"
    # Run SRC Optimization routine using ras2fim rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_ras2fim_rating.py \
        -huc_dir $tempHucDataDir \
        -ras_input $ras2fim_input_dir \
        -ras_rc $ras_rating_curve_csv_filename \
        -nwm_recur $nwm_recur_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINTS (.parquet files) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]  && [ "$skipcal" = "0" ]; then
    Tstart
    echo -e $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -huc_dir $tempHucDataDir -j $jobLimit
    Tcount
fi


## PERFORM MANUAL CALIBRATION
if [ "$manual_calb_toggle" = "True" ] && [ -f $man_calb_file ]; then
    echo -e $startDiv"Performing manual calibration"
    Tstart
    python3 $srcDir/src_manual_calibration.py \
        -huc_dir $tempHucDataDir \
        -calb_file $man_calb_file
    Tcount
fi


## AGGREGATE BRANCH TABLES ##
echo -e $startDiv"Aggregating branch hydrotables"
Tstart
python3 $srcDir/aggregate_by_huc.py \
    -huc_dir $tempHucDataDir \
    -htable \
    -bridge \
    -j $jobLimit
Tcount


