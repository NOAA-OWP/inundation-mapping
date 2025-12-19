
calibration_rerun=$1
jobBranchLimit=$2 # should allow new values for rerun_calibrate_rating_curves.py

source $srcDir/bash_variables.env
source $srcDir/bash_functions.env

# Check if it is a calibration rerun and create a special params_rerun.env from template
if [ "${calibration_rerun,,}" = "true" ]; then
    # Copy params_template.env to params_rerun.env for calibration rerun
    envFile=$projectDir/config/params_template.env
    cp $envFile $outputDestDir/params_rerun.env
    source $outputDestDir/params_rerun.env
else
    source $outputDestDir/params.env
fi

#get huc number
hucNumber=$(basename "${tempHucDataDir%/}")

l_echo ""
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
if [ "${calibration_rerun,,}" = "true" ]; then
    l_echo "---- Rerunning calibration for HUC $hucNumber"
else
    l_echo "---- Start of Calibration for HUC $hucNumber"
fi
l_echo "---- Started: `date -u`"
l_echo ""


# Check if it is a calibration rerun or not
if [ "${calibration_rerun,,}" = "true" ]; then
    l_echo $startDiv"Reseting hydroTable & scr_full_crosswalked for branches"
    Tstart
    python3 $srcDir/reset_htable_src.py -huc_dir $tempHucDataDir
    Tcount
fi

## RUN AGGREGATE BRANCH ELEV TABLES ##
l_echo $startDiv"Processing usgs, ras2fim & ripple1d elev table aggregation"
Tstart
python3 $srcDir/aggregate_branches_to_huc.py \
    -huc_dir $tempHucDataDir \
    -elev \
    -ras \
    -ripple1d \
    # -huc_level $huc_level \
    # -i $fim_inputs \
    # -j $jobLimit
Tcount

## RUN THALWEG NOTCHES ADJUSTMENT ROUTINE ##
if [ "$thalweg_notches_adjustment" = "True" ]; then
    l_echo $startDiv"Performing thalweg notches adjustment routine"
    Tstart
    python3 $srcDir/thalweg_notches_adjustment.py \
        -huc_dir $tempHucDataDir

    Tcount
fi


## RUN LONGITUDINAL FILTER ROUTINE ##
if [ "$logitudinal_filter" = "True" ]; then
    l_echo $startDiv"Performing longitudinal discharge adjustment routine"
    Tstart
    python3 $srcDir/longitudinal_flow_adjustment.py \
        -huc_dir $tempHucDataDir

    Tcount
fi

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    l_echo $startDiv"Performing Bathymetry Adjustment routine"
    Tstart
    # Run bathymetry adjustment routine
    aibathy_toggle=${ai_toggle} #:-0}
    python3 $srcDir/bathymetric_adjustment.py \
        -huc_dir $tempHucDataDir \
        -bathy_ehydro $bathy_file_ehydro \
        -bathy_aibased $bathy_file_aibased \
        -ait $aibathy_toggle
    Tcount
fi

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    l_echo $startDiv"Estimating bankfull stage in SRCs"
    Tstart
    # Run SRC bankfull estimation routine routine
    python3 $srcDir/identify_src_bankfull.py \
        -huc_dir $tempHucDataDir \
        -flows $bankfull_flows_file \
        -jb $jobBranchLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ] && [ "$src_bankfull_toggle" = "True" ]; then
    l_echo $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    python3 $srcDir/subdiv_chan_obank_src.py \
        -huc_dir $tempHucDataDir \
        -mann $vmann_input_file \
        -jb $jobBranchLimit
    Tcount
fi

## RUN NONMONOTONIC SRC ADJUSTMENT ROUTINE ##
if [ "$nonmonotonic_src_adjustment" = "True" ]; then
    l_echo $startDiv"Performing Nonmonotonic SRC Adjustment routine"
    # Run Nonmonotonic SRCs Adjustment routine -flows $bankfull_flows_file \
    Tstart
    python3 $srcDir/nonmonotonic_src_adjustment.py \
        -huc_dir $tempHucDataDir
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_usgs_rating_trace.py \
        -huc_dir $tempHucDataDir \
        -usgs_rc $usgs_rating_curve_csv \
        -usgs_sites $usgs_acceptable_gages_path \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit 
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ RAS2FIM CROSS SECTION RATING CURVES ##
if [ "$src_adjust_ras2fim" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using ras2fim rating curve database"
    # Run SRC Optimization routine using ras2fim rating curve data (WSE and flow @ NWM recur flow values)
    # TODO: why we do not have "-ras_input $ras2fim_input_dir \"
    python3 $srcDir/src_adjust_ras2fim_rating.py \
        -huc_dir $tempHucDataDir \
        -ras_rc $ras_rating_curve_csv_filename \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ RIPPLE1d CROSS SECTION RATING CURVES ##
if [ "$src_adjust_ripple1d" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using ripple1d rating curve database"
    # Run SRC Optimization routine using ripple1d rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_ripple1d_rating.py \
        -huc_dir $tempHucDataDir \
        -ripple1d_rc $ripple1d_rating_curve_filename \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit
        # -huc_level $huc_level \
        # -ripple1d_input $ripple1d_input_dir \
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINTS (.parquet files) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -huc_dir $tempHucDataDir -jb $jobBranchLimit 
    Tcount
fi


## PERFORM MANUAL CALIBRATION
if [ "$manual_calb_toggle" = "True" ] && [ -f $man_calb_file ]; then
    l_echo $startDiv"Performing manual calibration"
    Tstart
    python3 $srcDir/src_manual_calibration.py \
        -huc_dir $tempHucDataDir \
        -calb_file $man_calb_file
    Tcount
fi


## AGGREGATE BRANCH TABLES ##
l_echo $startDiv"Aggregating branch hydrotables"
Tstart
python3 $srcDir/aggregate_branches_to_huc.py \
    -huc_dir $tempHucDataDir \
    -htable \
    -bridge \
    -road
Tcount


l_echo $startDiv"Scanning logs ..."
# Output files are named differently whether it is a rerun or fim pipeline

echo "Results will be saved inside log folder of each HUC."
Tstart
    if [ "${calibration_rerun,,}" = "true" ]; then
        error_log_filename="huc_${hucNumber}_errors_calib_rerun.log"
    else
        error_log_filename="huc_${hucNumber}_errors.log"
    fi

    error_log_path="$tempHucDataDir/logs/$error_log_filename"

    # Make sure NOT TO delete error_log_path, because it started logging from fim_process_huc.sh if an error occurs
    # [ -f "$error_log_path" ] && rm -f "$error_log_path"

    # For rerun mode: Only scan src_calibrations subdirectory which is updated during rerun
    # For normal mode: Scan entire logs directory

    # Run grep into a temporary file--saves the lines that contain the word “error”
    if [ "${calibration_rerun,,}" = "true" ]; then
        grep -H -R -i -n "error" --exclude="$error_log_filename" --exclude="$error_log_filename.tmp" "$tempHucDataDir/logs/src_calibrations" > "$error_log_path".tmp
    else
        grep -H -R -i -n "error" --exclude="$error_log_filename" --exclude="$error_log_filename.tmp" "$tempHucDataDir/logs/" > "$error_log_path".tmp
    fi

    # Only keep the file if it's non-empty (error were found)
    if [ -s "$error_log_path".tmp ]; then
        cat "$error_log_path".tmp >> "$error_log_path"
        rm -f "$error_log_path".tmp
        l_echo "There are some errors reported in log file \"$error_log_path\""
    else
        rm -f "$error_log_path".tmp
        l_echo "No errors found"
    fi

    # repeat the workflow for warning files
    if [ "${calibration_rerun,,}" = "true" ]; then
        warning_log_filename="huc_${hucNumber}_warnings_calib_rerun.log"
    else
        warning_log_filename="huc_${hucNumber}_warnings.log"
    fi
    warning_log_path="$tempHucDataDir/logs/$warning_log_filename"
    [ -f "$warning_log_path" ] && rm -f "$warning_log_path"
    if [ "${calibration_rerun,,}" = "true" ]; then
        grep -H -R -i -n "warning" --exclude="$warning_log_filename" --exclude="$warning_log_filename.tmp" "$tempHucDataDir/logs/src_calibrations" > "$warning_log_path".tmp
    else
        grep -H -R -i -n "warning" --exclude="$warning_log_filename" --exclude="$warning_log_filename.tmp" "$tempHucDataDir/logs/" > "$warning_log_path".tmp
    fi

    if [ -s "$warning_log_path".tmp ]; then
        mv "$warning_log_path".tmp "$warning_log_path"
    else
        rm -f "$warning_log_path".tmp
    fi
    l_echo "scan of log files done"
Tcount


