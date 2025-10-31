
calibration_rerun=$1
jobBranchLimit=$2 # should allow new values for rerun_calibrate_rating_curves.py


source $outputDestDir/params.env
source $srcDir/bash_variables.env
source $srcDir/bash_functions.env

#get huc number
hucNumber=$(basename "${tempHucDataDir%/}")

l_echo ""
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
if [ "${calibration_rerun,,}" = "true" ]; then
    l_echo "---- Manual postprcessing for HUC $hucNumber"
else
    l_echo "---- Start of post_processing for HUC $hucNumber"
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
l_echo $startDiv"Processing usgs & ras2fim elev table aggregation"
Tstart
python3 $srcDir/aggregate_branches_to_huc.py -huc_dir $tempHucDataDir -elev -ras
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
    python3 $srcDir/src_adjust_ras2fim_rating.py \
        -huc_dir $tempHucDataDir \
        -ras_rc $ras_rating_curve_csv_filename \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit
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
    -bridge
Tcount



l_echo $startDiv"Scanning logs for errors and warnings..."
echo "Results will be saved inside log folder of each HUC."
Tstart
    out_name="huc_errors_from_logs.log"
    outpath="$tempHucDataDir/logs/$out_name"

    # Always delete old file if it exists
    [ -f "$outpath" ] && rm -f "$outpath"

    # Run grep into a temporary file
    grep -H -R -i -n "error" --exclude="$out_name" --exclude="$out_name.tmp" "$tempHucDataDir/logs/" > "$outpath".tmp

    # Only keep the file if it's non-empty
    if [ -s "$outpath".tmp ]; then
        mv "$outpath".tmp "$outpath"
    else
        rm -f "$outpath".tmp
    fi
    l_echo "scan of errors done"

    # repreat the workflow for warning files

    out_name="huc_warnings_from_logs.log"
    outpath="$tempHucDataDir/logs/$out_name"
    [ -f "$outpath" ] && rm -f "$outpath"
    grep -H -R -i -n "warning" --exclude="$out_name" --exclude="$out_name.tmp" "$tempHucDataDir/logs/" > "$outpath".tmp
    if [ -s "$outpath".tmp ]; then
        mv "$outpath".tmp "$outpath"
    else
        rm -f "$outpath".tmp
    fi
    l_echo "scan of warnings done"
Tcount



#TODO: make sure to clean the log folder in fim pipeline since logs should be huc-level created.


