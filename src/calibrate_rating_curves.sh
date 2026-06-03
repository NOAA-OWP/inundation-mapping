#!/bin/bash -e
umask 000

### Must have the -e flag as it auto aborts on page error. See more details
### below about error handling for it.

# Receive arguments passed from process_rerun_calibration_huc.sh
# (passed explicitly because source commands in the parent script can overwrite exported variables)
calibration_rerun=$1
jobBranchLimit=$2
hucNumber=$3


# ***************
# IMPORTANT NOTES:

# l_echo: We can not use the l_echo command in this script at all. echo
#       is designed to be log and echo and can only be used at by scripts
#       such as fim_post_processsing.sh and fim_process_huc.sh as they are the
#       top most shell scripts. 
#       We need to use echo in all scenarios as this script always
#       bubbles up to either run_huc.sh or rerun_calibration.py

# This script is called in one of two ways:
#   1)  As part of the fim_process_huc.sh -> run_huc.sh process. 
#       When we call this as part of the run_huc.sh, it is part of either an AWS
#       pipeline or via fim_pipeline.sh and all logs are auto bubble up as part of the
#       bash "tee" commands. But, when we are in rerun mode, there is no "tee" to catch
#       it and bubble it up. We will have to create our own log file and write to it
#       but only when we are in that mode.
#   2)  As part of the recalibration process: rerun-calibration.py -> process_rerun_calibration_huc.sh

#       Details coming

# In re-run mode, we are working against the actual outputs_temp directory, but 
# when we are not, we are working against the outputs directory. As a result,
# we will fake out what the value of tempHucDataDir.


echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
if [ "$calibration_rerun" = "true" ]; then
    echo "---- Rerunning Calibration for HUC $hucNumber"
else
    echo "---- Start of Calibration for HUC $hucNumber"
fi
echo "---- Started: `date -u`"
echo

# Check if it is a calibration rerun or not
if [ "$calibration_rerun" = "true" ]; then
    echo -e $startDiv"Reseting hydroTable & scr_full_crosswalked for branches"
    python3 $srcDir/reset_htable_src.py -huc_dir $tempHucDataDir
fi


## RUN AGGREGATE BRANCH ELEV TABLES ##
echo -e $startDiv"Processing usgs & ras2fim elev table aggregation"
python3 $srcDir/aggregate_branches_to_huc.py -huc_dir $tempHucDataDir -elev -ras


## RUN THALWEG NOTCHES ADJUSTMENT ROUTINE ##
if [ "$thalweg_notches_adjustment" = "True" ]; then
    echo -e $startDiv"Performing thalweg notches adjustment routine"
    python3 $srcDir/thalweg_notches_adjustment.py -huc_dir $tempHucDataDir
fi


## RUN LONGITUDINAL FILTER ROUTINE ##
if [ "$logitudinal_filter" = "True" ]; then
    echo -e $startDiv"Performing longitudinal discharge adjustment routine"
    python3 $srcDir/longitudinal_flow_adjustment.py -huc_dir $tempHucDataDir
fi

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    echo -e $startDiv"Performing Bathymetry Adjustment routine"
    # Run bathymetry adjustment routine
    aibathy_toggle=${ai_toggle} #:-0}
    python3 $srcDir/bathymetric_adjustment.py \
        -huc_dir $tempHucDataDir \
        -bathy_ehydro $bathy_file_ehydro \
        -bathy_aibased $bathy_file_aibased \
        -ait $aibathy_toggle
fi

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Estimating bankfull stage in SRCs"
    # Run SRC bankfull estimation routine routine
    python3 $srcDir/identify_src_bankfull.py \
        -huc_dir $tempHucDataDir \
        -flows $bankfull_flows_file \
        -jb $jobBranchLimit
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ] && [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    python3 $srcDir/subdiv_chan_obank_src.py \
        -huc_dir $tempHucDataDir \
        -mann $vmann_input_file \
        -jb $jobBranchLimit
fi

## RUN NONMONOTONIC SRC ADJUSTMENT ROUTINE ##
if [ "$nonmonotonic_src_adjustment" = "True" ]; then
    echo -e $startDiv"Performing Nonmonotonic SRC Adjustment routine"
    # Run Nonmonotonic SRCs Adjustment routine -flows $bankfull_flows_file \
    python3 $srcDir/nonmonotonic_src_adjustment.py \
        -huc_dir $tempHucDataDir
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_usgs_rating_trace.py \
        -huc_dir $tempHucDataDir \
        -usgs_rc $usgs_rating_curve_csv \
        -usgs_sites $usgs_acceptable_gages_path \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit 
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ RAS2FIM CROSS SECTION RATING CURVES ##
if [ "$src_adjust_ras2fim" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC adjustments using ras2fim rating curve database"
    # Run SRC Optimization routine using ras2fim rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_ras2fim_rating.py \
        -huc_dir $tempHucDataDir \
        -ras_rc $ras_rating_curve_csv_filename \
        -nwm_recur $nwm_recur_file \
        -jb $jobBranchLimit
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINTS (.parquet files) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -huc_dir $tempHucDataDir -jb $jobBranchLimit 
fi

## PERFORM MANUAL CALIBRATION
if [ "$manual_calb_toggle" = "True" ] && [ -f $man_calb_file ]; then
    echo -e $startDiv"Performing manual calibration"
    python3 $srcDir/src_manual_calibration.py \
        -huc_dir $tempHucDataDir \
        -calb_file $man_calb_file
fi

## AGGREGATE BRANCH TABLES ##
echo -e $startDiv"Aggregating branch hydrotables"
python3 $srcDir/aggregate_branches_to_huc.py \
    -huc_dir $tempHucDataDir \
    -htable \
    -bridge \
    -road \
    -building

echo "---- End of Calibration for HUC $hucNumber"
echo "---- Ended: `date -u`"
echo
