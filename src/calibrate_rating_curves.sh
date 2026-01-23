#!/bin/bash -e


# ***************
# IMPORTANT

# Note. We can not use the echo command in this script at all. echo
#       is designed to be log and echo and can only be used at by scripts
#       such as fim_post_processsing.sh and fim_process_huc.sh as they are the
#       top most shell scripts. 
#       We need to use echo in all scenarios as this script always
#       bubbles up to either run_huc.sh or rerun_calibration.py


# TODO: We don't have a wrapper script like fim_process_huc.sh or 
# process_branch.sh.  This script is called in one of two ways.
# 1) As part of run_huc.sh which rolls up to fim_process_huc.sh so it catches
#    errors and echo text. Those are done via run_huc.sh -> fim_process_huc.sh
#    via the "tee" in fim_process_huc.sh as well as it's exit code / error trapping
#    in it.

# 2) In re-run calibration mode, where this script is called from tools/rerun_calibration.py.
#    This results in a very wide range of by_products.
#    a) It does not have a "tee" command to catch all rolled up echo commmands.
#       

# Run as a standalone script, but it currently has no way to handle errors
#    as it does not have an error trap. We might want to add one directly in 
#    but then we have to make sure we are correctly handling errors if it is
#    Type 1 above.
# We have to do a little think here. We do want run_huc.sh to handle errors
# from here but what about when we run it stand alone. 
# hummmm. Might need to experiment.

# When we call this as part of the run_huc.sh, it is part of either an AWS
# pipeline or via fim_pipeline.sh and all logs are auto bubble up as part of the
# bash "tee" commands. But, when we are in rerun mode, there is no "tee" to catch
# it and bubble it up. We will have to create our own log file and write to it
# but only when we are in that mode.

# In re-run mode, we are workign against the actual outputs_temp directory, but 
# when we are not, we are workign against the outputs directory. As a result,
# we will fake out what the value of tempHucDataDir.


# source $srcDir/bash_variables.env
# source $srcDir/bash_functions.env


# Check if it is a calibration rerun and create a special params_rerun.env from template
# if [ "${calibration_rerun,,}" = "true" ]; then
#     # Copy params_template.env to params_rerun.env for calibration rerun

#     # This can not be done here as this script is part of a MP and we get duplication and file collisions if this
#     # works against the outpuDestDir. 
#     # It can/must run the source command to use it

#     envFile=$projectDir/config/params_template.env
#     cp $envFile $outputDestDir/params_rerun.env
#     source $outputDestDir/params_rerun.env

    
#     # TODO:
#     # see if we can use stdout to get all echo's from rerun_calibration.py

# else
#     source $outputDestDir/params.env
# fi

echo ""
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
if [ "$calibration_rerun" = "true" ]; then
    echo "---- Rerunning Calibration for HUC $hucNumber"
else
    echo "---- Start of Calibration for HUC $hucNumber"
fi
echo "---- Started: `date -u`"
echo ""


# Check if it is a calibration rerun or not
if [ "$calibration_rerun" = "true" ]; then
    echo $startDiv"Reseting hydroTable & scr_full_crosswalked for branches"
    Tstart
    python3 $srcDir/reset_htable_src.py -huc_dir $tempHucDataDir
    Tcount
fi


## RUN AGGREGATE BRANCH ELEV TABLES ##
echo $startDiv"Processing usgs & ras2fim elev table aggregation"
Tstart
python3 $srcDir/aggregate_branches_to_huc.py -huc_dir $tempHucDataDir -elev -ras
Tcount


## RUN THALWEG NOTCHES ADJUSTMENT ROUTINE ##
if [ "$thalweg_notches_adjustment" = "True" ]; then
    echo $startDiv"Performing thalweg notches adjustment routine"
    Tstart
    python3 $srcDir/thalweg_notches_adjustment.py \
        -huc_dir $tempHucDataDir
    Tcount
fi


## RUN LONGITUDINAL FILTER ROUTINE ##
if [ "$logitudinal_filter" = "True" ]; then
    echo $startDiv"Performing longitudinal discharge adjustment routine"
    Tstart
    python3 $srcDir/longitudinal_flow_adjustment.py \
        -huc_dir $tempHucDataDir

    Tcount
fi

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    echo $startDiv"Performing Bathymetry Adjustment routine"
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
    echo $startDiv"Estimating bankfull stage in SRCs"
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
    echo $startDiv"Performing SRC channel/overbank subdivision routine"
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
    echo $startDiv"Performing Nonmonotonic SRC Adjustment routine"
    # Run Nonmonotonic SRCs Adjustment routine -flows $bankfull_flows_file \
    Tstart
    python3 $srcDir/nonmonotonic_src_adjustment.py \
        -huc_dir $tempHucDataDir
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    echo $startDiv"Performing SRC adjustments using USGS rating curve database"
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
    echo $startDiv"Performing SRC adjustments using ras2fim rating curve database"
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
    echo $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -huc_dir $tempHucDataDir -jb $jobBranchLimit 
    Tcount
fi


## PERFORM MANUAL CALIBRATION
if [ "$manual_calb_toggle" = "True" ] && [ -f $man_calb_file ]; then
    echo $startDiv"Performing manual calibration"
    Tstart
    python3 $srcDir/src_manual_calibration.py \
        -huc_dir $tempHucDataDir \
        -calb_file $man_calb_file
    Tcount
fi


## AGGREGATE BRANCH TABLES ##
echo $startDiv"Aggregating branch hydrotables"
Tstart
python3 $srcDir/aggregate_branches_to_huc.py \
    -huc_dir $tempHucDataDir \
    -htable \
    -bridge \
    -road
Tcount

echo "---- End of Calibration for HUC $hucNumber"
echo "---- Ended: `date -u`"
echo ""

exit 0  # yes.. the default