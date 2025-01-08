#!/bin/bash -e

:
usage()
{
    echo "
    Post processing for creating FIM hydrofabric.

    Usage : fim_post_processing.sh [REQ: -n <run name> ] [OPT: -h -j <job limit>]

    REQUIRED:
       -n/--runName    : A name to tag the output directories and log files.

    OPTIONS:
       -h/--help       : help file
       -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs
                         stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest.
                         Note: Not the same variable name as fim_pipeline or fim_pre_processing
                         and can be the multiplication of jobHucLimit and jobBranchLimit
    "
    exit
}

while [ "$1" != "" ]; do
case $1
in
    -n|--runName)
        shift
        runName=$1
        ;;
    -j|--jobLimit)
        shift
        jobLimit=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
    exit 22
fi

outputDestDir=$outputsDir/$runName

## Check for output destination directory ##
if [ ! -d "$outputDestDir" ]; then
    echo "Depends on output from units and branches. "
    echo "Please provide an output folder name that has hucs/branches run."
    exit 1
fi

#########################################################################################
#                                                                                       #
# PLEASE DO NOT USE the job limits coming in from the runtime_args.env                  #
# Most of the time, post processing will not be run on the same servers                 #
# that is running fim_process_unit_wb.sh and the processing power                       #
# used to run fim_post_processing.sh will be different (hence.. different job limit)    #
#                                                                                       #
#########################################################################################

if [ "$jobLimit" = "" ]; then jobLimit=1; fi

# Clean out the other post processing files before starting
rm -rdf $outputDestDir/logs/src_optimization
rm -f $outputDestDir/logs/log_bankfull_indentify.log
rm -f $outputDestDir/logs/subdiv_src_.log
rm -f $log_file_name

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the post processing log
log_file_name=$outputDestDir/post_proc.log
Set_log_file_path $log_file_name

l_echo ""
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing"
l_echo "---- Started: `date -u`"
echo ""
T_total_start
post_proc_start_time=`date +%s`

## RUN UPDATE HYDROTABLE AND SRC ##
# Define the counter file

Tstart
COUNTER_FILE="${outputDestDir}/post_processing_attempt.txt"
# Function to clean up
cleanup() {
    if [ "$SUCCESS" = true ]; then
        if [ -f "$COUNTER_FILE" ]; then
            COUNTER=$(cat "$COUNTER_FILE")
            if [ "$COUNTER" -eq 1 ]; then
                l_echo "Counter is 1. Removing the counter file."
                rm "$COUNTER_FILE"
            fi
        fi
    fi
}

# Set up trap to call cleanup on EXIT, ERR, and INT (interrupt signal)
trap cleanup EXIT ERR INT
# Initialize the counter file if it doesn't exist
if [ ! -f "$COUNTER_FILE" ]; then
    echo 0 > "$COUNTER_FILE"
fi

# Read the current counter value
COUNTER=$(cat "$COUNTER_FILE")

# Increment the counter
COUNTER=$((COUNTER + 1))

# Save the new counter value
l_echo "$COUNTER" > "$COUNTER_FILE"

# Check if the counter is greater than one
if [ "$COUNTER" -gt 1 ]; then
    # Execute the Python file
    l_echo "Updating hydroTable & scr_full_crosswalked for branches"
    python3 $srcDir/update_htable_src.py -d $outputDestDir
    Tcount
else
    l_echo "Execution count is $COUNTER, not executing the update_htable_src.py file."
fi


## AGGREGATE BRANCH LISTS INTO ONE ##
l_echo $startDiv"Start branch aggregation"
Tstart
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f "branch_ids.csv" -o $fim_inputs
Tcount

## GET NON ZERO EXIT CODES FOR BRANCHES ##
l_echo $startDiv"Start non-zero exit code checking"
find $outputDestDir/logs/branch -name "*_branch_*.log" -type f | \
    xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > \
    "$outputDestDir/branch_errors/non_zero_exit_codes.log" &

## RUN AGGREGATE BRANCH ELEV TABLES ##
l_echo $startDiv"Processing usgs & ras2fim elev table aggregation"
Tstart
python3 $srcDir/aggregate_by_huc.py -fim $outputDestDir -i $fim_inputs -elev -ras -j $jobLimit
Tcount

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    echo -e $startDiv"Performing Bathymetry Adjustment routine"    
    # Run bathymetry adjustment routine
    ai_toggle=${ai_toggle:-0}
    Tstart
    python3 $srcDir/bathymetric_adjustment.py \
        -fim_dir $outputDestDir \
        -bathy_ehydro $bathy_file_ehydro \
        -bathy_aibased $bathy_file_aibased \
        -buffer $wbd_buffer \
        -wbd $inputsDir/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg \
        -j $jobLimit \
        -ait $ai_toggle
    Tcount
fi

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    l_echo $startDiv"Estimating bankfull stage in SRCs"
    Tstart
    # Run SRC bankfull estimation routine routine
    python3 $srcDir/identify_src_bankfull.py \
        -fim_dir $outputDestDir \
        -flows $bankfull_flows_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ] && [ "$src_bankfull_toggle" = "True" ]; then
    l_echo $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    python3 $srcDir/subdiv_chan_obank_src.py \
        -fim_dir $outputDestDir \
        -mann $vmann_input_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ] && [ "$skipcal" = "0" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_usgs_rating_trace.py \
        -run_dir $outputDestDir \
        -usgs_rc $usgs_rating_curve_csv \
        -nwm_recur $nwm_recur_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ RAS2FIM CROSS SECTION RATING CURVES ##
if [ "$src_adjust_ras2fim" = "True" ] && [ "$src_subdiv_toggle" = "True" ] && [ "$skipcal" = "0" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using ras2fim rating curve database"
    # Run SRC Optimization routine using ras2fim rating curve data (WSE and flow @ NWM recur flow values)
    python3 $srcDir/src_adjust_ras2fim_rating.py \
        -run_dir $outputDestDir \
        -ras_input $ras2fim_input_dir \
        -ras_rc $ras_rating_curve_csv_filename \
        -nwm_recur $nwm_recur_file \
        -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINTS (.parquet files) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]  && [ "$skipcal" = "0" ]; then
    Tstart
    l_echo $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -fim_dir $outputDestDir -j $jobLimit
    Tcount
fi

## AGGREGATE BRANCH TABLES ##
l_echo $startDiv"Aggregating branch hydrotables"

Tstart
python3 $srcDir/aggregate_by_huc.py \
    -fim $outputDestDir \
    -i $fim_inputs \
    -htable \
    -bridge \
    -j $jobLimit
Tcount


## PERFORM MANUAL CALIBRATION
if [ "$manual_calb_toggle" = "True" ] && [ -f $man_calb_file ]; then
    l_echo $startDiv"Performing manual calibration"
    Tstart
    python3 $srcDir/src_manual_calibration.py \
        -fim_dir $outputDestDir \
        -calb_file $man_calb_file
    Tcount
fi


l_echo $startDiv"Combining crosswalk tables"
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv
Tcount


l_echo $startDiv"Resetting Permissions"
Tstart
    # super slow to change chmod on the log folder. Not really manditory anyways
    find $outputDestDir -maxdepth 1 -type f -exec chmod 777 {} +  # just root level files
Tcount


l_echo $startDiv"Scanning logs for errors and warnings. This can take quite a few minutes so stand by."
echo "Results will be saved in root not inside the log folder."
Tstart
    # grep -H -r -i -n "error" $outputDestDir/logs/ > $outputDestDir/all_errors_from_logs.log
    find $outputDestDir -type f | grep -H -R -i -n ".*error.*" $outputDestDir/logs/ > \
         $outputDestDir/all_errors_from_logs.log &
    l_echo "error scan done, now on to warnings scan"

    find $outputDestDir -type f | grep -H -R -i -n ".*warning.*" $outputDestDir/logs/ > \
         $outputDestDir/all_warnings_from_logs.log &
    l_echo "warning scan done"
Tcount

echo
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- End of fim_post_processing"
l_echo "---- Ended: `date -u`"
Calc_Duration "Post Processing Duration:" $post_proc_start_time
echo
