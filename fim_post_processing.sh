#!/bin/bash -e

:
usage ()
{
    echo 'Post processing for creating FIM hydrofabric.'
    echo 'Usage : fim_post_processing.sh [REQ: -n <run name> ]'
    echo '  	 				         [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -n/--runName    : A name to tag the output directories and log files.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '                    Note: Not the same variable name as fim_pipeline or fim_pre_processing'
    echo '                    and can be the multiplication of jobHucLimit and jobBranchLimit'
    echo
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

# TODO
# upgrade Dockerfile to add this as an env value
projectDir=/foss_fim


# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
fi

outputRunDataDir=$outputDataDir/$runName

## Check for run data directory ##
if [ ! -d "$outputRunDataDir" ]; then 
    echo "Depends on output from units and branches. Please provide an output folder name that has hucs/branches run."
    exit 1
fi

if [ "$jobLimit" = "" ]; then jobLimit=1; fi

# Clean out the other post processing files before starting
rm -rdf $outputRunDataDir/logs/src_optimization
rm -f $outputRunDataDir/logs/log_bankfull_indentify.log
rm -f $outputRunDataDir/logs/subdiv_src_.log

# load up enviromental information
args_file=$outputRunDataDir/runtime_args.env
gms_inputs=$outputRunDataDir/gms_inputs.csv

source $args_file
source $outputRunDataDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env


echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of fim_post_processing"
echo "---- Started: `date -u`" 
T_total_start
post_proc_start_time=`date +%s`


## AGGREGATE BRANCH LISTS INTO ONE ##
echo -e $startDiv"Start branch aggregation"
python3 $srcDir/aggregate_branch_lists.py -d $outputRunDataDir -f "branch_ids.csv" -o $gms_inputs


## GET NON ZERO EXIT CODES FOR UNITS ##
# Needed in case aggregation fails, we will need the logs
echo -e $startDiv"Start of unit non zero exit codes check"
find $outputRunDataDir/logs/ -name "*_unit.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/unit_errors/non_zero_exit_codes.log" &


## GET NON ZERO EXIT CODES FOR BRANCHES ##
echo -e $startDiv"Start non-zero exit code checking"
find $outputRunDataDir/logs/branch -name $hucNumber"_branch_*.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > "$outputRunDataDir/branch_errors/non_zero_exit_codes.log" &


## REMOVE FAILED BRANCHES ##
# Needed in case aggregation fails, we will need the logs
echo -e $startDiv"Removing branches that failed with Exit status: 61"
Tstart
python3 $srcDir/gms/remove_error_branches.py -f "$outputRunDataDir/branch_errors/non_zero_exit_codes.log" -g $gms_inputs

## RUN AGGREGATE BRANCH ELEV TABLES ##
echo "Processing usgs gage aggregation"
python3 $srcDir/usgs_gage_aggregate.py -fim $outputRunDataDir -gms $gms_inputs

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Estimating bankfull stage in SRCs"
    # Run SRC bankfull estimation routine routine
    Tstart
    python3 $srcDir/identify_src_bankfull.py -fim_dir $outputRunDataDir -flows $bankfull_flows_file -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    python3 $srcDir/subdiv_chan_obank_src.py -fim_dir $outputRunDataDir -mann $vmann_input_file -j $jobLimit
    Tcount
fi

## CONNECT TO CALIBRATION POSTGRESQL DATABASE (OPTIONAL) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$skipcal" = "0" ]; then
    if [ ! -f $CALB_DB_KEYS_FILE ]; then
        echo "ERROR! - the src_adjust_spatial parameter in the params_template.env (or equiv) is set to "True" (see parameter file), but the provided calibration database access keys file does not exist: $CALB_DB_KEYS_FILE"
        exit 1
    else
        source $CALB_DB_KEYS_FILE

        : '
        This makes the local variables from the calb_db_keys files
        into global variables that can be used in other files, including python.

        Why not just leave the word export in front of each of the keys in the
        calb_db_keys.env? Becuase that file is used against docker-compose
        when we start up that part of the sytem and it does not like the word
        export.
        '
        export CALIBRATION_DB_HOST=$CALIBRATION_DB_HOST
        export CALIBRATION_DB_NAME=$CALIBRATION_DB_NAME
        export CALIBRATION_DB_USER_NAME=$CALIBRATION_DB_USER_NAME
        export CALIBRATION_DB_PASS=$CALIBRATION_DB_PASS
        export DEFAULT_FIM_PROJECTION_CRS=$DEFAULT_FIM_PROJECTION_CRS

        Tstart
        echo "Populate PostgrSQL database with benchmark FIM extent points and HUC attributes (the calibration database)"
        echo "Loading HUC Data"
        echo

        ogr2ogr -overwrite -nln hucs -t_srs $DEFAULT_FIM_PROJECTION_CRS -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $inputDataDir/wbd/WBD_National.gpkg WBDHU8

        echo "Loading Point Data"
        echo
        ogr2ogr -overwrite -t_srs $DEFAULT_FIM_PROJECTION_CRS -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $fim_obs_pnt_data usgs_nws_benchmark_points -nln points

        Tcount
    fi
else
    echo "Skipping Populate PostgrSQL database"
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    echo    
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    python3 $srcDir/src_adjust_usgs_rating.py -run_dir $outputRunDataDir -usgs_rc $inputDataDir/usgs_gages/usgs_rating_curves.csv -nwm_recur $nwm_recur_file -j $jobLimit
    Tcount
    date -u
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINT DATABASE (POSTGRESQL) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]  && [ "$skipcal" = "0" ]; then
    Tstart
    echo
    echo -e $startDiv"Performing SRC adjustments using benchmark point database"
    python3 $srcDir/src_adjust_spatial_obs.py -fim_dir $outputRunDataDir -j $jobLimit
    Tcount
    date -u
fi

echo
echo -e $startDiv"Combining crosswalk tables"
# aggregate outputs
Tstart
python3 /foss_fim/tools/gms_tools/combine_crosswalk_tables.py -d $outputRunDataDir -o $outputRunDataDir/crosswalk_table.csv
Tcount
date -u

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of fim_post_processing complete"
echo "---- Ended: `date -u`"
Calc_Duration $post_proc_start_time
echo
