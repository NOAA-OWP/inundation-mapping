#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric at levelpath/branch scale. Execute gms_run_unit.sh prior to.'
    echo 'Usage : gms_run_branch.sh [REQ: -c <config file> -n <run name> ] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -r/--retry      : retries failed jobs'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -d/--denylist   : file with line delimited list of files in branches directories to remove '
    echo '                    upon completion. ie) config/deny_gms_branches_default.lst'
    echo '  -u/--hucList    : HUC8s to run or multiple passed in quotes (space delimited).'
    echo '                    A line delimited file also acceptable. HUCs must present in inputs directory.'
	echo '  -s/--dropStreamOrders : If this flag is included, the system will leave out stream orders 1 and 2'
	echo '                    at the initial load of the nwm_subset_streams'
    echo
    exit
}

if [ "$#" -lt 4 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -c|--configFile )
        shift
        envFile=$1
        ;;
    -n|--runName)
        shift
        runName=$1
        ;;
    -j|--jobLimit)
        shift
        jobLimit=$1
        ;;
    -u|--hucList)
        shift
        hucList=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    -o|--overwrite)
        overwrite=1
        ;;
    -r|--retry)
        retry="--retry-failed"
        overwrite=1
        ;;
    -d|--denylist)
        shift
        deny_gms_branches_list=$1
        ;;
	 -s|--dropLowStreamOrders)
		  dropLowStreamOrders=1
		  ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$envFile" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi
if [ "$overwrite" = "" ]
then
    overwrite=0
fi
if [ -z "$retry" ]
then
    retry=""
fi
if [ -z "$dropLowStreamOrders" ]
then
    dropLowStreamOrders=0
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $srcDir/bash_functions.env

## CONNECT TO CALIBRATION POSTGRESQL DATABASE (OPTIONAL) ##
if [ "$src_adjust_spatial" = "True" ]; then
    if [ ! -f $CALB_DB_KEYS_FILE ]; then
        echo "ERROR! - src_adjust_spatial parameter is set to "True" (see parameter file), but the provided calibration database access keys file does not exist: $CALB_DB_KEYS_FILE"
        exit 1
    else
        source $CALB_DB_KEYS_FILE
        echo "Populate PostgrSQL database with benchmark FIM extent points and HUC attributes"
        echo "Loading HUC Data"
        time ogr2ogr -overwrite -nln hucs -a_srs ESRI:102039 -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=calibration user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $input_WBD_gdb WBDHU8
        echo "Loading Point Data"
        time ogr2ogr -overwrite -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=calibration user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $fim_obs_pnt_data usgs_nws_benchmark_points -nln points
    fi
fi

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$outputDataDir/$runName
export deny_gms_branches_list=$deny_gms_branches_list
logFile=$outputRunDataDir/logs/branch/summary_gms_branch.log
export extent=GMS
export overwrite=$overwrite
export dropLowStreamOrders=$dropLowStreamOrders

## Check for run data directory ##
if [ ! -d "$outputRunDataDir" ]; then 
    echo "Depends on output from gms_run_unit.sh. Please produce data with gms_run_unit.sh first."
    exit 1
fi

## Filter out hucs ##
if [ "$hucList" = "" ]; then
    gms_inputs=$outputRunDataDir/gms_inputs.csv
else
    $srcDir/gms/filter_gms_inputs_by_huc.py -g $outputRunDataDir/gms_inputs.csv -u $hucList -o $outputRunDataDir/gms_inputs_filtered.csv
    gms_inputs=$outputRunDataDir/gms_inputs_filtered.csv
fi

# Echo intent to retry
if [ "$retry" = "--retry-failed" ]; then
    echo "Retrying failed unit level jobs for $runName"
fi 

# make log dir
if [ ! -d "$outputRunDataDir/logs/branch" ]; then
    mkdir -p $outputRunDataDir/logs/branch
elif [ $overwrite -eq 1 ]; then
    # need to clean it out if we are overwriting
    rm -rf $outputRunDataDir/logs/branch
    mkdir -p $outputRunDataDir/logs/branch
fi

if [ ! -d "$outputRunDataDir/branch_errors" ]; then
    mkdir -p "$outputRunDataDir/branch_errors"
elif [ $overwrite -eq 1 ]; then
    rm -rf $outputRunDataDir/branch_errors
    mkdir -p $outputRunDataDir/branch_errors
fi

## RUN GMS BY BRANCH ##
echo "================================================================================"
echo "Start of branch processing"
echo "Started: `date -u`" 

## Track total time of the overall run
T_total_start
Tstart

if [ "$jobLimit" -eq 1 ]; then
    parallel $retry --verbose --timeout $branch_timeout --lb  -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
else
    parallel $retry --eta --timeout $branch_timeout -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
fi

echo "Branch processing is complete"
Tcount
date -u

## RUN AGGREGATE BRANCH ELEV TABLES ##
# TODO: How do we skip aggregation if there is a branch error
# maybe against the non_zero logs above
echo 
echo "Processing usgs gage aggregation"
python3 $srcDir/usgs_gage_aggregate.py -fim $outputRunDataDir -gms $gms_inputs

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ]; then
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"$stopDiv
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    time python3 $srcDir/src_adjust_usgs_rating.py -run_dir $outputRunDataDir -usgs_rc $inputDataDir/usgs_gages/usgs_rating_curves.csv -nwm_recur $nwm_recur_file -run_dir $outputRunDataDir -j $jobLimit
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINT DATABASE (POSTGRESQL) ##
if [ "$src_adjust_spatial" = "False" ]; then
    echo -e $startDiv"Performing SRC adjustments using benchmark point database"$stopDiv
    time python3 foss_fim/src/src_adjust_spatial_obs.py -fim_dir $outputRunDataDir -j $jobLimit
fi

# -------------------
## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
echo
echo "Start non-zero exit code checking"
find $outputRunDataDir/logs/branch -name "*_branch_*.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/branch_errors/non_zero_exit_codes.log" &

echo "================================================================================"
echo "GMS_run_branch complete"
echo "Ended: `date -u`" 
echo