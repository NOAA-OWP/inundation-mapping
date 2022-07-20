#!/bin/bash -e
:
usage ()
{
    echo
    echo 'Produce GMS hydrofabric datasets for unit and branch scale.'
    echo 'Usage : gms_pipeline.sh [REQ: -u <hucs> - -n <run name> ]'
    echo '                        [OPT: -h -c <config file> -j <job limit>] -o -r'
    echo '                         -ud <unit deny list file> -bd <branch deny list file>'
    echo '                         -so <drop stream orders 1 and 2> ]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC8s to run or multiple passed in quotes (space delimited) file.'
    echo '                    A line delimited file is also acceptable. HUCs must present in inputs directory.'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo 
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '                    Default (if arg not added) : /foss_fim/config/params_template.env'    
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time.'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -r/--retry      : retries failed jobs'
    echo '  -ud/--unitDenylist : A file with line delimited list of files in UNIT (HUC) directories to remove'
    echo '                    upon completion (see config/deny_gms_unit_default.lst for a starting point)'
    echo '                    Default (if arg not added) : /foss_fim/config/deny_gms_unit_default.lst'
    echo '  -bd/--branchDenylist : A file with line delimited list of files in branches directories to remove' 
    echo '                    upon completion (see config/deny_gms_branches_min.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_gms_branches_min.lst'    
	echo '  -a/--UseAllStreamOrders : If this flag is included, the system will INCLUDE stream orders 1 and 2'
	echo '                    at the initial load of the nwm_subset_streams.'
	echo '                    Default (if arg not added) is false and stream orders 1 and 2 will be dropped'    
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
    -u|--hucList)
        shift
        hucList="$1"
        ;;
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
    -ud|--unitDenylist)
        shift
        deny_gms_unit_list=$1
        ;;
    -bd|--branchDenylist)
        shift
        deny_gms_branches_list=$1
        ;;
    -a|--useAllStreamOrders)
        useAllStreamOrders=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi
if [ "$envFile" = "" ]
then
    envFile=/foss_fim/config/params_template.env
fi
if [ "$deny_gms_unit_list" = "" ]
then
   deny_gms_unit_list=/foss_fim/config/deny_gms_unit_default.lst
fi
if [ "$deny_gms_branches_list" = "" ]
then
   deny_gms_branches_list=/foss_fim/config/deny_gms_branches_min.lst
fi
if [ -z "$overwrite" ]
then
    # default is false (0)
    overwrite=0
fi
if [ -z "$retry" ]
then
    retry=""
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $srcDir/bash_functions.env

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi

## Define Outputs Data Dir ##
export outputRunDataDir=$outputDataDir/$runName
# make dirs
if [ ! -d $outputRunDataDir ]; then
    mkdir -p $outputRunDataDir
else
    if [ $overwrite -eq 0 ]; then
        echo
        echo "ERROR: Output dir $outputRunDataDir exists. Use overwrite -o to run."
        echo        
        usage
    fi
fi

## Set misc global variables
export overwrite=$overwrite

# invert useAllStreamOrders boolean (to make it historically compatiable
# with other files like gms/run_unit.sh and gms/run_branch.sh).
# Yet help user understand that the inclusion of the -a flag means
# to include the stream order (and not get mixed up with older versions
# where -s mean drop stream orders)
# This will encourage leaving stream orders 1 and 2 out.
if [ "$useAllStreamOrders" == "1" ]; then
    export dropLowStreamOrders=0
else
    export dropLowStreamOrders=1
fi

export deny_gms_branches_list=$deny_gms_branches_list

## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nhd_flowlines=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_streams_adj.gpkg
export input_nhd_headwaters=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_headwaters_adj.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg

export extent=GMS

## Input handling ##
# The very first "print" statement results in the .py file
# become standard out here. And the very first standard out value
# gets auto mapped to a new variable here called num_hucs

check_huc_cmd="python3 $srcDir/check_huc_inputs.py"
check_huc_cmd+=" -u $hucList"
IFS=, read num_hucs <<< $($check_huc_cmd)

## Make output and data directories ##
if [ "$retry" = "--retry-failed" ]; then
    echo "Retrying failed unit level jobs for $runName"
fi

# we need to clean out the all log files overwrite or not
# as we will count the number of those files and use the
# percent of hucs (failed / all) to determine if we continue

rm -rf $outputRunDataDir/logs/unit/
mkdir -p $outputRunDataDir/logs/unit

rm -rf $outputRunDataDir/unit_errors/
mkdir -p $outputRunDataDir/unit_errors

rm -rf $outputRunDataDir/logs/branch
mkdir -p $outputRunDataDir/logs/branch

rm -rf $outputRunDataDir/branch_errors
mkdir -p $outputRunDataDir/branch_errors

unit_logFile=$outputRunDataDir/logs/unit/summary_gms_unit.log
branch_logFile=$outputRunDataDir/logs/branch/summary_gms_branch.log

export deny_gms_unit_list=$deny_gms_unit_list
export deny_gms_branches_list=$deny_gms_branches_list

# copy over config file
cp -a $envFile $outputRunDataDir

## RUN GMS BY UNIT ##
echo "================================================================================"
echo "Start of unit processing"
echo "Started: `date -u`" 
echo "$num_hucs to be processed"

## Track total time of the overall run
T_total_start
Tstart

if [ -f "$hucList" ]; then
    if [ "$jobLimit" -eq 1 ]; then
        parallel $retry --verbose --lb  -j $jobLimit --joblog $unit_logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    else
        parallel $retry --eta -j $jobLimit --joblog $unit_logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    fi
else 
    if [ "$jobLimit" -eq 1 ]; then
        parallel $retry --verbose --lb  -j $jobLimit --joblog $unit_logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    else
        parallel $retry --eta -j $jobLimit --joblog $unit_logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    fi
 fi

echo "Units (HUC's) processing is complete"
Tcount
date -u

## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
echo "Start of unit non zero exit codes check"
find $outputRunDataDir/logs/ -name "*_unit.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/unit_errors/non_zero_exit_codes.log" &

## AGGREGATE BRANCH LISTS INTO ONE ##
echo "Start unit branch aggregation"
python3 $srcDir/gms/aggregate_branch_lists.py -d $outputRunDataDir -f "gms_inputs.csv" -l $hucList

echo "================================================================================"
echo "Unit processing is complete"
Tcount
date -u

## CHECK IF OK TO CONTINUE ON TO BRANCH STEPS
# Count the number of files in the $outputRunDataDir/unit_errors
# If no errors, there will be only one file, non_zero_exit_codes.log.
# Calculate the number of error files as a percent of the number of hucs 
# originally submitted. If the percent error is over "x" threshold stop processing
# Note: This applys only if there are a min number of hucs. Aka.. if min threshold
# is set to 10, then only return a sys.exit of > 1, if there is at least 10 errors

# if this has too many errors, it will return a sys.exit code (like 62 as per fim_enums)
# and we will stop the rest of the process. We have to catch stnerr as well.
python3 $srcDir/check_unit_errors.py -f $outputRunDataDir -n $num_hucs


## RUN GMS BY BRANCH ##
echo
echo "================================================================================"
echo "Start of branch processing"

gms_inputs=$outputRunDataDir/gms_inputs.csv

if [ "$jobLimit" -eq 1 ]; then
    parallel $retry --verbose --timeout $branch_timeout --lb  -j $jobLimit --joblog $branch_logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
else
    parallel $retry --eta --timeout $branch_timeout -j $jobLimit --joblog $branch_logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
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

# -------------------
## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
echo
echo "Start non-zero exit code checking"
find $outputRunDataDir/logs/branch -name "*_branch_*.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/branch_errors/non_zero_exit_codes.log" &

echo "================================================================================"
echo "gms_pipeline complete"
Tcount
date -u
echo