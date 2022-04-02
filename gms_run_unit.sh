#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric datasets for unit scale. Run after fim_run.sh but before gms_run_branch.sh'
    echo 'Usage : gms_run_unit.sh [REQ: -u <hucs> -c <config file> -n <run name> ]'
    echo '  	 						 [OPT: -h -j <job limit>] -o -r -d <deny list file> -s ]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC 4,6,or 8 to run or multiple passed in quotes. Line delimited file'
    echo '                     also accepted. HUCs must present in inputs directory.'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time.'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -r/--retry      : retries failed jobs'
    echo '  -d/--denylist   : file with line delimited list of files in huc directories to remove upon completion'
    echo '                   (see config/deny_gms_unit_default.lst for a starting point)'
	 echo '  -s/--dropStreamOrder_1_2 : If this flag is included, the system will leave out stream orders 1 and 2 at the very'
	 echo	'                     top initial load of the nwm_subset_streams'
    exit
}

if [ "$#" -lt 6 ]
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
    -d|--denylist)
        shift
        deny_gms_unit_list=$1
        ;;
	 -s|--dropLowStreamOrders)
		  dropLowStreamOrders=1
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
if [ "$envFile" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi
if [ "$deny_gms_unit_list" = "" ]
then
    usage
fi
if [ -z "$overwrite" ]
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

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$outputDataDir/$runName
logFile=$outputRunDataDir/logs/unit/summary_gms_unit.log

## Set misc global variables
export overwrite=$overwrite
export dropLowStreamOrders=$dropLowStreamOrders

## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nhd_flowlines=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_streams_adj.gpkg
export input_nhd_headwaters=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_headwaters_adj.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg
export deny_gms_unit_list=$deny_gms_unit_list

## Input handling ##
$srcDir/check_huc_inputs.py -u "$hucList"

## Make output and data directories ##
if [ "$retry" = "--retry-failed" ]; then
    echo "Retrying failed unit level jobs for $runName"
fi

# make dirs
if [ ! -d $outputRunDataDir ]; then
    mkdir -p $outputRunDataDir
fi
if [ ! -d "$outputRunDataDir/logs/unit" ]; then
    mkdir -p $outputRunDataDir/logs/unit
fi
if [ ! -d "$outputRunDataDir/unit_errors" ]; then
    mkdir -p $outputRunDataDir/unit_errors
else
    if [ $overwrite -eq 1 ]; then
        rm -rf $outputRunDataDir/unit_errors/*.*
        rm -rf $outputRunDataDir/logs/unit/*.*

        # remove branch logs as well
        rm -rf $outputRunDataDir/branch_errors/*.*
        rm -rf $outputRunDataDir/logs/branch/*.*
    fi
fi

# copy over config file
cp -a $envFile $outputRunDataDir

## GMS BY UNIT##
if [ -f "$hucList" ]; then
    if [ "$jobLimit" -eq 1 ]; then
        parallel $retry --verbose --lb  -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    else
        parallel $retry --eta -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    fi
else 
    if [ "$jobLimit" -eq 1 ]; then
        parallel $retry --verbose --lb  -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    else
        parallel $retry --eta -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    fi
 fi

## AGGREGATE BRANCH LISTS INTO ONE ##
python3 $srcDir/gms/aggregate_branch_lists.py -l $hucList

## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
#cd $outputRunDataDir/logs/unit
find $outputRunDataDir/logs/unit/ -name "*_unit.log" -type f | xargs grep "Exit status: [1-9]" >"$outputRunDataDir/unit_errors/non_zero_exit_codes.log"

echo "================================================================================"
echo "Unit processing is complete"
echo