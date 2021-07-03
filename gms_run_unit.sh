#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric datasets for unit scale. Run after fim_run.sh but before gms_run_branch.sh'
    echo 'Usage : gms_run.sh [REQ: -u <hucs> -c <config file> -n <run name> ] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC 4,6,or 8 to run or multiple passed in quotes. Line delimited file'
    echo '                     also accepted. HUCs must present in inputs directory.'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
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
if [ -z "$overwrite" ]
then
    overwrite=0
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
logFile=$outputRunDataDir/logs/summary_gms_unit.log

## Input handling ##
$srcDir/check_huc_inputs.py -u "$hucList"

## Make output and data directories ##
if [ -d "$outputRunDataDir" ]; then
    gms_directories_count=$(find $outputRunDataDir -iname "gms" -type d | wc -l)
    if [ $gms_directories_count -gt 0 ] && [ "$overwrite" -eq 1 ]; then
         rm -rf $gms_directories_list
    elif [ $gms_directories_count -gt 0 ] && [ "$overwrite" -eq 0 ] ; then
        echo "GMS data directory for $runName already exist. Use -o/--overwrite to continue"
        exit 1
    fi
else
    echo "GMS depends on Full Resolution Data. Please produce data with fim_run.sh first."
    exit 1
fi

# make log dir
mkdir -p $outputRunDataDir/logs


## GMS BY UNIT##
if [ -f "$hucList" ]; then
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb  -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh :::: $hucList
    fi
else 
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb  -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $srcDir/gms/time_and_tee_run_by_unit.sh ::: $hucList
    fi
 fi


## AGGREGATE BRANCH LISTS INTO ONE ##
python3 $srcDir/gms/aggregate_branch_lists.py -l $hucList
