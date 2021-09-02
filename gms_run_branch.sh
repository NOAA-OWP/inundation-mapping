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
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -d/--denylist  : file with line delimited list of files in branches directories to remove upon completion'
    echo '                   (see config/deny_gms_branches_default.lst for a starting point)'
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
    -h|--help)
        shift
        usage
        ;;
    -o|--overwrite)
        overwrite=1
        ;;
    -d|--denylist)
        shift
        deny_gms_branches_list=$1
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
if [ "$deny_gms_branches_list" = "" ]
then
    usage
fi
if [ "$overwrite" = "" ]
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
export deny_gms_branches_list=$deny_gms_branches_list
logFile=$outputRunDataDir/logs/summary_gms_branch.log
export extent=GMS

## Make output and data directories ##
if [ -d "$outputRunDataDir" ]; then 
    branch_directories_count=$(find $outputRunDataDir/*/branches/ -maxdepth 1 -mindepth 1 -type d | wc -l)
    
    if [ $branch_directories_count -gt 0 ] && [ "$overwrite" -eq 1 ]; then
        find $outputRunDataDir/*/branches/ -maxdepth 1 -mindepth 1 -type d | xargs rm -rf 
    elif [ $branch_directories_count -gt 0 ] && [ "$overwrite" -eq 0 ] ; then
        echo "GMS branch data directories for $runName already exist. Use -o/--overwrite to continue"
        exit 1
    fi
else
    echo "Depends on output from gms_run_unit.sh. Please produce data with gms_run_unit.sh first."
    exit 1
fi

# make log dir
mkdir -p $outputRunDataDir/logs


## RUN GMS BY BRANCH ##
if [ "$jobLimit" -eq 1 ]; then
    parallel --verbose --timeout $branch_timeout --lb  -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $outputRunDataDir/gms_inputs.csv
else
    parallel --eta --timeout $branch_timeout -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $outputRunDataDir/gms_inputs.csv
fi

