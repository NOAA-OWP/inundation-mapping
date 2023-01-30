#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric datasets for unit scale.'
    echo 'Usage : gms_run_unit.sh [REQ: -u <hucs> -n <run name> ]'
    echo '                        [OPT: -h -c <config file> -j <job limit> -o'
    echo '                         -ud <unit deny list file>'
    echo '                         -zd <branch zero deny list file>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC8s to run or multiple passed in quotes (space delimited) file.'
    echo '                    A line delimited file is also acceptable. HUCs must present in inputs directory.'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '                    Default (if arg not added) : /foss_fim/config/params_template.env'        
    echo '  -ud/--unitDenylist : A file with a line delimited list of files in UNIT (HUC) directories to be removed'
    echo '                    upon completion (see config/deny_gms_unit_prod.lst for a starting point)'
    echo '                    Default (if arg not added) : /foss_fim/config/deny_gms_unit_prod.lst'
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'
    echo '  -zd/--branchZeroDenylist : A file with a line delimited list of files in BRANCH ZERO directories to' 
    echo '                    be removed upon completion of branch zero processing.'
    echo '                    (see config/deny_gms_branch_zero.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_gms_branch_zero.lst'   
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'    
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time.'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo
    exit
}

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
    -ud|--unitDenylist)
        shift
        deny_unit_list=$1
        ;;
    -zd|--branchZeroDenylist)
        shift
        deny_branch_zero_list_for_units=$1
        ;;        
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    echo "ERROR: Missing -u Huclist argument"
    usage
fi
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
fi

if [ "$envFile" = "" ]
then
    envFile=/foss_fim/config/params_template.env
fi

if [ "$deny_unit_list" = "" ]
then
    deny_unit_list=/foss_fim/config/deny_gms_unit_prod.lst
elif [ "${deny_unit_list^^}" != "NONE" ] && [ ! -f "$deny_unit_list" ]
then
    # NONE is not case sensitive
    echo "Error: The -ud <unit deny file> does not exist and is not the word NONE"
    usage
fi

if [ "$deny_branch_zero_list_for_units" = "" ]
then
    deny_branch_zero_list_for_units=/foss_fim/config/deny_gms_branch_zero.lst
elif [ "${deny_branch_zero_list_for_units^^}" != "NONE" ]   # NONE is not case sensitive
then
    if [ ! -f "$deny_branch_zero_list_for_units" ]
    then
        echo "Error: The -zd <branch zero deny file> does not exist and is not the word NONE"
        usage
    fi
fi

if [ -z "$overwrite" ]
then
    overwrite=0
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=1
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$outputDataDir/$runName
logFile=$outputRunDataDir/logs/unit/summary_gms_unit.log

if [ -d $outputRunDataDir ] && [ $overwrite -eq 0 ]; then
    echo
    echo "ERROR: Output dir $outputRunDataDir exists. Use overwrite -o to run."
    echo        
    usage
fi

## Define inputs
export overwrite=$overwrite
export deny_gms_unit_list=$deny_gms_unit_list
export deny_unit_list=$deny_unit_list
export deny_branch_zero_list_for_units=$deny_branch_zero_list_for_units

# we are not using the variable output at this time, but keep it anways
num_hucs=$(python3 $srcDir/check_huc_inputs.py -u $hucList)

# make dirs
if [ ! -d $outputRunDataDir ]; then
    mkdir -p $outputRunDataDir
fi

# remove these directories on a new or overwrite run
rm -rdf $outputRunDataDir/logs
rm -rdf $outputRunDataDir/branch_errors
rm -rdf $outputRunDataDir/unit_errors

# we need to clean out the all log files and some other files overwrite or not
mkdir -p $outputRunDataDir/logs/unit
mkdir -p $outputRunDataDir/unit_errors
rm -f $outputRunDataDir/gms_inputs*

# copy over config file
cp -a $envFile $outputRunDataDir

## RUN GMS BY UNIT ##
echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of gms_run_unit"
echo "---- Started: `date -u`" 
all_units_start_time=`date +%s`

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

echo "Unit (HUC) processing is complete"
date -u

## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
echo -e $startDiv"Start of non zero exit codes check"$stopDiv
find $outputRunDataDir/logs/ -name "*_unit.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/unit_errors/non_zero_exit_codes.log" &

## AGGREGATE BRANCH LISTS INTO ONE ##
echo -e $startDiv"Start branch aggregation"$stopDiv
python3 $srcDir/gms/aggregate_branch_lists.py -d $outputRunDataDir -f "gms_inputs.csv" -l $hucList

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- gms_run_unit is complete"
echo "---- Ended: `date -u`"
Calc_Duration $all_units_start_time
echo