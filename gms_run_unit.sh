#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric datasets for unit scale.'
    echo 'Usage : gms_run_unit.sh [REQ: -u <hucs> -n <run name> ]'
    echo '                        [OPT: -h -j <job limit>]  -c <config file>'
    echo '                         -o -r -ud <unit deny list file>'
    echo '                         -zd <branch zero deny list file> -a <use all stream orders>]'
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
    echo '  -r/--retry      : retries failed jobs'
	echo '  -a/--UseAllStreamOrders : If this flag is included, the system will INCLUDE stream orders 1 and 2'
	echo '                    at the initial load of the nwm_subset_streams.'
	echo '                    Default (if arg not added) is false and stream orders 1 and 2 will be dropped'    
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
    -r|--retry)
        retry="--retry-failed"
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
if [ -z "$retry" ]
then
    retry=""
fi

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

if [ -d $outputRunDataDir ] && [ $overwrite -eq 0 ]; then
    echo
    echo "ERROR: Output dir $outputRunDataDir exists. Use overwrite -o to run."
    echo        
    usage
fi

## Set misc global variables
export overwrite=$overwrite
export dropLowStreamOrders=$dropLowStreamOrders

## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nwm_headwaters=$inputDataDir/nwm_hydrofabric/nwm_headwaters.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg
export input_nld_levee_protected_areas=$inputDataDir/nld_vectors/Levee_protected_areas.gpkg
export deny_gms_unit_list=$deny_gms_unit_list
export extent=GMS

export deny_unit_list=$deny_unit_list
export deny_branch_zero_list_for_units=$deny_branch_zero_list_for_units


# we are not using the variable output at this time, but keep it anways
num_hucs=$(python3 $srcDir/check_huc_inputs.py -u $hucList)

## Make output and data directories ##
if [ "$retry" = "--retry-failed" ]; then
    echo "Retrying failed unit level jobs for $runName"
fi

# make dirs
if [ ! -d $outputRunDataDir ]; then
    mkdir -p $outputRunDataDir
fi

# remove these directories on a new or overwrite run
rm -rdf $outputRunDataDir/logs
rm -rdf $outputRunDataDir/branch_errors
rm -rdf $outputRunDataDir/unit_errors

# we need to clean out the all log files overwrite or not
mkdir -p $outputRunDataDir/logs/unit
mkdir -p $outputRunDataDir/unit_errors

# copy over config file
cp -a $envFile $outputRunDataDir

## RUN GMS BY BRANCH ##
echo "=========================================================================="
echo "Start of unit processing"
echo "Started: `date -u`" 

## Track total time of the overall run
T_total_start
Tstart

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

echo "Unit (HUC) processing is complete"
Tcount
date -u

## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
echo -e $startDiv"Start of non zero exit codes check"$stopDiv
find $outputRunDataDir/logs/ -name "*_unit.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/unit_errors/non_zero_exit_codes.log" &

## AGGREGATE BRANCH LISTS INTO ONE ##
echo -e $startDiv"Start branch aggregation"$stopDiv
python3 $srcDir/gms/aggregate_branch_lists.py -d $outputRunDataDir -f "gms_inputs.csv" -l $hucList

echo "=========================================================================="
echo "gms_run_unit processing is complete"
Tcount
date -u
echo