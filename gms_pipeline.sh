#!/bin/bash -e
:
usage ()
{
    echo
    echo 'Produce GMS hydrofabric datasets for unit and branch scale.'
    echo 'Usage : gms_pipeline.sh [REQ: -u <hucs> - -n <run name> ]'
    echo '                        [OPT: -h -c <config file> -j <job limit>] -o -r'
    echo '                         -ud <unit deny list file> -bd <branch deny list file>'
    echo '                         -a <use all stream orders> ]'
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
    echo '                    upon completion (see config/deny_gms_unit_prod.lst for a starting point)'
    echo '                    Default (if arg not added) : /foss_fim/config/deny_gms_unit_prod.lst'
    echo '                    -- Note: if you want all output files (aka.. no files removed),'
    echo '                    use the word "none" as this value for this parameter.'
    echo '  -bd/--branchDenylist : A file with line delimited list of files in branches directories to remove' 
    echo '                    upon completion (see config/deny_gms_branches_prod.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_gms_branches_prod.lst'   
    echo '                    -- Note: if you want all output files (aka.. no files removed),'
    echo '                    use the word "none" as this value for this parameter.'
	echo '  -a/--UseAllStreamOrders : If this flag is included, the system will INCLUDE stream orders 1 and 2'
    echo '                    at the initial load of the nwm_subset_streams.'
    echo '                    Default (if arg not added) is false and stream orders 1 and 2 will be dropped'    
    echo
    exit
}

set -e

while [ "$1" != "" ]; do
case $1
in
    -u|--hucList)
        shift
        hucList=$1
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
if [ "$deny_gms_unit_list" = "" ]
then
   deny_gms_unit_list=/foss_fim/config/deny_gms_unit_prod.lst
fi
if [ "$deny_gms_branches_list" = "" ]
then
   deny_gms_branches_list=/foss_fim/config/deny_gms_branches_prod.lst
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

export outputRunDataDir=$outputDataDir/$runName

if [ -d $outputRunDataDir ] && [ $overwrite -eq 0 ]; then
    echo
    echo "ERROR: Output dir $outputRunDataDir exists. Use overwrite -o to run."
    echo        
    usage
fi

pipeline_start_time=`date +%s`

num_hucs=$(python3 $srcDir/check_huc_inputs.py -u $hucList)

echo
echo "======================= Start of gms_pipeline.sh ========================="
echo "Number of HUCs to process is $num_hucs"

## Produce gms hydrofabric at unit level first (gms_run_unit)

# We have to build this as a string as some args are optional.
# but the huclist doesn't always pass well, so just worry about
# the rest of the params.
run_cmd=" -n $runName"
run_cmd+=" -c $envFile"
run_cmd+=" -j $jobLimit"

if [ $overwrite -eq 1 ]; then run_cmd+=" -o" ; fi
if [ "$retry" == "--retry-failed" ]; then run_cmd+=" -r" ; fi
if [ $dropLowStreamOrders -eq 1 ]; then run_cmd+=" -s" ; fi
#echo "$run_cmd"
. /foss_fim/gms_run_unit.sh -u "$hucList" $run_cmd -d $deny_gms_unit_list

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


## Produce level path or branch level datasets
. /foss_fim/gms_run_branch.sh $run_cmd -d $deny_gms_branches_list


echo "======================== End of gms_pipeline.sh =========================="
pipeline_end_time=`date +%s`
total_sec=$(expr $pipeline_end_time - $pipeline_start_time)
dur_min=$((total_sec / 60))
dur_remainder_sec=$((total_sec % 60))
echo "Total Run Time = $dur_min min(s) and $dur_remainder_sec sec"
echo

