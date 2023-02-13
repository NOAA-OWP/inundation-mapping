#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric at levelpath/branch scale. Execute gms_run_unit.sh prior to.'
    echo 'Usage : gms_run_branch.sh [REQ: -n <run name> ]'
    echo '                          [OPT: -h -u <hucs> -c <config file> -j <job limit>] -o'
    echo '  	 				    -bd <branch deny list file>'
    echo '                          -zd <branch zero deny list file>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -n/--runName    : A name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -u/--hucList    : HUC8s to run or multiple passed in quotes (space delimited).'
    echo '                    A line delimited file also acceptable. HUCs must present in inputs directory.'
    echo '  -c/--config     : configuration file with bash environment variables to export'    
    echo '                    default (if arg not added) : /foss_fim/config/params_template.env'            
    echo '  -bd/--branchDenylist : A file with a line delimited list of files in BRANCHES directories to be removed' 
    echo '                    upon completion of branch processing.'
    echo '                    (see config/deny_gms_branches.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_gms_branches.lst'   
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'
    echo '  -zd/--branchZeroDenylist : A file with a line delimited list of files in BRANCH ZERO directories to' 
    echo '                    be removed upon completion of branch zero processing.'
    echo '                    (see config/deny_gms_branch_zero.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_gms_branch_zero.lst'   
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'    
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo
    exit
}

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
    -bd|--branchDenylist)
        shift
        deny_branches_list=$1
        ;;
    -zd|--branchZeroDenylist)
        shift
        deny_branch_zero_list_for_branches=$1
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
fi

if [ "$envFile" = "" ]
then
    envFile=/foss_fim/config/params_template.env
fi

if [ "$deny_branches_list" = "" ]
then
   deny_branches_list=/foss_fim/config/deny_gms_branches.lst
elif [ "${deny_branches_list^^}" != "NONE" ] && [ ! -f "$deny_branches_list" ]
then
    # NONE is not case sensitive
    echo "Error: The -bd <branch deny file> does not exist and is not the word NONE"
    usage
fi

# Yes.. we have to have a different variable names for the deny_branch_zero_list_for_branches
# and deny_branch_zero_list_for_units. While they both use the same input arg, they use
# the value slightly different and when using gms_pipeline, the values can impact
# other bash files.
if [ "$deny_branch_zero_list_for_branches" = "" ]
then
    deny_branch_zero_list_for_branches=/foss_fim/config/deny_gms_branch_zero.lst
elif [ "${deny_branch_zero_list_for_branches^^}" != "NONE" ]   # NONE is not case sensitive
then
    if [ ! -f "$deny_branch_zero_list_for_branches" ]
    then
        echo "Error: The -zd <branch zero deny file> does not exist and is not the word NONE"
        usage
    else
        # only if the deny branch zero has been overwritten and file exists
        has_deny_branch_zero_override=1 
    fi
else
    has_deny_branch_zero_override=1 # it is the value of NONE and is overridden
fi

if [ "$overwrite" = "" ]
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
export deny_branches_list=$deny_branches_list
logFile=$outputRunDataDir/logs/branch/summary_gms_branch.log
export overwrite=$overwrite

## Check for run data directory and the file. If gms_run_unit failed, the file will not be there ##
if [ ! -f "$outputRunDataDir/gms_inputs.csv" ]; then 
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

# make log dir
if [ ! -d "$outputRunDataDir/logs/branch" ]; then
    mkdir -p $outputRunDataDir/logs/branch
elif [ $overwrite -eq 1 ]; then
    # need to clean it out if we are overwriting
    rm -rdf $outputRunDataDir/logs/branch
    mkdir -p $outputRunDataDir/logs/branch
fi

# Note: Other parts of the program will check for the existance of the file
# /branch_errors/non_zero_exit_codes.log. It has to be removed no matter
# what on each run of gms_run_branch
if [ ! -d "$outputRunDataDir/branch_errors" ]; then
    mkdir -p "$outputRunDataDir/branch_errors"
elif [ $overwrite -eq 1 ]; then
    rm -rdf $outputRunDataDir/branch_errors
    mkdir -p $outputRunDataDir/branch_errors
fi

## RUN GMS BY BRANCH ##
echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of branch processing"
echo "---- Started: `date -u`" 
T_total_start
Tstart
all_branches_start_time=`date +%s`

if [ "$jobLimit" -eq 1 ]; then
    parallel --verbose --timeout $branch_timeout --lb  -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
else
    parallel --eta --timeout $branch_timeout -j $jobLimit --joblog $logFile --colsep ',' -- $srcDir/gms/time_and_tee_run_by_branch.sh :::: $gms_inputs
fi

echo "Branch processing is complete"
Tcount
date -u

# -------------------
## REMOVE FILES FROM DENY LIST FOR BRANCH ZERO (but using normal branch deny) ##
## but also do not remove if branch zero deny is NONE (any case)

# If the deny branch zero has been overridden, then use it (file path proven above).
# Override might be the value of None (case not sensitive)
# Else then use the default deny branch (not the zero) which might also be None and that is ok

# If deny branch zero is NONE.. then skip
# if deny branch zero has an override deny list, use it.
# if deny branch zero is not NONE and deny list is not overridden
#    then see if reg deny branch is NONE.  If so.. skip
#    else, use the deny branch list instead to do final cleanup on branch zero

if [ "$has_deny_branch_zero_override" == "1" ]
then
    echo -e $startDiv"Cleaning up (Removing) files for branch zero for all HUCs"
    $srcDir/gms/outputs_cleanup.py -d $outputRunDataDir -l $deny_branch_zero_list_for_branches -b 0

else 
    echo -e $startDiv"Cleaning up (Removing) files all branch zero for all HUCs using the default branch deny list"
    $srcDir/gms/outputs_cleanup.py -d $outputRunDataDir -l $deny_branches_list -b 0
fi


# -------------------
## GET NON ZERO EXIT CODES ##
# Needed in case aggregation fails, we will need the logs
# Note: Other parts of the program (gms_run_post_processing.sh) check to see
# if the branch_errors/non_zero_exit_codes.log exists. If it does not, it assumes
# that gms_run_branch did not complete (or was not run)
echo
echo -e $startDiv"Start non-zero exit code checking"$stopDiv
find $outputRunDataDir/logs/branch -name "*_branch_*.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" >"$outputRunDataDir/branch_errors/non_zero_exit_codes.log" &

# -------------------
## REMOVE FAILED BRANCHES ##
# Needed in case aggregation fails, we will need the logs
echo
echo -e $startDiv"Removing branches that failed with Exit status: 61"$stopDiv
Tstart
python3 $srcDir/gms/remove_error_branches.py -f "$outputRunDataDir/branch_errors/non_zero_exit_codes.log" -g $outputRunDataDir/gms_inputs.csv
Tcount
date -u

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- gms_run_branch complete"
echo "---- Ended: `date -u`"
Calc_Duration $all_branches_start_time
echo
