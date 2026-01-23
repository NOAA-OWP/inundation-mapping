#!/bin/bash
set -eEo pipefail

### set -eEvo pipefail
umask 000

:
usage()
{
    echo "
    Post processing for creating FIM hydrofabric.

    Usage : fim_post_processing.sh [REQ: -n <run name> ] [OPT: -h -j <job limit>]

    REQUIRED:
       -n/--runName    : A name to tag the output directories and log files.

    OPTIONS:
       -h/--help       : help file
    "
    exit
}

while [ "$1" != "" ]; do
case $1
in
    -n|--runName)
        shift
        runName=$1
        ;;
    -h|--help)
        shift
        usage
        exit
        ;;
    *) ;;
    esac
    shift
done

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the post processing log
log_file_name=$outputDestDir/logs/post_processing.log
rm -f $log_file_name  # If it already exists
Set_log_file_path $log_file_name
post_processing_error_log_file_name=$outputDestDir/logs/post_processing_errors.sh

# print usage if arguments empty
if [ "$runName" = "" ]
then
    l_echo "ERROR: Missing -n run time name argument"
    usage
    exit 22
fi

outputDestDir=$outputsDir/$runName

## Check for output destination directory ##
if [ ! -d "$outputDestDir" ]; then
    l_echo "Depends on output from units and branches. "
    l_echo "Please provide an output folder name that has hucs/branches run."
    exit 1
fi

#########################################################################################
#                                                                                       #
# PLEASE DO NOT USE the job limits coming in from the runtime_args.env                  #
# Most of the time, post processing will not be run on the same servers                 #
# that is running fim_process_huc.sh and the processing power                       #
# used to run fim_post_processing.sh will be different (hence.. different job limit)    #
#                                                                                       #
#########################################################################################

# Some simple error handling
handle_error(){
    echo "++++++++++++++++++++++++++++"
    msg="Critical error in fim_post_processing.sh, line number: $LINENO"
    l_echo "$msg"
    echo "$msg" >> $post_processing_error_log_file_name

    msg="Command submitted: $BASH_COMMAND"
    l_echo "$msg"
    echo "$msg" >> $post_processing_error_log_file_name
    echo "++++++++++++++++++++++++++++"
    l_echo $startDiv"Now compiling all HUCs error files"
    check_for_huc_errors  # we add it here in case it failed earlier in the script
    check_for_branch_errors
    echo ""
    exit 0  # we always return 0 (success) as we are fully handling error and logging
}

# This makes sure we always run this
check_for_huc_errors(){

    allErrorsLog="$outputDestDir/logs/all_errors.log"    
    find $outputDestDir -type f -name "*_errors.log" -exec cat {} + > $allErrorsLog
    l_echo "Collecting errors saved to ${allErrorsLog}."
}

check_for_branch_errors(){

    allBranchErrorsLog="$outputDestDir/logs/all_branch_errors.log"
    find $outputDestDir -type f -name "*branch_non_zero_exit_codes.log" -exec cat {} + > $allBranchErrorsLog
    l_echo "Collected branch non zero codes saved to ${allBranchErrorsLog}."
}

# In case there is a critical error with logic on this page.
trap 'handle_error $LINENO' ERR

# Note: Some commands in this script are explicitly catching StdErr and StnOut so
# we can put it in the logs, especially any error information. Normally, this is only
# needed by top level .sh scripts yhat do not use the "tee" command to catch all outputs and errors.
# You will see it via usage of a variable named "FIM_CMD"


l_echo ""
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing"
l_echo "---- Started: `date -u`"
l_echo ""
T_total_start
post_proc_start_time=`date +%s`

## ===============================
l_echo "Concatenate all processing time files into a CSV file"
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
# FIM_CMD="python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile"
# # echo $FIM_CMD
# cmd_response=$($FIM_CMD 2>&1)
# l_echo "$cmd_response"
python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile 2>&1 | tee -a $log_file_name
cmd_response=$?
echo "resp value is $cmd_response" #  what do to if it is a 1?

## ===============================
l_echo $startDiv"Compile all HUC branch non zero exit codes"
check_for_branch_errors

## ===============================
l_echo $startDiv"Compile all HUCs error files"
check_for_huc_errors

## ===============================
l_echo $startDiv"Start branch aggregation"
Tstart
# python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f "branch_ids.csv" -o $fim_inputs 2>&1 | tee -a $log_file_name
# # cmd_response=$($FIM_CMD 2>&1)
# cmd_response=$?
# echo "resp value is $cmd_response" #  what do to if it is a 1?

# cmd="python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $log_file_name"
# # cmd_response=$($FIM_CMD 2>&1)
# $cmd
# cmd_response=$?
# echo "resp value is $cmd_response" #  what do to if it is a 1?

# Execute the command and capture both standard output and standard error
# into a variable. The '|| true' ensures the script doesn't exit if 'set -e' is used.
CMD_STR="python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f 'branch_ids.csv' -o $fim_inputs"
cmd_response=$(eval "$CMD_STR" 2>&1 | tee -a $log_file_name)
cmd_rtn_code=$?
# l_echo $cmd_response
l_echo "return code is $cmd_rtn_code"
Tcount

## ===============================
l_echo $startDiv"Combining crosswalk tables"
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv  2>&1 | tee -a $log_file_name
cmd_response=$?
echo "resp value is $cmd_response" #  what do to if it is a 1?

Tcount

## ===============================
l_echo $startDiv"Resetting Permissions"
Tstart
    find $outputDestDir -maxdepth 1 -type f -exec chmod 777 {} +  # just root level files
Tcount

## ===============================
echo
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- End of fim_post_processing"
l_echo "---- Ended: `date -u`"
Calc_Duration "Post Processing Duration:" $post_proc_start_time
echo
