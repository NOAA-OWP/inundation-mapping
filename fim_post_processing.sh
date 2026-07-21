#!/bin/bash
set -e          # Critical: We need the -e in place which means file in place if an error occurs.
#                 HOWEVER.. as soon as we turn on the error TRAP, we need to have it turned back off again
#                 otherwise the trap will not fire. Just above the TRAP, I add set +e to let trapping handling it.

set -o pipefail  # Crucial: Forces the pipe to fail if the subscript fails but only when a pipe is used.
# set -o errtrace  # Inherit trap inside functions/subshells

# For this script, we do want to stop, but the error script will always be caught
# by the trap, then always hit the exit_and_copy if the error happens after the
# code is executed. We want this script to ALWAYS return a 0
### Yes.. not all of our .sh files are the same with the -e flag, by design.

:
usage()
{
    echo "
    Post processing for creating HAND datasets

    Usage : fim_post_processing.sh [REQ: -n <run name> ] [OPT: -h -j <job limit>]

    REQUIRED:
       -n/--runName    : A name to tag the output directories and log files.

    OPTIONS:
       -h/--help       : help file
    "
    exit 1
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

export outputDestDir=$outputsDir/$runName
export pp_log_file_name=$outputDestDir/logs/post_processing.log
export pp_error_log_file_name=$outputDestDir/logs/post_processing_errors.log
export all_errors_csv=$outputDestDir/logs/all_error_report.csv

if [ "$runName" = "" ]
then
    echo "+++++++++++++++++++++++"
    echo "ERROR: Missing -n run time name argument"
    echo "+++++++++++++++++++++++"    
    usage
    exit 22
fi

## Check for output destination directory ##
if [ ! -d "$outputDestDir" ]; then
    echo "+++++++++++++++++++++++"
    echo "Out folder of $outputDestDir does not appear to exist"
    echo "Please provide an output folder name that has hucs/branches run."
    echo "+++++++++++++++++++++++"
    usage
    exit 22
fi

########################
# Setup all key variables needed by any functioning code
# Do not call any commands here until after the error trapping has started
# We need this setup so the two error handling functions have variables to work with.
# Note: If the lines below fail before we add the "trap" command below, it will not be
# caught correctly and that is ok, as most of it is setting very simple variables or are part of input handling
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

########################
# Add error handling for any script call or even page leve
# No matter what happens, this will execute and copy the folder from temp to outputs
# starting from the trap 'exit_and_copy' EXIT and down, ie) the arg tests above
exit_and_copy() {

    echo
    l_echo "---- End of fim_post_processing" $pp_log_file_name
    l_echo "---- Ended: `date -u`" $pp_log_file_name
    Calc_Duration "Post Processing Duration:" $post_proc_start_time $pp_log_file_name
    echo
}

# =====================
# This safety net catches from here down always calls this block, even if an exit code or exception has been called
trap 'exit_and_copy' EXIT
set +e  # turns off, yes off, the system no longer auto aborts, as trapping will handle it from from here down.
trap 'handle_error $LINENO $pp_error_log_file_name' ERR

rm -f $pp_log_file_name  # If it already exists
rm -f $pp_error_log_file_name  # If it already exists

# load up enviromental information
args_file=${outputDestDir}/runtime_args.env
fim_inputs=${outputDestDir}/fim_inputs.csv

source $args_file
source $outputDestDir/params.env

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing" $pp_log_file_name
l_echo "---- Started: `date -u`" $pp_log_file_name
post_proc_start_time=`date +%s`

## ===============================
l_echo $startDiv"Compiling all HUC error reports" $pp_log_file_name

# Note: This is a special log file system.
# If it runs succesfully, it will add message to the standard huc log file.
# But if this script itself fails, it gets a specical log file.
python3 $srcDir/utils/post_process_error_report.py \
    -n $outputDestDir -o $all_errors_csv >> $pp_log_file_name 2>> $pp_error_log_file_name 

# TODO: July 2026: low importances.
# look for any of the huc error report .py file errors themselves which create a special log file
# per huc, named $tempHucDataDir/logs/log_scan_tool_failed_(huc).log. 
# But generally, if one fails, all will fail and get an warning.

## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Concatenate all processing time files into a CSV file" $pp_log_file_name
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $pp_log_file_name


## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Start branch aggregation" $pp_log_file_name
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
    -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $pp_log_file_name


## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Combining crosswalk tables" $pp_log_file_name
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv 2>&1 | tee -a $pp_log_file_name
Tcount

# it will auto run the exit_and_copy function
