#!/bin/bash
### set -e
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

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
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

# Some simple error handling
# We add it to the log file then scan for the word "error" later down.
# This will also handle errors in this script
# and is deliberately below the data input validation above.
handle_error(){

    local line_num=$1
    local err_msg=$2
    local exit_code=$3

    l_echo "++++++++++++++++++++++++++++" $pp_error_log_file_name
    msg="Critical error in fim_post_processing.sh itself, line number: $line_num"
    l_echo "$msg" $pp_error_log_file_name

    msg="Error Command Submitted: $BASH_COMMAND"
    l_echo "$msg" $pp_error_log_file_name

    msg="Exit Code: $exit_code : $err_msg"
    l_echo "$msg" $pp_error_log_file_name

    echo "++++++++++++++++++++++++++++"
    echo
    exit 1  # we always return 0 (success) as we are fully handling error and logging
}

# In case there is a critical error with logic on this page.
# Most errors are caught via Time and Tee, then the return status codes
# but errors can occur on this page itself. This helps trap those types of errors
# as well
trap 'handle_error $LINENO' ERR INT

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

# scan_for_huc_errors_complete, and branch_error... helps stop for multiple scans
#  as it is possible if there are multiple errors on this page.
huc_error_check_complete="False"
branch_error_check_complete="False"

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the post processing log
pp_log_file_name=$outputDestDir/logs/post_processing.log
rm -f $pp_log_file_name  # If it already exists

pp_error_log_file_name=$outputDestDir/logs/post_processing_errors.log
rm -f $pp_error_log_file_name  # If it already exists

# Tell the system the name and location of the log file
# l_echo is echo to screen and log at the same time.
Set_log_file_path $pp_log_file_name

echo
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing" $pp_log_file_name
l_echo "---- Started: `date -u`" $pp_log_file_name
T_total_start
post_proc_start_time=`date +%s`

## ===============================
all_errors_log="$outputDestDir/logs/all_errors.log" 
branch_non_zero_log="$outputDestDir/logs/branch_non_zero_exit_codes.log"

## GET NON ZERO EXIT CODES FOR HUCS ##
l_echo $startDiv"Start various types of errors and invalid exit codes"
echo "There will be lots of duplication of related errors and we will clean this up later."
find $outputDestDir -type f -name "huc_**_unit.log" -print0 | \
    xargs -0 grep -HinE "Exit status: ([1-9][0-9]{0,2}).*" >> $all_errors_log &

find $outputDestDir -type f -name "huc_*_unit.log" -exec grep -Hni "error" {} + >> $all_errors_log  || true
find $outputDestDir -type f -name "huc_*_unit.log" -exec grep -Hni "parallel" {} + >> $all_errors_log  || true
find $outputDestDir -type f -name "huc_*_unit.log" -exec grep -Hni "Exception" {} + >> $all_errors_log  || true
find $outputDestDir -type f -name "huc_*_unit.log" -exec grep -Hni "Command exited with non-zero status" {} + >> $all_errors_log  || true

## ===============================
l_echo $startDiv"Find all HUC branch non zero exit codes" $pp_log_file_name
find $outputDestDir -path "*/logs/branch/*_branch_*.log" -type f | \
    xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > \
    "$branch_non_zero_log" &

# why is this no longer working
find $outputDestDir -path "*/logs/branch/*_branch_*.log" -type f | \
    xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > \
    "$branch_non_zero_log" &


# find $outputDestDir -path "**/*/logs/branch/*_branch*.log" -type f -print0 | \
#   xargs -0 grep -HniE "Exit status: ([1-9][0-9]{0,2})" >> $branch_non_zero_log || true

# TODO: Get this working.
# Remove dup entries for acceptable branches from the all_errors as it will be in the branch errors file
# branch codes of 61, 64, 65 are acceptable branch error codes
# sed -i '/Exit status: 61 /d' $errorLogFile
# sed -i '/Exit status: 64 /d' $errorLogFile
# sed -i '/Exit status: 65 /d' $errorLogFile
# sed -i 'Exit status: ([6][0-9]{1,2})/d' $errorLogFile
# sed -i '/Exit status: ([6][0-9]{1,2})/d' $errorLogFile

## ===============================
l_echo $startDiv"Concatenate all processing time files into a CSV file" $pp_log_file_name
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
# /usr/bin/time -f "$time_cmd_format" python3 $srcDir/duration_system.py \
#   -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $pp_log_file_name
python3 $srcDir/duration_system.py \
  -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $pp_log_file_name

## ===============================
l_echo $startDiv"Start branch aggregation" $pp_log_file_name
# /usr/bin/time -f "$time_cmd_format" python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
#     -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $log_file_name
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
    -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $pp_log_file_name

## ===============================
l_echo $startDiv"Combining crosswalk tables" $pp_log_file_name
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv 2>&1 | tee -a $pp_log_file_name

## ===============================
# l_echo $startDiv"Resetting Permissions"
# Tstart
#     find $outputDestDir -maxdepth 1 -type f -exec chmod 777 {} +  # just root level files
# Tcount

# Grep Tech Tip.. use the -e flag when you are not useing any wildcards or patterns
# just a word in a line. If you need a regex type patter, use -E instead.
l_echo $startDiv"Searching for error and invalid exit codes from the post processing script"
grep -Hnie "Command exited with non-zero status" $pp_log_file_name >> $pp_error_log_file_name || true
grep -Hnie "Exception" $pp_log_file_name >> $pp_error_log_file_name || true
grep -HniE "Exit status: ([1-9][0-9]{0,2})" $pp_log_file_name >> $pp_error_log_file_name &
echo

# TODO:
# Add a tool that can check if any HUCs completely disappeared. ie) failed to move from temp to 
# outputs. It is possible so we need a double check tool some how.  Low priority
# If we can find an easy way to do it as is a very low possibility but moreso in AWS where
# it does have a shared "fim-temp" that would leave a HUC folder in it if something catestrophically happens

l_echo $startDiv"Compiling error report"
Tstart
error_report=$(
    python3 $srcDir/utils/post_process_error_report.py \
        -o $csvFile 2>&1 \
)
Tcount


## ===============================
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" $pp_log_file_name
l_echo "---- End of fim_post_processing" $pp_log_file_name
l_echo "---- Ended: `date -u`" $pp_log_file_name
Calc_Duration "Post Processing Duration:" $post_proc_start_time $pp_log_file_name
echo
