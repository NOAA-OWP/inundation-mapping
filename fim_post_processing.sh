#!/bin/bash -e
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

outputDestDir=$outputsDir/$runName

## Check for output destination directory ##
if [ ! -d "$outputDestDir" ]; then
    l_echo "Depends on output from units and branches. "
    l_echo "Please provide an output folder name that has hucs/branches run."
    exit 1
fi

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

huc_error_check_complete="False"
branch_error_check_complete="False"

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the post processing log
log_file_name=$outputDestDir/logs/post_processing.log
rm -f $log_file_name  # If it already exists
Set_log_file_path $log_file_name

post_processing_error_log_file_name=$outputDestDir/logs/post_processing_errors.sh
rm -f $post_processing_error_log_file_name  # If it already exists

# print usage if arguments empty
if [ "$runName" = "" ]
then
    l_echo "ERROR: Missing -n run time name argument"
    usage
    exit 22
fi

l_echo ""
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing"
l_echo "---- Started: `date -u`"
T_total_start
post_proc_start_time=`date +%s`

## ===============================
l_echo $startDiv"Concatenate all processing time files into a CSV file"
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
# /usr/bin/time -f "$time_cmd_format" python3 $srcDir/duration_system.py \
#   -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $log_file_name
python3 $srcDir/duration_system.py \
  -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $log_file_name

## ===============================
l_echo $startDiv"Concatenate all HUCs error files"
allErrorsLog="$outputDestDir/logs/all_errors.log"    
find $outputDestDir -path "**/*_errors.log" -type f -exec cat {} + >> $allErrorsLog
l_echo "Collecting errors saved to ${allErrorsLog}."

## ===============================
l_echo $startDiv"Find all HUC branch non zero exit codes"
Tstart
find $outputDestDir -path "**/*_branch_*.log" -type f | \
    xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > \
    "$outputDestDir/logs/branch_non_zero_exit_codes.log" 
Tcount

## ===============================
l_echo $startDiv"Start branch aggregation"
# /usr/bin/time -f "$time_cmd_format" python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
#     -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $log_file_name
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
    -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $log_file_name

## ===============================
l_echo $startDiv"Combining crosswalk tables"
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv 2>&1 | tee -a $log_file_name

## ===============================
# l_echo $startDiv"Resetting Permissions"
# Tstart
#     find $outputDestDir -maxdepth 1 -type f -exec chmod 777 {} +  # just root level files
# Tcount

l_echo $startDiv"Searching for error and invalid exit codes"
grep -H -n -i -E ".*Command exited with non-zero status" $log_file_name >> $post_processing_error_log_file_name &
grep -H -n -i -E ".*Exit status: [1-9]" $log_file_name >> $post_processing_error_log_file_name &
echo ""
## ===============================
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- End of fim_post_processing"
l_echo "---- Ended: `date -u`"
Calc_Duration "Post Processing Duration:" $post_proc_start_time
echo
