#!/bin/bash
# set -e         # Critical: Can not have a -e inplace in order for the error handline
set -o pipefail  # Crucial: Forces the pipe to fail if the subscript fails but only when a pipe is used.
set -o errtrace  # Inherit trap inside functions/subshells
# For this one, we do want to stop, but the error script will always be caught
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

outputDestDir=$outputsDir/$runName
pp_log_file_name=$outputDestDir/logs/post_processing.log
pp_error_log_file_name=$outputDestDir/logs/post_processing_errors.log

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
    exit 0  # yes, return sucess
}

rm -f $pp_log_file_name  # If it already exists
rm -f $pp_error_log_file_name  # If it already exists


# =====================
# This safety net catches from here down always calls this block, even if an exit code or exception has been called
trap 'exit_and_copy' EXIT

trap 'handle_error "${PIPESTATUS[*]}" $LINENO $pp_error_log_file_name "post"' ERR

# ls /nonexistent_directory_to_force_a_fail

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env



echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing" $pp_log_file_name
l_echo "---- Started: `date -u`" $pp_log_file_name
post_proc_start_time=`date +%s`


## ===============================
l_echo $startDiv"Compiling error report" $pp_log_file_name
# Tstart
all_errors_csv_log=$outputDestDir/logs/error_report.csv
python3 $srcDir/utils/post_process_error_report.py \
   -n $outputDestDir -o $all_errors_csv_log 2>&1 | tee -a -i $pp_log_file_name 
# ; pipe_codes=("${PIPESTATUS[@]}")

# python_exit_status=${PIPESTATUS[0]}

# echo "Python failed with exit code $python_exit_status"

# if [ $python_exit_status -ne 0 ]; then
#     echo "Python failed with exit code $python_exit_status"
#     echo "Check output.log for the full traceback."
# fi

# exit 1

# return_codes=( "${PIPESTATUS[@]}" )

# for code in "${return_codes[@]}"
# do
#     echo "return code is $code"
#     handle_error $code
# done

# return_code=( "${PIPESTATUS[@]}" )
# exit_code="${return_codes[0]}"
# # exit_code 0 will always be tghe first call (ie. the py file)
# if [ $exit_code -ne 0 ]; then 
#     echo "The code from pp  is $exit_code"
#      # l_echo "Aborting Script (Compiling error report)" $pp_error_log_file_name 
#      handle_error "$exit_code"
# #     # exit 1
# fi
# Call the error handler by hand to see if there are errors becuase we used a piped "|" command
# bash_error_handler "${BASH_SOURCE[1]}" \
#                    "Compiling error report" \
#                    "$pp_error_log_file_name" \
#                    "${PIPESTATUS[@]}"
# Tcount


## ===============================
l_echo $startDiv"Concatenate all processing time files into a CSV file" $pp_log_file_name
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile 2>&1 | tee -a -i $pp_log_file_name
# bash_error_handler "${PIPESTATUS[@]}" $LINENO "post_process_error_report.py" "$pp_error_log_file_name"


## ===============================
l_echo $startDiv"Start branch aggregation" $pp_log_file_name
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
    -f 'branch_ids.csv' -o $fim_inputs 2>&1 | tee -a $pp_log_file_name
# bash_error_handler "${PIPESTATUS[@]}" $LINENO "post_process_error_report.py" "$pp_error_log_file_name"


## ===============================
l_echo $startDiv"Combining crosswalk tables" $pp_log_file_name
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv 2>&1 | tee -a $pp_log_file_name
Tcount
# bash_error_handler "${PIPESTATUS[@]}" $LINENO "post_process_error_report.py" "$pp_error_log_file_name"

## ===============================
# l_echo $startDiv"Resetting Permissions"
# Tstart
#     find $outputDestDir -maxdepth 1 -type f -exec chmod 776 {} +  # just root level files
# Tcount

# Grep Tech Tip.. use the -e flag when you are not useing any wildcards or patterns
# just a word in a line. If you need a regex type patter, use -E instead.
# l_echo $startDiv"Searching for error and invalid exit codes from the post processing script" $pp_log_file_name
# grep -Hnie "Error" $pp_log_file_name >> $pp_error_log_file_name || true
# grep -Hnie "Exception" $pp_log_file_name >> $pp_error_log_file_name || true
# grep -HniE "Exit status: ([1-9][0-9]{0,2})" $pp_log_file_name >> $pp_error_log_file_name &
echo

# TODO:
# Add a tool that can check if any HUCs completely disappeared. ie) failed to move from temp to 
# outputs. It is possible so we need a double check tool some how.  Low priority
# If we can find an easy way to do it as is a very low possibility but moreso in AWS where
# it does have a shared "fim-temp" that would leave a HUC folder in it if something catestrophically happens


