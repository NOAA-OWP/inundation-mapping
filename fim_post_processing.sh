#!/bin/bash
# set -e         # Critical: Can not have a -e inplace in order for the error handline
set -o pipefail  # Crucial: Forces the pipe to fail if the subscript fails but only when a pipe is used.
# set -o errtrace  # Inherit trap inside functions/subshells
# For this one, we do want to stop, but the error script will always be caught
# by the trap, then always hit the exit_and_copy if the error happens after the
# code is executed. We want this script to ALWAYS return a 0
### Yes.. not all of our .sh files are the same with the -e flag, by design.

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

########################
# Setup all key variables needed by any functioning code
# Do not call any commands here until after the error trapping has started
# We need this setup so the two error handling functions have variables to work with.
# Note: If the lines below fail before we turn on the "trap" below, it will not be
# caught correctly. Fix anoter day, but super rare. We need the functions and
# args in the error handler.
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env
outputDestDir=$outputsDir/$runName
pp_log_file_name=$outputDestDir/logs/post_processing.log
pp_error_log_file_name=$outputDestDir/logs/post_processing_errors.log


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

# handle_error() {
#     # ${PIPESTATUS[0]} can return more than one status, often we using pip command as each part
#     # of the command gets it's own code. ie:
#     # 
#     # and it is in the PIPESTATUS array. The first code will always be the single
#     # code returned by run_sh.sh, the second is if it successfuly was able to pipe 
#     # it to the tee command. So, we check them both incase either has an error
#     # and yes.. we can not use the $? which is the LAST exit code (which is like success for the pipe to tee)
#     # When it is not used with "tee" it will have the only one status code in the array

#     # echo "we are in bash functions handle_error"

#     # local calling_name="$0"

#     # local exit_code="$?"
#     # # local exit_code="$0"
#     # echo "exit code returned is $exit_code"
#     # local line_number="$1"
#     # echo "On line number $line_number"
#     # local failed_command="$BASH_COMMAND"

#     # exit 1

#     # local line_number="$1"

#     # # 3. Fix the subshell/pipe limitation
#     # if [[ "$failed_command" == *"tee"* ]]; then
#     #     failed_command="python3 -u my_script.py (failed inside the pipe to tee)"
#     # fi

#     local pipeline_errors=("${PIPESTATUS[@]}")  # Has to be exactly the very first line inside the function, has to.
#     local line_number="$1"
#     local page_source="${BASH_SOURCE[1]}"

#     local err_code=0
#     # 2. Check if the error happened inside a pipeline
#     if [ "${#pipeline_errors[@]}" -gt 1 ]; then
#         echo "what is here"
#         # Loop through the array to find which index has a non-zero exit code
#         for i in "${!pipeline_errors[@]}"; do
#             echo "how about here"
#             echo "${pipeline_errors[$i]}" 
#             if [ "${pipeline_errors[$i]}" -ne 0 ]; then
#                 # failed_command="Command index $i inside the pipeline (Exit Code: ${pipeline_errors[$i]})"
#                 err_code=${pipeline_errors[$i]}
#                 break
#             fi
#         done
#     fi

#     if [[ $err_code -ne 0 ]]; then
#         echo "----------------------------------------"
#         echo "ERROR DETECTED!"
#         echo "${page_source} : Line Number = $line_number"
#         echo "----------------------------------------"
#         exit 1
#     fi

    # local err_code=



    # local python_status=${pipe_codes[0]}
    # local tee_status=${pipe_codes[1]}

    # echo "--- CRITICAL ERROR DETECTED ---"
    # echo "Python exited with: $python_status"
    # echo "Tee exited with:    $tee_status"

    # exit_codes=( "${PIPESTATUS[@]}" )
    # page_source="${BASH_SOURCE[1]}"

    # l_echo "++++++++++++++++++++++++++++" $error_log_filename
    # msg="Critical error detected in ${BASH_SOURCE[1]}, line number: ${BASH_LINENO}"
    # l_echo "$msg" $error_log_filename
    # l_echo "Error Command Submitted: ${BASH_COMMAND}" $pp_error_log_file_name
    # l_echo "Exit Code: $exit_code" $pp_error_log_file_name
    # echo "++++++++++++++++++++++++++++"
    # echo
    # exit 1


    # #for i in "${!exit_codes[@]}";
    # for exit_code in "${exit_codes[@]}" ;
    # do
    #     # Note: It was tricky to load in the fim_enum into bash, so we will just
    #     # go with the exit code for now
    #     if [ $exit_code -eq 0 ]; then
    #         echo 
    #         # do nothing
    #         echo "we are good"
    #     else 
    #         # local cmd_num=$((i + 1))
    #         # echo "Error: Command #${cmd_num} in the pipeline failed with exit code ${code}."

    #         l_echo "++++++++++++++++++++++++++++" $pp_error_log_file_name
    #         msg="Critical error caught on {$page_source}, line number: ${BASH_LINENO}"
    #         l_echo "${msg}" $pp_error_log_file_name
    #         # l_echo "Error Command Submitted: #${cmd_num}" $pp_error_log_file_name
    #         # l_echo "Error Command Submitted: $cmd_text" $pp_error_log_file_name            
    #         l_echo "Exit Code: $exit_code" $pp_error_log_file_name

    #      # could be an exit status of 1 but can be other codes as well.
    #         # It is possible that some errors may not show up huc log file depending
    #         # how catastrophic the error was. It is possible that an exception
    #         # could show up in our error log file twice and that is ok.
    #         # It may or may not already have been see by "tee" and is in the std log file.
    #         # err_msg="***** ERROR- Unknown Exit status: $code detected *****"
    #         # l_echo "$err_msg" $hucLogFileName
    #         # list_error_msg+="${err_msg}\n"
    #         exit  $exit_code
    #     fi
    # done
# }


# handle_error() {
#     # ${PIPESTATUS[0]} can return more than one status, often we using pip command as each part
#     # of the command gets it's own code. ie:
#     # 
#     # and it is in the PIPESTATUS array. The first code will always be the single
#     # code returned by run_sh.sh, the second is if it successfuly was able to pipe 
#     # it to the tee command. So, we check them both incase either has an error
#     # and yes.. we can not use the $? which is the LAST exit code (which is like success for the pipe to tee)
#     # When it is not used with "tee" it will have the only one status code in the array
#     exit_codes=( "${PIPESTATUS[@]}" )

#     #for exit_code in "${return_codes[@]}"
#     for i in "${!exit_codes[@]}";
#     do
#         # Note: It was tricky to load in the fim_enum into bash, so we will just
#         # go with the exit code for now
#         if [ $exit_code -eq 0 ]; then
#             echo 
#             # do nothing
#             echo "we are good"
#         else 
#             # local cmd_num=$((i + 1))
#             # echo "Error: Command #${cmd_num} in the pipeline failed with exit code ${code}."

#             l_echo "++++++++++++++++++++++++++++" $pp_error_log_file_name
#             msg="Critical error caught, line number: ${BASH_LINENO}"
#             l_echo "${msg}" $pp_error_log_file_name
#             # l_echo "Error Command Submitted: #${cmd_num}" $pp_error_log_file_name
#             # l_echo "Error Command Submitted: $cmd_text" $pp_error_log_file_name            
#             l_echo "Exit Code: $exit_code" $pp_error_log_file_name

#          # could be an exit status of 1 but can be other codes as well.
#             # It is possible that some errors may not show up huc log file depending
#             # how catastrophic the error was. It is possible that an exception
#             # could show up in our error log file twice and that is ok.
#             # It may or may not already have been see by "tee" and is in the std log file.
#             # err_msg="***** ERROR- Unknown Exit status: $code detected *****"
#             # l_echo "$err_msg" $hucLogFileName
#             # list_error_msg+="${err_msg}\n"
#             exit  $exit_code
#         fi
#     done



    # l_echo "++++++++++++++++++++++++++++" $pp_error_log_file_name
    # msg="Critical error in fim_post_processing.sh itself, line number: ${BASH_LINENO}"
    # l_echo "${msg}" $pp_error_log_file_name
    # l_echo "Error Command Submitted: ${BASH_COMMAND}" $pp_error_log_file_name
    # l_echo "Exit Code: $exit_code" $pp_error_log_file_name

#     echo "++++++++++++++++++++++++++++"
#     echo
# }


rm -f $pp_log_file_name  # If it already exists
rm -f $pp_error_log_file_name  # If it already exists

# More notes about error handling.
# The "trapping starts with the word "trap" and "ERR" on the end.
# above this line, will not be trapped by design.

# If a command line is pumped to another command like tee, this trap will not be
# triggered and we have to test it by hand
## trap 'bash_error_handler "${BASH_SOURCE[1]}" "$BASH_COMMAND" "$pp_error_log_file_name" "${PIPESTATUS[@]}"' ERR
# trap 'handle_error "$?"' ERR
# trap 'handle_error ${PIPESTATUS[0]}' ERR
# trap 'handle_error ( "${PIPESTATUS[@]}" ) ERR

# Note: Many files such as run_huc.sh, run_branch.sh and calibrate_rating_curves do not have
# there own error handling logic and don't want them. Their handling is done by parent scripts
# such as fim_process_huc.sh, process_branch.sh, etc

# for xx False

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

# =====================
# This safety net catches from here down always calls this block, even if an exit code or exception has been called
trap 'exit_and_copy' EXIT

trap 'handle_error $LINENO $pp_error_log_file_name' ERR


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


