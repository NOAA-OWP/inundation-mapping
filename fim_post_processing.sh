#!/bin/bash
set -e          # Critical: We need the -e in place which means file in place if an error occurs.
set -o errtrace  # Inherit trap inside functions/subshells

# For this script, we do want to stop, but the error script will always be caught
# by the trap, then always hit the handle_exit if the error happens after the
# code is executed. The TRAP for exits are applicable from the line trap 'handle_exit' EXIT.
#  We want this script to ALWAYS return a 0
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

# post_process_error_report.py splits the merged csv into two files. One that
# has all accepted branch codes (ie 60 - 69), and the rest to the error file
export all_errors_csv=$outputDestDir/logs/all_huc_errors_report.csv
export branch_accepted_exit_recs_csv=$outputDestDir/logs/all_branches_with_accepted_codes.csv

post_proc_start_time=`date +%s`

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
handle_exit() {

    # If the post processing error file exists and is not empty, lets tell the user onscreen.
    if [ ! -e "$pp_error_log_file_name" ] || [ -s "$pp_error_log_file_name" ]; then
        l_echo "" $pp_log_file_name
        l_echo "**** Errors were found while processing post processing" $pp_log_file_name
        echo "**** Check the post processing error log for details"
    fi

    l_echo "" $pp_log_file_name
    l_echo "---- End of fim_post_processing" $pp_log_file_name
    l_echo "---- Ended: `date -u`" $pp_log_file_name
    Calc_Duration "Post Processing Duration:" $post_proc_start_time $pp_log_file_name
    echo
}

# =====================
# This safety net catches from here down always calls this block, even if an exit code or exception has been called
trap 'handle_exit' EXIT
set +e  # turns off, yes off, the system no longer auto aborts, as trapping will handle it from from here down.
# This trap handles bash level errors, but for py file exceptions, we will handle by hand.
trap 'handle_error $LINENO $pp_error_log_file_name' ERR

# Yes.. this is a little weird. We use the trap for bash level, and for each of the .py files, we will check
# the exit code by hand and stop the processes.

rm -f $pp_log_file_name  # If it already exists
rm -f $pp_error_log_file_name  # If it already exists

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing" $pp_log_file_name
l_echo "---- Started: `date -u`" $pp_log_file_name

## ===============================
l_echo $startDiv"Compiling all HUC error reports" $pp_log_file_name

# Note: This is a special log file system.
# If it runs succesfully, it will add message to the standard huc log file.
# But if this script itself fails, it gets a specical log file.
python3 $srcDir/utils/post_process_error_report.py \
    -n $outputDestDir -o $all_errors_csv \
    -b $branch_accepted_exit_recs_csv > >(tee -a $pp_log_file_name) 2> >(tee -a $pp_error_log_file_name >&2)
if [ $? -ne 0 ]; then exit 0; fi  # this will auto jump in error to the handle_exit    
wait 

## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Concatenate all processing time files into a CSV file" $pp_log_file_name
csvFile=$outputDestDir/logs/total_duration_run_by_unit_all_HUCs.csv
python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile > >(tee -a $pp_log_file_name) 2> >(tee -a $pp_error_log_file_name >&2)
if [ $? -ne 0 ]; then exit 0; fi  # this will auto jump in error to the handle_exit
wait

## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Start branch aggregation" $pp_log_file_name
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir \
    -f 'branch_ids.csv' -o $fim_inputs > >(tee -a $pp_log_file_name) 2> >(tee -a $pp_error_log_file_name >&2)
if [ $? -ne 0 ]; then exit 0; fi  # this will auto jump in error to the handle_exit    
wait

## ===============================
## if fails, it might be due to all hucs failing, check post_processing.log
l_echo $startDiv"Combining crosswalk tables" $pp_log_file_name
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv > >(tee -a $pp_log_file_name) 2> >(tee -a $pp_error_log_file_name >&2)
    # -o $outputDestDir/crosswalk_table.csv >> $pp_log_file_name 2>> $pp_error_log_file_name
if [ $? -ne 0 ]; then exit 0; fi  # this will auto jump in error to the handle_exit    
wait

Tcount
echo ""
# it will auto run the handle_exit function, no matter what
