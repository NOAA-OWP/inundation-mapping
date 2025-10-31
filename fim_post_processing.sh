#!/bin/bash -e

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
    echo "Depends on output from units and branches. "
    echo "Please provide an output folder name that has hucs/branches run."
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

# TODO what is below coming from?
rm -f $log_file_name

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the post processing log
timestamp=$(date +"%Y_%m_%d-%H_%M_%S")
log_file_name=$outputDestDir/logs/post_proc_${timestamp}.log
Set_log_file_path $log_file_name

l_echo ""
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- Start of fim_post_processing"
l_echo "---- Started: `date -u`"
echo ""
T_total_start
post_proc_start_time=`date +%s`

echo "Concatenate all processing time files into a CSV file"
csvFile=$outputDestDir/logs/unit/total_duration_run_by_unit_all_HUCs.csv

# if [[ ! -f "$csvFile" ]]; then
python3 $srcDir/duration_system.py -fim $outputDestDir -o $csvFile
# else
#     echo "Duration CSV file already exists, skipping..."
# fi


## AGGREGATE BRANCH LISTS INTO ONE ##
l_echo $startDiv"Start branch aggregation"
Tstart
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f "branch_ids.csv" -o $fim_inputs
Tcount

## GET NON ZERO EXIT CODES FOR BRANCHES ##
l_echo $startDiv"Start non-zero exit code checking"
find $outputDestDir/logs/branch -name "*_branch_*.log" -type f | \
    xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > \
    "$outputDestDir/branch_errors/non_zero_exit_codes.log" &


l_echo $startDiv"Combining crosswalk tables"
Tstart
python3 $toolsDir/combine_crosswalk_tables.py \
    -d $outputDestDir \
    -o $outputDestDir/crosswalk_table.csv
Tcount


l_echo $startDiv"Resetting Permissions"
Tstart
    # super slow to change chmod on the log folder. Not really manditory anyways
    find $outputDestDir -maxdepth 1 -type f -exec chmod 777 {} +  # just root level files
Tcount


l_echo $startDiv"Compile all HUCs error files"
echo "Results will be saved in log folder."
Tstart
    timestamp=$(date +"%Y%m%d_%H%M")
    outfile="$outputDestDir/logs/all_errors_from_logs_$timestamp.log"
    # Collect all matching files into new output
    find "$outputDestDir" -type f -name "huc_errors_from_logs.log" -exec cat {} + > "$outfile"
    echo "Collected errors into: $outfile"
Tcount

echo
l_echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
l_echo "---- End of fim_post_processing"
l_echo "---- Ended: `date -u`"
Calc_Duration "Post Processing Duration:" $post_proc_start_time
echo
