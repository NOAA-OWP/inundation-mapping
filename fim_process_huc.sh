#!/bin/bash
### set -e  (Do not auto stop the script because of AWS)
### unless debugging also do not add -o or -u
### We have to get to the bottom and return 0. Log anything that goes wrong

:
usage ()
{
    echo "
    Why is this file here and it appears to be using duplicate export variables?
    For portability, we can make a direct call to this file with two parameters: HUC Number & Run Name;
    which correspond to the -n argument in fim_pipeline.sh and fim_pre_processing.sh.

    This file will catch any and all errors from src/run_huc.sh, even if that script aborts.

    It is not possible to call src/run_huc.sh directly, as it relies on exported values from this file.
        src/run_huc.sh will futher process branches (src/process_branch.sh) in parallel.

    Usage: ./fim_process_huc.sh <name_of_your_run> <huc8>

    Produce FIM hydrofabric datasets for a single unit and branch scale.
    - Note: fim_pre_processing.sh must have been already run. This script does
        not include post processing (see fim_pipeline.sh).
        Only a single HUC and its branches will be processed.

    Arguments:
        1) run name
        2) HUC number
            Example:
                ./fim_process_huc.sh test_name 05030104
    "
    exit
}

# print usage if agrument is '-h' or '--help'
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

export runName=$1
export hucNumber=$2

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing run time name argument (1st argument)"
    usage
fi

if [ "$hucNumber" = "" ]
then
    echo "ERROR: Missing hucNumber argument (2nd argument)"
    usage
fi

re='^[0-9]+$'
if ! [[ $hucNumber =~ $re ]] ; then
   echo "Error: hucNumber is not a number" >&2; exit 1
   usage
fi

source $srcDir/bash_functions.env

# outputsDir, srcDir, workDir and others come from the Dockerfile
export tempRunDir=$workDir/$runName
export outputDestDir=$outputsDir/$runName
export tempHucDataDir=$tempRunDir/$hucNumber
export outputHucDataDir=$outputDestDir/$hucNumber
export tempBranchDataDir=$tempHucDataDir/branches
export current_branch_id=0

hucLogFile=$tempHucDataDir/logs/huc_"$hucNumber"_unit.log
warningLogFile="$tempHucDataDir/logs/huc_${hucNumber}_warnings.log"

# TODO: Bug fix required.
# will catch errors from here down.
# trap 'handle_error $LINENO' ERR

## huc data
if [ -d "$outputHucDataDir" ]; then
    rm -rdf $outputHucDataDir
fi

# In case of aborted attempts earlier
if [ -d "$tempHucDataDir" ]; then
    rm -rdf $tempHucDataDir
fi

# make outputs directory
mkdir -p $tempHucDataDir
mkdir -p $tempBranchDataDir
mkdir -p $tempHucDataDir/logs
mkdir -p $tempHucDataDir/logs/branch
chmod 777 -R $tempHucDataDir
# These exist as OWP has tricky folder perms
chmod 777 -R $tempBranchDataDir
chmod 777 -R $tempHucDataDir/logs
chmod 777 -R $tempHucDataDir/logs/branch

# Tell the system the name and location of the log file
# l_echo is echo to screen and log at the same time.
Set_log_file_path $hucLogFile
hucLogFileName=$tempHucDataDir/logs/huc_"$hucNumber"_unit.log

echo "=========================================================================="
l_echo "---- Start of huc processing for $hucNumber" $hucLogFile

# Process the actual huc
# 'tee' catches all screen outputs from all pages and scripts all the way back, including
# all branch responses. Errors and output, even if an error occurs.

# Note... while each branch has its own log, that log data is also
# part of the hucLogFile as well (duplicate). We do not need to
# scan any logs in the logs/branch folder as they will come back into "tee"
/usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee $hucLogFile
# TODO (very low priority): fix this to the formatted version for the time command
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_huc.sh 2>&1 | tee $hucLogFile

#exit ${PIPESTATUS[0]} (and yes.. there can be more than one)
# and yes.. we can not use the $? as we are messing with exit codes

return_codes=( "${PIPESTATUS[@]}" )

# err_exists=0
# Exit codes of 60 and 61 are still true errors, but the code helps show the reason why it failed.
# The return_codes array can result in more than one loop below.
# Let each "code" print its own messages. We can get more than one exit code of 0 but we only want to
# honor it and use it if no other code has appeared
err_exists=0
# This list helps identify that errors did exist and encourage reviewing the other log files
# even though the errors are likely already in the standard log file
list_error_msg="" 
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo 
        # do nothing

    elif [ $code -eq 60 ]; then
        # Concat to the standard log file, but also make a seperate list to help it bubble up
        # even though it might already be there
        err_msg="***** Exit status: $code - Unit has no valid branches *****"
        l_echo "$err_msg" $hucLogFileName
        list_error_msg+="${err_msg}\n"
        err_exists=1

    elif [ $code -eq 61 ]; then
        # Concat to the standard log file, but also make a seperate list to help it bubble up
        # even though it might already be there
        err_msg="***** Exit status: $code - Unit has no remaining valid flowlines *****"
        l_echo "$err_msg" $hucLogFileName
        list_error_msg+="${err_msg}\n"
        err_exists=1

    else  # could be an exit status of 1 but can be other codes as well.
        # It is possible that some errors may not show up huc log file depending
        # how catastrophic the error was. It is possible that an exception
        # could show up in our error log file twice and that is ok.
        # It may or may not already have been see by "tee" and is in the std log file.
        err_msg="***** ERROR- Unknown Exit status: $code detected *****"
        l_echo "$err_msg" $hucLogFileName
        list_error_msg+="${err_msg}\n"
        err_exists=1
    fi
done

if [ "$err_exists" -ne 0 ]; then
    error_log_filename=$tempHucDataDir/logs/huc_"$hucNumber"_errors.log
    err_msg="Invalid status codes returned list:"
    echo $err_msg >> $error_log_filename
    echo -e $list_error_msg >> $error_log_filename
    echo "\n\nReview unit log file for more details" >> $error_log_filename
fi

# Move the contents of the temp directory into the outputs directory and update file permissions
# find $tempHucDataDir -type d -exec chmod -R 777 {} +
# In the OWP enviros, the perms are different, we have to copy them using a special copy
# flag which is not available in mv. We have to copy, then remove the temp version
# mv -f $tempHucDataDir $outputHucDataDir

find $tempHucDataDir -type d -exec chmod -R 777 {} + 
cp -r --no-preserve=ownership $tempHucDataDir $outputHucDataDir
rm -rdf $tempHucDataDir
echo

# we always return a success at this point (so we don't stop the loops / iterator)
exit 0
