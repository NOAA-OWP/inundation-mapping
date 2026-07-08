#!/bin/bash
# set -e         # Critical: Can not have a -e inplace in order for the error handline
set -o pipefail  # Crucial: Forces the pipe to fail if the subscript fails but only when a pipe is used.
# For this one, we do want to stop, but the error script will always be caught
# by the trap, then always hit the exit_and_copy if the error happens after the
# code is executed. We want this script to ALWAYS return a 0
### Yes.. not all of our .sh files are the same with the -e flag, by design.

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
}

# print usage if agrument is '-h' or '--help'
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
    exit 22
fi

export runName=$1
export hucNumber=$2

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing run time name argument (1st argument)"
    usage
    exit 22
fi

if [ "$hucNumber" = "" ]
then
    echo "ERROR: Missing hucNumber argument (2nd argument)"
    usage
    exit 22
fi

re='^[0-9]+$'
if ! [[ $hucNumber =~ $re ]] ; then
   echo "Error: hucNumber is not a number" >&2 
   usage
   exit 22
fi

## huc data

export tempRunDir=$workDir/$runName
export outputDestDir=$outputsDir/$runName
export tempHucDataDir=$tempRunDir/$hucNumber
export outputHucDataDir=$outputDestDir/$hucNumber
export tempBranchDataDir=$tempHucDataDir/branches
export current_branch_id=0

if [ -d "$outputHucDataDir" ]; then
    rm -rf $outputHucDataDir
fi

# In case of aborted attempts earlier
if [ -d "$tempHucDataDir" ]; then
    rm -rdf $tempHucDataDir
fi

hucLogFile="$tempHucDataDir/logs/huc_${hucNumber}_unit.log"
warningLogFile="$tempHucDataDir/logs/huc_${hucNumber}_warnings.log"
errorLogFile="$tempHucDataDir/logs/huc_${hucNumber}_error_report.csv"

# No matter what happens, this will execute and copy the folder from temp to outputs
# starting from the trap 'exit_and_copy' EXIT and down, ie) the arg tests above
exit_and_copy() {

    # Ensure all child folders and files are set to the perms we want
    chmod -R 774 $tempHucDataDir
    echo "============================================================================================="
    echo
    echo "Starting coping folder from temp directory to output directory"
    date -u +"%Y-%m-%d %H:%M:%S"  # to screen
    date -u +"%Y-%m-%d %H:%M:%S" >> $hucLogFile  # to file
    cp -r --no-preserve=ownership "${tempHucDataDir}/" "${outputHucDataDir}/"
    rm -rdf $tempHucDataDir
    echo "***** Moved temp directory: $tempHucDataDir to output directory: $outputHucDataDir  *****"
    echo
    echo "============================================================================================="    
    echo
    exit 0  # for fim_process_huc only, we do want to always return a 0.
}

source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# TODO: July 2026: using setfacl is a power tool to help manage perms settings
# but it is not yet in our Docker build. See notes in Dockerfile.dev
# Set default permissions for the owner, group, and others
# This forces 775 (rwxrwxr-x) on all newly created files and folders
# setfacl -d -m u::rwx $tempHucDataDir
# setfacl -d -m g::rwx $tempHucDataDir
# setfacl -d -m o::rx $tempHucDataDir
# In the meantime, we have a weird combination of inefficient chmod everywhere.

mkdir -p $tempHucDataDir
mkdir -p $tempBranchDataDir
mkdir -p $tempHucDataDir/logs
mkdir -p $tempHucDataDir/logs/branch
chmod 777 $tempHucDataDir
chmod 777 $tempBranchDataDir
chmod 777 $tempHucDataDir/logs

# This absolute safety net catches the exit command and runs your final lines
# but only from this point down.
trap 'exit_and_copy' EXIT
# Error handling starts from here down.
# Note: While the error log file will capture errors from this page or its "tee" returns,
# it does not include some errors and exceptions recorded inside the child .sh files. 
# We will catch those via error search tools.
trap 'handle_error "${PIPESTATUS[*]}" $LINENO $hucLogFile "huc"' ERR

echo "=========================================================================="
l_echo "---- Start of huc processing for $hucNumber" $hucLogFile
l_echo "---- Started: `date -u`" $hucLogFile

# Process the actual huc
# 'tee' catches all screen outputs from all pages and scripts all the way back, including
# all branch responses. Errors and output, even if an error occurs.

# ERR trap OFF as it will mess with the return Pipestatus, we turn it back on lower for page
# level errors or additional scripts called on the page.
# trap - ERR
/usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee -a $hucLogFile
# TODO (very low priority): fix this to the formatted version for the time command
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_huc.sh 2>&1 | tee $hucLogFile

## ===============================
l_echo $startDiv"Compiling err..or report" $hucLogFile
# Tstart
huc_errors_csv_log=$tempHucDataDir/logs/huc_${hucNumber}_error_report.csv
python3 $srcDir/utils/huc_process_error_report.py \
   -n $tempHucDataDir -u $hucNumber -o $errorLogFile 2>&1 | tee -a -i $hucLogFile 


# TODO: This only gets called if the pages has completed successfully.
grep -Hin "warning" "${hucLogFile}" > "${warningLogFile}"

# exit_and_copy will be copied here if not earlier, depending on exceptions or errors from the TRAP ... ERR and down.