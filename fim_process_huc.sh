#!/bin/bash
### set -e  # We explicitly do not want -e as that woudl early abort and we want
###           the files to copy from temp to outputs
### set -o pipefail  (debugging line)
umask 000

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
errorLogFile="$tempHucDataDir/logs/huc_${hucNumber}_errors.log"
warningLogFile="$tempHucDataDir/logs/huc_${hucNumber}_warnings.log"
branchNonZeroCodesLogFile="$tempHucDataDir/logs/huc_${hucNumber}_branch_non_zero_exit_codes.log"
scan_for_huc_errors_complete="False"

# Some simple error handling
# We add it to the log file then scan for the word "error" later down.
# This will also handle errors in this script and not just run_huc.sh
handle_error(){

    echo "++++++++++++++++++++++++++++"
    msg="Critical error in fim_process_huc.sh script itself, line number: $1"
    l_echo "$msg" $errorLogFile
    msg="Error Command Submitted: $BASH_COMMAND"
    l_echo "$msg" $errorLogFile
    echo "++++++++++++++++++++++++++++"
    check_for_huc_errors
    scan_for_huc_errors_complete="False"
    # move_output_files
    echo ""
    # exit 0  # we always return 0 (success) as we are fully handling error and logging
}

# and will do warnings as well.
check_for_huc_errors(){

    # Note: We do not need to scan any src_optimization or subfolders
    # as all warnings and errors roll up here to the hucLogFile when in
    # pipeline mode.

    # scan_for_huc_errors_complete helps stop for multiple scans as it is possible if
    # there are multiple errors on this page.
    if [ "$scan_for_huc_errors_complete" = "False" ]; then
        #l_echo $startDiv"Scanning for errors and exceptions in the HUC unit file" $hucLogFile
        l_echo $startDiv"Scanning for err..ors and exceptions in the HUC unit file" $hucLogFile
        # No.. the line above is not a mistype.
        # Can't put the word "error" as as a header in the log file as it finds itself in the log files
        # Scan for the word error in the log file. Exit codes were already managed above.
        # We may end up with dup entries but that is ok.
        # Everything else including branch errors are already rolled up in the huc log file
        # and huc error file.

        # Grep Tech Tip.. use the -e flag when you are not using any wildcards or patterns
        # just a word in a line. If you need a regex type patter, use -E instead.
        # touch $errorLogFile
        # This helps with errors in this fim_process_huc.sh script
        grep -H -i -n -e "Command exited with non-zero status" $hucLogFile >> $errorLogFile
        grep -H -i -n -e "error" $hucLogFile >> $errorLogFile
        grep -H -i -n -e "parallel" $hucLogFile >> $errorLogFile

        l_echo $startDiv"Find branch non zero exit codes for this huc" $hucLogFile
        find $tempHucDataDir -path "*/logs/branch/*_branch*.log" -type f | \
            xargs grep -H -n -i -E "Exit status: ([1-9][0-9]{0,2})" >> $branchNonZeroCodesLogFile

        # Scan for warnings too
        echo "Scanning for warnings"
        # warningLogFile
        grep -H -i -n -e "warning" $hucLogFile > $warningLogFile

        # agg_by_huc_errors
    fi
    scan_for_huc_errors_complete="True"
}

move_output_files() {

    # Move the contents of the temp directory into the outputs directory and update file permissions
    # The dir should be moved no matter what, except or not.
    l_echo "Moving temp directory for $hucNumber" $hucLogFile
    mv -f $tempHucDataDir $outputHucDataDir
    find $outputHucDataDir -type d -exec chmod -R 777 {} +

    echo "============================================================================================="
    echo
    echo "***** Moved temp directory: $tempHucDataDir to output directory: $outputHucDataDir  *****"
    echo
    echo "============================================================================================="
}

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

echo "=========================================================================="
l_echo "---- Start of huc processing for $hucNumber" $hucLogFile

# Clean out previous unit logs and branch logs starting with this huc
rm -f $tempHucDataDir/logs/"$hucNumber"_unit.log
rm -f $tempHucDataDir/logs/branch/"$hucNumber"_summary_branch.log
rm -f $tempHucDataDir/logs/branch/"$hucNumber"*.log
rm -f $outputDestDir/branch_errors/"$hucNumber"*.log

hucLogFileName=$tempHucDataDir/logs/"$hucNumber"_unit.log

# Process the actual huc
# Note... while each branch has its own log, that log data is also
# part of the hucLogFile as well (duplicate). We do not need to
# scan any logs in the logs/branch folder, just for exit codes and specific words (error, parallel)
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_huc.sh 2>&1 | tee $hucLogFile
# l_echo "----- Exit status: $?" $hucLogFile
/usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee $hucLogFile

return_codes=( "${PIPESTATUS[@]}" )
# return_codes=( "${PIPESTATUS[@]}" ) - yes, it is technically there can be more than one
# depending how run_huc.sh is configured in its header declaration. But we will also
# usually get just one return code.
# and yes.. we can not use the $? here as we are messing with exit codes as it is PIPESTATUS

# We do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
err_exists=0
err_msg=""
# Exit codes of 60 and 61 are true errors, but the code helps show the reason why it failed.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the unit log into a new folder.

    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 60 ]; then
        err_msg="***** Unit has no valid branches *****"
        err_exists="1"
    elif [ $code -eq 61 ]; then
        err_msg="***** Unit has no remaining valid flowlines *****"
        err_exists="1"
    else  # could be an exit status of 1 but can be other codes as well.
        # It is possible that some errors may not show up huc log file depeding
        # how catastrophic the error was. It is possible that an exception
        # could show up in our error log file twice and that is ok.
        err_msg="***** An Error has occurred - Exit Code is ${code} *****"
        err_exists="1"
    fi
done

if [ "$err_exists" = "1" ]; then
    l_echo "$err_msg" $errorLogFile
fi

# Rob_test_fail  # function call 

# In case there is a critical error with logic on this page.
# Most errors are caught via Time and Tee, then the return status codes
# but errors can occur on this page itself. This helps trap those types of errors
# as well
trap 'handle_error $LINENO' ERR

# These are now in functions as page level errors and exceptions can occur anywhere
# within this fim_process_huc.sh script itself. It script fails earlier then here
# we are still covered.
check_for_huc_errors
l_echo "---- End of huc processing for $hucNumber" $hucLogFile
# call function to move the files from temp
move_output_files
# we always return a success at this point (so we don't stop the loops / iterator)
exit 0
