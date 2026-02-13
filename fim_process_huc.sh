#!/bin/bash -e
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
# Note... while each branch has its own log, that log data is also
# part of the hucLogFile as well (duplicate). We do not need to
# scan any logs in the logs/branch folder as they will come back into "tee"

# todo: fix this to the formatted version below
/usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee $hucLogFile
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_huc.sh 2>&1 | tee $hucLogFile

return_codes=( "${PIPESTATUS[@]}" )
# return_codes=( "${PIPESTATUS[@]}" ) - yes, it is technically there can be more than one
# depending how run_huc.sh is configured in its header declaration. But we will also
# usually get just one return code.
# and yes.. we can not use the $? here as we are messing with exit codes as it is PIPESTATUS

# We do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.

# err_exists=0
# Exit codes of 60 and 61 are still true errors, but the code helps show the reason why it failed.
# The return_codes array can result in more than one loop below.
# Let each "code" print its own messages. We can get more than one exit code of 0 but we only want to
# honor it and use it if no other code has appeared
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo 
        # do nothing

    elif [ $code -eq 60 ]; then
        # Yes.. this is an error, and we know why
        err_msg="***** Exit status: $code - Unit has no valid branches *****"
        l_echo "$err_msg" $hucLogFileName
        # err_exists=1

    elif [ $code -eq 61 ]; then
        # Yes.. this is an error, and we know why
        err_msg="***** Exit status: $code - Unit has no remaining valid flowlines *****"
        l_echo "$err_msg" $hucLogFileName
        # err_exists=1

    else  # could be an exit status of 1 but can be other codes as well.
        # It is possible that some errors may not show up huc log file depending
        # how catastrophic the error was. It is possible that an exception
        # could show up in our error log file twice and that is ok.
        err_msg="***** Exit status: $code detected *****"
        l_echo "$err_msg" $hucLogFileName
        # err_exists=1
    fi
done

# +++++++++++++++++++
# TODO: Feb 2025. If a py file throws an error, it can return Command exited with non-zero status 1
# but we will get an exit code of 0 so we show success. Good enough for now as post processing logs
# catch it

# if [ "$does_error_exist" = "0" ]; then
#     l_echo "     ***** Exit status: 0 - Success *****" $hucLogFile
#     echo "Note: A temp bug may show this as success but python bugs often show up in the logs as " \
#         "Command exited ... with non-zero status 1. Good enough for now as post processing logs catch it."
# else
#     # This is insurance in case this completely fails and doesn't even move it from temp dir
#     # copy the error log over to the unit_errors folder to better isolate it
#     cp $hucLogFileName $outputDestDir/logs/unit_errors    
# fi

# Scan for huc warnings
grep -Hine "warning" $hucLogFile > $warningLogFile

echo
l_echo "Scanning src_calibration logs for issues"
# lets make sure their are some log files first or grep gets mad
if [[ -n $(find $outputDestDir -path "**/*/logs/src_calibrations/*.log" -type f) ]]; then
    find $outputDestDir -path "**/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "error" {} + >> $hucLogFile || true
    find $outputDestDir -path "**/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "exception" {} + >> $hucLogFile  || true
    find $outputDestDir -path "**/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "warning" {} + >> $warningLogFile || true    
fi

l_echo "---- End of huc processing for $hucNumber" $hucLogFile

echo
l_echo "Moving temp directory" $hucLogFile
echo "***** Moving temp directory: $tempHucDataDir to output directory: $outputHucDataDir  *****"

# Feb 2026, We can not use the "mv" command in OWP servers, so we have to do cp and rm
# This is related to permissions in the OWP servers.
# mv -f $tempHucDataDir $outputHucDataDir

find $tempHucDataDir -type d -exec chmod -R 777 {} + 
cp -r --no-preserve=ownership $tempHucDataDir $outputHucDataDir
rm -rdf $tempHucDataDir
echo

# we always return a success at this point (so we don't stop the loops / iterator)
exit 0
