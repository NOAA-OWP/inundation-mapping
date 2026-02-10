#!/bin/bash
### set -e  # We explicitly do not want -e as that would early abort and we want
###           the files to copy from temp to outputs
set -Eeuo pipefail
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

# Some simple error handling
# This helps ensure we always copy the temp dir to outputs dir
handle_error(){

    echo "+++++++++++++++++  Script ERROR   +++++++++++"
    local msg="Critical error in fim_process_huc.sh script itself"
    msg="${msg} : Command Submitted: $BASH_COMMAND - HUC $hucNumber"
    echo -e "$msg" ; echo -e "$msg" >> "$hucLogFile"   
    echo "++++++++++++++++++++++++++++"
    move_output_files
    echo ""
    exit 0  # we always return 0 (success) as we are fully handling error and logging
}

move_output_files() {

    # TODO: wow...  what if this throws an error (OwP maybe?) perms.  hummmmm..

    # Move the contents of the temp directory into the outputs directory and update file permissions
    # The dir should be moved no matter what, except or not.
    echo ""
    l_echo "Moving temp directory" $hucLogFile
    echo "***** Moving temp directory: $tempHucDataDir to output directory: $outputHucDataDir  *****"

    # Feb 2026, We can not use the "mv" command in OWP servers, so we have to do cp and rm
    # This is related to permissions in the OWP servers.
    # mv -f $tempHucDataDir $outputHucDataDir

    find $tempHucDataDir -type d -exec chmod -R 777 {} + 
    cp -R --no-preserve=all $tempHucDataDir $outputHucDataDir
    # cp -R $tempHucDataDir $outputHucDataDir
    # find $outputHucDataDir -type d -exec chmod -R 777 {} +
   
    rm -rdf $tempHucDataDir

    echo ""
    echo "============================================================================================="
}

# will catch errors from here down.
trap 'handle_error $LINENO' ERR

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

# Debugging tests
# Exit status: ([1-9][0-9]{0,2})
# l_echo "Exit status: 0"  $hucLogFile
# l_echo "Exit status: 1"  $hucLogFile
# l_echo "Exit status: 7" $hucLogFile
# l_echo "Exit status: 127" $hucLogFile
# l_echo "Exit status: 60" $hucLogFile
# l_echo "parallel" $hucLogFile
# robbbbb error here now

echo "=========================================================================="
l_echo "---- Start of huc processing for $hucNumber" $hucLogFile

hucLogFileName=$tempHucDataDir/logs/"$hucNumber"_unit.log

# Process the actual huc
# Note... while each branch has its own log, that log data is also
# part of the hucLogFile as well (duplicate). We do not need to
# scan any logs in the logs/branch folder as they will come back into "tee"

# /usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee $hucLogFile
/usr/bin/time -f "$time_cmd_format" $srcDir/run_huc.sh 2>&1 | tee $hucLogFile

return_codes=( "${PIPESTATUS[@]}" )
# return_codes=( "${PIPESTATUS[@]}" ) - yes, it is technically there can be more than one
# depending how run_huc.sh is configured in its header declaration. But we will also
# usually get just one return code.
# and yes.. we can not use the $? here as we are messing with exit codes as it is PIPESTATUS

# We do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.

echo ""
does_error_exist="False"
# Exit codes of 60 and 61 are still true errors, but the code helps show the reason why it failed.
# The return_codes array can result in more than one loop below.
# Let each "code" print its own messages. We can get more than one exit code of 0 but we only want to
# honor it and use it if no other code has appeared
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        #echo ""
        # do nothing

    elif [ $code -eq 60 ]; then
        # Yes.. this is an error, and we know why
        err_msg="***** Exit status: $code - Unit has no valid branches *****"
        l_echo "$err_msg" $errorLogFile
        does_error_exist="True"

    elif [ $code -eq 61 ]; then
        # Yes.. this is an error, and we know why
        err_msg="***** Exit status: $code - Unit has no remaining valid flowlines *****"
        l_echo "$err_msg" $errorLogFile
        does_error_exist="True"

    else  # could be an exit status of 1 but can be other codes as well.
        # It is possible that some errors may not show up huc log file depending
        # how catastrophic the error was. It is possible that an exception
        # could show up in our error log file twice and that is ok.
        err_msg="***** Exit status: ${code}  *****"
        l_echo "$err_msg" $errorLogFile
        does_error_exist="True"
    fi
done

# This is here are it is possible to have more than one code in the for loop above
# This way.. we only ever get one success message if even applicable.
if [ "$does_error_exist" = "False" ]; then
    l_echo "***** Exit status: 0 - Success *****" $hucLogFile
fi

echo ""
# Debug test
# rrob broke it again

# Scan for warnings too
echo "Scanning for warnings"
grep -Hine "warning" $hucLogFile > $warningLogFile

l_echo "---- End of huc processing for $hucNumber" $hucLogFile
move_output_files
# we always return a success at this point (so we don't stop the loops / iterator)
exit 0
