#!/bin/bash
set -ex  

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


echo "=========================================================================="
echo "---- Start of huc processing for $hucNumber"


# outputsDir, srcDir, workDir and others come from the Dockerfile
export tempRunDir=$workDir/$runName
export outputDestDir=$outputsDir/$runName
export tempHucDataDir=$tempRunDir/$hucNumber
export outputHucDataDir=$outputDestDir/$hucNumber
export tempBranchDataDir=$tempHucDataDir/branches
export current_branch_id=0

error_log_filename="huc_${hucNumber}_errors.log"
error_log_file_path="$tempHucDataDir/logs/$error_log_filename"
hucLogFileName=$tempHucDataDir/logs/"$hucNumber"_unit.log

# Some simple error handling
# handle_error(){

#     orig_fail_line_num=$1
#     echo "++++++++++++++++++++++++++++"
#     msg="Critical error in fim_process_huc.sh, line number:$orig_fail_line_num"
#     echo $msg
#     echo $msg >> $error_log_file_path

#     msg="  Command submitted: $BASH_COMMAND"
#     echo $msg
#     echo $msg >> $error_log_file_path
#     echo "++++++++++++++++++++++++++++"
#     echo
#     move_output_files
#     exit 0
# }

move_output_files() {

    # Move the contents of the temp directory into the outputs directory and update file permissions
    # The dir should be moved no matter what, except or not.
    echo "Moving temp directory for $hucNumber"
    mv -f $tempHucDataDir $outputHucDataDir
    find $outputHucDataDir -type d -exec chmod -R 777 {} +

    echo "============================================================================================="
    echo
    echo "***** Moved temp directory: $tempHucDataDir to output directory: $outputHucDataDir  *****"
    echo
    echo "============================================================================================="
}

# In case there is a critical error with logic on this page.
# hummm... how do we want to handle this without compromising
# the error codes being returned from run_huc.sh ??
# trap 'handle_error $LINENO' ERR

## huc data
if [ -d "$outputHucDataDir" ]; then
    rm -rf $outputHucDataDir
fi

# make outputs directory
mkdir -p $tempHucDataDir
mkdir -p $tempBranchDataDir
mkdir -p $tempHucDataDir/logs
mkdir -p $tempHucDataDir/logs/branch
chmod 777 $tempHucDataDir
chmod 777 $tempBranchDataDir

# Clean out previous unit logs and branch logs starting with this huc
rm -f $tempHucDataDir/logs/"$hucNumber"_unit.log
rm -f $tempHucDataDir/logs/branch/"$hucNumber"_summary_branch.log
rm -f $tempHucDataDir/logs/branch/"$hucNumber"*.log
# rm -f $outputDestDir/branch_errors/"$hucNumber"*.log

# Process the actual huc
/usr/bin/time -v $srcDir/run_huc.sh 2>&1 | tee $hucLogFileName

return_codes=( "${PIPESTATUS[@]}" )
# return_codes=( "${PIPESTATUS[@]}" ) - yes, it is technically there can be more than one
# depending how run_huc.sh is configured in its header declaration. But we will also
# usually get just one return code.
# and yes.. we can not use the $? as we are messing with exit codes

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
err_exists=0
for code in "${return_codes[@]}"
do
    # Make an extra copy of the unit log into a new folder.

    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 60 ]; then
        echo
        echo "***** Unit has no valid branches *****"
        err_exists=1
    elif [ $code -eq 61 ]; then
        echo
        echo "***** Unit has no remaining valid flowlines *****"
        err_exists=1
    else
        echo
        echo "***** An error has occurred - Code ("${code}") *****"
        err_exists=1
    fi
done

if [ "$err_exists" = "1" ]; then
    err_msg="Error: "$hucNumber". Invalid return status code. Exit status(es): ${my_array[*]}"
    echo $err_msg >> $error_log_file_path
fi

# Scan for the phrase "parallel:" and concat that to the error log even if it is just a
# parallel: Warning
echo "Scanning for the phrase 'parallel'"
# Test
# echo "parallel: test" >> $tempHucDataDir/logs/${hucNumber}_unit.log

# +++++++++++++++++++++++++++++
# TODO:  Finish this.


find "$tempHucDataDir/logs" -path "*.log" -type f -not -name $error_log_filename
# -exec  grep -H -i -n "parallel:" {} +  >> $error_log_file_path

# call function to move the files from temp
move_output_files

# we always return a success at this point (so we don't stop the loops / iterator)
exit 0
