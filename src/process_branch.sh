#!/bin/bash -e

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh as it will trap all 
# and any exceptions from run_by_branch.sh.
# This is a key part to handling .sh exceptions.

# NOTE: Do not use l_echo here, just echo as it auto bubbles up to run_huc.sh 
# This script does not need a "trap" or handle_errors as it is caught in upstream scripts

# Any actual script errors here including from process_branch.sh
# will bubble up to run_huc.sh and fim_process_huc.sh

runName=$1
hucNumber=$2
branchId=$3

source $srcDir/bash_functions.env

# tempHucDataDir come from fim_process_unit_wb.sh
branch_log_file_name=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId".log
error_log_filename=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId"_errors.log

/usr/bin/time -v $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branch_log_file_name
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branch_log_file_name
echo

# See note in fim_process_huc.sh talking about PIPESTATUS info
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
err_exists=0
last_error_code="0"
# Most of these can end up in the huc log twice and that is ok.
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 61 ]; then
        echo
        err_exists=1
        echo "the retrn code is $code"
        last_error_code="$code"
        echo "***** Branch has no valid flowlines *****"
        rm -rf $tempHucDataDir/branches/$branchId/
    elif [ $code -eq 64 ]; then
        echo
        err_exists=1
        echo "***** Branch has no crosswalks *****"
        rm -rf $tempHucDataDir/branches/$branchId/
    elif [ $code -eq 65 ]; then
        echo
        err_exists=1
        echo "***** Too many HydroIDs or a HydroID with more than 8 digits in gw catchments to convert to Int16 *****"
        rm -rf $tempHucDataDir/branches/$branchId/
    elif [ $code -ne 0 ]; then
        echo
        err_exists=1
        echo "***** An error has occured  *****"
        # cp $branchLogFileName $outputDestDir/logs/branch_errors
    fi
done

echo "err_exists is $err_exists"
echo "last error code is ${last_error_code}"
# Yes.. this is technically duplicate for what is in the huc log, but we only use
# it to help post processing get a list of branch errors
if [ "$err_exists" != "0" ]; then
    echo "last code is"
    err_msg="Error: ${hucNumber} / ${branchId}. Invalid return status code from branch. Exit status: $return_codes"
    echo "$err_msg"
    echo "$err_msg" >> $error_log_filename
fi

echo "Finished processing $hucNumber : $branchId"

# We always return a success at this point (so we don't stop the loops / iterator)
exit 0