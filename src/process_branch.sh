#!/bin/bash -e

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh.
# This file will trap any exceptions from run_by_branch.sh.
# This is a key part to handling .sh exceptions.

# Also.. remember.. that this file can be called explicitly, but will rarely need to be,
# as it is usually called through a parallelizing iterator in run_unit_wb.sh

# This file also has no named command line arguments, only positional args.

runName=$1
hucNumber=$2
branchId=$3

# outputDestDir & tempHucDataDir come from fim_process_unit_wb.sh
branchLogFileName=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId".log

/usr/bin/time -v $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branchLogFileName

#exit ${PIPESTATUS[0]}
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
err_exists=0
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 61 ]; then
        echo
        err_exists=1
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
    # elif [ $code -ne 0 ]; then
    else # could it be anything else? Yes.. might be a null/none, so stick with the "else"
        echo
        err_exists=1
        echo "***** An error has occurred while processing branch ${branchId} - Exit status is $code *****"
        # cp $branchLogFileName $outputDestDir/branch_errors  No longer has value, but having the word
        # having this show the code has value
    fi
done

# We always return a success at this point (so we don't stop the loops / iterator)
# Why? we log errors that are not 61, 62, etc and let the searchign for the word "error"
# roll it up later. If we send back somethign other than 0, it can stop the iterator in
# run_huc.sh and we don't want one branch to kill the HUC (or do we? hummm)
exit 0
