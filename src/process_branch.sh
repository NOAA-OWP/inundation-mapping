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
branch_list_csv_file=$tempHucDataDir/branch_ids.csv

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
        # this is tring to copy to the outDestDir which will be a pronlem as we are in the temp dirs
        # and not the parent outputDestDir at this point.
        # Do we even need it? hummmm. We may not anymore as other rollup will cat it becuase of the 
        # word "error" in this "else"
        # cp $branchLogFileName $outputDestDir/branch_errors
    fi
done

# Note: For branches, we do not copy over the log file for codes of 60 and 61.


# **********************
# TODO.... We need have to change this...  it can create collisions and also
# can not handle error trapping well. Currently, if an error comes back, it is 
# suppressed as we exit 0 and have too

# We will need another answer inside run_huc.sh for this, well at least
# we can not run generate_branch_list here. We can use the 'else' but I don't
# think we want to as the "echo *** An error has.. above will auto roll up
# and also avoid errors from things like 61, 65 rolling up into error logs
# as we dont' want them in error logs.

# Only add the huc and branch number to the csv if the branch was successful at processing
# if [ $err_exists -eq 0 ]; then
#     $srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber -b $branchId
# else
#     error_log_filename=$tempHucDataDir/logs/huc_"$hucNumber"_errors.log
#     err_msg="Error: ${hucNumber} / ${branchId}. Invalid return status code from branch. Exit status: $return_codes"
#     echo $err_msg >> $error_log_filename
# fi

# We have to change generate_branch_list_csv.py to be huc level and not branch level due to collisons
# which we are potentially seeing with fast branch iterating (simulatenous usage of csv by
# multiple branches)

# We always return a success at this point (so we don't stop the loops / iterator)
# Why? we log errors that are not 61, 62, etc and let the searchign for the word "error"
# roll it up later. If we send back somethign other than 0, it can stop the iterator in
# run_huc.sh and we don't want one branch to kill the HUC (or do we? hummm)
exit 0
