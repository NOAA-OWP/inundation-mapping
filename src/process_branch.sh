#!/bin/bash -e

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh.
# This file will trap any exceptions from run_by_branch.sh.

# Also.. remember.. that this file can be called explicitly, but will rarely need to be,
# as it is usually called through a parallelizing iterator in run_unit_wb.sh

# This file also has no named command line arguments, only positional args.

runName=$1
hucNumber=$2
branchId=$3

# outputDestDir & tempHucDataDir come from fim_process_unit_wb.sh
branchLogFileName=$outputDestDir/logs/branch/"$hucNumber"_branch_"$branchId".log
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
        # rm -rf $tempHucDataDir/branches/$branchId/
    elif [ $code -ne 0 ]; then
        echo
        err_exists=1
        echo "***** An error has occured  *****"
        cp $branchLogFileName $outputDestDir/branch_errors
    fi
done

# Note: For branches, we do not copy over the log file for codes of 60 and 61.

# Only add the huc and branch number to the csv if the branch was successful at processing
if [ "$err_exists" = "0" ]; then
    $srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber -b $branchId
fi

# We always return a success at this point (so we don't stop the loops / iterator)
exit 0
