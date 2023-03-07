#!/bin/bash -e

# it is strongly recommended that you do not call directly to src/run_by_branch.sh
# but call this file and let is call run_by_branch.
# This file will auto trap any exceptions from run_by_branch.

# also.. remember.. that this file will rarely need to be called (but can be)
# as it is usually called through a parallelizing iterator in run_unit_wb.sh

# this also has no named command line arguments, only positional args.

runName=$1
hucNumber=$2
branchId=$3

# outputsDir, srcDir and others come from the Dockerfile
export outputRunDir=$outputsDir/$runName
branchLogFileName=$outputRunDir/logs/branch/"$hucNumber"_branch_"$branchId".log
branch_list_csv_file=$outputRunDir/$hucNumber/branch_ids.csv

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
    elif [ $code -ne 0 ]; then
        echo
        err_exists=1
        echo "***** An error has occured  *****"
        cp $branchLogFileName $outputRunDir/branch_errors
    fi
done

# Note: For branches, we do not copy over the log file for codes of 60 and 61.

if [ "$err_exists" = "0" ]; then
    # Only add the huc and branch number to the csv is the branch was successful at processing
    # We also don't want to include 60's and 61's
    $srcDir/generate_branch_list_csv.py -o $branch_list_csv_file -u $hucNumber -b $branchId
fi

exit 0  # we always return a success at this point (so we don't stop the loops / iterator)
