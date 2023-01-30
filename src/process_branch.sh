#!/bin/bash -e

# it is strongly recommended that you do not call directly to src/gms/run_by_branch.sh
# but call this file and let is call run_by_branch.
# This file will auto trap any exceptions from run_by_branch.

# also.. remember.. that this file will rarely need to be called (but can be)
# as it is usually called through a parallelizing iterator in run_unit_wb.sh

# this also has no named command line arguments, onlly positional args.

runName=$1
hucNumber=$2
branchId=$3

# outputDataDir, srcDir and others come from the Dockerfile
export outputRunDataDir=$outputDataDir/$runName
branchLogFileName=$outputRunDataDir/logs/branch/"$hucNumber"_branch_$branchId.log

/usr/bin/time -v $srcDir/gms/run_by_branch.sh $hucNumber $branchId |& tee $branchLogFileName 

#exit ${PIPESTATUS[0]}
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
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
        echo "***** Branch has no valid flowlines *****"
    elif [ $code -ne 0 ]; then
        echo
        echo "***** An error has occured  *****"
        cp $branchLogFileName $outputRunDataDir/branch_errors
    fi
done

exit 0  # we always return a success at this point (so we don't stop the loops / iterator)
