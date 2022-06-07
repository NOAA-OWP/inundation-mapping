#!/bin/bash -e

echo 
echo "================================================================================"

/usr/bin/time -v $srcDir/gms/run_by_branch.sh $1 $2 |& tee $outputRunDataDir/logs/branch/$1_gms_branch_$2.log 

#exit ${PIPESTATUS[0]}
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder
    # Note: It was tricky to load in the fim_enum into bash, so we will just 
    # go with the code for now
    if [ $code -eq 61 ]; then
        echo
        echo "***** Branch has no valid flowlines *****"
    elif [ $code -ne 0 ]; then
        echo
        echo "***** An error has occured  *****"
        cp $outputRunDataDir/logs/branch/$1_gms_branch_$2.log $outputRunDataDir/branch_errors
    fi
done

echo "================================================================================"
exit
