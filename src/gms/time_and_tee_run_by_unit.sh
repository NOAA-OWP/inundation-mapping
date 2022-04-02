#!/bin/bash -e

echo 
echo "================================================================================"

# TODO: Jan 10, 2020: This rarely receives anything but an exit code of 1 (even if a huc fails). 
/usr/bin/time -v $srcDir/gms/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/unit/$1_gms_unit.log 

#exit ${PIPESTATUS[0]}
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder
    if [ $code -ne 0 ]; then
        echo
        echo "***** An error has occured  *****"
        cp $outputRunDataDir/logs/unit/$1_gms_unit.log $outputRunDataDir/unit_errors
    fi
done

echo "================================================================================"
exit
