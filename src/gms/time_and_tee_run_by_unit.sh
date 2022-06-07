#!/bin/bash -e

echo 
echo "================================================================================"

/usr/bin/time -v $srcDir/gms/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/unit/$1_gms_unit.log 

#exit ${PIPESTATUS[0]}
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder  if an error
    # Note: It was tricky to load in the fim_enum into bash, so we will just 
    # go with the code for now
    if [ $code -eq 60 ]; then
        echo
        echo "***** Unit has no valid branches *****"
    elif [ $code -ne 0 ]; then
        echo
        echo "***** An error has occured  *****"
        cp $outputRunDataDir/logs/unit/$1_gms_unit.log $outputRunDataDir/unit_errors
    fi
done

echo "================================================================================"
exit
