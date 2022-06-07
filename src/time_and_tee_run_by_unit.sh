#!/bin/bash -e

if [[ "$mem" == "1" ]] ; then
  mprof run -o $1.dat --include-children /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log
  mprof plot -o $outputRunDataDir/logs/$1_memory $1.dat
else
  /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log 
fi

#$exit ${PIPESTATUS[0]}

return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the  log in a new folder  if an error
    # Note: It was tricky to load in the fim_enum into bash, so we will just 
    # go with the code for now
    if [ $code -eq 61 ]; then
        echo
        echo "***** HUC has no valid flowlines *****"  #(for fim_run this is invalid)
    elif [ $code -ne 0 ]; then
        echo
        echo "***** An error has occured  *****"
        cp $outputRunDataDir/logs/$1.log $outputRunDataDir/unit_errors
        exit ${PIPESTATUS[0]}
    fi
done

echo "================================================================================"
exit

