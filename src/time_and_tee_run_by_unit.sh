#!/bin/bash -e

if [[ "$mem" == "1" ]] ; then
  mprof run -o $1.dat --include-children /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log
  mprof plot -o $outputRunDataDir/logs/$1_memory $1.dat
else
  /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log 
fi

exit ${PIPESTATUS[0]}
