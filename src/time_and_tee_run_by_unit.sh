#!/bin/bash -e

mprof run -o $1.dat --include-children /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log 
mprof plot -o $outputRunDataDir/logs/$1_memory $1.dat
exit ${PIPESTATUS[0]}
