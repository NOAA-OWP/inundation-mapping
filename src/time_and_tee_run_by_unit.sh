#!/bin/bash -e

mprof run --include-children /usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log
mprof plot -o $outputRunDataDir/logs/$1_memory
exit ${PIPESTATUS[0]}

