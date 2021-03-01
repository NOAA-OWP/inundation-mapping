#!/bin/bash -e

/usr/bin/time -v $srcDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log
exit ${PIPESTATUS[0]}

