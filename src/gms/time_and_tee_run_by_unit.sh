#!/bin/bash -e

/usr/bin/time -v $srcDir/gms/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1_gms_unit.log
exit ${PIPESTATUS[0]}

