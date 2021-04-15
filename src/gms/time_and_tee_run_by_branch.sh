#!/bin/bash -e

/usr/bin/time -v $srcDir/gms/run_by_branch.sh $1 |& tee $outputRunDataDir/logs/$1_gms.log
exit ${PIPESTATUS[0]}

