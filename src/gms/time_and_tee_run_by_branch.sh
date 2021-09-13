#!/bin/bash -e

/usr/bin/time -v $srcDir/gms/run_by_branch.sh $1 $2 |& tee $outputRunDataDir/logs/branch/$1_gms_branch_$2.log
exit ${PIPESTATUS[0]}

