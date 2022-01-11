#!/bin/bash -e

echo 
echo "================================================================================"
echo "Start Processing HUC $1 - Branch $2"

/usr/bin/time -v $srcDir/gms/run_by_branch.sh $1 $2 |& tee $outputRunDataDir/logs/branch/$1_gms_branch_$2.log

echo "================================================================================"
exit


