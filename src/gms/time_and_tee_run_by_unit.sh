#!/bin/bash -e

echo 
echo "================================================================================"
echo "Start Processing HUC $1"

# TODO: Jan 10, 2020: This rarely receives anything but an exit code of 1 (even if a huc fails). 
/usr/bin/time -v $srcDir/gms/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/unit/$1_gms_unit.log 

echo "================================================================================"
#exit ${PIPESTATUS[0]}
exit
