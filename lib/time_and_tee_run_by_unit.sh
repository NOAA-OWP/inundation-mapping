#!/bin/bash -e

/usr/bin/time -v $libDir/run_by_unit.sh $1 |& tee $outputRunDataDir/logs/$1.log
