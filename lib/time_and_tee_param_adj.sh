#!/bin/bash -e

/usr/bin/time -v $libDir/param_adj_wrapper.sh $1 |& tee $outputRunDataDir/param_logs/$1.log
