#!/bin/bash -e

/usr/bin/time -v $libDir/mannings_calibration.sh $1 |& tee 
exit ${PIPESTATUS[0]}
