#!/bin/bash -e

/usr/bin/time -v $testdir/mannings_calibration.sh $1 |& tee
exit ${PIPESTATUS[0]}
