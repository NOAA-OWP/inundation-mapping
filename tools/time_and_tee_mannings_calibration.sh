#!/bin/bash -e

/usr/bin/time -v $toolsdir/mannings_run_by_set.sh $1 |& tee
exit ${PIPESTATUS[0]}
