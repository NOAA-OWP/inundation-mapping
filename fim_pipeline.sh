#!/bin/bash -e

set -e

# TODO
# upgrade Dockerfile to add this as an env value
projectDir=/foss_fim

# See fim_pre_processing.sh for details of how to use this script. fim_pre_processing.sh
# is a proxy for collecting and validating input.

echo
echo "======================= Start of fim_pipeline.sh ========================="
echo "---- Started: `date -u`" 

## LOAD AND VALIDATE INCOMING ARGUMENTS
source $srcDir/bash_functions.env

. $projectDir/fim_pre_processing.sh "$@"

logFile=$outputRunDataDir/logs/pipeline_summary_unit.log
process_wb_file=$projectDir/fim_process_unit_wb.sh

pipeline_start_time=`date +%s`

#  Why an if and else? watch the number of colons
if [ -f "$hucList" ]; then
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName :::: $hucList 
    else
        parallel --eta -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName :::: $hucList
    fi
else 
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName ::: $hucList
    else
        parallel --eta -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file ::: $hucList
    fi
fi

echo
echo "---- Unit (HUC) processing is complete"
date -u

## POST PROCESSING

# TODO: multiply the two job limits together for the limit here ??
. $projectDir/fim_post_processing.sh -n $runName -j $jobHucLimit

echo
echo "======================== End of fim_pipeline.sh =========================="
date -u
Calc_Duration $pipeline_start_time
echo

