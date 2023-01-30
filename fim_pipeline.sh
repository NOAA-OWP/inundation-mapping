#!/bin/bash -e

set -e

# TODO
# upgrade Dockerfile to add this as an env value
projectDir=/foss_fim

echo
echo "======================= Start of fim_pipeline.sh ========================="
echo "---- Started: `date -u`" 

## LOAD AND VALIDATE INCOMING ARGUMENTS
source $srcDir/bash_functions.env

. $projectDir/fim_pre_processing.sh "$@"

logFile=$outputRunDataDir/logs/pipeline_summary_unit.log

pipeline_start_time=`date +%s`

#  Why an if and else? watch the number of colons

if [ -f "$hucList" ]; then
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $srcDir/process_unit_wb.sh $runName :::: $hucList 
    else
        parallel --eta -j $jobHucLimit --colsep ',' --joblog $logFile -- $srcDir/process_unit_wb.sh $runName :::: $hucList
    fi
else 
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $srcDir/process_unit_wb.sh $runName ::: $hucList
    else
        parallel --eta -j $jobHucLimit --colsep ',' --joblog $logFile -- $srcDir/process_unit_wb.sh $runName ::: $hucList
    fi
fi

echo
echo "---- Unit (HUC) processing is complete"
date -u

# cleanup unit files and create agg unit lists.
# source $srcDir/runtime_cleanup_units.sh $outputRunDataDir

## POST PROCESSING

# TODO: multiple the two limits together for the huc limit ??
. $projectDir/fim_post_processing.sh -n $runName -j $jobHucLimit

echo
echo "======================== End of fim_pipeline.sh =========================="
date -u
Calc_Duration $pipeline_start_time
echo

