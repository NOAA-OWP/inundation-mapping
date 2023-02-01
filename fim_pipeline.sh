#!/bin/bash -e

: '
fim_pipeline.sh -u <huc8> -n <name_your_run>

For more details on 

- There are a wide number of options and defaulted values, for details run ```gms_pipeline.sh -h```
- Manditory arguments:
    - `-u` can be a single huc, a series passed in quotes space delimited, or a line-delimited file
    i. To run entire domain of available data use one of the ```/data/inputs/included_huc[4,6,8].lst``` files or a huc list file of your choice.
    - `-n` is a name of your run (only alphanumeric)
- Outputs can be found under ```/data/outputs/<name_your_run>```

Processing of HUCs in FIM4 (GMS) comes in two pieces: gms_run_unit and gms_run_branch. `gms_pipeline.sh` above takes care of both steps however, you can run each part seperately for faster development if you like.

If you choose to do the two step hydrofabric creation, then run `gms_run_unit.sh`, then `gms_run_branch.sh`. See each of those files for details on arguments.
'

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

# PROCESS THE UNITS (And branches)
# Why an if and else? watch the number of colons
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

