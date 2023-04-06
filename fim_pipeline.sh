#!/bin/bash -e

: '
fim_pipeline.sh -u <huc8> -n <name_your_run>

For more details on 

- There are a wide number of options and defaulted values, for details run ```fim_pipeline.sh -h```
- Manditory arguments:
    - `-u` can be a single huc, a series passed in quotes space delimited, or a line-delimited file
    i. To run entire domain of available data use the ```/data/inputs/included_huc8.lst``` file or a huc list file of your choice.
    - `-n` is a name of your run (only alphanumeric)
- Outputs can be found under ```/outputs/<name_your_run>```

Processing of HUC''s in FIM4 comes in three pieces. You can run `fim_pipeline.sh` which automatically runs all of three major section, but you can run each of the sections independently if you like. The three sections are:
- `fim_pre_processing.sh` : This section must be run first as it creates the basic output folder for the run. It also creates a number of key files and folders for the next two sections. 
- `fim_process_unit_wb.sh` : This script processes one and exactly one HUC8 plus all of it''s related branches. While it can only process one, you can run this script multiple times, each with different HUC (or overwriting a HUC). When you run `fim_pipeline.sh`, it automatically iterates when more than one HUC number has been supplied either by command line arguments or via a HUC list. For each HUC provided, `fim_pipeline.sh` will `fim_process_unit_wb.sh`. Using the `fim_process_unit_wb.sh`  script allows for a run / rerun of a HUC, or running other HUCs at different times / days or even different docker containers.
- `fim_post_processing.sh` : This section takes all of the HUCs that have been processed, aggregates key information from each HUC directory and looks for errors across all HUC folders. It also processes the group in sub-steps such as usgs guages processesing, rating curve adjustments and more. Naturally, running or re-running this script can only be done after running `fim_pre_processing.sh` and at least one run of `fim_process_unit_wb.sh`.

Running the `fim_pipeline.sh` is a quicker process than running all three steps independently.
'

set -e

# See fim_pre_processing.sh for details of how to use this script. fim_pre_processing.sh
# is a proxy for collecting and validating input.

echo
echo "======================= Start of fim_pipeline.sh ========================="
echo "---- Started: `date -u`" 

## LOAD AND VALIDATE INCOMING ARGUMENTS
source $srcDir/bash_functions.env
. $projectDir/fim_pre_processing.sh "$@"

logFile=$outputDestDir/logs/unit/pipeline_summary_unit.log
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
        parallel --eta -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file  $runName  ::: $hucList
    fi
fi

echo
echo "---- Unit (HUC) processing is complete"
date -u

## POST PROCESSING

# Remove run from the fim_temp directory
rm -d $workDir/$runName

# TODO: multiply the two job limits together for the limit here ??
. $projectDir/fim_post_processing.sh -n $runName -j $jobHucLimit

echo
echo "======================== End of fim_pipeline.sh =========================="
date -u
Calc_Duration $pipeline_start_time
echo

